import asyncio
from urllib.parse import quote, unquote

import aiohttp
from RTN import normalize_title, title_match

from comet.core.execution import get_executor
from comet.core.logger import logger
from comet.core.models import settings
from comet.debrid.exceptions import DebridAuthError, DebridLinkGenerationError
from comet.debrid.stremthru import StremThru, batch_parse
from comet.services.debrid_cache import cache_availability
from comet.services.filtering import quick_alias_match
from comet.services.torrent_manager import torrent_update_queue
from comet.utils.parsing import is_video


class RealDebrid(StremThru):
    _TORRENT_READY_STATUS = "downloaded"
    _TORRENT_PENDING_STATUSES: frozenset[str] = frozenset(
        {
            "magnet_conversion",
            "waiting_files_selection",
            "queued",
            "downloading",
            "compressing",
            "uploading",
        }
    )
    _TORRENT_INVALID_STATUSES: frozenset[str] = frozenset(
        {"magnet_error", "error", "virus", "dead"}
    )
    _INSTANT_CHECK_CHUNK_SIZE = 100

    def __init__(
        self,
        session: aiohttp.ClientSession,
        video_id: str,
        media_only_id: str,
        token: str,
        ip: str,
        *,
        base_url: str | None = None,
        store_name: str = "realdebrid",
        display_name: str = "Real-Debrid",
    ):
        token = token or ""
        token_prefix = f"{store_name}:"
        if token.startswith(token_prefix):
            _, token = self.parse_store_creds(token)

        self.session = session
        self.base_url = (
            base_url
            or settings.REALDEBRID_BASE_URL
            or "https://api.real-debrid.com/rest/1.0/"
        ).rstrip("/")
        self.store_name = store_name
        self.display_name = display_name
        self.store_token = token
        self.client_ip = ip
        self.sid = video_id
        self.media_only_id = media_only_id

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.store_token}",
            "User-Agent": "comet",
        }

    def _url(self, endpoint: str) -> str:
        return f"{self.base_url}/{endpoint.lstrip('/')}"

    @staticmethod
    def _error_key(error_payload) -> str | None:
        if isinstance(error_payload, dict):
            error = error_payload.get("error")
            if error:
                return str(error)
            error_code = error_payload.get("error_code")
            if error_code is not None:
                return str(error_code)
        return None

    @staticmethod
    def _error_message(error_payload, fallback: str) -> str:
        if isinstance(error_payload, dict):
            error = error_payload.get("error")
            if error:
                return str(error)
        if isinstance(error_payload, str) and error_payload:
            return error_payload
        return fallback

    async def _request_json(
        self,
        method: str,
        endpoint: str,
        action: str,
        *,
        expected_statuses: tuple[int, ...] = (200,),
        **kwargs,
    ):
        response = await self.session.request(
            method,
            self._url(endpoint),
            headers=self._headers(),
            **kwargs,
        )

        try:
            if response.status == 204:
                data = None
            else:
                try:
                    data = await response.json(content_type=None)
                except Exception:
                    data = await response.text()

            if response.status not in expected_statuses:
                message = self._error_message(
                    data,
                    f"{self.display_name}: Failed to {action}.",
                )
                raise DebridLinkGenerationError(
                    self.display_name,
                    message,
                    error_code=self._error_key(data),
                    upstream_error_code=self._error_key(data),
                    payload={"response": data, "status_code": response.status},
                )

            return data, response
        finally:
            response.release()

    async def check_premium(self):
        try:
            user, _ = await self._request_json(
                "GET",
                "/user",
                "check account status",
            )

            if not isinstance(user, dict):
                raise DebridAuthError(
                    self.display_name,
                    f"{self.display_name}: Invalid API key.\nPlease check your configuration.",
                )

            account_type = user.get("type")
            premium_seconds = user.get("premium")
            try:
                premium_seconds = int(premium_seconds or 0)
            except (TypeError, ValueError):
                premium_seconds = 0

            if (account_type is not None or "premium" in user) and (
                account_type != "premium" and premium_seconds <= 0
            ):
                raise DebridAuthError(
                    self.display_name,
                    f"{self.display_name}: No active subscription.\nPlease renew your debrid account.",
                )
        except DebridAuthError:
            raise
        except DebridLinkGenerationError as e:
            raise DebridAuthError(
                self.display_name,
                f"{self.display_name}: Failed to check account status.\n{e.display_message}",
            ) from e
        except Exception as e:
            raise DebridAuthError(
                self.display_name,
                f"{self.display_name}: Failed to check account status.\n{e}",
            ) from e

    @staticmethod
    def _instant_availability_to_store_items(
        requested_hashes: list[str], availability: dict
    ) -> list[dict]:
        items = []

        for info_hash in requested_hashes:
            hash_availability = (
                availability.get(info_hash)
                or availability.get(info_hash.lower())
                or availability.get(info_hash.upper())
            )
            if not isinstance(hash_availability, dict):
                continue

            for variants in hash_availability.values():
                if not isinstance(variants, list):
                    continue

                for variant in variants:
                    if not isinstance(variant, dict):
                        continue

                    files = []
                    for file_id, file_info in variant.items():
                        if not isinstance(file_info, dict):
                            continue

                        try:
                            index = int(file_id)
                        except (TypeError, ValueError):
                            index = -1

                        files.append(
                            {
                                "index": index,
                                "name": file_info.get("filename") or "",
                                "size": file_info.get("filesize", -1),
                            }
                        )

                    if files:
                        items.append(
                            {
                                "hash": info_hash.lower(),
                                "status": "cached",
                                "files": files,
                            }
                        )

        return items

    async def get_instant(self, magnets: list):
        if len(magnets) > self._INSTANT_CHECK_CHUNK_SIZE:
            chunks = [
                magnets[i : i + self._INSTANT_CHECK_CHUNK_SIZE]
                for i in range(0, len(magnets), self._INSTANT_CHECK_CHUNK_SIZE)
            ]
            responses = await asyncio.gather(
                *[self.get_instant(chunk) for chunk in chunks]
            )
            items = []
            for response in responses:
                if response and "data" in response:
                    items.extend(response["data"].get("items", []))
            return {"data": {"items": items}}

        try:
            hashes = [str(magnet).lower() for magnet in magnets if magnet]
            if not hashes:
                return {"data": {"items": []}}

            availability, _ = await self._request_json(
                "GET",
                f"/torrents/instantAvailability/{'/'.join(hashes)}",
                "check hash instant availability",
            )

            if not isinstance(availability, dict):
                return {"data": {"items": []}}

            return {
                "data": {
                    "items": self._instant_availability_to_store_items(
                        hashes, availability
                    )
                }
            }
        except Exception as e:
            logger.warning(
                f"Exception while checking hash instant availability on {self.store_name}: {e}"
            )

    @staticmethod
    def _to_store_magnet(torrent: dict) -> dict:
        return {
            "id": str(torrent.get("id", "")),
            "hash": str(torrent.get("hash", "")).lower(),
            "name": torrent.get("filename")
            or torrent.get("original_filename")
            or torrent.get("name")
            or "",
            "size": torrent.get("bytes") or torrent.get("original_bytes") or 0,
            "status": torrent.get("status") or "unknown",
            "added_at": torrent.get("added"),
        }

    async def list_magnets(self, limit: int = 500, offset: int = 0):
        try:
            response_payload, response = await self._request_json(
                "GET",
                "/torrents",
                "list account magnets",
                params={"limit": limit, "offset": offset},
            )
            if not isinstance(response_payload, list):
                return None, 0

            total_header = response.headers.get("X-Total-Count")
            try:
                total_items = int(total_header) if total_header else 0
            except ValueError:
                total_items = 0

            return (
                [self._to_store_magnet(torrent) for torrent in response_payload],
                total_items,
            )
        except Exception as e:
            logger.warning(
                f"Exception while listing account magnets on {self.store_name}: {e}"
            )
            return None, 0

    @staticmethod
    def _is_already_active_error(error: DebridLinkGenerationError) -> bool:
        response = error.payload.get("response")
        if isinstance(response, dict) and response.get("error_code") == 33:
            return True
        return "already" in error.message.lower()

    async def _find_torrent_id_by_hash(self, info_hash: str) -> str | None:
        limit = 500
        offset = 0
        max_items = settings.DEBRID_ACCOUNT_SCRAPE_MAX_SNAPSHOT_ITEMS
        info_hash = info_hash.lower()

        while offset < max_items:
            page_limit = min(limit, max_items - offset)
            torrents, response = await self._request_json(
                "GET",
                "/torrents",
                "find existing torrent",
                params={"limit": page_limit, "offset": offset},
            )
            if not isinstance(torrents, list):
                return None

            for torrent in torrents:
                if not isinstance(torrent, dict):
                    continue
                if str(torrent.get("hash", "")).lower() == info_hash:
                    torrent_id = torrent.get("id")
                    return str(torrent_id) if torrent_id else None

            if len(torrents) < page_limit:
                return None

            offset += page_limit
            total_header = response.headers.get("X-Total-Count")
            try:
                total_items = int(total_header) if total_header else 0
            except ValueError:
                total_items = 0
            if total_items and offset >= total_items:
                return None

        return None

    async def _add_magnet(self, info_hash: str, magnet_uri: str) -> str:
        try:
            magnet, _ = await self._request_json(
                "POST",
                "/torrents/addMagnet",
                "add torrent to store",
                expected_statuses=(200, 201),
                data={"magnet": magnet_uri},
            )
        except DebridLinkGenerationError as e:
            if not self._is_already_active_error(e):
                raise

            torrent_id = await self._find_torrent_id_by_hash(info_hash)
            if torrent_id:
                return torrent_id
            raise

        torrent_id = magnet.get("id") if isinstance(magnet, dict) else None
        if not torrent_id:
            raise DebridLinkGenerationError(
                self.display_name,
                f"{self.display_name}: Failed to add torrent to store.",
                payload={"response": magnet},
            )
        return str(torrent_id)

    async def _select_files(self, torrent_id: str, index: str):
        selected_files = index if str(index).isdigit() else "all"
        await self._request_json(
            "POST",
            f"/torrents/selectFiles/{torrent_id}",
            "select torrent files",
            expected_statuses=(200, 202, 204),
            data={"files": selected_files},
        )

    async def _torrent_info(self, torrent_id: str) -> dict:
        torrent_info, _ = await self._request_json(
            "GET",
            f"/torrents/info/{torrent_id}",
            "get torrent info",
        )
        if not isinstance(torrent_info, dict):
            raise DebridLinkGenerationError(
                self.display_name,
                f"{self.display_name}: Failed to get torrent info.",
                payload={"response": torrent_info},
            )
        return torrent_info

    async def _wait_for_torrent_info(self, torrent_id: str) -> dict:
        torrent_info = {}
        for attempt in range(5):
            torrent_info = await self._torrent_info(torrent_id)
            status = torrent_info.get("status", "")
            if (
                status == self._TORRENT_READY_STATUS
                or status in self._TORRENT_INVALID_STATUSES
            ):
                return torrent_info
            if status not in self._TORRENT_PENDING_STATUSES:
                return torrent_info
            if attempt < 4:
                await asyncio.sleep(0.5)
        return torrent_info

    @staticmethod
    def _files_with_links(torrent_info: dict) -> list[dict]:
        files = torrent_info.get("files") or []
        links = list(torrent_info.get("links") or [])
        linked_files = []
        link_index = 0

        for file in files:
            if not isinstance(file, dict):
                continue
            if file.get("selected") not in (1, True, "1"):
                continue

            link = links[link_index] if link_index < len(links) else None
            link_index += 1

            linked_files.append(
                {
                    "index": file.get("id"),
                    "name": str(file.get("path") or file.get("name") or "").lstrip(
                        "/"
                    ),
                    "size": file.get("bytes", 0),
                    "link": link,
                }
            )

        return linked_files

    async def _unrestrict_link(self, link: str) -> str:
        unrestricted, _ = await self._request_json(
            "POST",
            "/unrestrict/link",
            "generate download link",
            data={"link": link},
        )
        if isinstance(unrestricted, dict):
            download_url = unrestricted.get("download") or unrestricted.get("link")
            if download_url:
                return download_url

        raise DebridLinkGenerationError(
            self.display_name,
            f"{self.display_name}: Failed to generate download link.",
            payload={"response": unrestricted},
        )

    async def generate_download_link(
        self,
        hash: str,
        index: str,
        name: str,
        torrent_name: str,
        season: int,
        episode: int,
        sources: list = None,
        aliases: dict = None,
    ):
        try:
            magnet_uri = f"magnet:?xt=urn:btih:{hash}&dn={quote(torrent_name)}"

            if sources:
                for source in sources:
                    magnet_uri += f"&tr={quote(source, safe='')}"

            torrent_id = await self._add_magnet(hash, magnet_uri)
            await self._select_files(torrent_id, index)

            torrent_info = await self._wait_for_torrent_info(torrent_id)
            torrent_status = torrent_info.get("status", "")

            if torrent_status in self._TORRENT_PENDING_STATUSES:
                raise DebridLinkGenerationError(
                    self.display_name,
                    f"{self.display_name}: Media is not cached yet (status: {torrent_status}).",
                    upstream_error_code="MEDIA_NOT_CACHED_YET",
                    payload={"status": torrent_status, "data": torrent_info},
                )
            if torrent_status in self._TORRENT_INVALID_STATUSES:
                raise DebridLinkGenerationError(
                    self.display_name,
                    f"{self.display_name}: Torrent cannot be processed (status: {torrent_status}).",
                    upstream_error_code="STORE_MAGNET_INVALID",
                    payload={"status": torrent_status, "data": torrent_info},
                )
            if torrent_status != self._TORRENT_READY_STATUS:
                logger.warning(
                    f"Unrecognized torrent status '{torrent_status}' for {hash} on {self.store_name}"
                )
                return

            name = unquote(name)
            torrent_name = unquote(torrent_name)

            aliases = aliases or {}
            ez_aliases = aliases.get("ez", [])
            if ez_aliases:
                ez_aliases_normalized = [normalize_title(a) for a in ez_aliases]

            debrid_files = self._files_with_links(torrent_info)

            video_files = []
            filenames_to_parse = []
            for file in debrid_files:
                filename = file["name"].split("/")[-1]
                filename_lower = filename.lower()

                if "sample" in filename_lower:
                    continue
                if not is_video(filename):
                    continue

                video_files.append(file)
                filenames_to_parse.append(filename)

            if not video_files:
                logger.warning(f"No video files found in torrent {hash}")
                return

            loop = asyncio.get_running_loop()
            parsed_results = await loop.run_in_executor(
                get_executor(), batch_parse, filenames_to_parse
            )

            scored_files = []
            (
                is_episode_request,
                season,
                episode,
                target_air_date,
            ) = await self._episode_request_context(self.media_only_id, season, episode)

            for file, filename, parsed in zip(
                video_files, filenames_to_parse, parsed_results
            ):
                file_index = file["index"]
                file_size = file["size"] or 0
                file_link = file.get("link")

                if not file_link:
                    continue

                file_season = parsed.seasons[0] if parsed.seasons else None
                file_episode = parsed.episodes[0] if parsed.episodes else None

                if is_episode_request:
                    if not self._strict_episode_match(
                        parsed,
                        season,
                        episode,
                        target_air_date,
                    ):
                        continue
                    file_season = season
                    file_episode = episode

                score = 0
                match_reason = []

                if season is not None and episode is not None:
                    season_matches = (not parsed.seasons) or (season in parsed.seasons)
                    episode_matches = parsed.episodes and episode in parsed.episodes

                    if season_matches and episode_matches:
                        if len(parsed.episodes) == 1:
                            score += 1000
                            match_reason.append("exact_episode")
                        else:
                            score += 500
                            match_reason.append("multi_episode")
                    elif episode_matches:
                        score += 200
                        match_reason.append("episode_only")

                if filename == torrent_name:
                    score += 100
                    match_reason.append("exact_name")

                if parsed.parsed_title:
                    if ez_aliases and quick_alias_match(
                        normalize_title(filename), ez_aliases_normalized
                    ):
                        score += 50
                        match_reason.append("alias")
                    elif title_match(name, parsed.parsed_title, aliases=aliases):
                        score += 50
                        match_reason.append("title")

                if file_index is not None and str(file_index) == str(index):
                    score += 25
                    match_reason.append("index")

                size_score = min(file_size / (10 * 1024 * 1024 * 1024), 10)
                score += size_score

                scored_files.append(
                    {
                        "index": file_index,
                        "title": filename,
                        "size": file_size if file_size > 0 else None,
                        "season": file_season,
                        "episode": file_episode,
                        "link": file_link,
                        "parsed": parsed,
                        "score": score,
                        "match_reason": match_reason,
                    }
                )

            if not scored_files:
                if is_episode_request:
                    raise DebridLinkGenerationError(
                        self.display_name,
                        f"{self.display_name}: No file matched requested episode.",
                        upstream_error_code="EPISODE_MATCH_NOT_FOUND",
                        payload={
                            "hash": hash,
                            "season": season,
                            "episode": episode,
                            "target_air_date": target_air_date,
                        },
                    )
                logger.log(
                    "PLAYBACK",
                    f"No valid video files with links found in torrent {hash}",
                )
                return

            scored_files.sort(key=lambda x: x["score"], reverse=True)
            target_file = scored_files[0]

            logger.log(
                "PLAYBACK",
                f"File selection for {hash}: selected '{target_file['title']}' "
                f"(score={target_file['score']:.1f}, reasons={target_file['match_reason']}) "
                f"from {len(scored_files)} candidates",
            )

            all_files_for_cache = []

            for f in scored_files:
                if f["season"] is not None or f["episode"] is not None:
                    all_files_for_cache.append(
                        {
                            "info_hash": hash,
                            "index": f["index"],
                            "title": f["title"],
                            "size": f["size"],
                            "season": f["season"]
                            if f["season"] is not None
                            else season,
                            "episode": f["episode"],
                            "parsed": f["parsed"],
                        }
                    )

            if season is not None or episode is not None:
                all_files_for_cache.append(
                    {
                        "info_hash": hash,
                        "index": target_file["index"],
                        "title": target_file["title"],
                        "size": target_file["size"],
                        "season": season,
                        "episode": episode,
                        "parsed": target_file["parsed"],
                    }
                )

            if all_files_for_cache:
                asyncio.create_task(
                    cache_availability(self.store_name, all_files_for_cache)
                )

            return await self._unrestrict_link(target_file["link"])
        except DebridLinkGenerationError:
            raise
        except Exception as e:
            logger.exception(
                f"Exception while getting download link for {hash} ({type(e).__name__}): {e!r}"
            )
