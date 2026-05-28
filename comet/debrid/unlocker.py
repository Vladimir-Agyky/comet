import aiohttp

from comet.core.models import settings
from comet.debrid.realdebrid import RealDebrid


class Unlocker(RealDebrid):
    def __init__(
        self,
        session: aiohttp.ClientSession,
        video_id: str,
        media_only_id: str,
        token: str,
        ip: str,
    ):
        super().__init__(
            session,
            video_id,
            media_only_id,
            token,
            ip,
            base_url=settings.UNLOCKER_BASE_URL,
            store_name="unlocker",
            display_name="Unlocker",
        )
