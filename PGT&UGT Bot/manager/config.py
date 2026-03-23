from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _require_int(name: str) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return int(value)


def _optional_int(name: str) -> int | None:
    value = os.getenv(name, "").strip()
    return int(value) if value else None


def _parse_int_list(name: str) -> list[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    return [int(part.strip()) for part in raw.split(",") if part.strip()]


def _parse_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    lowered = raw.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"Invalid boolean value for {name}: {raw}")


@dataclass(slots=True)
class Config:
    root_dir: Path
    data_dir: Path
    transcript_dir: Path
    discord_token: str
    discord_client_id: str
    discord_client_secret: str
    site_base_url: str
    site_host: str
    site_port: int
    enable_members_intent: bool
    enable_message_content_intent: bool
    support_guild_id: int
    pgt_guild_id: int | None
    ugt_guild_id: int | None
    target_ban_guild_ids: list[int]
    panel_channel_id: int
    terms_channel_id: int
    support_invite_url: str
    pgt_invite_url: str
    ugt_invite_url: str
    appeal_prompt: str
    trial_mod_role_id: int
    mod_role_id: int
    supervisor_role_id: int
    league_manager_role_id: int
    pgt_category_id: int
    ugt_category_id: int
    appeal_category_id: int
    management_category_id: int
    ticket_log_channel_id: int
    transcript_log_channel_id: int
    moderation_log_channel_id: int
    punishment_log_channel_id: int
    pgt_counter_start: int
    ugt_counter_start: int
    appeal_counter_start: int
    management_counter_start: int
    panel_title: str
    panel_description: str
    management_warning: str
    brand_name: str
    session_cookie_name: str

    @property
    def oauth_redirect_url(self) -> str:
        return f"{self.site_base_url.rstrip('/')}/auth/callback"


def load_config() -> Config:
    load_dotenv()
    root_dir = Path(__file__).resolve().parent.parent
    data_dir = root_dir / "data"
    transcript_dir = data_dir / "transcripts"
    pgt_guild_id = _optional_int("PGT_GUILD_ID")
    ugt_guild_id = _optional_int("UGT_GUILD_ID")
    target_ban_guild_ids = _parse_int_list("TARGET_BAN_GUILD_IDS")
    if not target_ban_guild_ids:
        target_ban_guild_ids = [guild_id for guild_id in [pgt_guild_id, ugt_guild_id] if guild_id]

    return Config(
        root_dir=root_dir,
        data_dir=data_dir,
        transcript_dir=transcript_dir,
        discord_token=os.getenv("DISCORD_TOKEN", "").strip(),
        discord_client_id=os.getenv("DISCORD_CLIENT_ID", "").strip(),
        discord_client_secret=os.getenv("DISCORD_CLIENT_SECRET", "").strip(),
        site_base_url=os.getenv("SITE_BASE_URL", "http://138.197.29.251:8085/").strip(),
        site_host=os.getenv("SITE_HOST", "0.0.0.0").strip(),
        site_port=int(os.getenv("SITE_PORT", "8080").strip()),
        enable_members_intent=_parse_bool("ENABLE_MEMBERS_INTENT", False),
        enable_message_content_intent=_parse_bool("ENABLE_MESSAGE_CONTENT_INTENT", True),
        support_guild_id=_require_int("SUPPORT_GUILD_ID"),
        pgt_guild_id=pgt_guild_id,
        ugt_guild_id=ugt_guild_id,
        target_ban_guild_ids=target_ban_guild_ids,
        panel_channel_id=_require_int("PANEL_CHANNEL_ID"),
        terms_channel_id=_require_int("TERMS_CHANNEL_ID"),
        support_invite_url=os.getenv("SUPPORT_INVITE_URL", "").strip(),
        pgt_invite_url=os.getenv("PGT_INVITE_URL", "").strip(),
        ugt_invite_url=os.getenv("UGT_INVITE_URL", "").strip(),
        appeal_prompt=os.getenv(
            "APPEAL_PROMPT",
            "If you believe this was a mistake, join the support server and open an Appeal ticket.",
        ).strip(),
        trial_mod_role_id=_require_int("TRIAL_MOD_ROLE_ID"),
        mod_role_id=_require_int("MOD_ROLE_ID"),
        supervisor_role_id=_require_int("SUPERVISOR_ROLE_ID"),
        league_manager_role_id=_require_int("LEAGUE_MANAGER_ROLE_ID"),
        pgt_category_id=_require_int("PGT_CATEGORY_ID"),
        ugt_category_id=_require_int("UGT_CATEGORY_ID"),
        appeal_category_id=_require_int("APPEAL_CATEGORY_ID"),
        management_category_id=_require_int("MANAGEMENT_CATEGORY_ID"),
        ticket_log_channel_id=_require_int("TICKET_LOG_CHANNEL_ID"),
        transcript_log_channel_id=_require_int("TRANSCRIPT_LOG_CHANNEL_ID"),
        moderation_log_channel_id=_require_int("MODERATION_LOG_CHANNEL_ID"),
        punishment_log_channel_id=_require_int("PUNISHMENT_LOG_CHANNEL_ID"),
        pgt_counter_start=int(os.getenv("PGT_COUNTER_START", "0").strip()),
        ugt_counter_start=int(os.getenv("UGT_COUNTER_START", "0").strip()),
        appeal_counter_start=int(os.getenv("APPEAL_COUNTER_START", "0").strip()),
        management_counter_start=int(os.getenv("MANAGEMENT_COUNTER_START", "0").strip()),
        panel_title=os.getenv("PANEL_TITLE", "UGT & PGT Manager").strip(),
        panel_description=os.getenv(
            "PANEL_DESCRIPTION",
            "Open the ticket type that fits your issue best. Read the Terms of Service first.",
        ).strip(),
        management_warning=os.getenv(
            "MANAGEMENT_WARNING",
            "Management tickets are for staff reports, sponsorships, or leadership issues.",
        ).strip(),
        brand_name=os.getenv("BRAND_NAME", "UGT & PGT Manager").strip(),
        session_cookie_name=os.getenv("SESSION_COOKIE_NAME", "ugt_pgt_session").strip(),
    )
