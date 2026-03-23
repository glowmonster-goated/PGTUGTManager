from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import discord
from aiohttp import ClientSession, web
from jinja2 import Environment, FileSystemLoader, select_autoescape

from .bot import ManagerBot
from .constants import StaffLevel, TicketSection, TicketState
from .permissions import get_staff_level, normalize_transcript_access, transcript_access_key_for_level

SESSION_TTL = timedelta(days=7)
STATE_COOKIE_NAME = "ugt_pgt_oauth_state"
TRANSCRIPT_ACCESS_FIELDS = (
    ("owner", "Ticket Owner"),
    ("trial_mod", "Trial Mods"),
    ("mod", "Mods"),
    ("supervisor", "Supervisors"),
    ("league_manager", "League Managers"),
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_dt(value: Any) -> str:
    parsed = _parse_iso_datetime(value)
    if parsed is None:
        return "Not recorded"
    return parsed.strftime("%b %d, %Y %I:%M %p UTC")


def _staff_level_label(level: StaffLevel) -> str:
    return {
        StaffLevel.NONE: "Ticket Owner Access",
        StaffLevel.TRIAL_MOD: "Trial Mod Access",
        StaffLevel.MOD: "Mod Access",
        StaffLevel.SUPERVISOR: "Supervisor Access",
        StaffLevel.LEAGUE_MANAGER: "League Manager Access",
    }[level]


def _section_label(value: str) -> str:
    try:
        return TicketSection(value).label
    except ValueError:
        return value.replace("_", " ").title()


def _state_label(value: str) -> str:
    return value.replace("_", " ").title()


def _section_bucket(value: str) -> str:
    if value in {TicketSection.PGT.value, TicketSection.UGT.value}:
        return "support"
    return value


def _summary_from_visible_messages(messages: list[dict[str, Any]]) -> dict[str, int]:
    attachments = 0
    images = 0
    videos = 0
    for message in messages:
        for attachment in message.get("attachments", []):
            attachments += 1
            preview_kind = attachment.get("preview_kind")
            if preview_kind == "image":
                images += 1
            elif preview_kind == "video":
                videos += 1
    return {
        "attachments": attachments,
        "images": images,
        "messages": len(messages),
        "videos": videos,
    }


def _session_cookie_kwargs(request: web.Request, *, max_age: int | None = None) -> dict[str, Any]:
    secure = request.url.scheme == "https"
    kwargs: dict[str, Any] = {
        "httponly": True,
        "path": "/",
        "samesite": "Lax",
        "secure": secure,
    }
    if max_age is not None:
        kwargs["max_age"] = max_age
    return kwargs


def _discord_authorize_url(config: Any, state: str) -> str:
    params = {
        "client_id": config.discord_client_id,
        "redirect_uri": config.oauth_redirect_url,
        "response_type": "code",
        "scope": "identify",
        "state": state,
    }
    return f"https://discord.com/api/oauth2/authorize?{urlencode(params)}"


def _prune_transient_state(app: web.Application) -> None:
    now = _now_utc()
    expired_sessions = [
        session_id
        for session_id, payload in app["sessions"].items()
        if payload.get("expires_at", now) <= now
    ]
    for session_id in expired_sessions:
        app["sessions"].pop(session_id, None)

    expired_oauth = [
        state
        for state, payload in app["oauth_states"].items()
        if payload.get("expires_at", now) <= now
    ]
    for state in expired_oauth:
        app["oauth_states"].pop(state, None)


def _discord_avatar_url(user_payload: dict[str, Any]) -> str:
    user_id = int(user_payload["id"])
    avatar_hash = user_payload.get("avatar")
    if avatar_hash:
        return f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.png?size=128"
    discriminator = str(user_payload.get("discriminator", "0") or "0")
    try:
        index = int(discriminator) % 5
    except ValueError:
        index = user_id % 5
    return f"https://cdn.discordapp.com/embed/avatars/{index}.png"


def _session_user_payload(user_payload: dict[str, Any]) -> dict[str, Any]:
    display_name = (
        str(user_payload.get("global_name", "")).strip()
        or str(user_payload.get("username", "")).strip()
        or f"User {user_payload['id']}"
    )
    return {
        "avatar_url": _discord_avatar_url(user_payload),
        "display_name": display_name,
        "global_name": str(user_payload.get("global_name", "")).strip(),
        "user_id": int(user_payload["id"]),
        "username": str(user_payload.get("username", "")).strip(),
    }


async def _fetch_logged_in_user(request: web.Request) -> dict[str, Any] | None:
    _prune_transient_state(request.app)
    session_id = request.cookies.get(request.app["bot"].config.session_cookie_name)
    if not session_id:
        return None
    session = request.app["sessions"].get(session_id)
    if not session:
        return None
    if session.get("expires_at", _now_utc()) <= _now_utc():
        request.app["sessions"].pop(session_id, None)
        return None
    return dict(session["user"])


async def _resolve_support_member(bot: ManagerBot, user_id: int) -> discord.Member | None:
    guild = bot.get_guild(bot.config.support_guild_id)
    if guild is None:
        return None
    member = guild.get_member(user_id)
    if member is not None:
        return member
    try:
        return await guild.fetch_member(user_id)
    except discord.DiscordException:
        return None


async def _fetch_staff_level(bot: ManagerBot, user_id: int) -> StaffLevel:
    member = await _resolve_support_member(bot, user_id)
    return get_staff_level(member, bot.config)


async def _require_user(request: web.Request) -> dict[str, Any]:
    user = await _fetch_logged_in_user(request)
    if user is None:
        destination = request.path_qs if request.path_qs else "/dashboard"
        raise web.HTTPFound(f"/login?next={destination}")
    return user


def _user_can_view_ticket(ticket: dict[str, Any], user: dict[str, Any], staff_level: StaffLevel) -> bool:
    access = normalize_transcript_access(TicketSection(ticket["section"]), ticket.get("transcript_access"))
    if int(user["user_id"]) == int(ticket.get("owner_id", 0)) and access["owner"]:
        return True
    role_key = transcript_access_key_for_level(staff_level)
    return bool(role_key and access.get(role_key, False))


def _user_can_manage_transcript(staff_level: StaffLevel) -> bool:
    return staff_level >= StaffLevel.LEAGUE_MANAGER


def _ticket_history_revoked_for_user(ticket: dict[str, Any], user_id: int) -> bool:
    for item in ticket.get("history_revocations", []):
        if isinstance(item, dict):
            value = item.get("user_id")
        else:
            value = item
        try:
            if int(value) == user_id:
                return True
        except (TypeError, ValueError):
            continue
    return False


def _punishment_history_revoked_for_user(punishment: dict[str, Any], user_id: int) -> bool:
    for item in punishment.get("history_revocations", []):
        if isinstance(item, dict):
            value = item.get("user_id")
        else:
            value = item
        try:
            if int(value) == user_id:
                return True
        except (TypeError, ValueError):
            continue
    return False


def _owner_visible_cutoff(ticket: dict[str, Any], messages: list[dict[str, Any]]) -> datetime | None:
    stored_cutoff = _parse_iso_datetime(ticket.get("owner_transcript_cutoff_at"))
    if stored_cutoff is not None:
        return stored_cutoff

    close_notice_message_id = ticket.get("close_notice_message_id")
    if close_notice_message_id:
        for message in messages:
            if int(message.get("message_id", 0) or 0) == int(close_notice_message_id):
                return _parse_iso_datetime(message.get("created_at"))

    for message in reversed(messages):
        content = str(message.get("content", "") or "")
        if " is closed." in content and "Trial Mod or higher can delete it" in content:
            return _parse_iso_datetime(message.get("created_at"))

    return _parse_iso_datetime(ticket.get("closed_at")) or _parse_iso_datetime(ticket.get("deleted_at"))


def _filter_post_close_content(
    transcript: dict[str, Any],
    ticket: dict[str, Any],
    user: dict[str, Any],
    staff_level: StaffLevel,
) -> dict[str, Any]:
    if staff_level >= StaffLevel.TRIAL_MOD:
        return transcript
    if int(user["user_id"]) != int(ticket.get("owner_id", 0)):
        return transcript

    cutoff = _owner_visible_cutoff(ticket, transcript.get("messages", []))
    if cutoff is None:
        return transcript

    visible_messages = []
    for message in transcript.get("messages", []):
        created_at = _parse_iso_datetime(message.get("created_at"))
        if created_at is None or created_at <= cutoff:
            visible_messages.append(message)

    visible_events = []
    for event in transcript.get("events", []):
        created_at = _parse_iso_datetime(event.get("created_at"))
        if created_at is None or created_at <= cutoff:
            visible_events.append(event)

    filtered = dict(transcript)
    filtered["messages"] = visible_messages
    filtered["events"] = visible_events
    return filtered


def _redact_transcript_for_viewer(transcript: dict[str, Any], staff_level: StaffLevel) -> dict[str, Any]:
    if staff_level >= StaffLevel.TRIAL_MOD:
        return transcript

    redacted_messages: list[dict[str, Any]] = []
    for message in transcript.get("messages", []):
        item = dict(message)
        if item.get("deleted") and item.get("author_is_staff"):
            item["attachments"] = []
            item["content"] = ""
            item["embeds"] = []
            item["redacted_deleted_staff_message"] = True
        redacted_messages.append(item)

    redacted = dict(transcript)
    redacted["messages"] = redacted_messages
    return redacted


def _participant_summary(ticket: dict[str, Any], transcript_payload: dict[str, Any]) -> list[str]:
    participant_map = transcript_payload.get("participants", {})
    labels: list[str] = []
    for user_id, participant in participant_map.items():
        names = [
            *participant.get("display_names", []),
            *participant.get("names", []),
        ]
        unique_names = list(dict.fromkeys([name for name in names if name]))
        if unique_names:
            labels.append(f"{unique_names[0]} ({user_id})")
        else:
            labels.append(str(user_id))
    if not labels and ticket.get("owner_id"):
        owner_label = ticket.get("owner_display_name") or ticket.get("owner_name") or str(ticket["owner_id"])
        labels.append(f"{owner_label} ({ticket['owner_id']})")
    return labels


def _format_recent_punishment(punishment: dict[str, Any], linked_ticket: dict[str, Any] | None) -> dict[str, Any]:
    proof = punishment.get("proof") or {}
    proof_url = str(proof.get("url", "")).strip()
    proof_name = str(proof.get("filename", "")).strip() or "Stored proof"
    source_label = (
        linked_ticket.get("channel_name", "Case")
        if linked_ticket
        else f"{punishment.get('context_guild_name', 'Unknown Guild')} / #{punishment.get('context_channel_name', 'unknown-channel')}"
    )
    return {
        "action": str(punishment.get("action", "punishment")).title(),
        "created_at": _format_dt(punishment.get("created_at")),
        "created_at_raw": str(punishment.get("created_at", "")),
        "duration_text": punishment.get("duration_text", "Unknown"),
        "ends_at": _format_dt(punishment.get("ends_at")),
        "history_revocations": punishment.get("history_revocations", []),
        "id": punishment.get("id"),
        "proof_name": proof_name,
        "proof_url": proof_url,
        "reason": punishment.get("reason", "No reason recorded"),
        "rule_label": punishment.get("rule_label") or punishment.get("rule_id") or "Manual / Unknown",
        "source_label": source_label,
        "status": str(punishment.get("status", "recorded")).title(),
        "ticket_id": linked_ticket.get("ticket_id") if linked_ticket else punishment.get("ticket_id"),
        "ticket_url": linked_ticket.get("transcript_url") if linked_ticket else None,
        "user_id": punishment.get("user_id"),
    }


def _sort_records_by_timestamp(items: list[dict[str, Any]], key: str, *, reverse: bool = True) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: _parse_iso_datetime(item.get(key)) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=reverse,
    )


def _render_page(
    request: web.Request,
    template_name: str,
    context: dict[str, Any],
    *,
    status: int = 200,
) -> web.Response:
    env: Environment = request.app["jinja"]
    template = env.get_template(template_name)
    return web.Response(
        text=template.render(**context),
        content_type="text/html",
        status=status,
    )


async def _load_case_records(
    bot: ManagerBot,
    user: dict[str, Any],
    staff_level: StaffLevel,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    tickets = await bot.store.list_tickets()
    tickets = _sort_records_by_timestamp(tickets, "created_at")
    raw_transcripts: dict[str, dict[str, Any]] = {}
    visible_records: list[dict[str, Any]] = []
    ticket_lookup = {str(ticket["ticket_id"]): ticket for ticket in tickets}

    for ticket in tickets:
        ticket_id = str(ticket["ticket_id"])
        raw_transcript = await bot.transcripts.get_transcript(ticket_id)
        if raw_transcript is None:
            raw_transcript = {"events": [], "messages": {}, "order": [], "participants": {}, "ticket": {}}
        raw_transcripts[ticket_id] = raw_transcript

        if not _user_can_view_ticket(ticket, user, staff_level):
            continue

        participants = _participant_summary(ticket, raw_transcript)
        raw_messages = list(raw_transcript.get("messages", {}).values())
        attachment_count = sum(len(item.get("attachments", [])) for item in raw_messages)
        message_count = len(raw_transcript.get("order", []))
        last_message = max(
            (_parse_iso_datetime(item.get("created_at")) for item in raw_messages),
            default=None,
        )
        activity_at = (
            last_message
            or _parse_iso_datetime(ticket.get("deleted_at"))
            or _parse_iso_datetime(ticket.get("closed_at"))
            or _parse_iso_datetime(ticket.get("created_at"))
            or _now_utc()
        )
        owner_label = (
            ticket.get("owner_display_name")
            or raw_transcript.get("ticket", {}).get("owner_display_name")
            or ticket.get("owner_name")
            or raw_transcript.get("ticket", {}).get("owner_name")
            or f"User {ticket.get('owner_id', 'Unknown')}"
        )
        search_parts = [
            str(ticket.get("ticket_id", "")),
            str(ticket.get("channel_name", "")),
            str(ticket.get("owner_id", "")),
            owner_label,
            *participants,
        ]
        visible_records.append(
            {
                "activity_at": activity_at.isoformat(),
                "added_user_count": len(ticket.get("added_user_ids", [])),
                "attachments": attachment_count,
                "channel_name": ticket.get("channel_name", ticket_id),
                "closed_at": _format_dt(ticket.get("closed_at")),
                "closed_at_raw": str(ticket.get("closed_at", "")),
                "created_at": _format_dt(ticket.get("created_at")),
                "created_at_raw": str(ticket.get("created_at", "")),
                "deleted_at": _format_dt(ticket.get("deleted_at")),
                "deleted_at_raw": str(ticket.get("deleted_at", "")),
                "display_number": ticket.get("display_number"),
                "linked_punishment_count": len(ticket.get("linked_punishment_ids", [])),
                "messages": message_count,
                "move_count": len(ticket.get("move_history", [])),
                "owner_id": ticket.get("owner_id"),
                "owner_label": owner_label,
                "participant_count": len(participants),
                "participant_labels": participants[:5],
                "search_blob": " ".join(part.lower() for part in search_parts if part),
                "section": ticket.get("section"),
                "section_bucket": _section_bucket(ticket.get("section", "")),
                "section_label": _section_label(ticket.get("section", "")),
                "state": ticket.get("state", TicketState.OPEN.value),
                "state_label": _state_label(ticket.get("state", TicketState.OPEN.value)),
                "ticket": ticket,
                "ticket_id": ticket_id,
                "transcript_url": ticket.get("transcript_url"),
            }
        )

    visible_records = _sort_records_by_timestamp(visible_records, "activity_at")
    return visible_records, raw_transcripts, ticket_lookup


def _punishment_is_visible_to_viewer(
    punishment: dict[str, Any],
    ticket_lookup: dict[str, dict[str, Any]],
    user: dict[str, Any],
    staff_level: StaffLevel,
) -> bool:
    linked_ticket_id = str(punishment.get("ticket_id", "") or "").strip()
    if linked_ticket_id:
        linked_ticket = ticket_lookup.get(linked_ticket_id)
        if linked_ticket is None:
            return False
        return _user_can_view_ticket(linked_ticket, user, staff_level)
    return staff_level >= StaffLevel.TRIAL_MOD


async def _load_punishment_records(
    bot: ManagerBot,
    user: dict[str, Any],
    staff_level: StaffLevel,
    ticket_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    punishments = await bot.store.list_punishments()
    visible_records: list[dict[str, Any]] = []
    for punishment in punishments:
        if not _punishment_is_visible_to_viewer(punishment, ticket_lookup, user, staff_level):
            continue
        linked_ticket = None
        ticket_id = str(punishment.get("ticket_id", "") or "").strip()
        if ticket_id:
            linked_ticket = ticket_lookup.get(ticket_id)
        visible_records.append(_format_recent_punishment(punishment, linked_ticket))
    return _sort_records_by_timestamp(visible_records, "created_at_raw")


def _build_user_directory(
    case_records: list[dict[str, Any]],
    raw_transcripts: dict[str, dict[str, Any]],
    punishment_records: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    directory: dict[int, dict[str, Any]] = {}
    visible_ticket_lookup = {record["ticket_id"]: record["ticket"] for record in case_records}

    def ensure_user(user_id: int, label: str | None = None, *, is_staff: bool = False) -> dict[str, Any]:
        entry = directory.setdefault(
            user_id,
            {
                "active_punishments": 0,
                "added_cases": 0,
                "aliases": [],
                "case_ids": set(),
                "is_staff": False,
                "last_activity_at": "",
                "last_punishment_at": "",
                "last_ticket_at": "",
                "owner_cases": 0,
                "participant_cases": 0,
                "proof_count": 0,
                "punishment_count": 0,
                "user_id": user_id,
            },
        )
        if label and label not in entry["aliases"]:
            entry["aliases"].append(label)
        entry["is_staff"] = entry["is_staff"] or is_staff
        return entry

    for record in case_records:
        ticket = record["ticket"]
        ticket_id = str(ticket["ticket_id"])
        timestamp = record["activity_at"]
        owner_id = int(ticket.get("owner_id", 0) or 0)
        if owner_id:
            owner_label = ticket.get("owner_display_name") or ticket.get("owner_name") or f"User {owner_id}"
            if not _ticket_history_revoked_for_user(ticket, owner_id):
                owner_entry = ensure_user(owner_id, owner_label)
                owner_entry["owner_cases"] += 1
                owner_entry["case_ids"].add(ticket_id)
                owner_entry["last_ticket_at"] = max(str(owner_entry["last_ticket_at"]), timestamp)
                owner_entry["last_activity_at"] = max(str(owner_entry["last_activity_at"]), timestamp)

        raw_transcript = raw_transcripts.get(ticket_id, {})
        for participant_id, participant in raw_transcript.get("participants", {}).items():
            try:
                parsed_id = int(participant_id)
            except (TypeError, ValueError):
                continue
            if _ticket_history_revoked_for_user(ticket, parsed_id):
                continue
            labels = participant.get("display_names", []) or participant.get("names", [])
            label = labels[0] if labels else f"User {parsed_id}"
            entry = ensure_user(parsed_id, label, is_staff=bool(participant.get("is_staff")))
            if parsed_id != owner_id:
                entry["participant_cases"] += 1
            entry["case_ids"].add(ticket_id)
            entry["last_ticket_at"] = max(str(entry["last_ticket_at"]), timestamp)
            entry["last_activity_at"] = max(str(entry["last_activity_at"]), timestamp)

        for added_user_id in ticket.get("added_user_ids", []):
            try:
                parsed_id = int(added_user_id)
            except (TypeError, ValueError):
                continue
            if _ticket_history_revoked_for_user(ticket, parsed_id):
                continue
            entry = ensure_user(parsed_id, f"User {parsed_id}")
            if parsed_id != owner_id:
                entry["added_cases"] += 1
            entry["case_ids"].add(ticket_id)
            entry["last_ticket_at"] = max(str(entry["last_ticket_at"]), timestamp)
            entry["last_activity_at"] = max(str(entry["last_activity_at"]), timestamp)

    for punishment in punishment_records:
        try:
            user_id = int(punishment["user_id"])
        except (TypeError, ValueError):
            continue
        linked_ticket_id = str(punishment.get("ticket_id", "") or "").strip()
        linked_ticket = visible_ticket_lookup.get(linked_ticket_id) if linked_ticket_id else None
        if linked_ticket and _ticket_history_revoked_for_user(linked_ticket, user_id):
            continue
        if _punishment_history_revoked_for_user(punishment, user_id):
            continue
        entry = ensure_user(user_id, f"User {user_id}")
        entry["punishment_count"] += 1
        if str(punishment.get("status", "")).lower() == "active":
            entry["active_punishments"] += 1
        if punishment.get("proof_url"):
            entry["proof_count"] += 1
        created_raw = str(punishment.get("created_at_raw", ""))
        entry["last_punishment_at"] = max(str(entry["last_punishment_at"]), created_raw)
        entry["last_activity_at"] = max(str(entry["last_activity_at"]), created_raw)

    return directory


def _directory_rows(directory: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in directory.values():
        aliases = entry.get("aliases", [])
        primary_label = aliases[0] if aliases else f"User {entry['user_id']}"
        rows.append(
            {
                **entry,
                "alias_count": len(aliases),
                "aliases_text": ", ".join(aliases[:4]),
                "display_name": primary_label,
                "last_activity": _format_dt(entry.get("last_activity_at")),
                "last_punishment": _format_dt(entry.get("last_punishment_at")),
                "last_ticket": _format_dt(entry.get("last_ticket_at")),
                "total_cases": len(entry.get("case_ids", set())),
            }
        )
    return sorted(
        rows,
        key=lambda item: (
            _parse_iso_datetime(item.get("last_activity_at")) or datetime.min.replace(tzinfo=timezone.utc),
            item.get("punishment_count", 0),
            item.get("total_cases", 0),
        ),
        reverse=True,
    )


async def _resolve_user_label(bot: ManagerBot, user_id: int, directory: dict[int, dict[str, Any]]) -> str:
    entry = directory.get(user_id)
    if entry and entry.get("aliases"):
        return entry["aliases"][0]
    member = await _resolve_support_member(bot, user_id)
    if member is not None:
        return member.display_name
    return f"User {user_id}"


async def _build_dashboard_payload(
    bot: ManagerBot,
    case_records: list[dict[str, Any]],
    punishment_records: list[dict[str, Any]],
    directory: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    metrics = {
        "active_punishments": sum(1 for item in punishment_records if str(item["status"]).lower() == "active"),
        "appeal_cases": sum(1 for item in case_records if item["section"] == TicketSection.APPEAL.value),
        "closed_cases": sum(1 for item in case_records if item["state"] == TicketState.CLOSED.value),
        "deleted_cases": sum(1 for item in case_records if item["state"] == TicketState.DELETED.value),
        "management_cases": sum(1 for item in case_records if item["section"] == TicketSection.MANAGEMENT.value),
        "open_cases": sum(1 for item in case_records if item["state"] == TicketState.OPEN.value),
        "proof_records": sum(1 for item in punishment_records if item.get("proof_url")),
        "support_cases": sum(1 for item in case_records if item["section_bucket"] == "support"),
        "total_cases": len(case_records),
        "tracked_users": len(directory),
    }

    open_queue = [item for item in case_records if item["state"] == TicketState.OPEN.value][:8]
    recent_closed = [
        item
        for item in case_records
        if item["state"] in {TicketState.CLOSED.value, TicketState.DELETED.value}
    ][:8]
    recent_punishments = punishment_records[:8]

    staff_stats = await bot.store.get_staff_stats()
    leaderboards: dict[str, list[dict[str, Any]]] = {
        "closed": [],
        "handled": [],
        "spoken": [],
    }
    for user_id_text, payload in staff_stats.items():
        try:
            staff_id = int(user_id_text)
        except (TypeError, ValueError):
            continue
        label = await _resolve_user_label(bot, staff_id, directory)
        leaderboards["handled"].append(
            {"label": label, "user_id": staff_id, "value": len(payload.get("handled_tickets", []))}
        )
        leaderboards["spoken"].append(
            {"label": label, "user_id": staff_id, "value": len(payload.get("spoken_tickets", []))}
        )
        leaderboards["closed"].append(
            {"label": label, "user_id": staff_id, "value": len(payload.get("closed_tickets", []))}
        )

    for key in leaderboards:
        leaderboards[key] = sorted(leaderboards[key], key=lambda item: item["value"], reverse=True)[:5]

    module_cards = [
        {
            "description": "Search open, closed, and deleted tickets with owner, participants, movement history, and linked proof in one place.",
            "eyebrow": "Archive",
            "href": "/cases",
            "title": "Case Library",
        },
        {
            "description": "Review punishments, proof files, lift activity, and source context without digging through Discord logs.",
            "eyebrow": "Moderation",
            "href": "/punishments",
            "title": "Punishment Desk",
        },
        {
            "description": "Pull together case involvement, proof-backed punishments, aliases, and staff context around one user.",
            "eyebrow": "Profiles",
            "href": "/users",
            "title": "User Dossiers",
        },
        {
            "description": "Attachments, screenshots, and video proof stay tied to each case file so staff can revisit evidence fast.",
            "eyebrow": "Media",
            "href": "/cases",
            "title": "Evidence Vault",
        },
    ]

    expansion_cards = [
        {
            "description": "This layout is ready for more staff tools later like private notes, season dashboards, roster tools, or queue automation.",
            "title": "Built For Growth",
        },
        {
            "description": "Cases, punishments, transcripts, and user history already point at each other, so later features can plug into the same records cleanly.",
            "title": "Shared Source Of Truth",
        },
    ]

    command_handbook = [
        "/punish reviews a rule, proof, and prior history before applying action.",
        "/manual-ban handles anything outside the rulebook while still storing the case link.",
        "/unban lifts active bans or mutes from a picker instead of guessing records.",
        "/user-info gives a compact report, while the Users page turns it into a browsable dossier.",
        "/hacked kicks across both league servers and clears recent messages from the account.",
    ]

    return {
        "command_handbook": command_handbook,
        "expansion_cards": expansion_cards,
        "leaderboards": leaderboards,
        "metrics": metrics,
        "module_cards": module_cards,
        "open_queue": open_queue,
        "recent_closed": recent_closed,
        "recent_punishments": recent_punishments,
    }


def _managed_guild_rows(bot: ManagerBot) -> list[dict[str, Any]]:
    seen: set[int] = set()
    rows: list[dict[str, Any]] = []
    guild_sources = [
        ("Support Hub", bot.config.support_guild_id),
        ("PGT Server", bot.config.pgt_guild_id),
        ("UGT Server", bot.config.ugt_guild_id),
    ]
    for label, guild_id in guild_sources:
        if not guild_id:
            continue
        try:
            parsed_id = int(guild_id)
        except (TypeError, ValueError):
            continue
        if parsed_id in seen:
            continue
        seen.add(parsed_id)
        guild = bot.get_guild(parsed_id)
        rows.append(
            {
                "display_name": guild.name if guild else label,
                "guild_id": parsed_id,
                "label": label,
            }
        )
    return rows


def _nav_items(staff_level: StaffLevel) -> list[dict[str, Any]]:
    items = [
        {"href": "/dashboard", "label": "Overview"},
        {"href": "/cases", "label": "Cases"},
    ]
    if staff_level >= StaffLevel.TRIAL_MOD:
        items.extend(
            [
                {"href": "/punishments", "label": "Punishments"},
                {"href": "/users", "label": "Users"},
            ]
        )
    return items


async def _base_context(
    request: web.Request,
    *,
    page_title: str,
    page_subtitle: str,
    user: dict[str, Any] | None = None,
    staff_level: StaffLevel = StaffLevel.NONE,
) -> dict[str, Any]:
    bot: ManagerBot = request.app["bot"]
    return {
        "brand_name": bot.config.brand_name,
        "current_path": request.path,
        "managed_guilds": _managed_guild_rows(bot),
        "nav_items": _nav_items(staff_level),
        "page_subtitle": page_subtitle,
        "page_title": page_title,
        "asset_version": request.app.get("asset_version", "1"),
        "site_base_url": bot.config.site_base_url.rstrip("/"),
        "user": user,
        "user_role_label": _staff_level_label(staff_level),
    }


async def _render_error(
    request: web.Request,
    *,
    title: str,
    message: str,
    status: int,
) -> web.Response:
    user = await _fetch_logged_in_user(request)
    staff_level = StaffLevel.NONE
    if user is not None:
        staff_level = await _fetch_staff_level(request.app["bot"], int(user["user_id"]))
    context = await _base_context(
        request,
        page_title=title,
        page_subtitle="Portal Notice",
        staff_level=staff_level,
        user=user,
    )
    context.update({"error_message": message, "error_title": title})
    return _render_page(request, "error.html", context, status=status)


async def index(request: web.Request) -> web.Response:
    user = await _fetch_logged_in_user(request)
    if user is not None:
        raise web.HTTPFound("/dashboard")
    context = await _base_context(
        request,
        page_title="Staff Portal",
        page_subtitle="Discord Login Required",
    )
    context.update({"login_url": "/login"})
    return _render_page(request, "index.html", context)


async def login(request: web.Request) -> web.Response:
    bot: ManagerBot = request.app["bot"]
    _prune_transient_state(request.app)
    state = secrets.token_urlsafe(24)
    next_path = str(request.query.get("next", "/dashboard") or "/dashboard")
    request.app["oauth_states"][state] = {
        "expires_at": _now_utc() + timedelta(minutes=10),
        "next": next_path,
    }
    response = web.HTTPFound(_discord_authorize_url(bot.config, state))
    response.set_cookie(
        STATE_COOKIE_NAME,
        state,
        **_session_cookie_kwargs(request, max_age=600),
    )
    raise response


async def logout(request: web.Request) -> web.Response:
    bot: ManagerBot = request.app["bot"]
    session_cookie = request.cookies.get(bot.config.session_cookie_name)
    if session_cookie:
        request.app["sessions"].pop(session_cookie, None)
    response = web.HTTPFound("/")
    response.del_cookie(bot.config.session_cookie_name, path="/")
    response.del_cookie(STATE_COOKIE_NAME, path="/")
    raise response


async def auth_callback(request: web.Request) -> web.Response:
    bot: ManagerBot = request.app["bot"]
    _prune_transient_state(request.app)

    if request.query.get("error"):
        return await _render_error(
            request,
            title="Login Failed",
            message=f"Discord returned: {request.query.get('error_description') or request.query.get('error')}",
            status=400,
        )

    state = str(request.query.get("state", "")).strip()
    code = str(request.query.get("code", "")).strip()
    cookie_state = request.cookies.get(STATE_COOKIE_NAME, "")
    stored_state = request.app["oauth_states"].pop(state, None)

    if not code or not state or not stored_state or cookie_state != state:
        return await _render_error(
            request,
            title="Login Failed",
            message="The Discord login state was invalid or expired. Try logging in again.",
            status=400,
        )

    form_data = {
        "client_id": bot.config.discord_client_id,
        "client_secret": bot.config.discord_client_secret,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": bot.config.oauth_redirect_url,
    }

    async with request.app["http"].post(
        "https://discord.com/api/oauth2/token",
        data=form_data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    ) as token_response:
        if token_response.status >= 400:
            return await _render_error(
                request,
                title="Login Failed",
                message="Discord rejected the login exchange. Check the OAuth redirect URL and client secret.",
                status=400,
            )
        token_payload = await token_response.json()

    access_token = str(token_payload.get("access_token", "")).strip()
    if not access_token:
        return await _render_error(
            request,
            title="Login Failed",
            message="Discord did not return an access token.",
            status=400,
        )

    async with request.app["http"].get(
        "https://discord.com/api/users/@me",
        headers={"Authorization": f"Bearer {access_token}"},
    ) as user_response:
        if user_response.status >= 400:
            return await _render_error(
                request,
                title="Login Failed",
                message="Discord login succeeded, but the user profile could not be loaded.",
                status=400,
            )
        discord_user = await user_response.json()

    session_id = secrets.token_urlsafe(32)
    request.app["sessions"][session_id] = {
        "expires_at": _now_utc() + SESSION_TTL,
        "user": _session_user_payload(discord_user),
    }

    response = web.HTTPFound(str(stored_state.get("next") or "/dashboard"))
    response.set_cookie(
        bot.config.session_cookie_name,
        session_id,
        **_session_cookie_kwargs(request, max_age=int(SESSION_TTL.total_seconds())),
    )
    response.del_cookie(STATE_COOKIE_NAME, path="/")
    raise response


async def dashboard(request: web.Request) -> web.Response:
    bot: ManagerBot = request.app["bot"]
    user = await _require_user(request)
    staff_level = await _fetch_staff_level(bot, int(user["user_id"]))

    case_records, raw_transcripts, ticket_lookup = await _load_case_records(bot, user, staff_level)
    punishment_records = await _load_punishment_records(bot, user, staff_level, ticket_lookup)
    directory = _build_user_directory(case_records, raw_transcripts, punishment_records)
    dashboard_payload = await _build_dashboard_payload(bot, case_records, punishment_records, directory)

    context = await _base_context(
        request,
        page_title="Operations Overview",
        page_subtitle="General Staff Portal",
        staff_level=staff_level,
        user=user,
    )
    context.update(dashboard_payload)
    return _render_page(request, "dashboard.html", context)


async def transcripts_index(request: web.Request) -> web.Response:
    bot: ManagerBot = request.app["bot"]
    user = await _require_user(request)
    staff_level = await _fetch_staff_level(bot, int(user["user_id"]))

    case_records, raw_transcripts, ticket_lookup = await _load_case_records(bot, user, staff_level)
    punishment_records = await _load_punishment_records(bot, user, staff_level, ticket_lookup)

    search_query = str(request.query.get("q", "")).strip().lower()
    state_filter = str(request.query.get("state", "")).strip().lower()
    section_filter = str(request.query.get("section", "")).strip().lower()

    filtered = []
    for record in case_records:
        if search_query and search_query not in record["search_blob"]:
            continue
        if state_filter and record["state"] != state_filter:
            continue
        if section_filter and record["section"] != section_filter:
            continue
        filtered.append(record)

    context = await _base_context(
        request,
        page_title="Case Library",
        page_subtitle="Searchable Transcript Archive",
        staff_level=staff_level,
        user=user,
    )
    context.update(
        {
            "case_metrics": {
                "filtered": len(filtered),
                "open": sum(1 for item in filtered if item["state"] == TicketState.OPEN.value),
                "closed": sum(1 for item in filtered if item["state"] == TicketState.CLOSED.value),
                "deleted": sum(1 for item in filtered if item["state"] == TicketState.DELETED.value),
                "with_punishments": sum(1 for item in filtered if item["linked_punishment_count"] > 0),
            },
            "case_records": filtered,
            "recent_punishments": punishment_records[:6],
            "search_query": request.query.get("q", ""),
            "section_filter": section_filter,
            "section_options": [
                {"value": "", "label": "All sections"},
                {"value": TicketSection.PGT.value, "label": TicketSection.PGT.label},
                {"value": TicketSection.UGT.value, "label": TicketSection.UGT.label},
                {"value": TicketSection.APPEAL.value, "label": TicketSection.APPEAL.label},
                {"value": TicketSection.MANAGEMENT.value, "label": TicketSection.MANAGEMENT.label},
            ],
            "state_filter": state_filter,
            "state_options": [
                {"value": "", "label": "All states"},
                {"value": TicketState.OPEN.value, "label": "Open"},
                {"value": TicketState.CLOSED.value, "label": "Closed"},
                {"value": TicketState.DELETED.value, "label": "Deleted"},
            ],
        }
    )
    return _render_page(request, "transcripts.html", context)


async def punishments_page(request: web.Request) -> web.Response:
    bot: ManagerBot = request.app["bot"]
    user = await _require_user(request)
    staff_level = await _fetch_staff_level(bot, int(user["user_id"]))
    if staff_level < StaffLevel.TRIAL_MOD:
        return await _render_error(
            request,
            title="Permission Required",
            message="Only Trial Mods or higher can browse the punishment desk.",
            status=403,
        )

    case_records, raw_transcripts, ticket_lookup = await _load_case_records(bot, user, staff_level)
    punishment_records = await _load_punishment_records(bot, user, staff_level, ticket_lookup)
    directory = _build_user_directory(case_records, raw_transcripts, punishment_records)

    search_query = str(request.query.get("q", "")).strip().lower()
    action_filter = str(request.query.get("action", "")).strip().lower()
    status_filter = str(request.query.get("status", "")).strip().lower()

    filtered = []
    for record in punishment_records:
        target_aliases = directory.get(int(record["user_id"]), {}).get("aliases", [])
        search_blob = " ".join(
            [
                str(record.get("user_id", "")),
                str(record.get("reason", "")),
                str(record.get("rule_label", "")),
                str(record.get("source_label", "")),
                str(record.get("proof_name", "")),
                *target_aliases,
            ]
        ).lower()
        if search_query and search_query not in search_blob:
            continue
        if action_filter and str(record.get("action", "")).lower() != action_filter:
            continue
        if status_filter and str(record.get("status", "")).lower() != status_filter:
            continue
        filtered.append(record)

    action_totals: dict[str, int] = {}
    status_totals: dict[str, int] = {}
    for record in punishment_records:
        action_key = str(record["action"]).lower()
        status_key = str(record["status"]).lower()
        action_totals[action_key] = action_totals.get(action_key, 0) + 1
        status_totals[status_key] = status_totals.get(status_key, 0) + 1

    context = await _base_context(
        request,
        page_title="Punishment Desk",
        page_subtitle="Moderation History, Proof, and Lift Records",
        staff_level=staff_level,
        user=user,
    )
    context.update(
        {
            "action_filter": action_filter,
            "action_options": [
                {"value": "", "label": "All actions"},
                {"value": "ban", "label": "Ban"},
                {"value": "mute", "label": "Mute"},
                {"value": "warn", "label": "Warn"},
                {"value": "manual", "label": "Staff Action"},
                {"value": "kick", "label": "Kick"},
            ],
            "action_totals": action_totals,
            "punishment_metrics": {
                "active": status_totals.get("active", 0),
                "filtered": len(filtered),
                "proof": sum(1 for item in filtered if item.get("proof_url")),
                "total": len(punishment_records),
            },
            "punishment_records": filtered,
            "search_query": request.query.get("q", ""),
            "status_filter": status_filter,
            "status_options": [
                {"value": "", "label": "All statuses"},
                {"value": "active", "label": "Active"},
                {"value": "failed", "label": "Failed"},
                {"value": "lifted", "label": "Lifted"},
                {"value": "expired", "label": "Expired"},
                {"value": "recorded", "label": "Recorded"},
            ],
            "status_totals": status_totals,
        }
    )
    return _render_page(request, "punishments.html", context)


async def users_page(request: web.Request) -> web.Response:
    bot: ManagerBot = request.app["bot"]
    user = await _require_user(request)
    staff_level = await _fetch_staff_level(bot, int(user["user_id"]))
    if staff_level < StaffLevel.TRIAL_MOD:
        return await _render_error(
            request,
            title="Permission Required",
            message="Only Trial Mods or higher can browse staff dossiers.",
            status=403,
        )

    case_records, raw_transcripts, ticket_lookup = await _load_case_records(bot, user, staff_level)
    punishment_records = await _load_punishment_records(bot, user, staff_level, ticket_lookup)
    directory = _build_user_directory(case_records, raw_transcripts, punishment_records)
    rows = _directory_rows(directory)

    search_query = str(request.query.get("q", "")).strip().lower()
    if search_query:
        rows = [
            row
            for row in rows
            if search_query in " ".join(
                [
                    str(row["user_id"]),
                    str(row["display_name"]),
                    str(row.get("aliases_text", "")),
                ]
            ).lower()
        ]

    context = await _base_context(
        request,
        page_title="User Dossiers",
        page_subtitle="Searchable People View Across Cases and Punishments",
        staff_level=staff_level,
        user=user,
    )
    context.update(
        {
            "search_query": request.query.get("q", ""),
            "user_metrics": {
                "active_flags": sum(1 for row in rows if row.get("active_punishments", 0) > 0),
                "filtered": len(rows),
                "staff": sum(1 for row in rows if row.get("is_staff")),
                "tracked": len(directory),
            },
            "user_rows": rows,
        }
    )
    return _render_page(request, "users.html", context)


async def user_detail(request: web.Request) -> web.Response:
    bot: ManagerBot = request.app["bot"]
    user = await _require_user(request)
    staff_level = await _fetch_staff_level(bot, int(user["user_id"]))
    if staff_level < StaffLevel.TRIAL_MOD:
        return await _render_error(
            request,
            title="Permission Required",
            message="Only Trial Mods or higher can view full user dossiers.",
            status=403,
        )

    try:
        target_user_id = int(request.match_info["user_id"])
    except (KeyError, TypeError, ValueError):
        return await _render_error(
            request,
            title="User Not Found",
            message="That user ID was not valid.",
            status=404,
        )

    case_records, raw_transcripts, ticket_lookup = await _load_case_records(bot, user, staff_level)
    punishment_records = await _load_punishment_records(bot, user, staff_level, ticket_lookup)
    directory = _build_user_directory(case_records, raw_transcripts, punishment_records)
    directory_rows = {row["user_id"]: row for row in _directory_rows(directory)}
    profile = directory_rows.get(
        target_user_id,
        {
            "active_punishments": 0,
            "added_cases": 0,
            "aliases": [],
            "aliases_text": "",
            "display_name": f"User {target_user_id}",
            "is_staff": False,
            "last_activity": "Not recorded",
            "last_punishment": "Not recorded",
            "last_ticket": "Not recorded",
            "owner_cases": 0,
            "participant_cases": 0,
            "proof_count": 0,
            "punishment_count": 0,
            "total_cases": 0,
            "user_id": target_user_id,
        },
    )

    visible_cases = []
    for record in case_records:
        ticket = record["ticket"]
        if _ticket_history_revoked_for_user(ticket, target_user_id):
            continue
        ticket_id = str(ticket["ticket_id"])
        raw_transcript = raw_transcripts.get(ticket_id, {})
        participant_ids = set(raw_transcript.get("participants", {}).keys())
        added_user_ids = {str(item) for item in ticket.get("added_user_ids", [])}
        involvement: list[str] = []
        if int(ticket.get("owner_id", 0) or 0) == target_user_id:
            involvement.append("Owner")
        if str(target_user_id) in participant_ids and int(ticket.get("owner_id", 0) or 0) != target_user_id:
            involvement.append("Participant")
        if str(target_user_id) in added_user_ids:
            involvement.append("Added User")
        if not involvement:
            continue
        visible_cases.append({**record, "involvement": " / ".join(involvement)})

    visible_punishments = []
    for item in punishment_records:
        if int(item["user_id"]) != target_user_id:
            continue
        linked_ticket_id = str(item.get("ticket_id", "") or "").strip()
        linked_ticket = ticket_lookup.get(linked_ticket_id) if linked_ticket_id else None
        if linked_ticket and _ticket_history_revoked_for_user(linked_ticket, target_user_id):
            continue
        if _punishment_history_revoked_for_user(item, target_user_id):
            continue
        visible_punishments.append(item)
    block = await bot.store.get_block(target_user_id)
    target_staff_level = await _fetch_staff_level(bot, target_user_id)
    staff_stats = await bot.store.get_staff_stats()
    staff_entry = staff_stats.get(str(target_user_id))

    context = await _base_context(
        request,
        page_title="User Dossier",
        page_subtitle="Cross-Case Staff View",
        staff_level=staff_level,
        user=user,
    )
    context.update(
        {
            "block_reason": block.get("reason") if block else "Not blocked from support tickets.",
            "block_status": "Blocked" if block else "Not Blocked",
            "profile": profile,
            "staff_activity": {
                "closed": len((staff_entry or {}).get("closed_tickets", [])),
                "handled": len((staff_entry or {}).get("handled_tickets", [])),
                "spoken": len((staff_entry or {}).get("spoken_tickets", [])),
            },
            "target_staff_level_label": _staff_level_label(target_staff_level) if target_staff_level > StaffLevel.NONE else "Not staff",
            "visible_cases": visible_cases,
            "visible_punishments": visible_punishments,
        }
    )
    return _render_page(request, "user_detail.html", context)


async def transcript_detail(request: web.Request) -> web.Response:
    bot: ManagerBot = request.app["bot"]
    user = await _require_user(request)
    staff_level = await _fetch_staff_level(bot, int(user["user_id"]))

    ticket_id = str(request.match_info["ticket_id"])
    ticket = await bot.store.get_ticket(ticket_id)
    if ticket is None:
        return await _render_error(
            request,
            title="Case Not Found",
            message="That case could not be found in the ticket archive.",
            status=404,
        )
    if not _user_can_view_ticket(ticket, user, staff_level):
        return await _render_error(
            request,
            title="Access Restricted",
            message="Your account does not have access to that transcript.",
            status=403,
        )

    transcript = await bot.transcripts.load_for_render(ticket)
    transcript = _filter_post_close_content(transcript, ticket, user, staff_level)
    transcript = _redact_transcript_for_viewer(transcript, staff_level)
    message_summary = _summary_from_visible_messages(transcript.get("messages", []))

    participant_rows: list[dict[str, Any]] = []
    if staff_level >= StaffLevel.TRIAL_MOD:
        participant_map = transcript.get("participants", {})
        for participant_id, participant in participant_map.items():
            names = participant.get("display_names", []) or participant.get("names", [])
            display_name = names[0] if names else f"User {participant_id}"
            participant_rows.append(
                {
                    "display_name": display_name,
                    "is_staff": bool(participant.get("is_staff")),
                    "user_id": participant_id,
                }
            )
        participant_rows = sorted(participant_rows, key=lambda item: (not item["is_staff"], item["display_name"].lower()))
    else:
        participant_rows = [
            {
                "display_name": ticket.get("owner_display_name") or ticket.get("owner_name") or f"User {ticket.get('owner_id')}",
                "is_staff": False,
                "user_id": ticket.get("owner_id"),
            }
        ]

    punishments = await bot.store.list_punishments()
    punishment_cards: list[dict[str, Any]] = []
    for punishment in punishments:
        if str(punishment.get("ticket_id", "")) != ticket_id:
            continue
        punishment_cards.append(_format_recent_punishment(punishment, ticket))
    punishment_cards = _sort_records_by_timestamp(punishment_cards, "created_at_raw")

    move_history = [
        {
            "from_label": _section_label(str(item.get("from", ""))),
            "moved_at": _format_dt(item.get("moved_at")),
            "moved_by": item.get("moved_by", "Unknown"),
            "to_label": _section_label(str(item.get("to", ""))),
        }
        for item in ticket.get("move_history", [])
    ]
    access_snapshot = normalize_transcript_access(TicketSection(ticket["section"]), ticket.get("transcript_access"))
    privacy_options = [
        {"checked": access_snapshot.get(key, False), "key": key, "label": label}
        for key, label in TRANSCRIPT_ACCESS_FIELDS
    ]

    context = await _base_context(
        request,
        page_title="Case File",
        page_subtitle="Transcript, Timeline, and Evidence",
        staff_level=staff_level,
        user=user,
    )
    context.update(
        {
            "access_snapshot": access_snapshot,
            "can_manage_privacy": _user_can_manage_transcript(staff_level),
            "limited_view_notice": (
                "You are viewing the owner-safe version of this transcript. Anything posted after the close notice is hidden."
                if staff_level < StaffLevel.TRIAL_MOD
                else ""
            ),
            "move_history": move_history,
            "participant_rows": participant_rows,
            "privacy_options": privacy_options,
            "privacy_saved": str(request.query.get("privacy", "")).strip().lower() == "saved",
            "punishment_cards": punishment_cards,
            "summary": message_summary,
            "ticket": ticket,
            "transcript": transcript,
        }
    )
    return _render_page(request, "transcript_detail.html", context)


async def transcript_privacy_update(request: web.Request) -> web.Response:
    bot: ManagerBot = request.app["bot"]
    user = await _require_user(request)
    staff_level = await _fetch_staff_level(bot, int(user["user_id"]))
    if not _user_can_manage_transcript(staff_level):
        return await _render_error(
            request,
            title="Permission Required",
            message="Only League Managers can update transcript privacy from the portal.",
            status=403,
        )

    ticket_id = str(request.match_info["ticket_id"])
    ticket = await bot.store.get_ticket(ticket_id)
    if ticket is None:
        return await _render_error(
            request,
            title="Case Not Found",
            message="That case could not be found in the archive.",
            status=404,
        )

    posted = await request.post()
    access_payload = {key: key in posted for key, _ in TRANSCRIPT_ACCESS_FIELDS}
    normalized = normalize_transcript_access(TicketSection(ticket["section"]), access_payload)
    await bot.store.update_ticket(
        ticket_id,
        lambda current: {
            **current,
            "transcript_access": normalized,
        },
    )
    raise web.HTTPFound(f"/transcripts/{ticket_id}?privacy=saved")


async def transcript_media(request: web.Request) -> web.StreamResponse:
    bot: ManagerBot = request.app["bot"]
    user = await _require_user(request)
    staff_level = await _fetch_staff_level(bot, int(user["user_id"]))

    ticket_id = str(request.match_info["ticket_id"])
    ticket = await bot.store.get_ticket(ticket_id)
    if ticket is None or not _user_can_view_ticket(ticket, user, staff_level):
        return await _render_error(
            request,
            title="Access Restricted",
            message="You do not have access to that media file.",
            status=403,
        )

    media_dir = bot.config.transcript_dir / ticket_id / "media"
    filename = Path(request.match_info["filename"]).name
    target_path = (media_dir / filename).resolve()
    if media_dir.resolve() not in target_path.parents and target_path != media_dir.resolve():
        return await _render_error(
            request,
            title="Media Not Found",
            message="That media path was not valid.",
            status=404,
        )
    if not target_path.exists():
        return await _render_error(
            request,
            title="Media Not Found",
            message="That media file no longer exists in the transcript store.",
            status=404,
        )
    return web.FileResponse(path=target_path)


async def _http_session_ctx(app: web.Application) -> Any:
    async with ClientSession() as session:
        app["http"] = session
        yield


def create_web_app(bot: ManagerBot) -> web.Application:
    template_dir = Path(__file__).with_name("templates")
    static_dir = Path(__file__).with_name("static")
    style_path = static_dir / "style.css"

    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["datetime"] = _format_dt

    app = web.Application()
    app["bot"] = bot
    app["jinja"] = env
    app["oauth_states"] = {}
    app["sessions"] = {}
    app["asset_version"] = str(int(style_path.stat().st_mtime)) if style_path.exists() else "1"
    app.cleanup_ctx.append(_http_session_ctx)

    app.router.add_get("/", index)
    app.router.add_get("/login", login)
    app.router.add_get("/logout", logout)
    app.router.add_get("/auth/callback", auth_callback)
    app.router.add_get("/dashboard", dashboard)
    app.router.add_get("/cases", transcripts_index)
    app.router.add_get("/transcripts", transcripts_index)
    app.router.add_get("/punishments", punishments_page)
    app.router.add_get("/users", users_page)
    app.router.add_get("/users/{user_id}", user_detail)
    app.router.add_get("/transcripts/{ticket_id}", transcript_detail)
    app.router.add_post("/transcripts/{ticket_id}/privacy", transcript_privacy_update)
    app.router.add_get("/transcripts/{ticket_id}/media/{filename}", transcript_media)
    app.router.add_static("/static/", static_dir)
    return app
