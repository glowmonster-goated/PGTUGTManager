from __future__ import annotations

import asyncio
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import discord

from .config import Config
from .permissions import get_staff_level
from .utils import iso_now


class TranscriptStore:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.lock = asyncio.Lock()

    def _ticket_dir(self, ticket_id: str) -> Path:
        return self.config.transcript_dir / ticket_id

    def _media_dir(self, ticket_id: str) -> Path:
        return self._ticket_dir(ticket_id) / "media"

    def _json_path(self, ticket_id: str) -> Path:
        return self._ticket_dir(ticket_id) / "transcript.json"

    def _default_payload(self, ticket: dict[str, Any]) -> dict[str, Any]:
        return {
            "events": [],
            "messages": {},
            "order": [],
            "participants": {},
            "ticket": {
                "channel_name": ticket.get("channel_name"),
                "display_name": ticket.get("display_name"),
                "display_number": ticket.get("display_number"),
                "owner_display_name": ticket.get("owner_display_name"),
                "owner_id": ticket.get("owner_id"),
                "owner_name": ticket.get("owner_name"),
                "section": ticket.get("section"),
                "state": ticket.get("state"),
                "ticket_id": ticket.get("ticket_id"),
            },
        }

    def _read_json(self, path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        temp_path = path.with_suffix(path.suffix + ".tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        os.replace(temp_path, path)

    async def ensure_ticket(self, ticket: dict[str, Any]) -> None:
        async with self.lock:
            ticket_dir = self._ticket_dir(ticket["ticket_id"])
            media_dir = self._media_dir(ticket["ticket_id"])
            ticket_dir.mkdir(parents=True, exist_ok=True)
            media_dir.mkdir(parents=True, exist_ok=True)
            path = self._json_path(ticket["ticket_id"])
            if not path.exists():
                await asyncio.to_thread(self._write_json, path, self._default_payload(ticket))
            else:
                payload = await asyncio.to_thread(self._read_json, path)
                payload["ticket"].update(
                    {
                        "channel_name": ticket.get("channel_name"),
                        "display_name": ticket.get("display_name"),
                        "display_number": ticket.get("display_number"),
                        "owner_display_name": ticket.get("owner_display_name"),
                        "owner_name": ticket.get("owner_name"),
                        "section": ticket.get("section"),
                        "state": ticket.get("state"),
                    }
                )
                await asyncio.to_thread(self._write_json, path, payload)

    async def get_transcript(self, ticket_id: str) -> dict[str, Any] | None:
        async with self.lock:
            path = self._json_path(ticket_id)
            if not path.exists():
                return None
            return await asyncio.to_thread(self._read_json, path)

    async def add_system_event(
        self,
        ticket: dict[str, Any],
        event_type: str,
        content: str,
        *,
        actor_id: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        async with self.lock:
            path = self._json_path(ticket["ticket_id"])
            if not path.exists():
                await asyncio.to_thread(self._write_json, path, self._default_payload(ticket))
            payload = await asyncio.to_thread(self._read_json, path)
            payload["ticket"].update(
                {
                    "channel_name": ticket.get("channel_name"),
                    "display_name": ticket.get("display_name"),
                    "display_number": ticket.get("display_number"),
                    "owner_display_name": ticket.get("owner_display_name"),
                    "owner_name": ticket.get("owner_name"),
                    "section": ticket.get("section"),
                    "state": ticket.get("state"),
                }
            )
            payload["events"].append(
                {
                    "actor_id": actor_id,
                    "content": content,
                    "created_at": iso_now(),
                    "extra": extra or {},
                    "type": event_type,
                }
            )
            await asyncio.to_thread(self._write_json, path, payload)

    async def record_message(self, ticket: dict[str, Any], message: discord.Message) -> None:
        attachments = await self._persist_attachments(ticket["ticket_id"], message)
        author_is_staff = False
        author_staff_level = 0
        if isinstance(message.author, discord.Member):
            author_staff_level = int(get_staff_level(message.author, self.config))
            author_is_staff = author_staff_level > 0
        message_payload = {
            "attachments": attachments,
            "author_avatar_url": str(message.author.display_avatar.url),
            "author_display_name": message.author.display_name,
            "author_id": message.author.id,
            "author_is_staff": author_is_staff,
            "author_name": str(message.author),
            "author_staff_level": author_staff_level,
            "content": message.clean_content,
            "created_at": message.created_at.isoformat(),
            "deleted": False,
            "edited_at": message.edited_at.isoformat() if message.edited_at else None,
            "edit_history": [],
            "embeds": [embed.to_dict() for embed in message.embeds],
            "is_bot": message.author.bot,
            "message_id": message.id,
            "reference_message_id": message.reference.message_id if message.reference else None,
        }
        async with self.lock:
            path = self._json_path(ticket["ticket_id"])
            if not path.exists():
                await asyncio.to_thread(self._write_json, path, self._default_payload(ticket))
            payload = await asyncio.to_thread(self._read_json, path)
            key = str(message.id)
            if key not in payload["messages"]:
                payload["order"].append(key)
            payload["messages"][key] = message_payload
            participants = payload.setdefault("participants", {})
            participant = participants.setdefault(
                str(message.author.id),
                {
                    "display_names": [],
                    "is_staff": author_is_staff,
                    "names": [],
                    "user_id": message.author.id,
                },
            )
            if message.author.display_name not in participant["display_names"]:
                participant["display_names"].append(message.author.display_name)
            author_name = str(message.author)
            if author_name not in participant["names"]:
                participant["names"].append(author_name)
            participant["is_staff"] = participant.get("is_staff", False) or author_is_staff
            await asyncio.to_thread(self._write_json, path, payload)

    async def record_edit(self, ticket: dict[str, Any], before: discord.Message, after: discord.Message) -> None:
        async with self.lock:
            path = self._json_path(ticket["ticket_id"])
            if not path.exists():
                return
            payload = await asyncio.to_thread(self._read_json, path)
            key = str(after.id)
            existing = payload["messages"].get(key)
            if existing is None:
                return
            existing["edit_history"].append(
                {
                    "content": existing.get("content", ""),
                    "edited_at": iso_now(),
                }
            )
            existing["content"] = after.clean_content
            existing["edited_at"] = after.edited_at.isoformat() if after.edited_at else iso_now()
            existing["embeds"] = [embed.to_dict() for embed in after.embeds]
            payload["messages"][key] = existing
            await asyncio.to_thread(self._write_json, path, payload)

    async def record_delete(self, ticket_id: str, message_id: int) -> None:
        async with self.lock:
            path = self._json_path(ticket_id)
            if not path.exists():
                return
            payload = await asyncio.to_thread(self._read_json, path)
            key = str(message_id)
            if key not in payload["messages"]:
                return
            payload["messages"][key]["deleted"] = True
            payload["messages"][key]["deleted_at"] = iso_now()
            await asyncio.to_thread(self._write_json, path, payload)

    async def get_summary(self, ticket_id: str) -> dict[str, int]:
        transcript = await self.get_transcript(ticket_id)
        if transcript is None:
            return {"attachments": 0, "messages": 0}
        messages = transcript.get("messages", {}).values()
        attachment_count = sum(len(item.get("attachments", [])) for item in messages)
        return {
            "attachments": attachment_count,
            "messages": len(transcript.get("order", [])),
        }

    async def _persist_attachments(
        self,
        ticket_id: str,
        message: discord.Message,
    ) -> list[dict[str, Any]]:
        stored: list[dict[str, Any]] = []
        media_dir = self._media_dir(ticket_id)
        media_dir.mkdir(parents=True, exist_ok=True)
        for index, attachment in enumerate(message.attachments):
            safe_name = f"{message.id}-{index}-{attachment.filename}"
            destination = media_dir / safe_name
            try:
                content = await attachment.read()
                await asyncio.to_thread(destination.write_bytes, content)
                stored.append(
                    {
                        "content_type": attachment.content_type,
                        "filename": attachment.filename,
                        "local_name": safe_name,
                        "size": attachment.size,
                        "url": attachment.url,
                    }
                )
            except discord.DiscordException:
                stored.append(
                    {
                        "content_type": attachment.content_type,
                        "filename": attachment.filename,
                        "local_name": None,
                        "size": attachment.size,
                        "url": attachment.url,
                    }
                )
        return stored

    async def load_for_render(self, ticket: dict[str, Any]) -> dict[str, Any]:
        transcript = await self.get_transcript(ticket["ticket_id"])
        if transcript is None:
            transcript = self._default_payload(ticket)

        messages: list[dict[str, Any]] = []
        for message_id in transcript.get("order", []):
            item = deepcopy(transcript["messages"][message_id])
            attachments = []
            for attachment in item.get("attachments", []):
                local_name = attachment.get("local_name")
                if local_name:
                    attachment["served_path"] = (
                        f"/transcripts/{ticket['ticket_id']}/media/{local_name}"
                    )
                content_type = str(attachment.get("content_type") or "").lower()
                if content_type.startswith("image/"):
                    attachment["preview_kind"] = "image"
                elif content_type.startswith("video/"):
                    attachment["preview_kind"] = "video"
                else:
                    attachment["preview_kind"] = None
                attachments.append(attachment)
            item["attachments"] = attachments
            messages.append(item)

        return {
            "events": transcript.get("events", []),
            "messages": messages,
            "participants": self._participants_from_payload(transcript),
            "ticket": transcript.get("ticket", {}),
        }

    async def get_search_metadata(self, ticket: dict[str, Any]) -> dict[str, Any]:
        transcript = await self.get_transcript(ticket["ticket_id"])
        if transcript is None:
            transcript = self._default_payload(ticket)
        participants = self._participants_from_payload(transcript)
        owner_id = ticket.get("owner_id")
        owner_display_name = (
            ticket.get("owner_display_name")
            or transcript.get("ticket", {}).get("owner_display_name")
            or ""
        )
        owner_name = (
            ticket.get("owner_name")
            or transcript.get("ticket", {}).get("owner_name")
            or ""
        )
        owner_participant = participants.get(str(owner_id), {})
        owner_labels = [owner_display_name, owner_name]
        owner_labels.extend(owner_participant.get("display_names", []))
        owner_labels.extend(owner_participant.get("names", []))
        owner_labels = [label for label in owner_labels if label]

        participant_labels: list[str] = []
        for user_id, participant in participants.items():
            names = [
                *participant.get("display_names", []),
                *participant.get("names", []),
                user_id,
            ]
            label = " / ".join(dict.fromkeys([name for name in names if name]))
            if label:
                participant_labels.append(label)

        search_parts = [
            ticket.get("ticket_id", ""),
            ticket.get("channel_name", ""),
            str(ticket.get("owner_id", "")),
            *owner_labels,
            *participant_labels,
        ]
        return {
            "owner_label": owner_labels[0] if owner_labels else f"User {owner_id}",
            "participant_labels": participant_labels,
            "search_blob": " ".join(search_parts).lower(),
        }

    def _participants_from_payload(self, transcript: dict[str, Any]) -> dict[str, Any]:
        participants = deepcopy(transcript.get("participants", {}))
        for message in transcript.get("messages", {}).values():
            user_id = str(message.get("author_id"))
            participant = participants.setdefault(
                user_id,
                {
                    "display_names": [],
                    "is_staff": bool(message.get("author_is_staff", False)),
                    "names": [],
                    "user_id": message.get("author_id"),
                },
            )
            display_name = message.get("author_display_name")
            if display_name and display_name not in participant["display_names"]:
                participant["display_names"].append(display_name)
            author_name = message.get("author_name")
            if author_name and author_name not in participant["names"]:
                participant["names"].append(author_name)
            participant["is_staff"] = participant.get("is_staff", False) or bool(message.get("author_is_staff", False))
        return participants
