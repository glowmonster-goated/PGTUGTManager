from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import discord

from .config import Config
from .storage import JsonStore
from .utils import human_duration, iso_now, parse_duration


class PunishmentService:
    MAX_TIMEOUT_SPAN = timedelta(days=27)
    MUTE_REFRESH_THRESHOLD = timedelta(hours=12)
    HACKED_PURGE_HISTORY_LIMIT = 75
    HACKED_PURGE_MAX_CHANNELS = 60

    def __init__(self, bot: discord.Client, config: Config, store: JsonStore) -> None:
        self.bot = bot
        self.config = config
        self.store = store

    async def punish_from_rule(
        self,
        ticket: dict[str, Any] | None,
        actor: discord.Member,
        user_id: int,
        rule: dict[str, Any],
        extra_comments: str | None,
        proof: dict[str, Any],
        source_context: dict[str, Any],
    ) -> dict[str, Any]:
        action = str(rule.get("action", "ban")).lower()
        duration_seconds = rule.get("duration_seconds")
        duration_text = rule.get("duration_text") or human_duration(duration_seconds)
        reason = rule.get("reason") or rule.get("label") or "Rule punishment"
        ban_results: list[dict[str, Any]] = []
        mute_results: list[dict[str, Any]] = []
        mute_target_guild_ids = self._get_mute_target_guild_ids(source_context)
        ticket_id = ticket["ticket_id"] if ticket else None
        punishment_key = ticket_id or f"guild-{source_context.get('guild_id', 'unknown')}"
        punishment = {
            "action": action,
            "ban_results": [],
            "ban_success_count": 0,
            "context_channel_id": source_context.get("channel_id"),
            "context_channel_name": source_context.get("channel_name"),
            "context_guild_id": source_context.get("guild_id"),
            "context_guild_name": source_context.get("guild_name"),
            "created_at": iso_now(),
            "created_by": actor.id,
            "dm_status": "pending",
            "duration_seconds": duration_seconds,
            "duration_text": duration_text,
            "ends_at": self._calculate_end_iso(duration_seconds),
            "extra_comments": extra_comments or "",
            "id": f"punishment-{punishment_key}-{user_id}-{discord.utils.utcnow().timestamp():.6f}",
            "notes": rule.get("notes", ""),
            "proof": proof,
            "reason": reason,
            "rule_id": rule.get("id"),
            "rule_label": rule.get("label"),
            "source": "rule",
            "mute_results": [],
            "mute_target_guild_ids": mute_target_guild_ids,
            "mute_success_count": 0,
            "status": "active" if action == "ban" else "recorded",
            "ticket_id": ticket_id,
            "user_id": user_id,
        }
        if action == "ban":
            ban_results = await self._apply_ban(user_id, f"{rule.get('label')}: {reason}")
            success_count = self._count_successful_results(ban_results, "banned")
            punishment["ban_results"] = ban_results
            punishment["ban_success_count"] = success_count
            punishment["status"] = "active" if success_count > 0 else "failed"
        elif action == "mute":
            mute_results = await self._apply_mutes(
                user_id,
                mute_target_guild_ids,
                f"{rule.get('label')}: {reason}",
                duration_seconds,
            )
            success_count = self._count_successful_results(mute_results, "muted")
            punishment["mute_results"] = mute_results
            punishment["mute_success_count"] = success_count
            punishment["timeout_applied_until"] = self._first_result_value(mute_results, "applied_until")
            punishment["status"] = "active" if success_count > 0 else "failed"
        punishment["dm_status"] = await self._send_rule_dm(
            user_id,
            action,
            reason,
            duration_text,
            extra_comments=extra_comments,
        )
        await self.store.add_punishment(punishment)
        if ticket_id:
            await self.store.update_ticket(ticket_id, self._link_punishment_updater(punishment["id"]))
            await self.store.record_staff_action(actor.id, ticket_id)
        await self.send_punishment_log(
            title=f"Rule {action.title()} Recorded",
            description=(
                f"User ID: `{user_id}`\n"
                f"{self._format_location_line(ticket, source_context)}"
                f"{self._format_transcript_line(ticket)}"
                f"{self._format_proof_line(proof)}"
                f"Rule: {rule.get('label')}\n"
                f"Action: {action.title()}\n"
                f"Reason: {reason}\n"
                f"Duration: {duration_text}\n"
                f"{self._format_ban_results(ban_results)}"
                f"{self._format_ban_results(mute_results, heading='Mute Results:')}"
                f"{self._format_log_notes(rule.get('notes'), extra_comments)}"
                f"By: {actor.mention}"
            ),
            color=discord.Color.red() if action == "ban" else discord.Color.orange(),
        )
        return punishment

    async def manual_ban(
        self,
        ticket: dict[str, Any],
        actor: discord.Member,
        user_id: int,
        raw_duration: str,
        reason: str,
    ) -> dict[str, Any]:
        duration_seconds, duration_text = parse_duration(raw_duration)
        ban_results: list[dict[str, Any]] = []
        punishment = {
            "action": "ban",
            "ban_results": [],
            "ban_success_count": 0,
            "created_at": iso_now(),
            "created_by": actor.id,
            "dm_status": "pending",
            "duration_seconds": duration_seconds,
            "duration_text": duration_text,
            "ends_at": self._calculate_end_iso(duration_seconds),
            "extra_comments": "",
            "id": f"manual-{ticket['ticket_id']}-{user_id}-{discord.utils.utcnow().timestamp():.6f}",
            "notes": "",
            "reason": reason,
            "rule_id": None,
            "rule_label": "Manual Ban",
            "source": "manual",
            "status": "active",
            "ticket_id": ticket["ticket_id"],
            "user_id": user_id,
        }
        ban_results = await self._apply_ban(user_id, f"Manual ban: {reason}")
        success_count = self._count_successful_results(ban_results, "banned")
        punishment["ban_results"] = ban_results
        punishment["ban_success_count"] = success_count
        punishment["status"] = "active" if success_count > 0 else "failed"
        punishment["dm_status"] = await self._send_punishment_dm(
            user_id,
            reason,
            duration_text,
            extra_comments=None,
        )
        await self.store.add_punishment(punishment)
        await self.store.update_ticket(ticket["ticket_id"], self._link_punishment_updater(punishment["id"]))
        await self.store.record_staff_action(actor.id, ticket["ticket_id"])
        await self.send_punishment_log(
            title="Manual Ban",
            description=(
                f"User ID: `{user_id}`\n"
                f"Ticket: `{ticket['channel_name']}`\n"
                f"{self._format_transcript_line(ticket)}"
                f"Reason: {reason}\n"
                f"Duration: {duration_text}\n"
                f"{self._format_ban_results(ban_results)}"
                f"By: {actor.mention}"
            ),
            color=discord.Color.dark_red(),
        )
        return punishment

    async def unban_user(self, actor: discord.Member, user_id: int, reason: str) -> tuple[int, str]:
        unbanned_count = 0
        for guild_id in self.config.target_ban_guild_ids:
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                continue
            try:
                await guild.unban(discord.Object(id=user_id), reason=f"{reason} | By {actor}")
                unbanned_count += 1
            except discord.NotFound:
                continue
            except discord.DiscordException:
                continue

        dm_status = await self._send_unban_dm(user_id, reason)
        punishments = await self.store.list_punishments()
        for punishment in punishments:
            if (
                punishment["user_id"] != user_id
                or punishment["status"] != "active"
                or str(punishment.get("action", "")).lower() != "ban"
            ):
                continue
            await self.store.update_punishment(
                punishment["id"],
                lambda current: {
                    **current,
                    "status": "lifted",
                    "lifted_at": iso_now(),
                    "lifted_by": actor.id,
                    "lift_reason": reason,
                },
            )

        await self.send_punishment_log(
            title="User Unbanned",
            description=(
                f"User ID: `{user_id}`\n"
                f"Reason: {reason}\n"
                f"Guilds Unbanned: {unbanned_count}\n"
                f"DM Status: {dm_status}\n"
                f"By: {actor.mention}"
            ),
            color=discord.Color.green(),
        )
        return unbanned_count, dm_status

    async def list_active_liftable_punishments(self, user_id: int) -> list[dict[str, Any]]:
        punishments = await self.store.list_punishments()
        active = [
            punishment
            for punishment in punishments
            if int(punishment.get("user_id", 0)) == user_id
            and punishment.get("status") == "active"
            and str(punishment.get("action", "")).lower() in {"ban", "mute"}
        ]
        active.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
        return active

    async def lift_selected_punishment(
        self,
        actor: discord.Member,
        punishment_id: str,
        reason: str,
    ) -> dict[str, Any]:
        punishments = await self.store.list_punishments()
        selected = next((item for item in punishments if item.get("id") == punishment_id), None)
        if selected is None:
            raise RuntimeError("That punishment record could not be found.")
        if selected.get("status") != "active":
            raise RuntimeError("That punishment is no longer active.")

        action = str(selected.get("action", "")).lower()
        user_id = int(selected["user_id"])
        if action == "ban":
            results = await self._unban_guilds(
                user_id,
                f"{reason} | By {actor}",
            )
            dm_status = await self._send_unban_dm(user_id, reason)
            related = [
                punishment
                for punishment in punishments
                if int(punishment.get("user_id", 0)) == user_id
                and punishment.get("status") == "active"
                and str(punishment.get("action", "")).lower() == "ban"
            ]
            result_heading = "Unban Results:"
            title = "Ban Lifted"
            success_status = "unbanned"
        elif action == "mute":
            guild_ids = self._coerce_guild_id_list(selected.get("mute_target_guild_ids"))
            if not guild_ids:
                guild_ids = self._get_mute_target_guild_ids(
                    {
                        "guild_id": selected.get("context_guild_id"),
                    }
                )
            results = await self._clear_mutes(
                user_id,
                guild_ids,
                f"{reason} | By {actor}",
            )
            dm_status = await self._send_unmute_dm(user_id, reason)
            related = [
                punishment
                for punishment in punishments
                if int(punishment.get("user_id", 0)) == user_id
                and punishment.get("status") == "active"
                and str(punishment.get("action", "")).lower() == "mute"
            ]
            result_heading = "Unmute Results:"
            title = "Mute Lifted"
            success_status = "cleared"
        else:
            raise RuntimeError("Only active ban and mute punishments can be lifted here.")

        lifted_ids: list[str] = []
        for punishment in related:
            await self.store.update_punishment(
                punishment["id"],
                lambda current: {
                    **current,
                    "status": "lifted",
                    "lifted_at": iso_now(),
                    "lifted_by": actor.id,
                    "lift_reason": reason,
                    "lift_dm_status": dm_status,
                    "lift_results": results,
                },
            )
            lifted_ids.append(punishment["id"])

        success_count = self._count_successful_results(results, success_status)
        await self.send_punishment_log(
            title=title,
            description=(
                f"User ID: `{user_id}`\n"
                f"Selected Record: `{selected.get('rule_label') or selected.get('id') or punishment_id}`\n"
                f"Reason: {reason}\n"
                f"Lifted Records: {len(lifted_ids)}\n"
                f"{self._format_ban_results(results, heading=result_heading)}"
                f"DM Status: {dm_status}\n"
                f"By: {actor.mention}"
            ),
            color=discord.Color.green(),
        )
        return {
            "action": action,
            "dm_status": dm_status,
            "lifted_ids": lifted_ids,
            "reason": reason,
            "results": results,
            "selected_punishment": selected,
            "success_count": success_count,
            "user_id": user_id,
        }

    async def auto_unban_due_users(self) -> list[dict[str, Any]]:
        now_iso = iso_now()
        due: list[dict[str, Any]] = []
        for punishment in await self.store.list_punishments():
            ends_at = punishment.get("ends_at")
            if (
                punishment.get("status") != "active"
                or str(punishment.get("action", "")).lower() != "ban"
                or not ends_at
            ):
                continue
            if ends_at <= now_iso:
                due.append(punishment)

        for punishment in due:
            for guild_id in self.config.target_ban_guild_ids:
                guild = self.bot.get_guild(guild_id)
                if guild is None:
                    continue
                try:
                    await guild.unban(
                        discord.Object(id=punishment["user_id"]),
                        reason=f"Automatic unban for {punishment['id']}",
                    )
                except discord.DiscordException:
                    continue

            dm_status = await self._send_unban_dm(
                punishment["user_id"],
                "Your punishment duration has ended.",
            )
            await self.store.update_punishment(
                punishment["id"],
                lambda current: {
                    **current,
                    "status": "expired",
                    "expired_at": iso_now(),
                    "auto_dm_status": dm_status,
                },
            )
            await self.send_punishment_log(
                title="Automatic Unban",
                description=(
                    f"User ID: `{punishment['user_id']}`\n"
                    f"Reason: {punishment['reason']}\n"
                    f"Original Duration: {punishment['duration_text']}\n"
                    f"DM Status: {dm_status}"
                ),
                color=discord.Color.green(),
            )
        return due

    async def auto_update_mutes(self) -> list[dict[str, Any]]:
        active_updates: list[dict[str, Any]] = []
        now = discord.utils.utcnow()
        for punishment in await self.store.list_punishments():
            if punishment.get("status") != "active" or str(punishment.get("action", "")).lower() != "mute":
                continue

            ends_at = self._parse_iso_datetime(punishment.get("ends_at"))
            guild_ids = self._coerce_guild_id_list(punishment.get("mute_target_guild_ids"))
            if not guild_ids:
                guild_ids = self._get_mute_target_guild_ids(
                    {
                        "guild_id": punishment.get("context_guild_id"),
                    }
                )
            if not guild_ids:
                continue

            if ends_at is not None and ends_at <= now:
                clear_results = await self._clear_mutes(
                    punishment["user_id"],
                    guild_ids,
                    f"Automatic unmute for {punishment['id']}",
                )
                dm_status = await self._send_mute_end_dm(
                    punishment["user_id"],
                    punishment.get("reason") or "Your mute duration has ended.",
                )
                await self.store.update_punishment(
                    punishment["id"],
                    lambda current: {
                        **current,
                        "status": "expired",
                        "expired_at": iso_now(),
                        "auto_dm_status": dm_status,
                        "mute_clear_results": clear_results,
                        "timeout_applied_until": None,
                    },
                )
                await self.send_punishment_log(
                    title="Automatic Unmute",
                    description=(
                        f"User ID: `{punishment['user_id']}`\n"
                        f"Reason: {punishment.get('reason', 'Mute duration completed.')}\n"
                        f"Original Duration: {punishment.get('duration_text', 'Unknown')}\n"
                        f"{self._format_ban_results(clear_results, heading='Unmute Results:')}"
                        f"DM Status: {dm_status}"
                    ),
                    color=discord.Color.green(),
                )
                active_updates.append(punishment)
                continue

            applied_until = self._parse_iso_datetime(punishment.get("timeout_applied_until"))
            if applied_until is not None and applied_until - now > self.MUTE_REFRESH_THRESHOLD:
                continue

            refresh_results = await self._apply_mutes(
                punishment["user_id"],
                guild_ids,
                f"Refreshing mute for {punishment['id']}: {punishment.get('reason', 'Mute punishment')}",
                punishment.get("duration_seconds"),
                ends_at_iso=punishment.get("ends_at"),
            )
            next_timeout = self._first_result_value(refresh_results, "applied_until")
            await self.store.update_punishment(
                punishment["id"],
                lambda current: {
                    **current,
                    "mute_results": refresh_results,
                    "mute_success_count": self._count_successful_results(refresh_results, "muted"),
                    "timeout_applied_until": next_timeout,
                    "last_mute_refresh_at": iso_now(),
                },
            )
            active_updates.append(punishment)
        return active_updates

    async def hacked_kick(
        self,
        ticket: dict[str, Any] | None,
        actor: discord.Member,
        user_id: int,
        source_context: dict[str, Any],
    ) -> dict[str, Any]:
        purge_results, kick_results = await asyncio.gather(
            self._purge_recent_user_messages(user_id, per_guild_limit=30),
            self._apply_kick(
                user_id,
                f"Hacked account safety kick | By {actor}",
            ),
        )
        purged_message_count = sum(int(result.get("deleted_count", 0)) for result in purge_results)
        kick_success_count = self._count_successful_results(kick_results, "kicked")
        dm_status = (
            await self._send_hacked_dm(user_id)
            if kick_success_count > 0
            else "skipped"
        )
        punishment_key = (ticket or {}).get("ticket_id") or f"guild-{source_context.get('guild_id', 'unknown')}"
        punishment = {
            "action": "kick",
            "context_channel_id": source_context.get("channel_id"),
            "context_channel_name": source_context.get("channel_name"),
            "context_guild_id": source_context.get("guild_id"),
            "context_guild_name": source_context.get("guild_name"),
            "created_at": iso_now(),
            "created_by": actor.id,
            "dm_status": dm_status,
            "duration_seconds": None,
            "duration_text": "Immediate",
            "ends_at": None,
            "extra_comments": "",
            "id": f"hacked-{punishment_key}-{user_id}-{discord.utils.utcnow().timestamp():.6f}",
            "kick_results": kick_results,
            "kick_success_count": kick_success_count,
            "notes": "",
            "proof": {},
            "purge_results": purge_results,
            "purged_message_count": purged_message_count,
            "reason": "Account appears to be hacked.",
            "rule_id": None,
            "rule_label": "Hacked Account Kick",
            "source": "hacked",
            "status": "completed" if kick_success_count > 0 else "failed",
            "ticket_id": (ticket or {}).get("ticket_id"),
            "user_id": user_id,
        }
        await self.store.add_punishment(punishment)
        if ticket:
            await self.store.update_ticket(ticket["ticket_id"], self._link_punishment_updater(punishment["id"]))
            await self.store.record_staff_action(actor.id, ticket["ticket_id"])
        await self.send_punishment_log(
            title="Hacked Account Kick",
            description=(
                f"User ID: `{user_id}`\n"
                f"{self._format_location_line(ticket, source_context)}"
                f"{self._format_transcript_line(ticket)}"
                f"Reason: Account appears to be hacked.\n"
                f"{self._format_ban_results(purge_results, heading='Message Purge Results:')}"
                f"{self._format_ban_results(kick_results, heading='Kick Results:')}"
                f"DM Status: {punishment['dm_status']}\n"
                f"By: {actor.mention}"
            ),
            color=discord.Color.orange(),
        )
        return punishment

    async def block_user(self, actor: discord.Member, user_id: int, reason: str) -> None:
        await self.store.set_block(
            user_id,
            {
                "blocked_at": iso_now(),
                "blocked_by": actor.id,
                "reason": reason,
            },
        )
        await self.send_moderation_log(
            title="User Blocked From Support",
            description=(
                f"User ID: `{user_id}`\n"
                f"Reason: {reason}\n"
                f"By: {actor.mention}"
            ),
            color=discord.Color.orange(),
        )

    async def unblock_user(self, actor: discord.Member, user_id: int) -> bool:
        block = await self.store.get_block(user_id)
        if not block:
            return False
        await self.store.clear_block(user_id)
        await self.send_moderation_log(
            title="User Unblocked From Support",
            description=(
                f"User ID: `{user_id}`\n"
                f"Previous Reason: {block['reason']}\n"
                f"By: {actor.mention}"
            ),
            color=discord.Color.green(),
        )
        return True

    async def contact_user(self, actor: discord.Member, user_id: int) -> tuple[bool, str]:
        try:
            user = await self.bot.fetch_user(user_id)
        except discord.DiscordException:
            return False, "I could not fetch that user."
        try:
            await user.send(
                f"You are needed in the {self.config.brand_name} support server. "
                f"Please join here: {self.config.support_invite_url}"
            )
        except discord.DiscordException:
            await self.send_moderation_log(
                title="Contact User Failed",
                description=f"User ID: `{user_id}`\nAttempted by: {actor.mention}",
                color=discord.Color.orange(),
            )
            return False, "DM failed to send."

        await self.send_moderation_log(
            title="Contact User Sent",
            description=f"User ID: `{user_id}`\nSent by: {actor.mention}",
            color=discord.Color.blurple(),
        )
        return True, "DM sent successfully."

    async def send_punishment_log(self, *, title: str, description: str, color: discord.Color) -> None:
        channel = await self._fetch_text_channel(self.config.punishment_log_channel_id)
        if channel is None:
            return
        embed = discord.Embed(title=title, description=description, color=color)
        embed.timestamp = discord.utils.utcnow()
        await channel.send(embed=embed)

    async def send_moderation_log(self, *, title: str, description: str, color: discord.Color) -> None:
        channel = await self._fetch_text_channel(self.config.moderation_log_channel_id)
        if channel is None:
            return
        embed = discord.Embed(title=title, description=description, color=color)
        embed.timestamp = discord.utils.utcnow()
        await channel.send(embed=embed)

    def _link_punishment_updater(self, punishment_id: str):
        def updater(current: dict[str, Any]) -> dict[str, Any]:
            if punishment_id not in current["linked_punishments"]:
                current["linked_punishments"].append(punishment_id)
            return current

        return updater

    async def _apply_ban(self, user_id: int, reason: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for guild_id in self.config.target_ban_guild_ids:
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                results.append(
                    {
                        "guild_id": guild_id,
                        "guild_name": "Unavailable Guild",
                        "status": "missing_guild",
                        "detail": "Bot could not access this guild from cache.",
                    }
                )
                continue
            try:
                await guild.ban(discord.Object(id=user_id), reason=reason)
                results.append(
                    {
                        "guild_id": guild_id,
                        "guild_name": guild.name,
                        "status": "banned",
                        "detail": "Ban applied successfully.",
                    }
                )
            except discord.Forbidden:
                results.append(
                    {
                        "guild_id": guild_id,
                        "guild_name": guild.name,
                        "status": "forbidden",
                        "detail": await self._describe_ban_forbidden(guild, user_id),
                    }
                )
                continue
            except discord.HTTPException as exc:
                results.append(
                    {
                        "guild_id": guild_id,
                        "guild_name": guild.name,
                        "status": "failed",
                        "detail": f"Discord API error {exc.status}: {exc.text or exc}",
                    }
                )
                continue
            except discord.DiscordException as exc:
                results.append(
                    {
                        "guild_id": guild_id,
                        "guild_name": guild.name,
                        "status": "failed",
                        "detail": str(exc),
                    }
                )
                continue
        return results

    async def _unban_guilds(self, user_id: int, reason: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for guild_id in self.config.target_ban_guild_ids:
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                results.append(
                    {
                        "guild_id": guild_id,
                        "guild_name": "Unavailable Guild",
                        "status": "missing_guild",
                        "detail": "Bot could not access this guild from cache.",
                    }
                )
                continue
            try:
                await guild.unban(discord.Object(id=user_id), reason=reason)
                results.append(
                    {
                        "guild_id": guild.id,
                        "guild_name": guild.name,
                        "status": "unbanned",
                        "detail": "Unban applied successfully.",
                    }
                )
            except discord.NotFound:
                results.append(
                    {
                        "guild_id": guild.id,
                        "guild_name": guild.name,
                        "status": "not_banned",
                        "detail": "User was not banned in this guild.",
                    }
                )
            except discord.Forbidden:
                results.append(
                    {
                        "guild_id": guild.id,
                        "guild_name": guild.name,
                        "status": "forbidden",
                        "detail": "Discord denied the unban. Check Ban Members permission and role hierarchy.",
                    }
                )
            except discord.HTTPException as exc:
                results.append(
                    {
                        "guild_id": guild.id,
                        "guild_name": guild.name,
                        "status": "failed",
                        "detail": f"Discord API error {exc.status}: {exc.text or exc}",
                    }
                )
            except discord.DiscordException as exc:
                results.append(
                    {
                        "guild_id": guild.id,
                        "guild_name": guild.name,
                        "status": "failed",
                        "detail": str(exc),
                    }
                )
        return results

    async def _apply_mute(
        self,
        user_id: int,
        guild_id: int,
        reason: str,
        duration_seconds: int | None,
        *,
        ends_at_iso: str | None = None,
    ) -> list[dict[str, Any]]:
        if not guild_id:
            return [
                {
                    "guild_id": 0,
                    "guild_name": "Unknown Guild",
                    "status": "missing_guild",
                    "detail": "No guild context was provided for this mute.",
                }
            ]

        guild = self.bot.get_guild(guild_id)
        if guild is None:
            return [
                {
                    "guild_id": guild_id,
                    "guild_name": "Unavailable Guild",
                    "status": "missing_guild",
                    "detail": "Bot could not access this guild from cache.",
                }
            ]

        member = await self._fetch_member(guild, user_id)
        if member is None:
            return [
                {
                    "guild_id": guild.id,
                    "guild_name": guild.name,
                    "status": "missing_member",
                    "detail": "User is not currently in this guild, so a timeout could not be applied.",
                }
            ]

        timeout_until = self._calculate_timeout_until(duration_seconds, ends_at_iso=ends_at_iso)
        try:
            await member.edit(timed_out_until=timeout_until, reason=reason)
            return [
                {
                    "applied_until": timeout_until.isoformat(),
                    "guild_id": guild.id,
                    "guild_name": guild.name,
                    "status": "muted",
                    "detail": f"Timeout applied until {timeout_until.isoformat()}",
                }
            ]
        except discord.Forbidden:
            return [
                {
                    "guild_id": guild.id,
                    "guild_name": guild.name,
                    "status": "forbidden",
                    "detail": await self._describe_timeout_forbidden(guild, user_id),
                }
            ]
        except discord.HTTPException as exc:
            return [
                {
                    "guild_id": guild.id,
                    "guild_name": guild.name,
                    "status": "failed",
                    "detail": f"Discord API error {exc.status}: {exc.text or exc}",
                }
            ]
        except discord.DiscordException as exc:
            return [
                {
                    "guild_id": guild.id,
                    "guild_name": guild.name,
                    "status": "failed",
                    "detail": str(exc),
                }
            ]

    async def _apply_mutes(
        self,
        user_id: int,
        guild_ids: list[int],
        reason: str,
        duration_seconds: int | None,
        *,
        ends_at_iso: str | None = None,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for guild_id in guild_ids:
            results.extend(
                await self._apply_mute(
                    user_id,
                    guild_id,
                    reason,
                    duration_seconds,
                    ends_at_iso=ends_at_iso,
                )
            )
        return results

    async def _clear_mute(self, user_id: int, guild_id: int, reason: str) -> list[dict[str, Any]]:
        guild = self.bot.get_guild(guild_id)
        if guild is None:
            return [
                {
                    "guild_id": guild_id,
                    "guild_name": "Unavailable Guild",
                    "status": "missing_guild",
                    "detail": "Bot could not access this guild from cache.",
                }
            ]

        member = await self._fetch_member(guild, user_id)
        if member is None:
            return [
                {
                    "guild_id": guild.id,
                    "guild_name": guild.name,
                    "status": "missing_member",
                    "detail": "User is no longer in this guild, so the timeout could not be cleared.",
                }
            ]

        try:
            await member.edit(timed_out_until=None, reason=reason)
            return [
                {
                    "guild_id": guild.id,
                    "guild_name": guild.name,
                    "status": "cleared",
                    "detail": "Timeout removed successfully.",
                }
            ]
        except discord.Forbidden:
            return [
                {
                    "guild_id": guild.id,
                    "guild_name": guild.name,
                    "status": "forbidden",
                    "detail": await self._describe_timeout_forbidden(guild, user_id),
                }
            ]
        except discord.HTTPException as exc:
            return [
                {
                    "guild_id": guild.id,
                    "guild_name": guild.name,
                    "status": "failed",
                    "detail": f"Discord API error {exc.status}: {exc.text or exc}",
                }
            ]
        except discord.DiscordException as exc:
            return [
                {
                    "guild_id": guild.id,
                    "guild_name": guild.name,
                    "status": "failed",
                    "detail": str(exc),
                }
            ]

    async def _clear_mutes(self, user_id: int, guild_ids: list[int], reason: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for guild_id in guild_ids:
            results.extend(await self._clear_mute(user_id, guild_id, reason))
        return results

    async def _kick_user_from_guild(
        self,
        user_id: int,
        guild: discord.Guild,
        reason: str,
    ) -> dict[str, Any]:
        member = await self._fetch_member(guild, user_id)
        if member is None:
            return {
                "guild_id": guild.id,
                "guild_name": guild.name,
                "status": "missing_member",
                "detail": "User is not currently in this guild, so the kick could not be applied.",
            }

        try:
            await guild.kick(member, reason=reason)
            return {
                "guild_id": guild.id,
                "guild_name": guild.name,
                "status": "kicked",
                "detail": "Kick applied successfully.",
            }
        except discord.Forbidden:
            return {
                "guild_id": guild.id,
                "guild_name": guild.name,
                "status": "forbidden",
                "detail": await self._describe_kick_forbidden(guild, user_id),
            }
        except discord.HTTPException as exc:
            return {
                "guild_id": guild.id,
                "guild_name": guild.name,
                "status": "failed",
                "detail": f"Discord API error {exc.status}: {exc.text or exc}",
            }
        except discord.DiscordException as exc:
            return {
                "guild_id": guild.id,
                "guild_name": guild.name,
                "status": "failed",
                "detail": str(exc),
            }

    async def _apply_kick(self, user_id: int, reason: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for guild_id in self.config.target_ban_guild_ids:
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                results.append(
                    {
                        "guild_id": guild_id,
                        "guild_name": "Unavailable Guild",
                        "status": "missing_guild",
                        "detail": "Bot could not access this guild from cache.",
                    }
                )
                continue
            results.append(await self._kick_user_from_guild(user_id, guild, reason))
        return results

    async def _purge_recent_user_messages(self, user_id: int, *, per_guild_limit: int) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for guild_id in self.config.target_ban_guild_ids:
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                results.append(
                    {
                        "deleted_count": 0,
                        "guild_id": guild_id,
                        "guild_name": "Unavailable Guild",
                        "status": "missing_guild",
                        "detail": "Bot could not access this guild from cache.",
                    }
                )
                continue

            bot_member = await self._get_bot_member(guild)
            if bot_member is None:
                results.append(
                    {
                        "deleted_count": 0,
                        "guild_id": guild.id,
                        "guild_name": guild.name,
                        "status": "failed",
                        "detail": "Bot member could not be resolved in this guild.",
                    }
                )
                continue

            matching_messages: list[discord.Message] = []
            seen_channel_ids: set[int] = set()
            channels: list[Any] = [*guild.text_channels, *guild.threads]
            channels.sort(
                key=lambda channel: int(getattr(channel, "last_message_id", 0) or 0),
                reverse=True,
            )
            scanned_channels = 0
            for channel in channels:
                if channel.id in seen_channel_ids:
                    continue
                seen_channel_ids.add(channel.id)
                if scanned_channels >= self.HACKED_PURGE_MAX_CHANNELS:
                    break
                try:
                    permissions = channel.permissions_for(bot_member)
                except AttributeError:
                    continue
                if not permissions.view_channel or not permissions.read_message_history or not permissions.manage_messages:
                    continue
                current_last_message_id = int(getattr(channel, "last_message_id", 0) or 0)
                if len(matching_messages) >= per_guild_limit:
                    oldest_kept_id = min(message.id for message in matching_messages)
                    if current_last_message_id and current_last_message_id < oldest_kept_id:
                        break
                try:
                    async for message in channel.history(limit=self.HACKED_PURGE_HISTORY_LIMIT):
                        if message.author.id == user_id:
                            matching_messages.append(message)
                            matching_messages.sort(key=lambda item: item.id, reverse=True)
                            if len(matching_messages) > per_guild_limit:
                                matching_messages = matching_messages[:per_guild_limit]
                except discord.DiscordException:
                    continue
                scanned_channels += 1

            if not matching_messages:
                results.append(
                    {
                        "deleted_count": 0,
                        "guild_id": guild.id,
                        "guild_name": guild.name,
                        "status": "no_messages",
                        "detail": (
                            "No recent messages from that user were found to delete "
                            f"after scanning {scanned_channels} recent channels."
                        ),
                    }
                )
                continue

            deleted_count = 0
            failed_count = 0
            for message in matching_messages:
                try:
                    await message.delete()
                    deleted_count += 1
                except discord.DiscordException:
                    failed_count += 1

            status = "purged" if failed_count == 0 else ("partial" if deleted_count > 0 else "failed")
            detail = f"Deleted {deleted_count} recent messages after scanning {scanned_channels} recent channels."
            if failed_count:
                detail = f"{detail} Failed to delete {failed_count}."
            results.append(
                {
                    "deleted_count": deleted_count,
                    "guild_id": guild.id,
                    "guild_name": guild.name,
                    "status": status,
                    "detail": detail,
                }
            )
        return results

    async def _send_punishment_dm(
        self,
        user_id: int,
        reason: str,
        duration_text: str,
        *,
        extra_comments: str | None,
    ) -> str:
        try:
            user = await self.bot.fetch_user(user_id)
            embed = discord.Embed(
                title=f"{self.config.brand_name} Ban Notice",
                description="You have been banned from the configured UGT and PGT servers.",
                color=discord.Color.red(),
            )
            embed.add_field(name="Reason", value=reason[:1024], inline=False)
            embed.add_field(name="Duration", value=duration_text[:1024], inline=False)
            if extra_comments and extra_comments.strip():
                embed.add_field(
                    name="Extra Notes",
                    value=extra_comments.strip()[:1024],
                    inline=False,
                )
            if self.config.support_invite_url:
                embed.add_field(
                    name="Support Server",
                    value=f"Join here: {self.config.support_invite_url}"[:1024],
                    inline=False,
                )
            if self.config.appeal_prompt:
                embed.add_field(
                    name="Appeal",
                    value=self.config.appeal_prompt[:1024],
                    inline=False,
                )
            embed.timestamp = discord.utils.utcnow()
            await user.send(embed=embed)
            return "sent"
        except discord.DiscordException:
            return "failed"

    async def _send_rule_dm(
        self,
        user_id: int,
        action: str,
        reason: str,
        duration_text: str,
        *,
        extra_comments: str | None,
    ) -> str:
        if action == "ban":
            return await self._send_punishment_dm(
                user_id,
                reason,
                duration_text,
                extra_comments=extra_comments,
            )
        try:
            user = await self.bot.fetch_user(user_id)
            embed = discord.Embed(
                title=f"{self.config.brand_name} {self._rule_action_title(action)}",
                description=self._rule_action_description(action),
                color=self._rule_action_color(action),
            )
            embed.add_field(name="Reason", value=reason[:1024], inline=False)
            embed.add_field(name="Duration", value=duration_text[:1024], inline=False)
            if extra_comments and extra_comments.strip():
                embed.add_field(
                    name="Extra Notes",
                    value=extra_comments.strip()[:1024],
                    inline=False,
                )
            if self.config.support_invite_url:
                embed.add_field(
                    name="Support Server",
                    value=self.config.support_invite_url[:1024],
                    inline=False,
                )
            embed.timestamp = discord.utils.utcnow()
            await user.send(embed=embed)
            return "sent"
        except discord.DiscordException:
            return "failed"

    async def _send_mute_end_dm(self, user_id: int, reason: str) -> str:
        try:
            user = await self.bot.fetch_user(user_id)
            embed = discord.Embed(
                title=f"{self.config.brand_name} Mute Notice",
                description="Your mute has ended in the server where it was applied.",
                color=discord.Color.green(),
            )
            embed.add_field(name="Reason", value=reason[:1024], inline=False)
            if self.config.support_invite_url:
                embed.add_field(
                    name="Support Server",
                    value=self.config.support_invite_url[:1024],
                    inline=False,
                )
            embed.timestamp = discord.utils.utcnow()
            await user.send(embed=embed)
            return "sent"
        except discord.DiscordException:
            return "failed"

    async def _send_unmute_dm(self, user_id: int, reason: str) -> str:
        try:
            user = await self.bot.fetch_user(user_id)
            embed = discord.Embed(
                title=f"{self.config.brand_name} Mute Lifted",
                description="Your mute has been removed from the configured PGT and UGT servers.",
                color=discord.Color.green(),
            )
            embed.add_field(name="Reason", value=reason[:1024], inline=False)
            if self.config.support_invite_url:
                embed.add_field(
                    name="Support Server",
                    value=self.config.support_invite_url[:1024],
                    inline=False,
                )
            embed.timestamp = discord.utils.utcnow()
            await user.send(embed=embed)
            return "sent"
        except discord.DiscordException:
            return "failed"

    async def _send_hacked_dm(self, user_id: int) -> str:
        try:
            user = await self.bot.fetch_user(user_id)
            embed = discord.Embed(
                title=f"{self.config.brand_name} Safety Kick",
                description="You have been kicked from the configured UGT and PGT servers because your account appears to be hacked.",
                color=discord.Color.orange(),
            )
            embed.add_field(
                name="What To Do",
                value=(
                    "Secure your Discord account, change your password, enable 2FA, "
                    "and rejoin once the account is safe."
                )[:1024],
                inline=False,
            )
            if self.config.support_invite_url:
                embed.add_field(
                    name="Support Server",
                    value=self.config.support_invite_url[:1024],
                    inline=False,
                )
            embed.timestamp = discord.utils.utcnow()
            await user.send(embed=embed)
            return "sent"
        except discord.DiscordException:
            return "failed"

    async def _send_unban_dm(self, user_id: int, reason: str) -> str:
        try:
            user = await self.bot.fetch_user(user_id)
            embed = discord.Embed(
                title=f"{self.config.brand_name} Unban Notice",
                description="You have been unbanned from the configured UGT and PGT servers. You can rejoin with the invites below.",
                color=discord.Color.green(),
            )
            embed.add_field(name="Reason", value=reason[:1024], inline=False)
            if self.config.pgt_invite_url:
                embed.add_field(name="Join PGT", value=self.config.pgt_invite_url[:1024], inline=False)
            if self.config.ugt_invite_url:
                embed.add_field(name="Join UGT", value=self.config.ugt_invite_url[:1024], inline=False)
            embed.timestamp = discord.utils.utcnow()
            await user.send(embed=embed)
            return "sent"
        except discord.DiscordException:
            return "failed"

    def _rule_action_title(self, action: str) -> str:
        return {
            "manual": "Staff Action Notice",
            "mute": "Mute Notice",
            "warn": "Warning Notice",
        }.get(action, "Punishment Notice")

    def _rule_action_description(self, action: str) -> str:
        return {
            "manual": "A staff action has been recorded for your account.",
            "mute": "You have been muted in the configured PGT and UGT servers.",
            "warn": "A warning has been recorded for your account.",
        }.get(action, "A punishment has been recorded for your account.")

    def _rule_action_color(self, action: str) -> discord.Color:
        return {
            "manual": discord.Color.blurple(),
            "mute": discord.Color.orange(),
            "warn": discord.Color.gold(),
        }.get(action, discord.Color.blurple())

    def _format_log_notes(self, rule_notes: Any, extra_comments: str | None) -> str:
        lines: list[str] = []
        if rule_notes and str(rule_notes).strip():
            lines.append(f"Rule Notes: {str(rule_notes).strip()}")
        if extra_comments and extra_comments.strip():
            lines.append(f"Extra Notes: {extra_comments.strip()}")
        if not lines:
            return ""
        return "".join(f"{line}\n" for line in lines)

    def _format_location_line(
        self,
        ticket: dict[str, Any] | None,
        source_context: dict[str, Any],
    ) -> str:
        if ticket:
            return f"Ticket: `{ticket['channel_name']}`\n"
        guild_name = source_context.get("guild_name", "Unknown Guild")
        channel_name = source_context.get("channel_name", "unknown-channel")
        return f"Location: {guild_name} / #{channel_name}\n"

    def _format_transcript_line(self, ticket: dict[str, Any] | None) -> str:
        if not ticket:
            return ""
        transcript_url = str(ticket.get("transcript_url", "")).strip()
        if not transcript_url:
            return ""
        return f"Transcript: {transcript_url}\n"

    def _format_proof_line(self, proof: dict[str, Any]) -> str:
        proof_url = str(proof.get("url", "")).strip()
        if not proof_url:
            return ""
        filename = proof.get("filename", "proof")
        return f"Proof: {filename} - {proof_url}\n"

    def _count_successful_results(self, results: list[dict[str, Any]], *success_statuses: str) -> int:
        statuses = set(success_statuses or ("banned",))
        return sum(1 for result in results if result.get("status") in statuses)

    def _first_result_value(self, results: list[dict[str, Any]], key: str) -> str | None:
        for result in results:
            value = result.get(key)
            if value:
                return str(value)
        return None

    def _format_ban_results(self, ban_results: list[dict[str, Any]], *, heading: str = "Ban Results:") -> str:
        if not ban_results:
            return ""
        lines = [
            heading,
            *[
                f"- {result['guild_name']} ({result['status']}): {result['detail']}"
                for result in ban_results
            ],
        ]
        return "".join(f"{line}\n" for line in lines)

    def _parse_iso_datetime(self, value: Any) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _coerce_guild_id_list(self, payload: Any) -> list[int]:
        guild_ids: list[int] = []
        if not isinstance(payload, list):
            return guild_ids
        for value in payload:
            try:
                guild_id = int(value)
            except (TypeError, ValueError):
                continue
            if guild_id and guild_id not in guild_ids:
                guild_ids.append(guild_id)
        return guild_ids

    def _get_mute_target_guild_ids(self, source_context: dict[str, Any]) -> list[int]:
        guild_ids: list[int] = []
        preferred_values = [
            self.config.pgt_guild_id,
            self.config.ugt_guild_id,
            *self.config.target_ban_guild_ids,
        ]
        for value in preferred_values:
            try:
                guild_id = int(value)
            except (TypeError, ValueError):
                continue
            if guild_id == self.config.support_guild_id:
                continue
            if guild_id and guild_id not in guild_ids:
                guild_ids.append(guild_id)
        if not guild_ids:
            try:
                fallback_guild_id = int(source_context.get("guild_id"))
            except (TypeError, ValueError):
                fallback_guild_id = 0
            if fallback_guild_id and fallback_guild_id != self.config.support_guild_id:
                guild_ids.append(fallback_guild_id)
        return guild_ids

    def _calculate_timeout_until(
        self,
        duration_seconds: int | None,
        *,
        ends_at_iso: str | None = None,
    ) -> datetime:
        now = discord.utils.utcnow()
        absolute_end = self._parse_iso_datetime(ends_at_iso)
        if absolute_end is None and duration_seconds is not None:
            absolute_end = now + timedelta(seconds=int(duration_seconds))
        if absolute_end is None:
            absolute_end = now + self.MAX_TIMEOUT_SPAN
        return min(absolute_end, now + self.MAX_TIMEOUT_SPAN)

    async def _fetch_member(self, guild: discord.Guild, user_id: int) -> discord.Member | None:
        member = guild.get_member(user_id)
        if member is not None:
            return member
        try:
            return await guild.fetch_member(user_id)
        except discord.DiscordException:
            return None

    async def _describe_ban_forbidden(self, guild: discord.Guild, user_id: int) -> str:
        bot_member = await self._get_bot_member(guild)
        target_member = await self._fetch_member(guild, user_id)
        if bot_member is not None and target_member is not None:
            if bot_member.top_role.position <= target_member.top_role.position:
                return (
                    "Role hierarchy issue. "
                    f"Bot top role `{bot_member.top_role.name}` ({bot_member.top_role.position}) "
                    f"is not above target top role `{target_member.top_role.name}` ({target_member.top_role.position})."
                )
        return "Discord denied the ban. Check Ban Members permission and role hierarchy."

    async def _describe_timeout_forbidden(self, guild: discord.Guild, user_id: int) -> str:
        bot_member = await self._get_bot_member(guild)
        target_member = await self._fetch_member(guild, user_id)
        if bot_member is not None and target_member is not None:
            if bot_member.top_role.position <= target_member.top_role.position:
                return (
                    "Role hierarchy issue. "
                    f"Bot top role `{bot_member.top_role.name}` ({bot_member.top_role.position}) "
                    f"is not above target top role `{target_member.top_role.name}` ({target_member.top_role.position})."
                )
        return "Discord denied the timeout. Check Moderate Members permission and role hierarchy."

    async def _describe_kick_forbidden(self, guild: discord.Guild, user_id: int) -> str:
        bot_member = await self._get_bot_member(guild)
        target_member = await self._fetch_member(guild, user_id)
        if bot_member is not None and target_member is not None:
            if bot_member.top_role.position <= target_member.top_role.position:
                return (
                    "Role hierarchy issue. "
                    f"Bot top role `{bot_member.top_role.name}` ({bot_member.top_role.position}) "
                    f"is not above target top role `{target_member.top_role.name}` ({target_member.top_role.position})."
                )
        return "Discord denied the kick. Check Kick Members permission and role hierarchy."

    async def _get_bot_member(self, guild: discord.Guild) -> discord.Member | None:
        bot_member = guild.me
        if bot_member is not None:
            return bot_member
        if self.bot.user is None:
            return None
        try:
            return await guild.fetch_member(self.bot.user.id)
        except discord.DiscordException:
            return None

    def _calculate_end_iso(self, duration_seconds: int | None) -> str | None:
        if duration_seconds is None:
            return None
        return (discord.utils.utcnow() + timedelta(seconds=duration_seconds)).isoformat()

    async def _fetch_text_channel(self, channel_id: int) -> discord.TextChannel | None:
        channel = self.bot.get_channel(channel_id)
        if isinstance(channel, discord.TextChannel):
            return channel
        try:
            fetched = await self.bot.fetch_channel(channel_id)
        except discord.DiscordException:
            return None
        return fetched if isinstance(fetched, discord.TextChannel) else None
