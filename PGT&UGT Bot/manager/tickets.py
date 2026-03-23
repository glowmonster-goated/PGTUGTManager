from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import discord

from .config import Config
from .constants import TicketSection, TicketState
from .permissions import (
    build_channel_overwrites,
    get_section_category_id,
    get_staff_level,
    is_staff_level,
    normalize_transcript_access,
)
from .storage import JsonStore
from .transcripts import TranscriptStore
from .utils import iso_now, slugify


@dataclass(slots=True)
class TicketCreateResult:
    ok: bool
    message: str
    ticket: dict[str, Any] | None = None
    channel: discord.TextChannel | None = None


class TicketService:
    def __init__(
        self,
        bot: discord.Client,
        config: Config,
        store: JsonStore,
        transcripts: TranscriptStore,
    ) -> None:
        self.bot = bot
        self.config = config
        self.store = store
        self.transcripts = transcripts

    def build_panel_embed(self) -> discord.Embed:
        terms_mention = f"<#{self.config.terms_channel_id}>"
        embed = discord.Embed(
            title=self.config.panel_title,
            description=(
                f"{self.config.panel_description}\n\n"
                f"Before opening a ticket, read {terms_mention}."
            ),
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Appeal",
            value="Use this for punishments, bans, or appeal reviews.",
            inline=False,
        )
        embed.add_field(
            name="UGT Support",
            value="Use this for UGT support requests and reports.",
            inline=False,
        )
        embed.add_field(
            name="PGT Support",
            value="Use this for PGT support requests and reports.",
            inline=False,
        )
        embed.add_field(
            name="Management",
            value=(
                "Use this for sponsorship deals, staff reports, leadership issues, "
                "or other serious management-only topics."
            ),
            inline=False,
        )
        return embed

    def build_management_warning_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Management Ticket Warning",
            description=self.config.management_warning,
            color=discord.Color.orange(),
        )
        embed.add_field(
            name="Do not use this for",
            value="General support, routine questions, or requesting member removals.",
            inline=False,
        )
        embed.add_field(
            name="Use this for",
            value="Sponsorship deals, reporting a staff member, or leadership issues.",
            inline=False,
        )
        return embed

    def build_ticket_embed(self, ticket: dict[str, Any], opener: discord.abc.User) -> discord.Embed:
        terms_mention = f"<#{self.config.terms_channel_id}>"
        embed = discord.Embed(
            title=f"{TicketSection(ticket['section']).label} Ticket",
            description=(
                f"Read {terms_mention} before continuing.\n\n"
                f"Opened by {opener.mention}\n"
                f"Ticket ID: `{ticket['ticket_id']}`\n"
                f"Channel: `{ticket['channel_name']}`"
            ),
            color=discord.Color.green(),
        )
        embed.add_field(
            name="Need to finish?",
            value="Press the close button below when the issue is resolved.",
            inline=False,
        )
        return embed

    def build_close_request_message(self, owner_id: int) -> str:
        return (
            f"<@{owner_id}> is there anything else you need here? "
            "If everything is sorted, use the close button below."
        )

    def build_evidence_message(self, owner_id: int) -> str:
        return (
            f"<@{owner_id}>, We require all evidence to be submitted in a specific format to reduce the "
            "possibility of fabricated proof. Please re-send your evidence using the format below:\n\n"
            "**Please send a video of you doing the following on your device:**\n"
            "- Close out of the app, reopen it, and go straight to the evidence\n"
            "- Click or tap on the profile of the player you are reporting\n"
            "- Copy their User ID and paste it into a channel while still recording\n"
            "- Submit or send the video in this ticket"
        )

    def build_delete_prompt(self, ticket: dict[str, Any]) -> str:
        return (
            f"Ticket `{ticket['channel_name']}` is closed. "
        )

    def build_transcript_url(self, ticket_id: str) -> str:
        return f"{self.config.site_base_url.rstrip('/')}/transcripts/{ticket_id}"

    def build_channel_name(
        self,
        section: TicketSection,
        display_number: int,
        display_name: str | None,
        *,
        closed: bool,
    ) -> str:
        base = f"{section.prefix}-{display_number}"
        if display_name:
            base = f"{base}-{display_name}"
        return f"closed-{base}" if closed else base

    async def ensure_panel(self, panel_view: discord.ui.View) -> discord.Message:
        channel = await self._fetch_text_channel(self.config.panel_channel_id)
        if channel is None:
            raise RuntimeError("Panel channel was not found.")
        panel_message_id = await self.store.get_panel_message_id()
        embed = self.build_panel_embed()
        if panel_message_id:
            try:
                message = await channel.fetch_message(panel_message_id)
                await message.edit(embed=embed, view=panel_view)
                return message
            except discord.NotFound:
                pass
        message = await channel.send(embed=embed, view=panel_view)
        await self.store.set_panel_message_id(message.id)
        return message

    async def create_ticket(
        self,
        member: discord.Member,
        section: TicketSection,
        *,
        created_by: discord.Member | None = None,
    ) -> TicketCreateResult:
        block = await self.store.get_block(member.id)
        creator = created_by or member
        owner_level = get_staff_level(member, self.config)
        owner_is_staff = is_staff_level(owner_level)

        if block and section in {TicketSection.PGT, TicketSection.UGT, TicketSection.MANAGEMENT}:
            return TicketCreateResult(
                ok=False,
                message=(
                    "You are blocked from opening support or management tickets.\n"
                    f"Reason: {block['reason']}"
                ),
            )

        if not owner_is_staff:
            existing = await self.store.find_open_ticket_for_owner(member.id, section)
            if existing:
                return TicketCreateResult(
                    ok=False,
                    message=(
                        f"You already have an open {section.label} ticket: "
                        f"<#{existing['channel_id']}>"
                    ),
                )

        guild = member.guild
        category = guild.get_channel(get_section_category_id(section, self.config))
        if not isinstance(category, discord.CategoryChannel):
            return TicketCreateResult(ok=False, message="That ticket category is missing.")

        ticket_id, display_number = await self.store.next_ticket_identity(section)
        channel_name = self.build_channel_name(section, display_number, None, closed=False)
        overwrites = build_channel_overwrites(
            guild,
            self.config,
            section,
            TicketState.OPEN,
            member,
            owner_is_staff,
        )
        channel = await guild.create_text_channel(
            name=channel_name,
            category=category,
            overwrites=overwrites,
            topic=f"{ticket_id} | {section.label} | owner:{member.id}",
            reason=f"New {section.label} ticket for {member}",
        )
        ticket = {
            "added_user_ids": [],
            "channel_id": channel.id,
            "channel_name": channel_name,
            "close_notice_message_id": None,
            "close_requested_message_id": None,
            "closed_at": None,
            "closed_by": None,
            "created_at": iso_now(),
            "created_by_id": creator.id,
            "deleted_at": None,
            "deleted_by": None,
            "display_name": None,
            "display_number": display_number,
            "linked_punishments": [],
            "move_history": [],
            "owner_transcript_cutoff_at": None,
            "owner_id": member.id,
            "owner_display_name": member.display_name,
            "owner_is_staff": owner_is_staff,
            "owner_name": str(member),
            "section": section.value,
            "state": TicketState.OPEN.value,
            "ticket_id": ticket_id,
            "transcript_access": normalize_transcript_access(section, None),
            "transcript_url": self.build_transcript_url(ticket_id),
        }
        await self.store.create_ticket(ticket)
        await self.transcripts.ensure_ticket(ticket)
        created_message = (
            f"{creator} opened {section.label} for {member}."
            if creator.id != member.id
            else f"{member} opened {section.label}."
        )
        await self.transcripts.add_system_event(
            ticket,
            "ticket_created",
            created_message,
            actor_id=creator.id,
        )
        return TicketCreateResult(ok=True, message="Ticket created.", ticket=ticket, channel=channel)

    async def rename_ticket(
        self,
        ticket: dict[str, Any],
        channel: discord.TextChannel,
        new_name: str,
        actor: discord.Member,
    ) -> dict[str, Any]:
        slug = slugify(new_name)[:40] or None

        def updater(current: dict[str, Any]) -> dict[str, Any]:
            current["display_name"] = slug
            current["channel_name"] = self.build_channel_name(
                TicketSection(current["section"]),
                current["display_number"],
                slug,
                closed=current["state"] == TicketState.CLOSED.value,
            )
            return current

        updated = await self.store.update_ticket(ticket["ticket_id"], updater)
        await channel.edit(name=updated["channel_name"], reason=f"Renamed by {actor}")
        await self.transcripts.ensure_ticket(updated)
        await self.transcripts.add_system_event(
            updated,
            "ticket_renamed",
            f"{actor} renamed this ticket to {updated['channel_name']}.",
            actor_id=actor.id,
        )
        await self.store.record_staff_action(actor.id, ticket["ticket_id"])
        await self.send_ticket_log(
            title="Ticket Renamed",
            description=(
                f"Channel: <#{channel.id}>\n"
                f"New name: `{updated['channel_name']}`\n"
                f"By: {actor.mention}"
            ),
            color=discord.Color.gold(),
        )
        return updated

    async def move_ticket(
        self,
        ticket: dict[str, Any],
        channel: discord.TextChannel,
        new_section: TicketSection,
        actor: discord.Member,
    ) -> dict[str, Any]:
        owner = await self._resolve_member(channel.guild, ticket["owner_id"])
        state = TicketState(ticket["state"])

        def updater(current: dict[str, Any]) -> dict[str, Any]:
            old_section = TicketSection(current["section"])
            current["move_history"].append(
                {
                    "from": old_section.value,
                    "to": new_section.value,
                    "moved_at": iso_now(),
                    "moved_by": actor.id,
                }
            )
            current["section"] = new_section.value
            current["channel_name"] = self.build_channel_name(
                new_section,
                current["display_number"],
                current["display_name"],
                closed=state == TicketState.CLOSED,
            )
            return current

        updated = await self.store.update_ticket(ticket["ticket_id"], updater)
        category = channel.guild.get_channel(get_section_category_id(new_section, self.config))
        if not isinstance(category, discord.CategoryChannel):
            raise RuntimeError("Destination category could not be found.")
        overwrites = await self._build_ticket_overwrites(channel.guild, updated, new_section, state, owner=owner)
        await channel.edit(
            name=updated["channel_name"],
            category=category,
            overwrites=overwrites,
            reason=f"Moved by {actor}",
        )
        await self.transcripts.ensure_ticket(updated)
        await self.transcripts.add_system_event(
            updated,
            "ticket_moved",
            (
                f"{actor} moved this ticket from "
                f"{TicketSection(ticket['section']).label} to {new_section.label}."
            ),
            actor_id=actor.id,
        )
        await self.store.record_staff_action(actor.id, ticket["ticket_id"])
        await self.send_ticket_log(
            title="Ticket Moved",
            description=(
                f"Ticket: <#{channel.id}>\n"
                f"From: {TicketSection(ticket['section']).label}\n"
                f"To: {new_section.label}\n"
                f"By: {actor.mention}"
            ),
            color=discord.Color.orange(),
        )
        return updated

    async def close_ticket(
        self,
        ticket: dict[str, Any],
        channel: discord.TextChannel,
        actor: discord.abc.User,
    ) -> dict[str, Any]:
        owner = await self._resolve_member(channel.guild, ticket["owner_id"])
        actor_id = actor.id

        def updater(current: dict[str, Any]) -> dict[str, Any]:
            current["state"] = TicketState.CLOSED.value
            current["closed_at"] = iso_now()
            current["closed_by"] = actor_id
            current["channel_name"] = self.build_channel_name(
                TicketSection(current["section"]),
                current["display_number"],
                current["display_name"],
                closed=True,
            )
            return current

        updated = await self.store.update_ticket(ticket["ticket_id"], updater)
        overwrites = await self._build_ticket_overwrites(
            channel.guild,
            updated,
            TicketSection(updated["section"]),
            TicketState.CLOSED,
            owner=owner,
        )
        await channel.edit(name=updated["channel_name"], overwrites=overwrites, reason=f"Closed by {actor}")
        await self.transcripts.ensure_ticket(updated)
        await self.transcripts.add_system_event(
            updated,
            "ticket_closed",
            f"{actor} closed this ticket.",
            actor_id=actor.id,
        )
        await self.send_ticket_log(
            title="Ticket Closed",
            description=(
                f"Ticket: <#{channel.id}>\n"
                f"Closed by: <@{actor.id}>\n"
                f"Owner: <@{updated['owner_id']}>"
            ),
            color=discord.Color.red(),
        )
        summary = await self.transcripts.get_summary(updated["ticket_id"])
        await self.send_transcript_log(updated, summary)
        try:
            user = owner or await self.bot.fetch_user(updated["owner_id"])
            await user.send(embed=self.build_transcript_dm_embed(updated, summary))
        except discord.DiscordException:
            pass
        if isinstance(actor, discord.Member):
            await self.store.record_staff_close(actor.id, ticket["ticket_id"])
        return updated

    async def delete_ticket(
        self,
        ticket: dict[str, Any],
        channel: discord.TextChannel,
        actor: discord.Member,
    ) -> None:
        def updater(current: dict[str, Any]) -> dict[str, Any]:
            current["state"] = TicketState.DELETED.value
            current["deleted_at"] = iso_now()
            current["deleted_by"] = actor.id
            return current

        updated = await self.store.update_ticket(ticket["ticket_id"], updater)
        await self.transcripts.ensure_ticket(updated)
        await self.transcripts.add_system_event(
            updated,
            "ticket_deleted",
            f"{actor} deleted this ticket channel.",
            actor_id=actor.id,
        )
        await self.send_ticket_log(
            title="Ticket Deleted",
            description=(
                f"Ticket: `{ticket['channel_name']}`\n"
                f"Deleted by: {actor.mention}\n"
                f"Transcript: {updated['transcript_url']}"
            ),
            color=discord.Color.dark_red(),
        )
        await self.store.record_staff_action(actor.id, ticket["ticket_id"])
        await channel.delete(reason=f"Deleted by {actor}")

    async def add_user_to_ticket(
        self,
        ticket: dict[str, Any],
        channel: discord.TextChannel,
        target: discord.Member,
        actor: discord.Member,
        *,
        log_action: bool = True,
    ) -> tuple[dict[str, Any], bool]:
        if target.id == ticket["owner_id"]:
            return ticket, False

        def updater(current: dict[str, Any]) -> dict[str, Any]:
            added_user_ids = list(current.get("added_user_ids", []))
            if target.id not in added_user_ids:
                added_user_ids.append(target.id)
            current["added_user_ids"] = added_user_ids
            return current

        updated = await self.store.update_ticket(ticket["ticket_id"], updater)
        overwrites = await self._build_ticket_overwrites(
            channel.guild,
            updated,
            TicketSection(updated["section"]),
            TicketState(updated["state"]),
        )
        await channel.edit(overwrites=overwrites, reason=f"Added {target} to ticket by {actor}")
        if log_action:
            await self.transcripts.ensure_ticket(updated)
            await self.transcripts.add_system_event(
                updated,
                "ticket_user_added",
                f"{actor} added {target} to this ticket.",
                actor_id=actor.id,
            )
            await self.send_ticket_log(
                title="User Added To Ticket",
                description=(
                    f"Ticket: <#{channel.id}>\n"
                    f"Added User: {target.mention} (`{target.id}`)\n"
                    f"By: {actor.mention}"
                ),
                color=discord.Color.blue(),
            )
            await self.store.record_staff_action(actor.id, ticket["ticket_id"])
        already_present = target.id in ticket.get("added_user_ids", [])
        return updated, not already_present

    async def ensure_management_contact_ticket(
        self,
        actor: discord.Member,
        target: discord.Member,
    ) -> TicketCreateResult:
        existing = await self.store.find_open_ticket_for_owner(target.id, TicketSection.MANAGEMENT)
        if existing:
            channel = await self._fetch_text_channel(existing["channel_id"])
            if channel is None:
                return TicketCreateResult(
                    ok=False,
                    message="The existing management ticket could not be found.",
                )
            updated, added = await self.add_user_to_ticket(existing, channel, actor, actor, log_action=False)
            await self.transcripts.add_system_event(
                updated,
                "contact_ticket_reused",
                f"{actor} reused this management ticket to contact {target}.",
                actor_id=actor.id,
            )
            await self.send_ticket_log(
                title="Management Contact Ticket Reused",
                description=(
                    f"Ticket: <#{channel.id}>\n"
                    f"Target User: {target.mention} (`{target.id}`)\n"
                    f"Requested by: {actor.mention}\n"
                    f"Staff Added: {'Yes' if added else 'Already had access'}"
                ),
                color=discord.Color.orange(),
            )
            return TicketCreateResult(
                ok=True,
                message="Existing management ticket found.",
                ticket=updated,
                channel=channel,
            )

        result = await self.create_ticket(target, TicketSection.MANAGEMENT, created_by=actor)
        if not result.ok or not result.ticket or not result.channel:
            return result
        updated, _ = await self.add_user_to_ticket(result.ticket, result.channel, actor, actor, log_action=False)
        await self.transcripts.add_system_event(
            updated,
            "contact_ticket_opened",
            f"{actor} opened this management ticket to contact {target}.",
            actor_id=actor.id,
        )
        await self.send_ticket_log(
            title="Management Contact Ticket Opened",
            description=(
                f"Ticket: <#{result.channel.id}>\n"
                f"Target User: {target.mention} (`{target.id}`)\n"
                f"Opened by: {actor.mention}"
            ),
            color=discord.Color.orange(),
        )
        return TicketCreateResult(
            ok=True,
            message="Management contact ticket opened.",
            ticket=updated,
            channel=result.channel,
        )

    async def send_ticket_log(
        self,
        *,
        title: str,
        description: str,
        color: discord.Color,
    ) -> None:
        channel = await self._fetch_text_channel(self.config.ticket_log_channel_id)
        if channel is None:
            return
        embed = discord.Embed(title=title, description=description, color=color)
        embed.timestamp = discord.utils.utcnow()
        await channel.send(embed=embed)

    async def send_transcript_log(self, ticket: dict[str, Any], summary: dict[str, int]) -> None:
        channel = await self._fetch_text_channel(self.config.transcript_log_channel_id)
        if channel is None:
            return
        embed = discord.Embed(
            title=f"Transcript Ready: {ticket['channel_name']}",
            description=(
                f"Ticket Owner: <@{ticket['owner_id']}>\n"
                f"Ticket Owner ID: `{ticket['owner_id']}`\n"
                f"Ticket Type: {TicketSection(ticket['section']).label}\n"
                f"Messages: {summary['messages']}\n"
                f"Attachments: {summary['attachments']}\n"
                f"Direct Link: {ticket['transcript_url']}"
            ),
            color=discord.Color.dark_teal(),
        )
        await channel.send(embed=embed)

    def build_transcript_dm_embed(self, ticket: dict[str, Any], summary: dict[str, int]) -> discord.Embed:
        embed = discord.Embed(
            title=f"{self.config.brand_name} Transcript",
            description=(
                f"Your ticket `{ticket['channel_name']}` has been closed.\n\n"
                f"Messages saved: {summary['messages']}\n"
                f"Attachments saved: {summary['attachments']}"
            ),
            color=discord.Color.dark_teal(),
        )
        embed.add_field(name="View Transcript", value=ticket["transcript_url"], inline=False)
        embed.add_field(
            name="Access",
            value="You can view your own transcripts after logging in with Discord.",
            inline=False,
        )
        return embed

    async def _fetch_text_channel(self, channel_id: int) -> discord.TextChannel | None:
        channel = self.bot.get_channel(channel_id)
        if isinstance(channel, discord.TextChannel):
            return channel
        try:
            fetched = await self.bot.fetch_channel(channel_id)
        except discord.DiscordException:
            return None
        return fetched if isinstance(fetched, discord.TextChannel) else None

    async def _resolve_member(self, guild: discord.Guild, user_id: int) -> discord.Member | None:
        member = guild.get_member(user_id)
        if member is not None:
            return member
        try:
            return await guild.fetch_member(user_id)
        except discord.DiscordException:
            return None

    async def _resolve_additional_members(
        self,
        guild: discord.Guild,
        ticket: dict[str, Any],
    ) -> list[discord.Member]:
        members: list[discord.Member] = []
        for user_id in ticket.get("added_user_ids", []):
            member = await self._resolve_member(guild, int(user_id))
            if member is not None:
                members.append(member)
        return members

    async def _build_ticket_overwrites(
        self,
        guild: discord.Guild,
        ticket: dict[str, Any],
        section: TicketSection,
        state: TicketState,
        *,
        owner: discord.Member | None = None,
    ) -> dict[discord.abc.Snowflake, discord.PermissionOverwrite]:
        resolved_owner = owner if owner is not None else await self._resolve_member(guild, ticket["owner_id"])
        extra_members = await self._resolve_additional_members(guild, ticket)
        return build_channel_overwrites(
            guild,
            self.config,
            section,
            state,
            resolved_owner,
            ticket["owner_is_staff"],
            extra_members=extra_members,
        )
