# UGT & PGT Manager

Custom Discord bot and transcript website for the combined UGT and PGT staff workflow.

## What it does

- Posts one shared ticket panel with `Appeal`, `UGT Support`, `PGT Support`, and `Management`
- Creates numbered ticket channels like `pgt-101` or `closed-appeal-20`
- Uses your role hierarchy for command access and ticket visibility
- Lets staff move, rename, request close, close, and delete tickets
- Stores transcripts, attachments, system events, and linked punishments in JSON
- Supports rule-based punishments, manual bans, timed auto-unbans, user contact, and support blocking
- Runs a small transcript website with Discord login

## Project layout

- `main.py`: starts the bot and transcript website
- `manager/`: bot code, ticket logic, web app, templates, CSS
- `data/`: JSON storage and transcript media
- `.env.example`: required configuration variables

## Before you run it

1. Copy `.env.example` to `.env`
2. Fill in every required ID, token, URL, and role
3. Put your real rulebook into `data/rules.json`
4. Install requirements with `pip install -r requirements.txt`
5. Start the app with `python main.py`

## Discord application setup

In the Discord Developer Portal:

- Enable `Message Content Intent`
- Enable `Server Members Intent` only if you set `ENABLE_MEMBERS_INTENT=true`
- Add the OAuth redirect URL:
  `YOUR_SITE_BASE_URL/auth/callback`

## Required env values

- `DISCORD_TOKEN`: bot token
- `DISCORD_CLIENT_ID`: application client ID
- `DISCORD_CLIENT_SECRET`: application client secret
- `SITE_BASE_URL`: public URL for the transcript website
- `SITE_HOST`: host for the local web server
- `SITE_PORT`: port for the local web server
- `ENABLE_MEMBERS_INTENT`: optional, defaults to `false`
- `ENABLE_MESSAGE_CONTENT_INTENT`: optional, defaults to `true`
- `SUPPORT_GUILD_ID`: support server where tickets live
- `PGT_GUILD_ID`: PGT server ID
- `UGT_GUILD_ID`: UGT server ID
- `TARGET_BAN_GUILD_IDS`: optional comma-separated list of guild IDs to punish in
- `PANEL_CHANNEL_ID`: channel where the ticket panel should be posted
- `TERMS_CHANNEL_ID`: terms-of-service channel
- `SUPPORT_INVITE_URL`: invite used by `/contact` and ban DMs
- `PGT_INVITE_URL`: invite sent in unban DMs for the PGT server
- `UGT_INVITE_URL`: invite sent in unban DMs for the UGT server
- `APPEAL_PROMPT`: appeal text shown in ban DMs
- `TRIAL_MOD_ROLE_ID`
- `MOD_ROLE_ID`
- `SUPERVISOR_ROLE_ID`
- `LEAGUE_MANAGER_ROLE_ID`
- `PGT_CATEGORY_ID`
- `UGT_CATEGORY_ID`
- `APPEAL_CATEGORY_ID`
- `MANAGEMENT_CATEGORY_ID`
- `TICKET_LOG_CHANNEL_ID`
- `TRANSCRIPT_LOG_CHANNEL_ID`
- `MODERATION_LOG_CHANNEL_ID`
- `PUNISHMENT_LOG_CHANNEL_ID`

## Counter seeds

If you already have existing numbering, set these before first launch:

- `PGT_COUNTER_START`
- `UGT_COUNTER_START`
- `APPEAL_COUNTER_START`
- `MANAGEMENT_COUNTER_START`

The bot will continue from those values.

## Rulebook file

`/punish` reads from `data/rules.json`.

Example format:

```json
{
  "rules": [
    {
      "id": "rule-1",
      "label": "Rule 1 - Harassment",
      "action": "ban",
      "reason": "Harassment and abusive conduct",
      "duration_text": "7 days",
      "duration_seconds": 604800
    },
    {
      "id": "rule-2",
      "label": "Rule 2 - Severe Abuse",
      "action": "ban",
      "reason": "Severe abuse",
      "duration_text": "Permanent",
      "duration_seconds": null
    }
  ]
}
```

## Commands

- `/moveticket`
- `/punish`
- `/manual-ban`
- `/unban`
- `/contact`
- `/rename`
- `/close-request`
- `/about`
- `/block`
- `/unblock`
- `/stats`
- `/refreshpanel`

## Notes

- The panel is created or refreshed automatically when the bot starts
- If startup says privileged intents are required, enable `Message Content Intent` in the Discord Developer Portal Bot tab
- Normal users can only have one open ticket per section
- Blocked users can still open appeals
- Closing a ticket removes a normal user from the channel, renames it, and opens transcript access
- Trial Mods and above can delete closed tickets
- Transcript media is stored locally in `data/transcripts/<ticket_id>/media`
