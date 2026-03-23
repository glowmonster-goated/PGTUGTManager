from __future__ import annotations

import asyncio

import discord
from aiohttp import web

from manager.bot import ManagerBot
from manager.config import load_config
from manager.web import create_web_app


async def start_web_server(app: web.Application, host: str, port: int) -> web.AppRunner:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner


async def main() -> None:
    config = load_config()
    missing = [
        name
        for name, value in {
            "DISCORD_TOKEN": config.discord_token,
            "DISCORD_CLIENT_ID": config.discord_client_id,
            "DISCORD_CLIENT_SECRET": config.discord_client_secret,
            "SUPPORT_INVITE_URL": config.support_invite_url,
        }.items()
        if not value
    ]
    if missing:
        missing_text = ", ".join(missing)
        raise RuntimeError(f"Missing required environment values: {missing_text}")

    bot = ManagerBot(config)
    app = create_web_app(bot)
    runner = await start_web_server(app, config.site_host, config.site_port)
    local_host = "127.0.0.1" if config.site_host == "0.0.0.0" else config.site_host
    print(f"Transcript site: {config.site_base_url.rstrip('/')}")
    print(f"Local bind: http://{local_host}:{config.site_port}")
    try:
        try:
            await bot.start(config.discord_token)
        except discord.errors.PrivilegedIntentsRequired:
            required = ["Message Content Intent"] if config.enable_message_content_intent else []
            if config.enable_members_intent:
                required.append("Server Members Intent")
            names = ", ".join(required) if required else "a privileged intent"
            raise RuntimeError(
                "Discord blocked the bot because the following privileged intents are disabled "
                f"in the Developer Portal: {names}. "
                "Go to the Bot tab for your Discord application, enable those intents, save, "
                "and then run `python main.py` again."
            ) from None
    finally:
        await bot.close()
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as exc:
        print(exc)
        raise SystemExit(1)
