import asyncio
import os

import discord
from discord.ext import commands
from dotenv import load_dotenv

from yfin import (
    run_market_stream,
    stop_market_stream,
    get_market_status,
    search_ticker_yfin,
    add_target_yfin,
    list_targets_yfin,
    delete_target_yfin,
)

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID_RAW = os.getenv("GUILD_ID")
CHANNEL_ID_RAW = os.getenv("CHANNEL_ID")

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN not found in .env")
if not GUILD_ID_RAW:
    raise RuntimeError("GUILD_ID not found in .env")
if not CHANNEL_ID_RAW:
    raise RuntimeError("CHANNEL_ID not found in .env")

try:
    GUILD_ID = int(GUILD_ID_RAW)
except ValueError as e:
    raise RuntimeError(f"Invalid GUILD_ID: {GUILD_ID_RAW!r}") from e

try:
    CHANNEL_ID = int(CHANNEL_ID_RAW)
except ValueError as e:
    raise RuntimeError(f"Invalid CHANNEL_ID: {CHANNEL_ID_RAW!r}") from e


class MyBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)

    async def send_market_alert(self, text: str):
        try:
            channel = self.get_channel(CHANNEL_ID)
            if channel is None:
                channel = await self.fetch_channel(CHANNEL_ID)
            await channel.send(text)
            print(f"[ALERT->DISCORD] sent to channel {CHANNEL_ID}: {text}")
        except Exception as e:
            print(f"[ALERT->DISCORD] failed for channel {CHANNEL_ID}: {e!r}")

    async def setup_hook(self):
        from yfin import set_alert_sender

        def sender(text: str):
            self.loop.call_soon_threadsafe(
                lambda: asyncio.create_task(self.send_market_alert(text))
            )

        set_alert_sender(sender)

        run_market_stream()
        print("[BOT] MarketWatcher started")


bot = MyBot()


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (id={bot.user.id})")
    guild = discord.Object(id=GUILD_ID)
    synced = await bot.tree.sync(guild=guild)
    print(f"Synced {len(synced)} command(s).")


@bot.event
async def on_close():
    stop_market_stream()


@bot.tree.command(
    name="ping",
    description="Antwortet mit pong",
    guild=discord.Object(id=GUILD_ID),
)
async def ping(interaction: discord.Interaction):
    if interaction.channel_id != CHANNEL_ID:
        await interaction.response.send_message("Please use the Bot channel", ephemeral=True)
        return
    
    await interaction.response.send_message("pong", ephemeral=True)


@bot.tree.command(
    name="add_target",
    description="Adds a price target for a ticker",
    guild=discord.Object(id=GUILD_ID),
)
async def add_target(
    interaction: discord.Interaction,
    ticker: str,
    price: float,
):
    if interaction.channel_id != CHANNEL_ID:
        await interaction.response.send_message("Please use the Bot channel", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    ticker = ticker.strip().upper()
    result = add_target_yfin(ticker, price)

    if result is not None:
        await interaction.followup.send(
            f"{result}\nStream reload requested.",
            ephemeral=True,
        )
    else:
        await interaction.followup.send(search_ticker_yfin(ticker), ephemeral=True)


@bot.tree.command(
    name="search_ticker",
    description="Searches Yahoo Finance for matching Tickers",
    guild=discord.Object(id=GUILD_ID),
)
async def search_ticker(
    interaction: discord.Interaction,
    search_str: str,
):
    if interaction.channel_id != CHANNEL_ID:
        await interaction.response.send_message("Please use the Bot channel", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    search_str = search_str.strip().upper()
    result = search_ticker_yfin(search_str)
    await interaction.followup.send(result, ephemeral=True)


@bot.tree.command(
    name="list_targets",
    description="List all configured targets",
    guild=discord.Object(id=GUILD_ID),
)
async def list_targets(interaction: discord.Interaction):
    if interaction.channel_id != CHANNEL_ID:
        await interaction.response.send_message("Please use the Bot channel", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    result = list_targets_yfin()
    await interaction.followup.send(result, ephemeral=True)


@bot.tree.command(
    name="delete_target",
    description="Delete one configured target by ticker and index",
    guild=discord.Object(id=GUILD_ID),
)
async def delete_target(
    interaction: discord.Interaction,
    ticker: str,
    idx: int,
):
    if interaction.channel_id != CHANNEL_ID:
        await interaction.response.send_message("Please use the Bot channel", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    ticker = ticker.strip().upper()
    result = delete_target_yfin(ticker, idx)
    await interaction.followup.send(
        f"{result}\nStream reload requested.",
        ephemeral=True,
    )


@bot.tree.command(
    name="status",
    description="Show market watcher status",
    guild=discord.Object(id=GUILD_ID),
)
async def status(interaction: discord.Interaction):
    if interaction.channel_id != CHANNEL_ID:
        await interaction.response.send_message("Please use the Bot channel", ephemeral=True)
        return

    st = get_market_status()
    msg = (
        f"running={st.get('running')}\n"
        f"symbol_count={st.get('symbol_count')}\n"
        f"symbols={', '.join(st.get('symbols', [])) or 'none'}\n"
        f"queue_size={st.get('queue_size')}"
    )
    await interaction.response.send_message(msg, ephemeral=True)


try:
    bot.run(TOKEN)
finally:
    stop_market_stream()
