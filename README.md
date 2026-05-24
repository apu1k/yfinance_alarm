# yfinance_alarm

Simple Discord bot that watches Yahoo Finance live prices and posts alerts when targets are hit.

## Features

- Add/delete/list price targets from Discord slash commands
- Live monitoring via `yfinance`
- One-shot alerts posted into your configured Discord channel
- Targets stored in `data.json`

## Commands

- `/add_target <ticker> <price>`
- `/delete_target <ticker> <idx>`
- `/list_targets`
- `/search_ticker <search_str>`
- `/status`

## Setup

1. Clone repo and create venv
2. Install dependencies:
   - `discord.py`
   - `python-dotenv`
   - `yfinance`
3. Copy `.env.example` to `.env` and fill values:

```bash
cp .env.example .env
```

## Run

```bash
python dc.py
```

## Notes

- Bot is limited to one guild/channel configured in `.env`.
- Alerts are one-shot: after trigger, that target is removed.
- After add/delete, bot restarts itself to refresh live subscriptions.
