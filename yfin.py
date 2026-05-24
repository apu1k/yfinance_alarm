from pathlib import Path

import yfinance as yf

from market_watcher import MarketWatcher

"""yfinance live market stream helper."""

DATA_FILE = Path("data.json")
ALERT_SENDER = None
WATCHER = None


def _ensure_watcher() -> MarketWatcher:
    global WATCHER
    if WATCHER is None:
        WATCHER = MarketWatcher(data_file=str(DATA_FILE), alert_sender=ALERT_SENDER)
    return WATCHER


def set_alert_sender(sender):
    global ALERT_SENDER
    ALERT_SENDER = sender
    if WATCHER is not None:
        WATCHER.alert_sender = sender


def request_stream_reload():
    _ensure_watcher().request_reload()


def run_market_stream():
    watcher = _ensure_watcher()
    watcher.start()


def stop_market_stream():
    if WATCHER is not None:
        WATCHER.stop()


def get_market_status() -> dict:
    return _ensure_watcher().status()


def search_ticker_yfin(search_str: str, limit: int = 5) -> str:
    query = " ".join(search_str.strip().split())
    if not query:
        return "Please provide a search string."

    q_upper = query.upper()
    looks_like_symbol = all(ch.isalnum() or ch in ".-" for ch in q_upper)

    candidates = []
    seen = set()

    # 1) exact-symbol attempt
    if looks_like_symbol:
        try:
            t = yf.Ticker(q_upper)
            h = t.history(period="1d", interval="1m")
            if not h.empty and not h["Close"].dropna().empty:
                fi = t.fast_info or {}
                currency = fi.get("currency")
                price = float(h["Close"].dropna().iloc[-1])
                candidates.append({
                    "symbol": q_upper,
                    "shortname": q_upper,
                    "quoteType": "SYMBOL",
                    "exchDisp": "n/a",
                    "_price": price,
                    "_currency": currency,
                    "_score_boost": 1000,
                })
                seen.add(q_upper)
        except Exception:
            pass

    # 2) Yahoo search
    searches = [query]
    if " " in query:
        searches.append(query.replace(" ", ""))

    for s_query in searches:
        try:
            s = yf.Search(s_query, max_results=max(limit * 3, 10))
            quotes = s.quotes or []
        except Exception:
            quotes = []

        for q in quotes:
            sym = (q.get("symbol") or "").upper()
            if not sym or sym in seen:
                continue
            q["_score_boost"] = 0
            if sym == q_upper:
                q["_score_boost"] += 500
            elif sym.startswith(q_upper):
                q["_score_boost"] += 100
            candidates.append(q)
            seen.add(sym)

    if not candidates:
        return f"No results for '{search_str}'."

    # Rank: boost exact-like matches first, then provider score
    def rank_key(q):
        score = q.get("score") or 0
        return (q.get("_score_boost", 0), score)

    candidates.sort(key=rank_key, reverse=True)

    lines = [f"Top matches for '{search_str}':"]
    for q in candidates[:limit]:
        symbol = (q.get("symbol") or "n/a").upper()
        name = q.get("shortname") or q.get("longname") or "n/a"
        exch = q.get("exchDisp") or q.get("exchange") or "n/a"
        qtype = q.get("quoteType") or q.get("typeDisp") or "n/a"

        # price best-effort (fast path only, avoids Discord interaction timeout)
        price = q.get("_price")
        currency = q.get("_currency") or q.get("currency")
        if price is None:
            try:
                fi = yf.Ticker(symbol).fast_info or {}
                price = fi.get("last_price")
                currency = currency or fi.get("currency")
            except Exception:
                price = None

        price_txt = f"{price:.4f}" if isinstance(price, (int, float)) else "n/a"
        if currency:
            price_txt = f"{price_txt} {currency}"

        lines.append(f"{symbol} | {name[:32]} | {qtype} | {exch} | {price_txt}")

    out = "\n".join(lines)

    # Discord hard cap safety
    if len(out) > 1900:
        out = out[:1900] + "\n…(truncated)"
    return out


def add_target_yfin(ticker: str, target_price: float):
    if target_price <= 0:
        return "Target price must be > 0."

    t = yf.Ticker(ticker)
    hist = t.history(period="1d", interval="1m")

    if hist.empty:
        return None

    close_series = hist["Close"].dropna()
    if close_series.empty:
        return None

    current_price = float(close_series.iloc[-1])
    last_dt = close_series.index[-1].to_pydatetime()
    last_check = int(last_dt.timestamp() * 1000)

    return _ensure_watcher().add_target(ticker, target_price, current_price, last_check)


def list_targets_yfin() -> str:
    return _ensure_watcher().list_targets()


def delete_target_yfin(ticker: str, idx: int) -> str:
    return _ensure_watcher().delete_target(ticker, idx)
