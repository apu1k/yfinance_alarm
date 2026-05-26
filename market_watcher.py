import datetime as dt
import json
import queue
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

import yfinance as yf
from yfinance.live import WebSocket


class MarketWatcher:
    """Dedicated market-stream owner.

    Responsibilities:
    - Own one supervisor thread and the active live stream lifecycle.
    - Keep rules in-memory and persist to disk safely.
    - Accept runtime commands (reload/stop) via a thread-safe queue.
    - Dispatch alert texts via injected callback.

    Note: command methods are thread-safe.
    """

    def __init__(
        self,
        data_file: str = "data.json",
        alert_sender: Optional[Callable[[str], None]] = None,
        fallback_reload_seconds: float = 30.0,
        idle_sleep_seconds: float = 1.0,
    ) -> None:
        self.data_file = Path(data_file)
        self.alert_sender = alert_sender
        self.fallback_reload_seconds = fallback_reload_seconds
        self.idle_sleep_seconds = idle_sleep_seconds

        self._json_lock = threading.Lock()
        self._rules_lock = threading.Lock()
        self._cmd_q: "queue.Queue[dict]" = queue.Queue()

        self._rules: Dict[str, List[dict]] = self._load_rules()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        self._stream_lock = threading.Lock()
        self._stream_thread: Optional[threading.Thread] = None
        self._stream_stop_event: Optional[threading.Event] = None
        self._stream_ws = None
        self._stream_symbols: List[str] = []
        self._stream_generation = 0

        self._logged_sample = False
        self._last_msg_ts = 0.0
        self._reconnect_count = 0

    # ---------------------------
    # Public lifecycle API
    # ---------------------------
    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="MarketWatcher", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        self._cmd_q.put({"type": "stop"})
        if self._stream_stop_event is not None:
            self._stream_stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def request_reload(self) -> None:
        self._cmd_q.put({"type": "reload"})

    def status(self) -> dict:
        with self._rules_lock:
            symbols = sorted(self._rules.keys())
        running = self._thread is not None and self._thread.is_alive() and not self._stop_event.is_set()
        now = time.time()
        age = (now - self._last_msg_ts) if self._last_msg_ts else None
        return {
            "running": running,
            "symbols": symbols,
            "symbol_count": len(symbols),
            "queue_size": self._cmd_q.qsize(),
            "connected": self._stream_thread is not None and self._stream_thread.is_alive(),
            "stream_symbols": list(self._stream_symbols),
            "last_msg_age_sec": age,
            "reconnect_count": self._reconnect_count,
        }

    def list_targets(self) -> str:
        with self._rules_lock:
            rules = dict(self._rules)

        if not rules:
            return "No targets configured."

        lines = ["Configured targets:"]
        for ticker in sorted(rules.keys()):
            entries = rules.get(ticker)
            if isinstance(entries, dict):
                entries = [entries]
            if not isinstance(entries, list):
                continue
            for idx, rule in enumerate(entries, start=1):
                if not isinstance(rule, dict):
                    continue
                target = rule.get("price-target", "n/a")
                above = rule.get("above", "n/a")
                last_check = rule.get("last_check", "n/a")
                lines.append(f"{ticker} [{idx}] target={target} above={above} last_check={last_check}")

        out = "\n".join(lines)
        if len(out) > 1900:
            out = out[:1900] + "\n…(truncated)"
        return out

    def add_target(self, ticker: str, target_price: float, current_price: float, last_check: int) -> str:
        if target_price <= 0:
            return "Target price must be > 0."

        above = target_price >= current_price

        with self._rules_lock:
            existing = self._rules.get(ticker)
            if isinstance(existing, list):
                alert_list = existing
            elif isinstance(existing, dict):
                alert_list = [existing]
            else:
                alert_list = []

            alert_list.append({
                "price-target": target_price,
                "above": above,
                "last_check": last_check,
            })
            self._rules[ticker] = alert_list

        self._save_rules()
        self.request_reload()

        direction = "above" if above else "below"
        return f"Added target for {ticker}: {target_price} ({direction}, current ~ {current_price:.4f})"

    def delete_target(self, ticker: str, idx: int) -> str:
        with self._rules_lock:
            if ticker not in self._rules:
                return f"Ticker not found: {ticker}"

            entries = self._rules.get(ticker)
            if isinstance(entries, dict):
                entries = [entries]

            if not isinstance(entries, list) or not entries:
                return f"No targets found for {ticker}"

            if idx < 1 or idx > len(entries):
                return f"Invalid index for {ticker}. Use 1..{len(entries)}"

            removed = entries.pop(idx - 1)

            if entries:
                self._rules[ticker] = entries
            else:
                self._rules.pop(ticker, None)

        self._save_rules()
        self.request_reload()

        return f"Deleted target for {ticker} [{idx}]: {removed}"

    # ---------------------------
    # Internal persistence
    # ---------------------------
    def _load_rules(self) -> Dict[str, List[dict]]:
        with self._json_lock:
            if not self.data_file.exists():
                self.data_file.write_text("{}", encoding="utf-8")
                return {}

            raw = self.data_file.read_text(encoding="utf-8").strip()
            if not raw:
                self.data_file.write_text("{}", encoding="utf-8")
                return {}

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                self.data_file.write_text("{}", encoding="utf-8")
                return {}

            if not isinstance(data, dict):
                self.data_file.write_text("{}", encoding="utf-8")
                return {}

        normalized: Dict[str, List[dict]] = {}
        for ticker, entries in data.items():
            if isinstance(entries, dict):
                entries = [entries]
            if isinstance(entries, list):
                normalized[ticker] = [r for r in entries if isinstance(r, dict)]
        return normalized

    def _save_rules(self) -> None:
        with self._rules_lock:
            snapshot = dict(self._rules)
        with self._json_lock:
            self.data_file.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")

    # ---------------------------
    # Offline catch-up checks
    # ---------------------------
    def _catch_up_missed_targets(self) -> None:
        """Check whether stored targets were hit while the bot was offline.

        The live websocket only sees ticks while the process is running. On
        startup/reload, use historical candles from each rule's last_check until
        now and remove/send alerts for targets that were touched in that range.
        """
        now_ms = int(time.time() * 1000)

        with self._rules_lock:
            snapshot = {}
            for ticker, entries in self._rules.items():
                if isinstance(entries, dict):
                    entries = [entries]
                if isinstance(entries, list):
                    snapshot[ticker] = [dict(rule) for rule in entries if isinstance(rule, dict)]

        if not snapshot:
            return

        print(f"[WATCHER] catch-up check starting for {len(snapshot)} ticker(s)")

        changed = False
        next_rules: Dict[str, List[dict]] = {}

        for ticker, rules in snapshot.items():
            remaining: List[dict] = []

            for rule in rules:
                target = rule.get("price-target")
                above = rule.get("above", True)
                last_check = rule.get("last_check")

                if target is None:
                    remaining.append(rule)
                    continue

                try:
                    target_price = float(target)
                except (TypeError, ValueError):
                    remaining.append(rule)
                    continue

                try:
                    since_ms = int(last_check)
                except (TypeError, ValueError):
                    since_ms = now_ms
                    rule["last_check"] = now_ms
                    changed = True

                if since_ms >= now_ms:
                    if since_ms > now_ms:
                        print(
                            f"[WATCHER] last_check for {ticker} is in the future; "
                            "resetting it to now"
                        )
                        rule["last_check"] = now_ms
                        changed = True
                    remaining.append(rule)
                    continue

                hit, checked_until_ms, hit_price = self._check_target_range(
                    ticker=ticker,
                    target=target_price,
                    above=bool(above),
                    since_ms=since_ms,
                    until_ms=now_ms,
                )

                if hit:
                    direction = "above" if above else "below"
                    price_part = f" at {hit_price:.4f}" if hit_price is not None else ""
                    alert_text = (
                        f"🚨 ALERT: {ticker} hit target {target_price:g}{price_part} "
                        f"while bot was offline ({direction})"
                    )
                    print(alert_text)
                    if self.alert_sender is not None:
                        try:
                            self.alert_sender(alert_text)
                        except Exception as e:
                            print(f"[WATCHER] alert sender error during catch-up: {e}")
                    changed = True
                    continue

                if checked_until_ms is None:
                    print(
                        f"[WATCHER] catch-up could not verify {ticker}; "
                        "keeping old last_check"
                    )
                    remaining.append(rule)
                    continue

                new_last_check = max(since_ms, checked_until_ms)
                if rule.get("last_check") != new_last_check:
                    rule["last_check"] = new_last_check
                    changed = True
                    print(
                        f"[WATCHER] catch-up checked {ticker}: "
                        f"{since_ms} -> {new_last_check}"
                    )
                remaining.append(rule)

            if remaining:
                next_rules[ticker] = remaining
            else:
                changed = True

        if changed:
            with self._rules_lock:
                self._rules = next_rules
            self._save_rules()

        print("[WATCHER] catch-up check finished")

    def _check_target_range(
        self,
        ticker: str,
        target: float,
        above: bool,
        since_ms: int,
        until_ms: int,
    ) -> tuple[bool, Optional[int], Optional[float]]:
        """Return whether target was hit between since_ms and until_ms.

        Uses candle High/Low so short touches are not missed. For very recent
        intraday data, Yahoo/yfinance is more reliable with period=... than with
        a narrow start/end window, so fetch a wider period and filter locally.
        """
        if since_ms >= until_ms:
            return False, None, None

        age_seconds = max(0.0, (until_ms - since_ms) / 1000.0)
        if age_seconds <= 24 * 3600:
            interval = "1m"
            period = "1d"
        elif age_seconds <= 5 * 24 * 3600:
            interval = "1h"
            period = "5d"
        elif age_seconds <= 30 * 24 * 3600:
            interval = "1d"
            period = "1mo"
        elif age_seconds <= 60 * 24 * 3600:
            interval = "1d"
            period = "3mo"
        else:
            interval = "1w"
            period = "1y"

        ticker_obj = yf.Ticker(ticker)
        hist = None

        try:
            hist = ticker_obj.history(
                period=period,
                interval=interval,
                prepost=True,
                actions=False,
                auto_adjust=False,
                raise_errors=False,
            )
            hist_first = hist.index[0] if hist is not None and not hist.empty else None
            hist_last = hist.index[-1] if hist is not None and not hist.empty else None
            print(
                f"[WATCHER] catch-up fetched {ticker}: rows={0 if hist is None else len(hist)} "
                f"period={period} interval={interval} first={hist_first} last={hist_last} "
                f"requested={dt.datetime.fromtimestamp(since_ms / 1000, tz=dt.timezone.utc).isoformat()}"
                f"..{dt.datetime.fromtimestamp(until_ms / 1000, tz=dt.timezone.utc).isoformat()}"
            )
        except Exception as e:
            print(f"[WATCHER] catch-up period history failed for {ticker}: {e}")

        if hist is None or hist.empty:
            start = dt.datetime.fromtimestamp(since_ms / 1000, tz=dt.timezone.utc)
            end = dt.datetime.fromtimestamp(until_ms / 1000, tz=dt.timezone.utc)
            if interval == "1m":
                end += dt.timedelta(minutes=5)
            elif interval in {"5m", "15m"}:
                end += dt.timedelta(minutes=30)
            else:
                end += dt.timedelta(days=1)

            try:
                hist = ticker_obj.history(
                    start=start,
                    end=end,
                    interval=interval,
                    prepost=True,
                    actions=False,
                    auto_adjust=False,
                    raise_errors=False,
                )
            except Exception as e:
                print(f"[WATCHER] catch-up start/end history failed for {ticker}: {e}")
                return False, None, None

        if hist is None or hist.empty:
            print(
                f"[WATCHER] catch-up history empty for {ticker} "
                f"(period={period}, interval={interval})"
            )
            return False, None, None

        try:
            index_ms = []
            for ts in hist.index:
                py_dt = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
                if py_dt.tzinfo is None:
                    py_dt = py_dt.replace(tzinfo=dt.timezone.utc)
                else:
                    py_dt = py_dt.astimezone(dt.timezone.utc)
                index_ms.append(int(py_dt.timestamp() * 1000))
        except Exception as e:
            print(f"[WATCHER] catch-up could not normalize timestamps for {ticker}: {e}")
            return False, None, None

        interval_ms = {
            "1m": 60_000,
            "5m": 5 * 60_000,
            "15m": 15 * 60_000,
            "1h": 60 * 60_000,
            "1d": 24 * 60 * 60_000,
            "1w": 7 * 24 * 60 * 60_000,
        }.get(interval, 0)

        # Candle timestamps are candle START times. A candle overlaps the
        # requested range if its start is before/until the range end and its
        # approximate end is after the previous last_check. Comparing only
        # candle_start > since_ms drops the candle that contains since_ms.
        mask = [((ts_ms + interval_ms) > since_ms and ts_ms <= until_ms) for ts_ms in index_ms]
        if not any(mask):
            oldest_ms = min(index_ms) if index_ms else None
            newest_ms = max(index_ms) if index_ms else None
            oldest_utc = (
                dt.datetime.fromtimestamp(oldest_ms / 1000, tz=dt.timezone.utc).isoformat()
                if oldest_ms is not None else None
            )
            newest_utc = (
                dt.datetime.fromtimestamp(newest_ms / 1000, tz=dt.timezone.utc).isoformat()
                if newest_ms is not None else None
            )
            since_utc = dt.datetime.fromtimestamp(since_ms / 1000, tz=dt.timezone.utc).isoformat()
            until_utc = dt.datetime.fromtimestamp(until_ms / 1000, tz=dt.timezone.utc).isoformat()
            print(
                f"[WATCHER] catch-up no overlapping candles for {ticker} "
                f"(period={period}, interval={interval}, "
                f"requested={since_ms}..{until_ms} UTC={since_utc}..{until_utc}, "
                f"available={oldest_ms}..{newest_ms} UTC={oldest_utc}..{newest_utc})"
            )
            return False, None, None

        hist = hist.loc[mask]
        filtered_index_ms = [ts_ms for ts_ms, keep in zip(index_ms, mask) if keep]

        if hist.empty or not filtered_index_ms:
            return False, None, None

        checked_until_ms = max(filtered_index_ms)

        if above:
            highs = hist.get("High")
            if highs is None:
                return False, checked_until_ms, None
            highs = highs.dropna()
            if highs.empty:
                return False, checked_until_ms, None
            max_high = float(highs.max())
            return max_high >= target, checked_until_ms, max_high if max_high >= target else None

        lows = hist.get("Low")
        if lows is None:
            return False, checked_until_ms, None
        lows = lows.dropna()
        if lows.empty:
            return False, checked_until_ms, None
        min_low = float(lows.min())
        return min_low <= target, checked_until_ms, min_low if min_low <= target else None

    # ---------------------------
    # Stream supervision
    # ---------------------------
    def _run(self) -> None:
        current_symbols: List[str] = []
        needs_catch_up = True

        while not self._stop_event.is_set():
            reloaded = self._drain_commands()
            if reloaded:
                needs_catch_up = True

            if needs_catch_up:
                self._catch_up_missed_targets()
                needs_catch_up = False

            with self._rules_lock:
                desired_symbols = sorted(self._rules.keys())

            stream_dead = (
                bool(desired_symbols)
                and (self._stream_thread is None or not self._stream_thread.is_alive())
            )

            if desired_symbols != current_symbols or stream_dead:
                self._replace_stream(desired_symbols)
                current_symbols = desired_symbols

            time.sleep(self.idle_sleep_seconds)

        self._stop_stream(timeout=2.0)

    def _replace_stream(self, symbols: List[str]) -> None:
        self._stop_stream(timeout=2.0)

        self._stream_symbols = list(symbols)
        if not symbols:
            return

        self._stream_generation += 1
        generation = self._stream_generation
        stream_stop_event = threading.Event()
        self._stream_stop_event = stream_stop_event
        self._logged_sample = False

        self._stream_thread = threading.Thread(
            target=self._stream_worker,
            args=(list(symbols), stream_stop_event, generation),
            name=f"MarketStream-{generation}",
            daemon=True,
        )
        self._stream_thread.start()

    def _stop_stream(self, timeout: float = 2.0) -> None:
        thread = self._stream_thread
        stop_event = self._stream_stop_event

        if stop_event is not None:
            stop_event.set()

        self._close_stream_socket()

        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
            if thread.is_alive():
                print(
                    f"[WATCHER] yfinance live stream did not exit within {timeout:.1f}s; "
                    "stale callbacks are disabled"
                )

        if thread is self._stream_thread and (thread is None or not thread.is_alive()):
            self._stream_thread = None
            self._stream_stop_event = None
            self._stream_ws = None
            self._stream_symbols = []

    def _close_stream_socket(self) -> None:
        with self._stream_lock:
            ws = self._stream_ws

        if ws is None:
            return

        try:
            ws.close()
        except Exception as e:
            print(f"[WATCHER] failed to close yfinance websocket: {e!r}")

    def _stream_worker(
        self,
        symbols: List[str],
        stream_stop_event: threading.Event,
        generation: int,
    ) -> None:
        print(f"[WATCHER] streaming: {' '.join(symbols)}")

        ws = None
        try:
            ws = WebSocket(verbose=False)
            with self._stream_lock:
                if generation == self._stream_generation:
                    self._stream_ws = ws

            ws.subscribe(symbols)

            while (
                not self._stop_event.is_set()
                and not stream_stop_event.is_set()
                and generation == self._stream_generation
            ):
                try:
                    raw_message = ws._ws.recv(timeout=1.0)
                except TimeoutError:
                    continue
                except Exception:
                    if (
                        self._stop_event.is_set()
                        or stream_stop_event.is_set()
                        or generation != self._stream_generation
                    ):
                        break
                    raise

                message_json = json.loads(raw_message)
                encoded_data = message_json.get("message", "")
                decoded_message = ws._decode_message(encoded_data)
                self._handle_stream_message(decoded_message, stream_stop_event, generation)
        except Exception as e:
            if not self._stop_event.is_set() and not stream_stop_event.is_set():
                self._reconnect_count += 1
                print(f"[WATCHER] stream error: {e} (reconnect_count={self._reconnect_count})")
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass

            with self._stream_lock:
                if generation == self._stream_generation and self._stream_ws is ws:
                    self._stream_ws = None

    def _handle_stream_message(
        self,
        msg,
        stream_stop_event: threading.Event,
        generation: int,
    ) -> None:
        if (
            self._stop_event.is_set()
            or stream_stop_event.is_set()
            or generation != self._stream_generation
        ):
            return

        self._last_msg_ts = time.time()

        if not self._logged_sample:
            try:
                print(f"[WATCHER] sample message type={type(msg).__name__}: {msg}")
            except Exception:
                print("[WATCHER] sample message could not be printed")
            self._logged_sample = True

        if isinstance(msg, dict):
            self._handle_tick(msg)
        elif isinstance(msg, list):
            for item in msg:
                if isinstance(item, dict):
                    self._handle_tick(item)

    def _drain_commands(self) -> bool:
        reloaded = False
        while True:
            try:
                cmd = self._cmd_q.get_nowait()
            except queue.Empty:
                break

            ctype = cmd.get("type")
            if ctype == "stop":
                self._stop_event.set()
            elif ctype == "reload":
                reloaded = True

        if reloaded:
            with self._rules_lock:
                self._rules = self._load_rules()

        return reloaded

    # ---------------------------
    # Tick processing
    # ---------------------------
    @staticmethod
    def _should_trigger(rule: dict, price: float) -> bool:
        target = rule.get("price-target")
        above = rule.get("above", True)
        if target is None:
            return False
        return price >= target if above else price <= target

    def _handle_tick(self, message: dict) -> None:
        ticker_symbol = message.get("id")
        price = message.get("price")
        currency = message.get("currency")
        tick_time = message.get("time")

        if ticker_symbol is None or price is None:
            return

        try:
            price = float(price)
        except (TypeError, ValueError):
            return

        with self._rules_lock:
            rules = self._rules.get(ticker_symbol)
            if rules is None:
                return

            if isinstance(rules, dict):
                rules = [rules]
            if not isinstance(rules, list):
                return

            changed = False
            touched = False
            remaining: List[dict] = []

            for rule in rules:
                if not isinstance(rule, dict):
                    continue

                normalized_tick_time = None
                if tick_time is not None:
                    try:
                        normalized_tick_time = int(float(tick_time))
                    except (TypeError, ValueError):
                        normalized_tick_time = None

                # yfinance live messages are epoch milliseconds today, but keep
                # this defensive: if seconds ever arrive, normalize to ms.
                if normalized_tick_time is not None and normalized_tick_time < 10_000_000_000:
                    normalized_tick_time *= 1000

                prev_last_check = rule.get("last_check")
                try:
                    prev_last_check_ms = int(prev_last_check) if prev_last_check is not None else None
                except (TypeError, ValueError):
                    prev_last_check_ms = None

                should_update_last_check = (
                    normalized_tick_time is not None
                    and (prev_last_check_ms is None or normalized_tick_time > prev_last_check_ms)
                )

                if should_update_last_check:
                    touched = True
                    rule["last_check"] = normalized_tick_time
                    print(
                        f"[WATCHER] last_check updated {ticker_symbol}: {prev_last_check} -> {normalized_tick_time}"
                    )

                if self._should_trigger(rule, price):
                    alert_text = f"🚨 ALERT: {ticker_symbol} hit target at {price} {currency}"
                    print(alert_text)
                    if self.alert_sender is not None:
                        try:
                            self.alert_sender(alert_text)
                        except Exception as e:
                            print(f"[WATCHER] alert sender error: {e}")
                    changed = True
                    continue

                remaining.append(rule)

            if changed or touched:
                if remaining:
                    self._rules[ticker_symbol] = remaining
                else:
                    self._rules.pop(ticker_symbol, None)

        # Persist outside of _rules_lock critical section
        self._save_rules()
