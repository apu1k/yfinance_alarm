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
    # Stream supervision
    # ---------------------------
    def _run(self) -> None:
        current_symbols: List[str] = []

        while not self._stop_event.is_set():
            self._drain_commands()
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

    def _drain_commands(self) -> None:
        drained = False
        while True:
            try:
                cmd = self._cmd_q.get_nowait()
            except queue.Empty:
                break

            drained = True
            ctype = cmd.get("type")
            if ctype == "stop":
                self._stop_event.set()
            elif ctype == "reload":
                with self._rules_lock:
                    self._rules = self._load_rules()

        if drained:
            with self._rules_lock:
                self._rules = self._load_rules()

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

                normalized_tick_time = tick_time
                try:
                    normalized_tick_time = int(tick_time) if tick_time is not None else tick_time
                except (TypeError, ValueError):
                    pass

                prev_last_check = rule.get("last_check")
                if prev_last_check != normalized_tick_time:
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
