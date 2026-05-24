import inspect
import yfinance as yf


def safe_sig(obj):
    try:
        return str(inspect.signature(obj))
    except Exception as e:
        return f"<no signature: {e}>"


def main():
    cls = getattr(yf, "AsyncWebSocket", None)
    print("AsyncWebSocket exists:", cls is not None)
    if cls is None:
        return

    print("Class:", cls)
    print("__init__ signature:", safe_sig(cls.__init__))

    names = [n for n in dir(cls) if not n.startswith("__")]
    print("\nClass attrs/methods:")
    for n in names:
        print(" -", n)

    ws = None
    try:
        ws = cls()
        print("\nInstance created:", ws)
    except Exception as e:
        print("Could not instantiate AsyncWebSocket():", repr(e))
        return

    inames = [n for n in dir(ws) if not n.startswith("__")]
    print("\nInstance attrs/methods:")
    for n in inames:
        print(" -", n)

    print("\nCandidate method signatures:")
    for n in [
        "subscribe",
        "unsubscribe",
        "listen",
        "start",
        "run",
        "recv",
        "close",
        "connect",
        "disconnect",
        "send",
        "_message_handler",
    ]:
        if hasattr(ws, n):
            fn = getattr(ws, n)
            print(f"{n}: {safe_sig(fn)}")

    print("\nCallable attrs with signatures (best effort):")
    for n in inames:
        attr = getattr(ws, n)
        if callable(attr):
            print(f"{n}: {safe_sig(attr)}")


if __name__ == "__main__":
    main()
