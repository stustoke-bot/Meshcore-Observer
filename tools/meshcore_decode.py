import sys
import json
from collections import defaultdict
from datetime import datetime

# ======================================================
# CHANNEL NAME LOOKUP
# Map channel hash byte -> human readable #channel name
# Add new entries here as you discover them
# ======================================================

CHANNEL_MAP = {
    "11": "#public",
    "D9": "#test",
    # add more later like:
    # "2A": "#boats"
}

# Track repeats (same fingerprint heard multiple times)
seen = defaultdict(int)

# ======================================================
# Helpers
# ======================================================

def extract_printable_utf8(raw: bytes) -> str:
    """
    Pull out readable UTF-8 text (names, emojis) from packets
    """
    try:
        s = raw.decode("utf-8", errors="ignore")
        return "".join(ch for ch in s if ch.isprintable())
    except Exception:
        return ""

def decode_frame(raw: bytes) -> dict:
    """
    Very lightweight MeshCORE-aware frame classifier
    """
    if not raw:
        return {"kind": "empty"}

    ftype = raw[0]

    # 0x11 = advert / beacon (often plaintext names)
    if ftype == 0x11:
        return {
            "kind": "advert",
            "type": "0x11",
            "path_len": raw[1] if len(raw) > 1 else None,
            "node_id": raw[2:10].hex() if len(raw) >= 10 else None,
            "text": extract_printable_utf8(raw),
        }

    # 0x15 = group text (usually encrypted)
    if ftype == 0x15:
        return {
            "kind": "grouptext",
            "type": "0x15",
            "path_len": raw[1] if len(raw) > 1 else None,
            "chan_hash": f"{raw[2]:02X}" if len(raw) > 2 else None,
            "mac": raw[3:5].hex() if len(raw) >= 5 else None,
            "cipher_len": max(0, len(raw) - 5),
        }

    return {
        "kind": "unknown",
        "type": f"0x{ftype:02X}",
        "len": len(raw),
    }

# ======================================================
# Main loop – read JSON lines from PlatformIO monitor
# ======================================================

for line in sys.stdin:
    line = line.strip()

    # Pass through non-JSON lines (boot messages etc.)
    if not line.startswith("{"):
        if line:
            print(line)
        continue

    try:
        pkt = json.loads(line)
    except Exception:
        print(line)
        continue

    if pkt.get("type") != "rf" or "hex" not in pkt:
        print(line)
        continue

    raw = bytes.fromhex(pkt["hex"])
    fp = pkt.get("fp", "????????????????")
    seen[fp] += 1

    ts = datetime.now().strftime("%H:%M:%S")

    print("")
    print("────────────────────────────────────────")
    print(
        f"[{ts}] FP {fp}  "
        f"seen:{seen[fp]}  "
        f"LEN:{pkt.get('len')}  "
        f"CRC:{pkt.get('crc')}  "
        f"RSSI:{pkt.get('rssi')}  "
        f"SNR:{pkt.get('snr')}"
    )

    d = decode_frame(raw)
    print(f"TYPE: {d.get('kind')} ({d.get('type')})")

    # -------- Advert packets --------
    if d["kind"] == "advert":
        print(f"node_id: {d.get('node_id')}")
        print(f"path_len: {d.get('path_len')}")
        if d.get("text"):
            print(f"text: {d['text']}")

    # -------- Group text packets --------
    elif d["kind"] == "grouptext":
        ch = d.get("chan_hash")
        name = CHANNEL_MAP.get(ch, "#unknown")

        print(f"channel: {name} (hash:{ch})")
        print(f"path_len: {d.get('path_len')}")
        print(f"cipher_len: {d.get('cipher_len')} bytes")

        if seen[fp] > 1:
            print("repeat: YES (likely via repeater)")

    # -------- Unknown packets --------
    else:
        h = raw.hex().upper()
        print(f"head: {h[:64]}{'…' if len(h) > 64 else ''}")
