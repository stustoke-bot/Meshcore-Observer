# Decode-error duplicate repeaters

## What you see

A repeater appears twice with nearly the same name but different public key hashes, e.g.:

- **Ghost (decode error):** "Newnastje ST5" `EB2C4061C4397ABD8BC71B1BED607C58F6CA27E6CEA51DFED36758FD0A2D9C8D`
- **Canonical (correct):** "Newcastle ST5" `EB2C4061C4397ABD8BC71B1BED607C58F6CA7793C6A91F6ED33758FD0A2D9C8D`

## Why it happens

Both the **public key** and the **name** come from the same decoded advert payload (RF → `MeshCoreDecoder.decode(payloadHex)` → `advert.publicKey`, `advert.appData.name`).

When the RF packet is corrupted (bit errors in transit, bad decode, or marginal reception):

1. **Public key** – a few bytes in the 32-byte key decode wrong → different 64‑char hex → a new device identity is created.
2. **Name** – bit errors in the UTF‑8 name string → e.g. "Newcastle" decodes as "Newnastje".

So one bad packet produces a **ghost repeater**: same logical device, wrong pub and wrong name.

## How we handle it

- **At ingest** (mqtt_ingest): before creating/updating a device, we check for an existing repeater with a **very similar name** and a **very similar pub** (same length, only a few bytes different). If we find one, we treat the packet as a decode-error version of that repeater and update the **canonical** pub’s record (last heard, etc.) instead of creating a new device. We log when this happens.
- **Detection thresholds**: name similarity = small edit distance (e.g. "Newnastje" vs "Newcastle"); pub similarity = same 64‑hex length and ≤ 10 bytes different.

## Identifying pairs manually

To find likely decode-error pairs in existing data:

- Same or very similar name (typos / character swaps).
- Pub keys same length (64 hex chars) and only a short middle segment different (e.g. 7 bytes different in the example above).

A script can list candidates by comparing all repeater names (e.g. Levenshtein distance ≤ 4) and, for each pair, counting differing bytes in the pub hex; flag pairs with byte-diff ≤ 12.

## Cleaning up existing ghosts

After deploy, new packets that decode with the wrong pub/name will be canonicalized at ingest. Existing ghost rows in `devices` / `current_repeaters` can be:

- **Hidden on map** (e.g. set `hidden_on_map = 1` for the ghost pub), or
- **Merged** by hand (pick canonical pub, update any references to the ghost pub, then delete or hide the ghost).
