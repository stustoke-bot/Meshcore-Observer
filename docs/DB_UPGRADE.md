# DB Upgrade Notes

## Schema additions
- `messages.path_text` and `message_observers.path_text`
  - `|`-delimited hop list to avoid JSON parsing on hot paths.
  - Server falls back to `path_json` if `path_text` is null.
- `stats_5m`
  - 5-minute rollups with per-channel and `__all__` totals.
  - Used by SSE counters and dashboard bootstrap.

## Index additions
- `idx_messages_ts` on `messages(ts)`
- `idx_messages_sender_channel_ts` on `messages(sender, channel_name, ts)`
- `idx_repeater_rank_history_recorded_at` on `repeater_rank_history(recorded_at)`
- `idx_stats_5m_bucket` on `stats_5m(bucket_start)`

## Pragmas
Applied on both server and ingest:
- `journal_mode = WAL`
- `synchronous = NORMAL`
- `temp_store = MEMORY`
- `cache_size = -64000`
- `foreign_keys = ON`

## Migration behavior
- Tables are created if missing.
- `path_text` columns are added via `ALTER TABLE` if absent.
- Existing rows remain readable via `path_json`; new ingest writes `path_text`.
