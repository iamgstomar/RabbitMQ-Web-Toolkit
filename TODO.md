# Task Progress

## ✅ Completed: Backup JSON format changed to compact (single-line)

### Changes made to `app.py`

1. **`api_backup_messages()` — JSON content serialization:**
   - Removed `indent=2` from `json.dumps()` calls and used compact separators `(",", ":")`
   - Single message: `json.dumps(payload_only[0], ensure_ascii=False)` (no indent)
   - Multiple messages: `json.dumps(payload_only, ensure_ascii=False)` (no indent)
   - CSV: also updated to use compact separators for JSON values

2. **`api_backup_multiple_queues()` — JSON/ZIP backup content:**
   - Single queue: `json.dumps(msgs, ensure_ascii=False, separators=(",", ":"))`
   - ZIP content: `json.dumps(msgs, ensure_ascii=False, separators=(",", ":"))`

### Result

Backup files now produce compact JSON format matching the exact format stored in RabbitMQ queues:

**Before (pretty-printed):**
```json
{
  "TR": "20260720103545_0E49469B-7060-46A7-868C-33AB60056352_321_2_1032",
  "TI": 1,
  ...
}
```

**After (compact, single-line):**
```json
{"TR":"20260720103545_0E49469B-7060-46A7-868C-33AB60056352_321_2_1032","TI":1,...}
