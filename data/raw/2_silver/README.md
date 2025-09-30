Data processing step here

- Convert entries into suitable types, e.x `monthly_charge`, `total_charges`, `cltv`, scores → `double`/`int`
- Cast booleans: turn `Yes/No` into `true/false` (e.g., `paperless_billing`, `phone_service`, `streaming_tv`, etc.).
- Quarters standardized (if needed)
- Normalize scores if needed
- Remove duplicates
- Update all tables
- JOIN all tables with a suiting condition.