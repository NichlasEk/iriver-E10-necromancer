# iriver-E10-necromancer

Native Linux tooling for reverse-engineering and eventually regenerating the iRiver E10 media database.

## Current state

This repo does not write a new `db.dat` or `db.idx` yet.

It currently provides:
- schema inspection for `System/db.dat`, `db.idx`, and `db.dic`
- a canonicalized model of the visible E10 media library
- source-media modeling from one selected directory or the full `Music/` tree
- rebuild planning for selected directories or full-library regeneration

## Files

- `e10db_tool.py`: main CLI for analysis and planning
- `E10DB_NOTES.md`: reverse-engineering notes and current findings
- `podcast_inventory.json`: example inventory for one podcast directory

## Example usage

Inspect the current device database:

```bash
python3 e10db_tool.py db-summary /run/media/nichlas/E10
python3 e10db_tool.py model-export /run/media/nichlas/E10
```

Plan additions from one selected directory:

```bash
python3 e10db_tool.py rebuild-plan /run/media/nichlas/E10 /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
```

Plan a full regeneration against the whole music tree:

```bash
python3 e10db_tool.py rebuild-plan /run/media/nichlas/E10 --full-db
```

## Notes

- The repository intentionally does not vendor PMPlib or other third-party source trees with uncertain mixed licensing.
- The next major step is the write phase: generating a fresh canonical E10 database from the normalized source model.
