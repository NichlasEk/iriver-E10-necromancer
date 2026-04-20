# iriver-E10-necromancer

Native Linux tooling for reverse-engineering and eventually regenerating the iRiver E10 media database.

## Current state

This repo does not write a firmware-compatible `db.dat` or `db.idx` yet.

It currently provides:
- schema inspection for `System/db.dat`, `db.idx`, and `db.dic`
- a canonicalized model of the visible E10 media library
- source-media modeling from one selected directory or the full `Music/` tree
- rebuild planning for selected directories or full-library regeneration
- a safe write phase that emits a rebuild snapshot directory for later binary serialization
- a first `db.dat` prototype writer for the folder/file object graph
- a first `db.idx` observed-chain prototype writer that covers the full target library
- a combined rebuild-prototype writer that emits the whole native work bundle in one run

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

Write a safe rebuild snapshot for one selected directory:

```bash
python3 e10db_tool.py write-rebuild-snapshot /run/media/nichlas/E10 /tmp/e10_snapshot_podcast /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
```

The snapshot currently contains:
- `manifest.json`
- `canonical_entries.json`
- `planned_additions.json`
- `target_library.json`
- `collisions.json`

Write a first `db.dat` prototype from the target library:

```bash
python3 e10db_tool.py write-dbdat-prototype /run/media/nichlas/E10 /tmp/e10_dbdat_proto /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
```

The prototype currently writes:
- `db.dat.prototype`
- `db.dat.records.json`

Write a first `db.idx` prototype from the target library:

```bash
python3 e10db_tool.py write-idx-prototype /run/media/nichlas/E10 /tmp/e10_idx_proto /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
```

The prototype currently writes:
- `db.idx.prototype`
- `db.idx.pages.json`

Write a complete rebuild-prototype bundle in one run:

```bash
python3 e10db_tool.py write-rebuild-prototype /run/media/nichlas/E10 /tmp/e10_rebuild_proto /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
```

The bundle currently writes:
- `manifest.json`
- `canonical_entries.json`
- `planned_additions.json`
- `target_library.json`
- `collisions.json`
- `db.dat.prototype`
- `db.dat.records.json`
- `db.idx.prototype`
- `db.idx.pages.json`
- `db.dic.reference`

## Notes

- The repository intentionally does not vendor PMPlib or other third-party source trees with uncertain mixed licensing.
- The current `db.dat` writer only covers the validated folder/file object graph, not the full E10 database model.
- The current `db.idx` writer now uses an observed chain-style page layout with 24-byte nodes, absolute next-pointers, and inline UTF-16BE payloads, but it is still not the final firmware-compatible layout.
- The next major step is turning the prototype bundle into a real E10-compatible serializer pair.
