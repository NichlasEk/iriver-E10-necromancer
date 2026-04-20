# E10 Database Notes

This directory now contains a local reverse-engineering helper for the iRiver E10 database.

Files:
- `e10db_tool.py`: CLI for summarizing `System/db.dat`, `db.idx`, and `db.dic`
- `podcast_inventory.json`: optional ffprobe-based inventory for the podcast files

Why this exists:
- E10 does not use the older H10 `H10DB.*` layout.
- The mounted player shows `System/db.dat`, `db.idx`, and `db.dic`.
- `db.dic` exposes field names like `RMusic`, `Artist`, `Album`, `Genre`, `Title`, `FilePath`, `FileName`, `Duration`, `SampleRate`, and `BitRate`.
- `db.dat` and `db.idx` already contain UTF-16LE strings for file paths and file names.
- `db.idx` appears to hold the real object graph and indexes. `db.dat` looks like a string pool or backing store.
- `db.dic` is not UTF-16LE like the other two files. It uses big-endian integers and UTF-16BE field names.
- `db.dic` currently parses into about 60 field definitions, including `Artist`, `Album`, `Genre`, `FilePath`, `FileName`, `Duration`, `SampleRate`, `BitRate`, `Objects`, `ParentUid`, and `Properties`.
- `db.idx` inline strings are not laid out like `db.dat`. They behave like UTF-16BE payloads embedded in a binary object graph, often at odd byte offsets.
- `db.dat` has a repeatable string-record layout: `u32_be object_id`, `u32_be parent_id`, `u32_be kind`, then a UTF-16LE string. Example: `01 Hello.wma` starts at record offset `0x57a` and has header `(0x14, 0x13, 0x200)`.
- `db.idx` stores references to those `db.dat` record starts. Example: the `db.dat` record for `01 Hello.wma` at `0x57a` is referenced from `db.idx` at offsets `0x1fc4` and `0x57e88`.
- PMPlib's `pmp_iriverplus2` source is useful as family reference material, but it is not a direct drop-in parser for this E10 snapshot:
  - PMPlib expects `U10.dat` fixed-size records of `0x13f` bytes, while this `db.dat` is a full `0x80000` byte object store and does not divide cleanly by `0x13f`.
  - PMPlib has no `db.dic` equivalent, while E10 clearly uses `db.dic` as a schema dictionary.
  - `db.idx` still uses `0x400`-byte pages, but the first E10 pages do not line up with the fixed U10 header/descriptor layout from PMPlib.
  - Conclusion: PMPlib is still valuable for index concepts and page mechanics, but E10 needs a native parser/regenerator for its own schema-driven layout.

Useful commands:

```bash
python3 e10db_tool.py db-summary /run/media/nichlas/E10
python3 e10db_tool.py schema-summary /run/media/nichlas/E10
python3 e10db_tool.py missing-media /run/media/nichlas/E10
python3 e10db_tool.py media-inventory /run/media/nichlas/E10/Music/Podcast --out podcast_inventory.json
python3 e10db_tool.py record-context /run/media/nichlas/E10 db.idx "01 Trash.wma"
python3 e10db_tool.py u32-context /run/media/nichlas/E10 db.idx 0xbc --endian big --aligned-only
python3 e10db_tool.py media-xref /run/media/nichlas/E10 "01 Hello.wma"
python3 e10db_tool.py dat-tree /run/media/nichlas/E10
python3 e10db_tool.py idx-page-map /run/media/nichlas/E10 --page 7
python3 e10db_tool.py media-cluster /run/media/nichlas/E10 "01 Hello.wma"
python3 e10db_tool.py model-export /run/media/nichlas/E10
python3 e10db_tool.py source-model /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
python3 e10db_tool.py rebuild-plan /run/media/nichlas/E10 /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
python3 e10db_tool.py rebuild-plan /run/media/nichlas/E10 --full-db
```

What the current tooling gives us:
- `db-summary` now treats `db.dic` separately and reports parsed field definitions instead of garbage strings.
- `schema-summary` counts which field definitions are actually referenced from `db.idx`.
- `media-xref` resolves one exact title or file name across both files and shows the `db.dat` record header plus the matching `db.idx` pointer refs.
- `dat-tree` renders the parseable `db.dat` object records as a parent/child tree. By default it focuses on the validated folder/file kinds (`0x100`, `0x200`), which is useful for modeling folder/file identity and object IDs without the noisier unknown record shapes.
- `idx-page-map` summarizes `db.idx` page by page with inline UTF-16BE strings, `db.dat` record pointers, and `db.dic` field-entry references.
- `media-cluster` groups the `db.idx` pages that mention one exact media string, so one track can be studied as a cross-page cluster instead of a single hit.
- `model-export` builds a normalized per-file model from validated folder/file records, inferred ancestry, index coverage, and a simple canonicalization pass over duplicates.
- `source-model` builds the same kind of normalized source view from one or more selected filesystem directories.
- `rebuild-plan` compares selected source directories, or the full `Music/` tree, against the canonical E10 database model.
- `missing-media` confirms that the 32 podcast episodes in this directory are on disk but absent from the E10 database.

What `media-cluster` has shown so far:
- One media item can be distributed across multiple `db.idx` pages. Example: `01 Hello.wma` appears in one page with `Oasis/` and playlist references, and in another much later page with a `FileFormat` field reference.
- Some tracks expose aligned `db.dat` record pointers in `db.idx`, while others currently show up only as inline UTF-16BE strings on the index pages.
- Duplicate file names can map to multiple parseable `db.dat` records. Example: `07_takida_-_reason_to_cry-tlb.mp3` currently resolves to three separate `db.dat` records with the same text but different offsets/object ancestry.
- The practical consequence is that a future regenerator should rebuild E10's normalized media/index model from scratch, rather than trying to patch one isolated page or append one isolated object.

What `model-export` has shown so far:
- The current snapshot contains 1087 parseable file-shaped `db.dat` entries, but many are shadows or duplicates.
- A simple canonicalization heuristic collapses those to 554 canonical file entries, which matches the earlier visible database audio-name count of 554.
- The heuristic currently prefers deeper ancestry, then stronger `db.idx` pointer coverage, then broader page coverage.
- This gives a workable regenerator-facing invariant: the rebuilt database should target one canonical entry per visible media file, with shadow records eliminated.

What `rebuild-plan` has shown so far:
- For `Music/Podcast`, the current plan is clean: 32 planned additions, 0 exact path collisions, 0 name collisions.
- The selected-folder workflow is therefore good enough to drive a future “regenerate just this subtree” mode.
- The full-db mode exists in the CLI now as `--full-db`, but on a large library it should be paired with cached inventory JSON if you want faster repeat runs.

Current working hypothesis:
- The player database is closer to an object store than a flat list.
- The minimum viable modern workflow is:
  1. inventory the media files
  2. dump the existing database and schema
  3. infer one record layout from existing entries and `db.dic` field refs
  4. append or rebuild records deterministically
  5. verify the player sees the new items

Current known constraints:
- `System/db.dat` is completely full on this device snapshot.
- `System/db.idx` still has a small trailing free region.
- The current `db.idx` string scan is good enough for targeted reverse-engineering, but still too noisy to be treated as a canonical file list.
- The final patcher will probably need either compaction/rebuild or record reuse, not a naive append-only write.

The next engineering step is a generator that targets the canonical media model, not more GUI automation and not one-off page patching.
