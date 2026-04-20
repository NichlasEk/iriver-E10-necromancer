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
python3 e10db_tool.py idx-observed-page /run/media/nichlas/E10 --page 7
python3 e10db_tool.py idx-observed-summary /run/media/nichlas/E10
python3 e10db_tool.py media-cluster /run/media/nichlas/E10 "01 Hello.wma"
python3 e10db_tool.py model-export /run/media/nichlas/E10
python3 e10db_tool.py source-model /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
python3 e10db_tool.py rebuild-plan /run/media/nichlas/E10 /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
python3 e10db_tool.py rebuild-plan /run/media/nichlas/E10 --full-db
python3 e10db_tool.py write-rebuild-snapshot /run/media/nichlas/E10 /tmp/e10_snapshot_podcast /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
python3 e10db_tool.py write-dbdat-prototype /run/media/nichlas/E10 /tmp/e10_dbdat_proto /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
python3 e10db_tool.py write-idx-prototype /run/media/nichlas/E10 /tmp/e10_idx_proto /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
python3 e10db_tool.py write-rebuild-prototype /run/media/nichlas/E10 /tmp/e10_rebuild_proto /run/media/nichlas/E10/Music/Podcast --inventory /run/media/nichlas/E10/Music/Podcast/podcast_inventory.json
python3 e10db_tool.py test-install-prototype /run/media/nichlas/E10 /tmp/e10_rebuild_proto
python3 e10db_tool.py restore-system-backup /run/media/nichlas/E10 /tmp/iriver-e10-backup-YYYYMMDD-HHMMSS
```

What the current tooling gives us:
- `db-summary` now treats `db.dic` separately and reports parsed field definitions instead of garbage strings.
- `schema-summary` counts which field definitions are actually referenced from `db.idx`.
- `media-xref` resolves one exact title or file name across both files and shows the `db.dat` record header plus the matching `db.idx` pointer refs.
- `dat-tree` renders the parseable `db.dat` object records as a parent/child tree. By default it focuses on the validated folder/file kinds (`0x100`, `0x200`), which is useful for modeling folder/file identity and object IDs without the noisier unknown record shapes.
- `idx-page-map` summarizes `db.idx` page by page with inline UTF-16BE strings, `db.dat` record pointers, and `db.dic` field-entry references.
- `idx-observed-page` parses one real `db.idx` page using the current chained-node heuristic, exposing header words, absolute next pointers, payload text, and anchor-group patterns.
- `idx-observed-summary` scans the full `db.idx` and summarizes the observed chain families by anchor class, node-type distribution, and payload class.
- `media-cluster` groups the `db.idx` pages that mention one exact media string, so one track can be studied as a cross-page cluster instead of a single hit.
- `model-export` builds a normalized per-file model from validated folder/file records, inferred ancestry, index coverage, and a simple canonicalization pass over duplicates.
- `source-model` builds the same kind of normalized source view from one or more selected filesystem directories.
- `rebuild-plan` compares selected source directories, or the full `Music/` tree, against the canonical E10 database model.
- `write-rebuild-snapshot` writes a safe on-disk rebuild snapshot with canonical entries, planned additions, collisions, and a merged target library for later binary serialization.
- `write-dbdat-prototype` takes the merged target library and emits a first `db.dat` prototype for the validated folder/file object graph.
- `write-idx-prototype` takes the same target library plus the current `db.dic` schema and emits a first page-aligned observed-chain `db.idx` prototype.
- `write-rebuild-prototype` packages the full current rebuild work product in one directory: snapshot JSON, `db.dat` prototype, `db.idx` prototype, and a `db.dic` reference copy.
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

What `write-rebuild-snapshot` has shown so far:
- The first write phase is now real, but still safe: it writes JSON artifacts rather than touching `System/db.dat` or `System/db.idx`.
- For `Music/Podcast`, the snapshot currently produces 554 canonical existing entries, 32 planned additions, and a merged target library of 586 entries.
- This is now the handoff boundary for the next implementation step: a serializer that converts the snapshot into new E10 database binaries.

What `write-dbdat-prototype` has shown so far:
- The first binary writer now exists for the validated `db.dat` subset: folder and file object records.
- For `Music/Podcast`, the current prototype emits 815 records and reparses cleanly back to 815 validated folder/file records.
- The generated prototype contains the expected `Music/` root, a new `Podcast/` folder under it, and the planned podcast files with provisional object IDs.
- This is still only one part of the final rebuild. The full E10 database also depends on `db.idx` and likely additional object semantics beyond the validated folder/file subset.

What `write-idx-prototype` has shown so far:
- The first native `db.idx` prototype writer now exists and covers the full current target library instead of just top-level items.
- The serializer no longer uses the earlier custom magic-header page format. It now emits observed chain-style pages with a 32-byte page summary, then 24-byte big-endian nodes linked by absolute next offsets, followed by inline UTF-16BE payloads.
- For `Music/Podcast`, the current observed-chain prototype emits 206 pages, reparses cleanly back to 206 pages, and still covers all 586 target-library entries.
- The page summaries now carry deterministic `target_path`, `object_id`, `db.dat` record-start links, and node counts for both existing tracks and planned additions.
- This is still not the final firmware-compatible layout, but it is materially closer to the original E10 pages than the earlier fixed-header prototype.

What `write-rebuild-prototype` gives us:
- The current serializer path can now be run as one command for either one selected subtree or the full `Music/` library.
- The output bundle is self-contained for debugging and iteration: snapshot JSON, `db.dat.prototype`, `db.idx.prototype`, and the reference `db.dic`.
- This is the current handoff boundary for turning the prototype formats into real E10-compatible database files.

What `idx-observed-page` has shown so far:
- On real chained pages like page 7, the header contains at least one absolute pointer back into the same page; that pointer is a strong candidate for the first node in the page-local chain.
- The sixth node word behaves like an absolute next pointer. Following it on page 7 yields a stable 10-node chain.
- The first node word is stable across several adjacent nodes and looks like an anchor for one logical media object. On page 7, `0x0002060c` spans seven nodes and carries three separate `Saturday Night` payloads.
- The fifth node word varies in a small integer range such as `1`, `2`, and `7`, which now looks more like a real node-type or field-kind discriminator than random metadata.
- Extending the parser to look for multiple starts per page shows that a single page can contain several independent chains. On page 7, that now includes a playlist chain, a folder chain for `Oasis/`, and a file chain for `01 Hello.wma`.

What `idx-observed-summary` has shown so far:
- The current snapshot contains about 846 observed chains across 384 pages.
- Those chains already split into useful families:
  - about 309 `dat_audio` chains anchored by `db.dat` file records
  - about 97 `dat_folder` chains anchored by `db.dat` folder records
  - about 426 `non_dat` chains that look like metadata/object-side chains rather than direct file anchors
- `dat_audio` chains are usually short: most are 2-node chains, often one with the extension-bearing filename and another with a title-like payload.
- `non_dat` chains are where richer metadata appears. They commonly carry artist/genre/album-like payload groups and dominate node types `0x2`, `0x3`, and `0x4`.

What the current prototype serializer does with that:
- The `db.idx` prototype writer now emits separate chain families instead of one flat chain per entry.
- For each target audio file it emits:
  - one `dat_audio_file` chain anchored by the `db.dat` record start
  - one `dat_audio_title` chain anchored by the same `db.dat` record start
  - one synthetic `non_dat_metadata` chain anchored by `0x20000 + object_id`
- It also emits `dat_folder` chains for the folder records from the `db.dat` prototype.
- On the current podcast rebuild bundle this yields one file-chain, one title-chain, and one metadata-chain for each of the 586 target entries.

What `test-install-prototype` does:
- It validates the bundle manifest against the generated `db.dat.prototype` and `db.idx.prototype`.
- It pads those prototype files up to the current device file lengths before installation, so the player still sees the expected `System/db.dat` and `System/db.idx` sizes.
- It writes a full backup of `System/db.dat`, `System/db.idx`, and `System/db.dic` to a timestamped directory under `/tmp` before replacing anything.
- `restore-system-backup` can then put the original files back if the firmware does not accept the test install.

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

The next engineering step is to infer the actual meaning of the observed node words and replace the remaining placeholder semantics while keeping the same high-level rebuild pipeline.
