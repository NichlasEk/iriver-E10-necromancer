#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import struct
import subprocess
import sys
import unicodedata
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


AUDIO_EXTENSIONS = {".mp3", ".wma", ".ogg", ".asf", ".wav"}
PLAYLIST_EXTENSIONS = {".plp", ".plx", ".m3u", ".pls", ".pla"}
IDX_PAGE_SIZE = 0x400
IDX_PROTO_MAGIC = b"E10IPX1\x00"
IDX_OBSERVED_HEADER_BYTES = 0x20
IDX_OBSERVED_NODE_BYTES = 24


@dataclass
class StringHit:
    offset: int
    text: str


@dataclass
class DicField:
    name: str
    name_offset: int
    entry_offset: int
    next_offset: int | None
    value_type: int | None
    aux_value: int | None
    layout: str


@dataclass
class DatRecord:
    record_start: int
    object_id: int
    parent_id: int
    kind: int
    text_offset: int
    text: str


@dataclass
class NormalizedMediaEntry:
    object_id: int
    parent_id: int
    record_start: int
    file_name: str
    inferred_path: str
    ancestor_object_ids: list[int]
    ancestor_names: list[str]
    idx_ref_offsets: list[int]
    idx_inline_offsets: list[int]
    idx_pages: list[int]
    idx_field_names: list[str]


@dataclass
class SourceMediaEntry:
    path: str
    relative_path: str
    file_name: str
    title: str | None
    artist: str | None
    album: str | None
    genre: str | None
    track_number: int | None
    year: int | None
    size: int
    duration_seconds: float | None
    bit_rate_bps: int | None
    sample_rate_hz: int | None


@dataclass
class TargetLibraryEntry:
    source_kind: str
    target_path: str
    file_name: str
    title: str | None
    artist: str | None
    album: str | None
    genre: str | None
    track_number: int | None
    year: int | None
    size: int | None
    duration_seconds: float | None
    bit_rate_bps: int | None
    sample_rate_hz: int | None
    existing_object_id: int | None
    provisional_object_id: int | None


@dataclass
class TargetDatRecord:
    record_start: int
    object_id: int
    parent_id: int
    kind: int
    text: str
    target_path: str


def is_latinish_char(ch: str) -> bool:
    if ch.isascii():
        return ch.isprintable() and ch not in {"\t", "\n", "\r", "\x0b", "\x0c"}
    category = unicodedata.category(ch)
    if category.startswith("L"):
        return "LATIN" in unicodedata.name(ch, "")
    return category.startswith(("N", "P", "Z"))


def is_reasonable_text_char(ch: str) -> bool:
    if ch in {"\t", "\n", "\r"}:
        return False
    category = unicodedata.category(ch)
    if category.startswith("C"):
        return False
    return True


def text_quality_ok(text: str) -> bool:
    if not text:
        return False
    latinish = sum(1 for ch in text if is_latinish_char(ch))
    ratio = latinish / len(text)
    return ratio >= 0.65


def decode_utf16_codepoint(data: bytes, offset: int, byteorder: str) -> int:
    if byteorder == "little":
        return data[offset] | (data[offset + 1] << 8)
    return (data[offset] << 8) | data[offset + 1]


def trim_leading_noise(text: str, offset: int) -> tuple[int, str]:
    trimmed = 0
    while text and not likely_media_string_start(text[0]):
        text = text[1:]
        trimmed += 1
    return offset + trimmed * 2, text


def dedupe_string_hits(hits: list[StringHit]) -> list[StringHit]:
    hits.sort(key=lambda hit: (hit.offset, -len(hit.text), hit.text))
    deduped: list[StringHit] = []
    last_end = -1
    seen: set[tuple[int, str]] = set()
    for hit in hits:
        key = (hit.offset, hit.text)
        if key in seen:
            continue
        seen.add(key)
        hit_end = hit.offset + len(hit.text) * 2 + 2
        if hit.offset < last_end:
            continue
        deduped.append(hit)
        last_end = hit_end
    return deduped


def extract_utf16_strings(
    data: bytes,
    min_chars: int = 4,
    byteorder: str = "little",
    offset_step: int = 2,
    normalize_leading_noise: bool = False,
) -> list[StringHit]:
    hits: list[StringHit] = []
    i = 0
    data_len = len(data)
    while i + 1 < data_len:
        chars: list[str] = []
        j = i
        while j + 1 < data_len:
            codepoint = decode_utf16_codepoint(data, j, byteorder)
            if codepoint == 0:
                break
            ch = chr(codepoint)
            if not is_reasonable_text_char(ch):
                break
            chars.append(ch)
            j += 2
        if len(chars) >= min_chars and j + 1 < data_len and data[j:j + 2] == b"\x00\x00":
            text = "".join(chars)
            offset = i
            if normalize_leading_noise:
                offset, text = trim_leading_noise(text, offset)
            if text_quality_ok(text):
                hits.append(StringHit(offset=offset, text=text))
            i += offset_step
        else:
            i += offset_step
    return dedupe_string_hits(hits)


def extract_utf16le_strings(data: bytes, min_chars: int = 4) -> list[StringHit]:
    return extract_utf16_strings(data, min_chars=min_chars, byteorder="little")


def extract_utf16be_strings(data: bytes, min_chars: int = 4) -> list[StringHit]:
    return extract_utf16_strings(data, min_chars=min_chars, byteorder="big")


def extract_db_dat_strings(data: bytes, min_chars: int = 4) -> list[StringHit]:
    return extract_utf16_strings(
        data,
        min_chars=min_chars,
        byteorder="little",
        offset_step=1,
    )


def extract_db_idx_strings(data: bytes, min_chars: int = 4) -> list[StringHit]:
    return extract_utf16_strings(
        data,
        min_chars=min_chars,
        byteorder="big",
        offset_step=1,
        normalize_leading_noise=True,
    )


def last_nonzero_offset(data: bytes) -> int:
    for index in range(len(data) - 1, -1, -1):
        if data[index] != 0:
            return index
    return -1


def iter_files(root: Path) -> Iterable[Path]:
    for path in sorted(root.rglob("*")):
        if path.is_file():
            yield path


def file_extension_members(paths: Iterable[Path], exts: set[str]) -> list[Path]:
    return [path for path in paths if path.suffix.lower() in exts]


def find_db_files(root: Path) -> dict[str, Path]:
    system_dir = root / "System"
    files = {
        "db.dat": system_dir / "db.dat",
        "db.idx": system_dir / "db.idx",
        "db.dic": system_dir / "db.dic",
    }
    missing = [name for name, path in files.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing database files under {system_dir}: {', '.join(missing)}")
    return files


def summarized_strings(hits: list[StringHit], limit: int = 20) -> list[dict[str, object]]:
    return [asdict(hit) for hit in hits[:limit]]


def filesystem_audio_names(root: Path) -> list[str]:
    return [path.name for path in file_extension_members(iter_files(root), AUDIO_EXTENSIONS)]


def likely_media_string_start(ch: str) -> bool:
    return (ch.isascii() and ch.isalnum()) or ch in {"(", "[", "/", "_", "'", "\""}


def normalize_index_string(text: str) -> str:
    text = text.strip()
    while text.startswith("./"):
        text = text[2:]
    while text and not likely_media_string_start(text[0]):
        text = text[1:]
    return text.strip()


def database_audio_names(db_dat_hits: list[StringHit], db_idx_hits: list[StringHit]) -> list[str]:
    names: set[str] = set()
    for hit in db_dat_hits + db_idx_hits:
        normalized = normalize_index_string(hit.text)
        suffix = Path(normalized).suffix.lower()
        if suffix in AUDIO_EXTENSIONS:
            names.add(Path(normalized).name)
    return sorted(names)


def find_all_occurrences(data: bytes, needle: bytes, step: int = 1) -> list[int]:
    offsets: list[int] = []
    start = 0
    while True:
        idx = data.find(needle, start)
        if idx < 0:
            return offsets
        offsets.append(idx)
        start = idx + step


def is_plausible_dic_name(text: str) -> bool:
    if len(text) < 2:
        return False
    if not text[0].isalpha():
        return False
    return all(ch.isalnum() or ch in {"_", "/"} for ch in text)


def infer_dic_field(data: bytes, hit: StringHit) -> DicField | None:
    if not is_plausible_dic_name(hit.text):
        return None

    if hit.offset >= 12:
        next_offset, value_type, aux_value = struct.unpack(">III", data[hit.offset - 12:hit.offset])
        if value_type in {1, 2, 3, 4} and hit.offset < next_offset <= len(data):
            return DicField(
                name=hit.text,
                name_offset=hit.offset,
                entry_offset=hit.offset - 12,
                next_offset=next_offset,
                value_type=value_type,
                aux_value=aux_value,
                layout="next,type,aux",
            )

    if hit.offset >= 16:
        value_type, _unknown, next_offset, aux_value = struct.unpack(">IIII", data[hit.offset - 16:hit.offset])
        if value_type in {1, 2, 3, 4} and hit.offset < next_offset <= len(data):
            return DicField(
                name=hit.text,
                name_offset=hit.offset,
                entry_offset=hit.offset - 16,
                next_offset=next_offset,
                value_type=value_type,
                aux_value=aux_value,
                layout="type,unknown,next,aux",
            )

    return None


def parse_db_dic(data: bytes) -> list[DicField]:
    raw_hits = extract_utf16be_strings(data, min_chars=2)
    fields: list[DicField] = []
    seen_offsets: set[int] = set()
    for hit in raw_hits:
        field = infer_dic_field(data, hit)
        if field is None:
            continue
        if field.entry_offset in seen_offsets:
            continue
        fields.append(field)
        seen_offsets.add(field.entry_offset)
    return fields


def count_aligned_be_u32(data: bytes, value: int) -> int:
    needle = struct.pack(">I", value)
    count = 0
    start = 0
    while True:
        idx = data.find(needle, start)
        if idx < 0:
            return count
        if idx % 4 == 0:
            count += 1
        start = idx + 1


def summarize_dic_fields(fields: list[DicField], idx_data: bytes, limit: int = 40) -> list[dict[str, object]]:
    summary = []
    for field in fields[:limit]:
        entry = asdict(field)
        entry["entry_ref_count_in_idx"] = count_aligned_be_u32(idx_data, field.entry_offset)
        entry["aux_ref_count_in_idx"] = count_aligned_be_u32(idx_data, field.aux_value) if field.aux_value else 0
        summary.append(entry)
    return summary


def parse_dat_record_at(data: bytes, record_start: int) -> DatRecord | None:
    if record_start < 0 or record_start + 14 > len(data):
        return None
    object_id, parent_id, kind = struct.unpack(">III", data[record_start:record_start + 12])
    text_offset = record_start + 12
    chars: list[str] = []
    j = text_offset
    while j + 1 < len(data):
        codepoint = decode_utf16_codepoint(data, j, "little")
        if codepoint == 0:
            break
        ch = chr(codepoint)
        if not is_reasonable_text_char(ch):
            return None
        chars.append(ch)
        j += 2
    if len(chars) < 2 or j + 1 >= len(data) or data[j:j + 2] != b"\x00\x00":
        return None
    text = "".join(chars)
    if not text_quality_ok(text):
        return None
    return DatRecord(
        record_start=record_start,
        object_id=object_id,
        parent_id=parent_id,
        kind=kind,
        text_offset=text_offset,
        text=text,
    )


def dat_records_by_text(data: bytes, text: str) -> list[DatRecord]:
    needle = text.encode("utf-16le")
    records: list[DatRecord] = []
    seen: set[int] = set()
    for match_offset in find_all_occurrences(data, needle, step=2):
        record = parse_dat_record_at(data, match_offset - 12)
        if record is None or record.text != text:
            continue
        if record.record_start in seen:
            continue
        records.append(record)
        seen.add(record.record_start)
    return records


def collect_dat_records(data: bytes) -> list[DatRecord]:
    hits = extract_db_dat_strings(data, min_chars=2)
    records: list[DatRecord] = []
    seen: set[int] = set()
    for hit in hits:
        record = parse_dat_record_at(data, hit.offset - 12)
        if record is None:
            continue
        if record.record_start in seen:
            continue
        records.append(record)
        seen.add(record.record_start)
    return records


def aligned_u32_refs(data: bytes, value: int, endian: str = "big") -> list[int]:
    needle = struct.pack(">I", value) if endian == "big" else struct.pack("<I", value)
    return [offset for offset in find_all_occurrences(data, needle) if offset % 4 == 0]


def kind_name(kind: int) -> str:
    return {
        0x100: "folder",
        0x200: "file",
    }.get(kind, f"0x{kind:x}")


def build_annotation_maps(dic_fields: list[DicField], dat_records: list[DatRecord]) -> dict[str, dict[int, str]]:
    field_map: dict[int, str] = {}
    aux_map: dict[int, str] = {}
    dat_map: dict[int, str] = {}
    for field in dic_fields:
        if field.entry_offset not in field_map:
            field_map[field.entry_offset] = field.name
        if field.aux_value:
            aux_map.setdefault(field.aux_value, field.name)
    for record in dat_records:
        dat_map.setdefault(record.record_start, record.text)
    return {
        "field_entry": field_map,
        "field_aux": aux_map,
        "dat_record": dat_map,
    }


def collect_idx_page_links(
    block: bytes,
    page_offset: int,
    dat_map: dict[int, str],
    field_map: dict[int, str],
) -> tuple[list[StringHit], list[dict[str, object]], list[dict[str, object]]]:
    strings = extract_db_idx_strings(block, min_chars=4)
    dat_refs = []
    field_refs = []
    for rel in range(0, len(block) - 3, 4):
        value = struct.unpack(">I", block[rel:rel + 4])[0]
        absolute_offset = page_offset + rel
        if value in dat_map:
            dat_refs.append({"offset": absolute_offset, "record_start": value, "text": dat_map[value]})
        if value in field_map:
            field_refs.append({"offset": absolute_offset, "entry_offset": value, "name": field_map[value]})
    return strings, dat_refs, field_refs


def summarize_idx_page(
    db_idx: bytes,
    page_index: int,
    dat_map: dict[int, str],
    field_map: dict[int, str],
    max_samples: int = 3,
) -> dict[str, object]:
    offset = page_index * IDX_PAGE_SIZE
    block = db_idx[offset:offset + IDX_PAGE_SIZE]
    strings, dat_refs, field_refs = collect_idx_page_links(block, offset, dat_map, field_map)
    page_type = "opaque"
    if strings and dat_refs:
        page_type = "strings+dat-refs"
    elif strings and field_refs:
        page_type = "strings+field-refs"
    elif dat_refs:
        page_type = "dat-refs"
    elif field_refs:
        page_type = "field-refs"
    elif strings:
        page_type = "strings"
    return {
        "page_index": page_index,
        "offset": offset,
        "nonzero_bytes": sum(1 for byte in block if byte),
        "first_u32_be": [f"0x{struct.unpack('>I', block[i:i + 4])[0]:08x}" for i in range(0, min(16, len(block)), 4)],
        "last_u32_be": [f"0x{struct.unpack('>I', block[i:i + 4])[0]:08x}" for i in range(max(0, len(block) - 16), len(block), 4)],
        "page_type": page_type,
        "inline_string_count": len(strings),
        "inline_strings": [{"offset": offset + hit.offset, "text": hit.text} for hit in strings[:max_samples]],
        "dat_ref_count": len(dat_refs),
        "dat_ref_samples": [{"offset": ref["offset"], "text": ref["text"]} for ref in dat_refs[:max_samples]],
        "field_ref_count": len(field_refs),
        "field_ref_samples": [{"offset": ref["offset"], "name": ref["name"]} for ref in field_refs[:max_samples]],
    }


def decode_observed_idx_payload_text(payload: bytes) -> str | None:
    if not payload or len(payload) % 2 != 0:
        return None
    try:
        text = payload.decode("utf-16be").split("\x00", 1)[0]
    except UnicodeDecodeError:
        return None
    if not text or not all(is_reasonable_text_char(ch) for ch in text):
        return None
    return text


def parse_observed_idx_page(
    db_idx: bytes,
    page_index: int,
    annotation_maps: dict[str, dict[int, str]],
    max_nodes: int = 64,
    max_groups: int = 24,
) -> dict[str, object]:
    page_offset = page_index * IDX_PAGE_SIZE
    block = db_idx[page_offset:page_offset + IDX_PAGE_SIZE]
    if len(block) < IDX_PAGE_SIZE:
        raise ValueError(f"page {page_index} is out of range")

    header_words = [struct.unpack_from(">I", block, offset)[0] for offset in range(0, IDX_OBSERVED_HEADER_BYTES, 4)]
    header_page_pointers = [
        value
        for value in header_words
        if page_offset + IDX_OBSERVED_HEADER_BYTES <= value < page_offset + IDX_PAGE_SIZE
    ]
    first_node_abs = min(header_page_pointers) if header_page_pointers else header_words[0]
    last_nonzero = last_nonzero_offset(block)
    page_end_used = last_nonzero + 1 if last_nonzero >= 0 else 0

    nodes: list[dict[str, object]] = []
    groups: dict[int, list[dict[str, object]]] = {}
    chains: list[dict[str, object]] = []
    node_offsets_in_order: list[int] = []

    def parse_candidate_node(rel: int) -> tuple[list[int], bytes, str | None] | None:
        if rel + IDX_OBSERVED_NODE_BYTES > IDX_PAGE_SIZE:
            return None
        words = [struct.unpack_from(">I", block, rel + off)[0] for off in range(0, IDX_OBSERVED_NODE_BYTES, 4)]
        next_abs = words[5]
        next_rel = next_abs - page_offset if page_offset <= next_abs < page_offset + IDX_PAGE_SIZE else None
        if next_rel is not None and next_rel <= rel + IDX_OBSERVED_NODE_BYTES:
            return None
        payload_start = rel + IDX_OBSERVED_NODE_BYTES
        if next_rel is not None and next_rel > payload_start:
            payload_end = next_rel
        else:
            payload_end = page_end_used
        if payload_end <= payload_start:
            payload = b""
        else:
            payload = block[payload_start:payload_end]
        payload_text = decode_observed_idx_payload_text(payload)
        return words, payload, payload_text

    def consume_chain(start_abs: int) -> None:
        current_abs = start_abs
        chain_start_index = len(nodes)
        while (
            len(nodes) < max_nodes
            and page_offset <= current_abs < page_offset + IDX_PAGE_SIZE
        ):
            rel = current_abs - page_offset
            if rel in seen_offsets or rel + IDX_OBSERVED_NODE_BYTES > IDX_PAGE_SIZE:
                break
            parsed = parse_candidate_node(rel)
            if parsed is None:
                break
            words, payload, payload_text = parsed
            next_abs = words[5]
            next_rel = next_abs - page_offset if page_offset <= next_abs < page_offset + IDX_PAGE_SIZE else None
            seen_offsets.add(rel)
            node_offsets_in_order.append(rel)
            annotations = [annotate_u32_value(value, annotation_maps) for value in words]
            node = {
                "start_offset": page_offset + rel,
                "start_rel": rel,
                "words": [f"0x{value:08x}" for value in words],
                "word_annotations": annotations,
                "payload_length": len(payload),
                "payload_text": payload_text,
                "next_offset": next_abs if next_rel is not None else None,
                "next_rel": next_rel,
            }
            nodes.append(node)
            groups.setdefault(words[0], []).append(node)
            if next_rel is None or next_abs == 0:
                break
            current_abs = next_abs
        chain_nodes = nodes[chain_start_index:]
        if chain_nodes:
            anchor_word = int(chain_nodes[0]["words"][0], 16)
            chains.append(
                {
                    "start_offset": chain_nodes[0]["start_offset"],
                    "start_rel": chain_nodes[0]["start_rel"],
                    "node_count": len(chain_nodes),
                    "anchor_value": anchor_word,
                    "anchor_value_hex": chain_nodes[0]["words"][0],
                    "anchor_annotation": chain_nodes[0]["word_annotations"][0],
                    "payload_texts": [node["payload_text"] for node in chain_nodes if node["payload_text"]][:8],
                }
            )

    current_abs = first_node_abs
    seen_offsets: set[int] = set()
    consume_chain(current_abs)

    for rel in range(IDX_OBSERVED_HEADER_BYTES, IDX_PAGE_SIZE - IDX_OBSERVED_NODE_BYTES + 1, 2):
        if rel in seen_offsets:
            continue
        parsed = parse_candidate_node(rel)
        if parsed is None:
            continue
        words, payload, payload_text = parsed
        next_abs = words[5]
        next_rel = next_abs - page_offset if page_offset <= next_abs < page_offset + IDX_PAGE_SIZE else None
        if next_rel is None or next_rel <= rel + IDX_OBSERVED_NODE_BYTES:
            continue
        if payload_text is None:
            continue
        consume_chain(page_offset + rel)
        if len(nodes) >= max_nodes:
            break

    group_summaries = []
    for anchor_value, grouped_nodes in sorted(groups.items(), key=lambda item: (-len(item[1]), item[0]))[:max_groups]:
        group_summaries.append(
            {
                "anchor_value": anchor_value,
                "anchor_value_hex": f"0x{anchor_value:08x}",
                "anchor_annotation": annotate_u32_value(anchor_value, annotation_maps),
                "node_count": len(grouped_nodes),
                "payload_texts": [node["payload_text"] for node in grouped_nodes if node["payload_text"]][:8],
                "fieldish_annotations": sorted(
                    {
                        annotation
                        for node in grouped_nodes
                        for annotation in node["word_annotations"]
                        if annotation and annotation.startswith(("field:", "field_aux:"))
                    }
                ),
            }
        )

    return {
        "page_index": page_index,
        "offset": page_offset,
        "nonzero_bytes": sum(1 for byte in block if byte),
        "header_words": [f"0x{value:08x}" for value in header_words],
        "header_annotations": [annotate_u32_value(value, annotation_maps) for value in header_words],
        "header_page_pointers": [f"0x{value:08x}" for value in header_page_pointers],
        "observed_first_node_offset": first_node_abs,
        "observed_last_nonzero_offset": page_offset + last_nonzero if last_nonzero >= 0 else None,
        "parsed_node_count": len(nodes),
        "chain_count": len(chains),
        "chains": chains,
        "nodes": nodes,
        "anchor_groups": group_summaries,
    }


def classify_payload_text(text: str) -> str:
    suffix = Path(text).suffix.lower()
    if text.endswith("/"):
        return "folder"
    if suffix in AUDIO_EXTENSIONS:
        return "audio_file"
    if suffix in PLAYLIST_EXTENSIONS:
        return "playlist"
    if "/" in text:
        return "path_like"
    return "metadata"


def render_dat_tree_node(
    record: DatRecord,
    children_by_parent: dict[int, list[DatRecord]],
    max_depth: int,
    max_children: int,
    depth: int = 0,
    visited: set[int] | None = None,
) -> dict[str, object]:
    node = {
        "object_id": record.object_id,
        "parent_id": record.parent_id,
        "kind": record.kind,
        "kind_name": kind_name(record.kind),
        "record_start": record.record_start,
        "text": record.text,
    }
    if depth >= max_depth:
        node["child_count"] = len(children_by_parent.get(record.object_id, []))
        return node
    if visited is None:
        visited = set()
    if record.object_id in visited:
        node["cycle"] = True
        return node
    visited = set(visited)
    visited.add(record.object_id)
    children = sorted(children_by_parent.get(record.object_id, []), key=lambda item: (item.kind, item.text.lower(), item.object_id))
    node["children"] = [
        render_dat_tree_node(child, children_by_parent, max_depth=max_depth, max_children=max_children, depth=depth + 1, visited=visited)
        for child in children[:max_children]
    ]
    if len(children) > max_children:
        node["children_truncated"] = len(children) - max_children
    return node


def validated_folder_file_records(data: bytes) -> list[DatRecord]:
    return [
        record for record in collect_dat_records(data)
        if record.kind in {0x100, 0x200} and 0 < record.object_id < 0xFFFFFFFF
    ]


def clean_folder_name(text: str) -> str:
    return text[:-1] if text.endswith("/") else text


def infer_ancestor_chain(record: DatRecord, records_by_id: dict[int, DatRecord]) -> list[DatRecord]:
    chain: list[DatRecord] = []
    seen: set[int] = set()
    current_parent = record.parent_id
    while current_parent in records_by_id and current_parent not in seen:
        parent = records_by_id[current_parent]
        chain.append(parent)
        seen.add(current_parent)
        current_parent = parent.parent_id
    chain.reverse()
    return chain


def infer_record_path(record: DatRecord, records_by_id: dict[int, DatRecord]) -> tuple[list[int], list[str], str]:
    ancestors = infer_ancestor_chain(record, records_by_id)
    ancestor_ids = [ancestor.object_id for ancestor in ancestors]
    ancestor_names = [clean_folder_name(ancestor.text) for ancestor in ancestors if ancestor.kind == 0x100]
    leaf_name = clean_folder_name(record.text) if record.kind == 0x100 else record.text
    inferred_path = "/".join([*ancestor_names, leaf_name]) if ancestor_names else leaf_name
    return ancestor_ids, ancestor_names, inferred_path


def build_idx_page_cache(
    db_idx: bytes,
    dat_map: dict[int, str],
    field_map: dict[int, str],
) -> dict[int, dict[str, object]]:
    cache: dict[int, dict[str, object]] = {}
    page_count = len(db_idx) // IDX_PAGE_SIZE
    for page_index in range(page_count):
        page_offset = page_index * IDX_PAGE_SIZE
        block = db_idx[page_offset:page_offset + IDX_PAGE_SIZE]
        strings, dat_refs, field_refs = collect_idx_page_links(block, page_offset, dat_map, field_map)
        cache[page_index] = {
            "strings": strings,
            "dat_refs": dat_refs,
            "field_refs": field_refs,
        }
    return cache


def build_normalized_media_entries(
    db_dat: bytes,
    db_idx: bytes,
    db_dic: bytes,
) -> list[NormalizedMediaEntry]:
    records = validated_folder_file_records(db_dat)
    records_by_id = {record.object_id: record for record in records}
    file_records = [record for record in records if record.kind == 0x200]
    dic_fields = parse_db_dic(db_dic)
    dat_map = {record.record_start: record.text for record in records}
    field_map = {field.entry_offset: field.name for field in dic_fields}
    idx_page_cache = build_idx_page_cache(db_idx, dat_map, field_map)

    entries: list[NormalizedMediaEntry] = []
    for record in file_records:
        ancestor_ids, ancestor_names, inferred_path = infer_record_path(record, records_by_id)
        idx_ref_offsets = aligned_u32_refs(db_idx, record.record_start, endian="big")
        idx_inline_offsets = find_all_occurrences(db_idx, record.text.encode("utf-16be"))
        page_indices = sorted({offset // IDX_PAGE_SIZE for offset in idx_ref_offsets + idx_inline_offsets})
        idx_field_names = sorted(
            {
                ref["name"]
                for page_index in page_indices
                for ref in idx_page_cache[page_index]["field_refs"]
            }
        )
        entries.append(
            NormalizedMediaEntry(
                object_id=record.object_id,
                parent_id=record.parent_id,
                record_start=record.record_start,
                file_name=record.text,
                inferred_path=inferred_path,
                ancestor_object_ids=ancestor_ids,
                ancestor_names=ancestor_names,
                idx_ref_offsets=idx_ref_offsets,
                idx_inline_offsets=idx_inline_offsets,
                idx_pages=page_indices,
                idx_field_names=idx_field_names,
            )
        )
    entries.sort(key=lambda entry: (entry.inferred_path.lower(), entry.object_id, entry.record_start))
    return entries


def canonical_entry_score(entry: NormalizedMediaEntry) -> tuple[int, int, int, int, int]:
    return (
        len(entry.ancestor_names),
        len(entry.idx_ref_offsets),
        len(entry.idx_pages),
        len(entry.idx_field_names),
        -entry.record_start,
    )


def canonicalize_entries(entries: list[NormalizedMediaEntry]) -> dict[str, object]:
    grouped: dict[str, list[NormalizedMediaEntry]] = {}
    for entry in entries:
        grouped.setdefault(entry.file_name, []).append(entry)

    canonical_entries: list[NormalizedMediaEntry] = []
    shadow_groups: list[dict[str, object]] = []
    for file_name, group in sorted(grouped.items()):
        ordered = sorted(group, key=canonical_entry_score, reverse=True)
        canonical_entries.append(ordered[0])
        if len(ordered) > 1:
            shadow_groups.append(
                {
                    "file_name": file_name,
                    "canonical": asdict(ordered[0]),
                    "shadowed": [asdict(entry) for entry in ordered[1:]],
                }
            )

    canonical_entries.sort(key=lambda entry: (entry.inferred_path.lower(), entry.record_start))
    return {
        "canonical_entries": canonical_entries,
        "shadow_groups": shadow_groups,
    }


def annotate_u32_value(value: int, annotation_maps: dict[str, dict[int, str]]) -> str | None:
    if value in annotation_maps["field_entry"]:
        return f"field:{annotation_maps['field_entry'][value]}"
    if value in annotation_maps["dat_record"]:
        return f"dat:{annotation_maps['dat_record'][value]}"
    if value in annotation_maps["field_aux"]:
        return f"field_aux:{annotation_maps['field_aux'][value]}"
    return None


def u32_context_entries(
    data: bytes,
    match_offset: int,
    context: int = 64,
    annotation_maps: dict[str, dict[int, str]] | None = None,
) -> list[dict[str, object]]:
    left = max(0, match_offset - context)
    right = min(len(data), match_offset + 4 + context)
    aligned_left = left - (left % 4)
    aligned_right = min(len(data), right + (4 - (right % 4)) % 4)
    entries = []
    for off in range(aligned_left, aligned_right, 4):
        if off + 4 > len(data):
            break
        chunk = data[off:off + 4]
        be_value = struct.unpack(">I", chunk)[0]
        entries.append(
            {
                "offset": off,
                "big_endian_hex": f"{be_value:08x}",
                "little_endian_hex": f"{struct.unpack('<I', chunk)[0]:08x}",
                **({"annotation": annotate_u32_value(be_value, annotation_maps)} if annotation_maps else {}),
            }
        )
    return entries


def probe_media_file(path: Path) -> dict[str, object]:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration,bit_rate:format_tags=title,artist,album,genre,track,date:stream=codec_type,sample_rate,bit_rate",
        "-of",
        "json",
        str(path),
    ]
    completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
    result: dict[str, object] = {
        "path": str(path),
        "name": path.name,
        "size": path.stat().st_size,
    }
    if completed.returncode != 0:
        result["ffprobe_error"] = completed.stderr.strip()
        return result

    payload = json.loads(completed.stdout or "{}")
    result["duration_seconds"] = None
    result["bit_rate_bps"] = None
    result["sample_rate_hz"] = None

    fmt = payload.get("format", {})
    if isinstance(fmt, dict):
        if fmt.get("duration") is not None:
            result["duration_seconds"] = float(fmt["duration"])
        if fmt.get("bit_rate") is not None:
            result["bit_rate_bps"] = int(fmt["bit_rate"])
        tags = fmt.get("tags", {})
        if isinstance(tags, dict):
            for key in ("title", "artist", "album", "genre", "track", "date"):
                if key in tags:
                    result[key] = tags[key]

    streams = payload.get("streams", [])
    if isinstance(streams, list):
        for stream in streams:
            if stream.get("codec_type") != "audio":
                continue
            if stream.get("sample_rate") is not None:
                result["sample_rate_hz"] = int(stream["sample_rate"])
            if stream.get("bit_rate") is not None and result["bit_rate_bps"] is None:
                result["bit_rate_bps"] = int(stream["bit_rate"])
            break

    return result


def parse_track_number(value: object) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    head = value.split("/", 1)[0].strip()
    return int(head) if head.isdigit() else None


def parse_year(value: object) -> int | None:
    if not isinstance(value, str) or len(value) < 4:
        return None
    head = value[:4]
    return int(head) if head.isdigit() else None


def load_source_inventory(media_dir: Path, inventory_path: Path | None = None) -> list[SourceMediaEntry]:
    if inventory_path:
        payload = json.loads(inventory_path.read_text(encoding="utf-8"))
    else:
        files = [path for path in iter_files(media_dir) if path.suffix.lower() in AUDIO_EXTENSIONS]
        payload = [probe_media_file(path) for path in files]

    entries: list[SourceMediaEntry] = []
    for item in payload:
        path = Path(item["path"]).resolve()
        try:
            relative_path = path.relative_to(media_dir.resolve()).as_posix()
        except ValueError:
            relative_path = path.name
        entries.append(
            SourceMediaEntry(
                path=str(path),
                relative_path=relative_path,
                file_name=path.name,
                title=item.get("title"),
                artist=item.get("artist"),
                album=item.get("album"),
                genre=item.get("genre"),
                track_number=parse_track_number(item.get("track")),
                year=parse_year(item.get("date")),
                size=int(item["size"]),
                duration_seconds=float(item["duration_seconds"]) if item.get("duration_seconds") is not None else None,
                bit_rate_bps=int(item["bit_rate_bps"]) if item.get("bit_rate_bps") is not None else None,
                sample_rate_hz=int(item["sample_rate_hz"]) if item.get("sample_rate_hz") is not None else None,
            )
        )
    entries.sort(key=lambda entry: entry.relative_path.lower())
    return entries


def load_source_entries_from_dirs(
    root: Path,
    media_dirs: list[Path],
    inventory_paths: list[Path] | None = None,
) -> tuple[list[SourceMediaEntry], list[dict[str, str]]]:
    entries: list[SourceMediaEntry] = []
    manifests: list[dict[str, str]] = []
    inventory_paths = inventory_paths or []

    for index, media_dir in enumerate(media_dirs):
        inventory_path = inventory_paths[index] if index < len(inventory_paths) else None
        dir_entries = load_source_inventory(media_dir, inventory_path=inventory_path)
        try:
            relative_media_dir = media_dir.relative_to(root).as_posix()
        except ValueError:
            relative_media_dir = media_dir.name
        manifests.append(
            {
                "media_dir": str(media_dir),
                "relative_media_dir": relative_media_dir,
                **({"inventory": str(inventory_path)} if inventory_path else {}),
            }
        )
        entries.extend(dir_entries)

    entries.sort(key=lambda entry: entry.path.lower())
    return entries, manifests


def build_rebuild_plan_data(
    root: Path,
    media_dirs: list[Path],
    full_db: bool = False,
    inventory_paths: list[Path] | None = None,
    max_collisions: int = 8,
) -> dict[str, object]:
    selected_media_dirs = [root / "Music"] if full_db else media_dirs
    if not selected_media_dirs:
        raise ValueError("Provide at least one media directory, or use --full-db")

    files = find_db_files(root)
    db_entries = build_normalized_media_entries(
        files["db.dat"].read_bytes(),
        files["db.idx"].read_bytes(),
        files["db.dic"].read_bytes(),
    )
    canonicalized = canonicalize_entries(db_entries)
    canonical_entries: list[NormalizedMediaEntry] = canonicalized["canonical_entries"]
    source_entries, manifests = load_source_entries_from_dirs(root, selected_media_dirs, inventory_paths=inventory_paths)

    canonical_by_name: dict[str, list[NormalizedMediaEntry]] = {}
    canonical_by_path: dict[str, list[NormalizedMediaEntry]] = {}
    for entry in canonical_entries:
        canonical_by_name.setdefault(entry.file_name, []).append(entry)
        canonical_by_path.setdefault(entry.inferred_path, []).append(entry)

    planned_additions = []
    exact_path_collisions = []
    name_collisions = []
    for entry in source_entries:
        absolute_path = Path(entry.path).resolve()
        try:
            target_path = absolute_path.relative_to(root).as_posix()
        except ValueError:
            target_path = entry.relative_path
        same_path = canonical_by_path.get(target_path, [])
        same_name = canonical_by_name.get(entry.file_name, [])
        if same_path:
            exact_path_collisions.append(
                {
                    "target_path": target_path,
                    "source": asdict(entry),
                    "existing": [asdict(existing) for existing in same_path[:max_collisions]],
                }
            )
            continue
        if same_name:
            name_collisions.append(
                {
                    "target_path": target_path,
                    "source": asdict(entry),
                    "existing": [asdict(existing) for existing in same_name[:max_collisions]],
                }
            )
        planned_additions.append(
            {
                "target_path": target_path,
                "source": asdict(entry),
            }
        )

    return {
        "canonical_entries": canonical_entries,
        "source_entries": source_entries,
        "selected_sources": manifests,
        "mode": "full-db" if full_db else "selected-dirs",
        "planned_additions": planned_additions,
        "exact_path_collisions": exact_path_collisions,
        "name_collisions": name_collisions,
    }


def build_target_library_entries(
    canonical_entries: list[NormalizedMediaEntry],
    planned_additions: list[dict[str, object]],
) -> list[TargetLibraryEntry]:
    entries: list[TargetLibraryEntry] = []
    max_object_id = max((entry.object_id for entry in canonical_entries), default=0)
    next_object_id = max_object_id + 1

    for entry in canonical_entries:
        entries.append(
            TargetLibraryEntry(
                source_kind="existing",
                target_path=entry.inferred_path,
                file_name=entry.file_name,
                title=None,
                artist=None,
                album=None,
                genre=None,
                track_number=None,
                year=None,
                size=None,
                duration_seconds=None,
                bit_rate_bps=None,
                sample_rate_hz=None,
                existing_object_id=entry.object_id,
                provisional_object_id=None,
            )
        )

    for item in planned_additions:
        source = item["source"]
        entries.append(
            TargetLibraryEntry(
                source_kind="planned_addition",
                target_path=item["target_path"],
                file_name=source["file_name"],
                title=source.get("title"),
                artist=source.get("artist"),
                album=source.get("album"),
                genre=source.get("genre"),
                track_number=source.get("track_number"),
                year=source.get("year"),
                size=source.get("size"),
                duration_seconds=source.get("duration_seconds"),
                bit_rate_bps=source.get("bit_rate_bps"),
                sample_rate_hz=source.get("sample_rate_hz"),
                existing_object_id=None,
                provisional_object_id=next_object_id,
            )
        )
        next_object_id += 1

    entries.sort(key=lambda entry: (entry.target_path.lower(), entry.source_kind, entry.provisional_object_id or -1))
    return entries


def build_existing_folder_entries(db_dat: bytes) -> dict[str, DatRecord]:
    records = validated_folder_file_records(db_dat)
    records_by_id = {record.object_id: record for record in records}
    folder_map: dict[str, DatRecord] = {}
    for record in records:
        if record.kind != 0x100:
            continue
        _ancestor_ids, _ancestor_names, inferred_path = infer_record_path(record, records_by_id)
        existing = folder_map.get(inferred_path)
        if existing is None or record.record_start < existing.record_start:
            folder_map[inferred_path] = record
    return folder_map


def target_entry_object_id(entry: TargetLibraryEntry) -> int:
    if entry.existing_object_id is not None:
        return entry.existing_object_id
    if entry.provisional_object_id is not None:
        return entry.provisional_object_id
    raise ValueError(f"Target entry has no object id: {entry.target_path}")


def serialize_dat_record_bytes(object_id: int, parent_id: int, kind: int, text: str) -> bytes:
    return struct.pack(">III", object_id, parent_id & 0xFFFFFFFF, kind) + text.encode("utf-16le") + b"\x00\x00"


def build_dbdat_prototype_records(
    root: Path,
    target_library: list[TargetLibraryEntry],
) -> list[TargetDatRecord]:
    db_dat = find_db_files(root)["db.dat"].read_bytes()
    existing_folder_map = build_existing_folder_entries(db_dat)
    max_existing_folder_id = max((record.object_id for record in existing_folder_map.values()), default=0)
    max_existing_target_id = max((target_entry_object_id(entry) for entry in target_library), default=0)
    next_object_id = max(max_existing_folder_id, max_existing_target_id) + 1

    folder_path_set: set[str] = set()
    for entry in target_library:
        parent = str(Path(entry.target_path).parent).replace("\\", "/")
        if parent in {"", "."}:
            continue
        parts = Path(parent).parts
        for index in range(1, len(parts) + 1):
            folder_path_set.add("/".join(parts[:index]))

    folder_paths = sorted(folder_path_set, key=lambda path: (path.count("/"), path.lower()))

    folder_ids: dict[str, int] = {}
    for folder_path in folder_paths:
        if folder_path in existing_folder_map:
            folder_ids[folder_path] = existing_folder_map[folder_path].object_id
        else:
            folder_ids[folder_path] = next_object_id
            next_object_id += 1

    folders_by_parent: dict[str, list[str]] = {}
    files_by_parent: dict[str, list[TargetLibraryEntry]] = {}
    for folder_path in folder_paths:
        parent = str(Path(folder_path).parent).replace("\\", "/")
        if parent == ".":
            parent = ""
        folders_by_parent.setdefault(parent, []).append(folder_path)
    for entry in target_library:
        parent = str(Path(entry.target_path).parent).replace("\\", "/")
        if parent == ".":
            parent = ""
        files_by_parent.setdefault(parent, []).append(entry)

    for paths in folders_by_parent.values():
        paths.sort(key=lambda path: (0 if path == "Music" else 1, path.lower()))
    for entries in files_by_parent.values():
        entries.sort(key=lambda entry: entry.target_path.lower())

    records: list[TargetDatRecord] = []
    offset = 0

    def append_record(object_id: int, parent_id: int, kind: int, text: str, target_path: str) -> None:
        nonlocal offset
        records.append(
            TargetDatRecord(
                record_start=offset,
                object_id=object_id,
                parent_id=parent_id,
                kind=kind,
                text=text,
                target_path=target_path,
            )
        )
        offset += len(serialize_dat_record_bytes(object_id, parent_id, kind, text))

    def emit_folder(folder_path: str) -> None:
        folder_name = Path(folder_path).name + "/"
        parent_path = str(Path(folder_path).parent).replace("\\", "/")
        if parent_path == ".":
            parent_path = ""
        parent_id = 0xFFFFFFFF if folder_path == "Music" else folder_ids.get(parent_path, 0)
        append_record(folder_ids[folder_path], parent_id, 0x100, folder_name, folder_path)
        for entry in files_by_parent.get(folder_path, []):
            append_record(target_entry_object_id(entry), folder_ids[folder_path], 0x200, entry.file_name, entry.target_path)
        for child_folder in folders_by_parent.get(folder_path, []):
            emit_folder(child_folder)

    for entry in files_by_parent.get("", []):
        append_record(target_entry_object_id(entry), 0, 0x200, entry.file_name, entry.target_path)
    for folder_path in folders_by_parent.get("", []):
        emit_folder(folder_path)

    return records


def serialize_dbdat_prototype(records: list[TargetDatRecord]) -> bytes:
    return b"".join(
        serialize_dat_record_bytes(record.object_id, record.parent_id, record.kind, record.text)
        for record in records
    )


def target_title_for_idx(entry: TargetLibraryEntry) -> str:
    return entry.title or Path(entry.file_name).stem


def pack_utf16be_null_terminated(text: str) -> bytes:
    return text.encode("utf-16be") + b"\x00\x00"


def build_idx_prototype_pages(
    root: Path,
    target_library: list[TargetLibraryEntry],
    dbdat_records: list[TargetDatRecord],
    db_dic: bytes,
) -> tuple[list[bytes], list[dict[str, object]]]:
    field_offsets = {
        field.name: field.entry_offset
        for field in parse_db_dic(db_dic)
    }
    dbdat_by_target_path = {
        record.target_path: record
        for record in dbdat_records
        if record.kind == 0x200
    }

    pages: list[bytes] = []
    page_summaries: list[dict[str, object]] = []
    current_items: list[dict[str, object]] = []
    current_nodes: list[dict[str, object]] = []
    current_offset = IDX_OBSERVED_HEADER_BYTES

    text_node_specs = [
        ("FilePath", lambda entry: entry.target_path, 0x11),
        ("FileName", lambda entry: entry.file_name, 0x02),
        ("Title", lambda entry: target_title_for_idx(entry), 0x07),
        ("Artist", lambda entry: entry.artist or "", 0x08),
        ("Album", lambda entry: entry.album or "", 0x04),
        ("Genre", lambda entry: entry.genre or "", 0x05),
    ]
    numeric_node_specs = [
        ("Duration", lambda entry: int((entry.duration_seconds or 0) * 1000), 0x21),
        ("BitRate", lambda entry: entry.bit_rate_bps or 0, 0x22),
        ("SampleRate", lambda entry: entry.sample_rate_hz or 0, 0x23),
    ]

    def finalize_page(page_index: int) -> None:
        nonlocal current_offset
        if not current_nodes:
            return
        page = bytearray(IDX_PAGE_SIZE)
        page_base = page_index * IDX_PAGE_SIZE
        first_node_abs_offset = page_base + current_nodes[0]["start_rel"]
        last_node_abs_offset = page_base + current_nodes[-1]["start_rel"]
        struct.pack_into(">I", page, 0, first_node_abs_offset)
        struct.pack_into(">I", page, 4, last_node_abs_offset)
        struct.pack_into(">I", page, 8, len(current_nodes))
        struct.pack_into(">I", page, 12, len(current_items))
        struct.pack_into(">I", page, 16, current_items[0]["dat_record_start"])
        struct.pack_into(">I", page, 20, current_items[-1]["dat_record_start"])
        struct.pack_into(">I", page, 24, current_items[0]["object_id"])
        struct.pack_into(">I", page, 28, current_offset)

        for index, node in enumerate(current_nodes):
            base = node["start_rel"]
            next_rel = current_nodes[index + 1]["start_rel"] if index + 1 < len(current_nodes) else 0
            next_abs = page_base + next_rel if next_rel else 0
            struct.pack_into(">I", page, base + 0, node["dat_record_start"])
            struct.pack_into(">I", page, base + 4, node["object_id"])
            struct.pack_into(">I", page, base + 8, node["field_entry_offset"])
            struct.pack_into(">I", page, base + 12, node["value"])
            struct.pack_into(">I", page, base + 16, node["node_type"])
            struct.pack_into(">I", page, base + 20, next_abs)
            payload = node["payload"]
            page[base + IDX_OBSERVED_NODE_BYTES:base + IDX_OBSERVED_NODE_BYTES + len(payload)] = payload

        pages.append(bytes(page))
        page_summaries.append(
            {
                "page_index": page_index,
                "item_count": len(current_items),
                "node_count": len(current_nodes),
                "first_node_abs_offset": first_node_abs_offset,
                "last_node_abs_offset": last_node_abs_offset,
                "bytes_used": current_offset,
                "items": [
                    {
                        "target_path": item["target_path"],
                        "object_id": item["object_id"],
                        "dat_record_start": item["dat_record_start"],
                        "flags": item["flags"],
                        "node_count": item["node_count"],
                    }
                    for item in current_items
                ],
            }
        )
        current_items.clear()
        current_nodes.clear()
        current_offset = IDX_OBSERVED_HEADER_BYTES

    sorted_entries = sorted(target_library, key=lambda entry: entry.target_path.lower())
    for entry in sorted_entries:
        dbdat_record = dbdat_by_target_path.get(entry.target_path)
        if dbdat_record is None:
            continue
        entry_nodes: list[dict[str, object]] = []
        for field_name, getter, node_type in text_node_specs:
            text = getter(entry)
            if not text:
                continue
            entry_nodes.append(
                {
                    "field_name": field_name,
                    "field_entry_offset": field_offsets.get(field_name, 0),
                    "value": 0,
                    "node_type": node_type,
                    "payload": pack_utf16be_null_terminated(text),
                    "payload_text": text,
                }
            )
        for field_name, getter, node_type in numeric_node_specs:
            value = getter(entry)
            if not value:
                continue
            entry_nodes.append(
                {
                    "field_name": field_name,
                    "field_entry_offset": field_offsets.get(field_name, 0),
                    "value": value,
                    "node_type": node_type,
                    "payload": b"",
                    "payload_text": "",
                }
            )

        entry_size = sum(IDX_OBSERVED_NODE_BYTES + len(node["payload"]) for node in entry_nodes)
        if current_items and current_offset + entry_size > IDX_PAGE_SIZE:
            finalize_page(len(pages))
        if IDX_OBSERVED_HEADER_BYTES + entry_size > IDX_PAGE_SIZE:
            raise ValueError(f"One observed idx prototype item exceeds page size: {entry.target_path}")

        item_node_count = 0
        for node in entry_nodes:
            node["dat_record_start"] = dbdat_record.record_start
            node["object_id"] = dbdat_record.object_id
            node["start_rel"] = current_offset
            current_nodes.append(node)
            current_offset += IDX_OBSERVED_NODE_BYTES + len(node["payload"])
            item_node_count += 1

        current_items.append(
            {
                "target_path": entry.target_path,
                "object_id": dbdat_record.object_id,
                "dat_record_start": dbdat_record.record_start,
                "flags": 1 if entry.source_kind == "existing" else 2,
                "node_count": item_node_count,
            }
        )

    if current_nodes:
        finalize_page(len(pages))

    return pages, page_summaries


def parse_idx_prototype_pages(data: bytes) -> list[dict[str, object]]:
    if len(data) % IDX_PAGE_SIZE != 0:
        raise ValueError("idx prototype length must be page-aligned")
    pages = []
    for page_index in range(len(data) // IDX_PAGE_SIZE):
        block = data[page_index * IDX_PAGE_SIZE:(page_index + 1) * IDX_PAGE_SIZE]
        page_base = page_index * IDX_PAGE_SIZE
        first_node_abs = struct.unpack_from(">I", block, 0)[0]
        last_node_abs = struct.unpack_from(">I", block, 4)[0]
        node_count_header = struct.unpack_from(">I", block, 8)[0]
        item_count_header = struct.unpack_from(">I", block, 12)[0]
        bytes_used = struct.unpack_from(">I", block, 28)[0]
        nodes = []
        next_abs = first_node_abs
        seen: set[int] = set()
        while page_base <= next_abs < page_base + IDX_PAGE_SIZE:
            rel = next_abs - page_base
            if rel in seen or rel + IDX_OBSERVED_NODE_BYTES > IDX_PAGE_SIZE:
                break
            seen.add(rel)
            dat_record_start, object_id, field_entry_offset, value, node_type, node_next_abs = struct.unpack_from(">IIIIII", block, rel)
            if page_base <= node_next_abs < page_base + IDX_PAGE_SIZE:
                payload_end = node_next_abs - page_base
            else:
                payload_end = bytes_used if bytes_used and bytes_used <= IDX_PAGE_SIZE else IDX_PAGE_SIZE
            payload = block[rel + IDX_OBSERVED_NODE_BYTES:payload_end] if payload_end > rel + IDX_OBSERVED_NODE_BYTES else b""
            payload_text = ""
            if payload and len(payload) % 2 == 0:
                try:
                    payload_text = payload.decode("utf-16be").split("\x00", 1)[0]
                except UnicodeDecodeError:
                    payload_text = ""
            nodes.append(
                {
                    "start_rel": rel,
                    "dat_record_start": dat_record_start,
                    "object_id": object_id,
                    "field_entry_offset": field_entry_offset,
                    "value": value,
                    "node_type": node_type,
                    "next_abs_offset": node_next_abs,
                    "payload_text": payload_text,
                }
            )
            if node_next_abs == 0:
                break
            next_abs = node_next_abs
        item_count = sum(1 for node in nodes if node["node_type"] == 0x11)
        pages.append(
            {
                "page_index": page_index,
                "item_count": item_count,
                "item_count_header": item_count_header,
                "node_count": len(nodes),
                "node_count_header": node_count_header,
                "first_node_abs_offset": first_node_abs,
                "last_node_abs_offset": last_node_abs,
                "bytes_used": bytes_used,
            }
        )
    return pages


def command_db_summary(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)

    db_dat = files["db.dat"].read_bytes()
    db_idx = files["db.idx"].read_bytes()
    db_dic = files["db.dic"].read_bytes()

    dat_hits = extract_db_dat_strings(db_dat)
    idx_hits = extract_db_idx_strings(db_idx)
    dic_fields = parse_db_dic(db_dic)

    all_files = list(iter_files(root))
    audio_files = file_extension_members(all_files, AUDIO_EXTENSIONS)
    playlist_files = file_extension_members(all_files, PLAYLIST_EXTENSIONS)

    report = {
        "root": str(root),
        "database": {
            "db.dat": {
                "size_bytes": len(db_dat),
                "last_nonzero_offset": last_nonzero_offset(db_dat),
                "string_count": len(dat_hits),
                "sample_strings": summarized_strings(dat_hits),
            },
            "db.idx": {
                "size_bytes": len(db_idx),
                "last_nonzero_offset": last_nonzero_offset(db_idx),
                "free_trailing_bytes": len(db_idx) - last_nonzero_offset(db_idx) - 1,
                "string_count": len(idx_hits),
                "sample_strings": summarized_strings(idx_hits),
            },
            "db.dic": {
                "size_bytes": len(db_dic),
                "header_u32_be": [struct.unpack(">I", db_dic[i:i + 4])[0] for i in range(0, min(32, len(db_dic)), 4)],
                "field_count": len(dic_fields),
                "fields": summarize_dic_fields(dic_fields, db_idx),
            },
        },
        "filesystem": {
            "audio_file_count": len(audio_files),
            "playlist_file_count": len(playlist_files),
        },
        "crosscheck": {
            "database_audio_name_count": len(database_audio_names(dat_hits, idx_hits)),
        },
    }

    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_missing_media(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    media_dir = Path(args.media_dir).resolve() if args.media_dir else root
    files = find_db_files(root)
    db_dat_hits = extract_db_dat_strings(files["db.dat"].read_bytes())
    db_idx_hits = extract_db_idx_strings(files["db.idx"].read_bytes())

    fs_names = filesystem_audio_names(media_dir)
    db_names = database_audio_names(db_dat_hits, db_idx_hits)

    missing = sorted(set(fs_names) - set(db_names))
    extra = sorted(set(db_names) - set(fs_names))

    report = {
        "root": str(root),
        "media_dir": str(media_dir),
        "filesystem_audio_count": len(fs_names),
        "database_audio_count": len(db_names),
        "missing_from_database": missing,
        "database_entries_without_matching_file": extra,
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_media_inventory(args: argparse.Namespace) -> int:
    media_dir = Path(args.media_dir).resolve()
    if not media_dir.exists():
        raise FileNotFoundError(media_dir)

    files = [path for path in sorted(media_dir.iterdir()) if path.is_file() and path.suffix.lower() in AUDIO_EXTENSIONS]
    inventory = [probe_media_file(path) for path in files]

    output = json.dumps(inventory, indent=2, ensure_ascii=False)
    if args.out:
        Path(args.out).write_text(output + "\n", encoding="utf-8")
    else:
        sys.stdout.write(output + "\n")
    return 0


def command_source_model(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve() if args.root else None
    media_dirs = [Path(path).resolve() for path in args.media_dirs]
    if args.full_db:
        if root is None:
            raise ValueError("--full-db requires --root")
        media_dirs = [root / "Music"]
    if not media_dirs:
        raise ValueError("Provide at least one media directory, or use --full-db with --root")
    inventory_paths = [Path(path).resolve() for path in args.inventory] if args.inventory else None
    entries, manifests = load_source_entries_from_dirs(root or media_dirs[0], media_dirs, inventory_paths=inventory_paths)
    report = {
        **({"root": str(root)} if root else {}),
        "selected_sources": manifests,
        "entry_count": len(entries),
        "entries": [asdict(entry) for entry in entries[: args.limit]],
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_record_context(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    needle = args.needle.encode("utf-16le")
    target = files[args.file_name].read_bytes()

    offsets: list[int] = []
    start = 0
    while True:
        idx = target.find(needle, start)
        if idx < 0:
            break
        offsets.append(idx)
        start = idx + 2

    contexts = []
    for idx in offsets[: args.limit]:
        left = max(0, idx - args.context)
        right = min(len(target), idx + len(needle) + args.context)
        chunk = target[left:right]
        u32 = []
        aligned_left = left - (left % 4)
        aligned_right = min(len(target), right + (4 - (right % 4)) % 4)
        for off in range(aligned_left, aligned_right, 4):
            if off + 4 > len(target):
                break
            u32.append({"offset": off, "big_endian_hex": f"{struct.unpack('>I', target[off:off + 4])[0]:08x}"})
        contexts.append(
            {
                "match_offset": idx,
                "slice_hex": chunk.hex(" "),
                "u32_be_context": u32,
            }
        )

    json.dump(
        {
            "file": args.file_name,
            "needle": args.needle,
            "match_count": len(offsets),
            "contexts": contexts,
        },
        sys.stdout,
        indent=2,
        ensure_ascii=False,
    )
    sys.stdout.write("\n")
    return 0


def parse_int_value(text: str) -> int:
    return int(text, 0)


def command_u32_context(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    value = parse_int_value(args.value)
    target = files[args.file_name].read_bytes()
    needle = struct.pack(">I", value) if args.endian == "big" else struct.pack("<I", value)

    offsets: list[int] = []
    start = 0
    while True:
        idx = target.find(needle, start)
        if idx < 0:
            break
        if not args.aligned_only or idx % 4 == 0:
            offsets.append(idx)
        start = idx + 1

    contexts = []
    for idx in offsets[: args.limit]:
        left = max(0, idx - args.context)
        right = min(len(target), idx + 4 + args.context)
        u32 = []
        aligned_left = left - (left % 4)
        aligned_right = min(len(target), right + (4 - (right % 4)) % 4)
        for off in range(aligned_left, aligned_right, 4):
            if off + 4 > len(target):
                break
            chunk = target[off:off + 4]
            u32.append(
                {
                    "offset": off,
                    "big_endian_hex": f"{struct.unpack('>I', chunk)[0]:08x}",
                    "little_endian_hex": f"{struct.unpack('<I', chunk)[0]:08x}",
                }
            )
        contexts.append(
            {
                "match_offset": idx,
                "slice_hex": target[left:right].hex(" "),
                "u32_context": u32,
            }
        )

    json.dump(
        {
            "file": args.file_name,
            "value": value,
            "value_hex": f"0x{value:08x}",
            "endian": args.endian,
            "match_count": len(offsets),
            "contexts": contexts,
        },
        sys.stdout,
        indent=2,
        ensure_ascii=False,
    )
    sys.stdout.write("\n")
    return 0


def command_media_xref(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    db_dat = files["db.dat"].read_bytes()
    db_idx = files["db.idx"].read_bytes()
    db_dic = files["db.dic"].read_bytes()
    needle = args.needle
    dic_fields = parse_db_dic(db_dic)
    dat_records_all = collect_dat_records(db_dat)
    annotation_maps = build_annotation_maps(dic_fields, dat_records_all)

    dat_records = dat_records_by_text(db_dat, needle)
    idx_inline_offsets = find_all_occurrences(db_idx, needle.encode("utf-16be"))
    idx_inline_matches = [
        {
            "string_offset": offset,
            "context_hex": db_idx[max(0, offset - args.context):min(len(db_idx), offset + len(needle.encode('utf-16be')) + args.context)].hex(" "),
            "u32_context": u32_context_entries(db_idx, offset, context=args.context, annotation_maps=annotation_maps),
        }
        for offset in idx_inline_offsets[: args.limit]
    ]

    pointer_refs = []
    for record in dat_records:
        ref_offsets = aligned_u32_refs(db_idx, record.record_start, endian="big")
        pointer_refs.append(
            {
                "record": asdict(record),
                "idx_ref_offsets": ref_offsets,
                "idx_ref_contexts": [
                    {
                        "match_offset": offset,
                        "slice_hex": db_idx[max(0, offset - args.context):min(len(db_idx), offset + 4 + args.context)].hex(" "),
                        "u32_context": u32_context_entries(db_idx, offset, context=args.context, annotation_maps=annotation_maps),
                    }
                    for offset in ref_offsets[: args.limit]
                ],
            }
        )

    report = {
        "root": str(root),
        "needle": needle,
        "db_dat_records": [asdict(record) for record in dat_records],
        "db_idx_inline_offsets": idx_inline_offsets,
        "db_idx_inline_matches": idx_inline_matches,
        "db_idx_pointer_refs": pointer_refs,
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_schema_summary(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    db_dic = files["db.dic"].read_bytes()
    db_idx = files["db.idx"].read_bytes()
    fields = parse_db_dic(db_dic)

    report = {
        "root": str(root),
        "field_count": len(fields),
        "fields": summarize_dic_fields(fields, db_idx, limit=args.limit),
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_idx_page_map(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    db_idx = files["db.idx"].read_bytes()
    db_dic = files["db.dic"].read_bytes()
    db_dat = files["db.dat"].read_bytes()
    dic_fields = parse_db_dic(db_dic)
    dat_records = collect_dat_records(db_dat)
    dat_map = {record.record_start: record.text for record in dat_records}
    field_map = {field.entry_offset: field.name for field in dic_fields}

    page_count = len(db_idx) // IDX_PAGE_SIZE
    selected_pages = range(page_count) if args.page is None else [args.page]
    pages = []
    for page_index in selected_pages:
        page = summarize_idx_page(db_idx, page_index, dat_map, field_map, max_samples=args.max_samples)
        if not args.include_empty and page["nonzero_bytes"] == 0:
            continue
        pages.append(page)

    report = {
        "root": str(root),
        "page_size": IDX_PAGE_SIZE,
        "page_count": page_count,
        "pages": pages,
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_idx_observed_page(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    db_idx = files["db.idx"].read_bytes()
    db_dic = files["db.dic"].read_bytes()
    db_dat = files["db.dat"].read_bytes()
    dic_fields = parse_db_dic(db_dic)
    dat_records = collect_dat_records(db_dat)
    annotation_maps = build_annotation_maps(dic_fields, dat_records)

    page_count = len(db_idx) // IDX_PAGE_SIZE
    selected_pages = range(page_count) if args.page is None else [args.page]
    pages = [
        parse_observed_idx_page(
            db_idx,
            page_index,
            annotation_maps,
            max_nodes=args.max_nodes,
            max_groups=args.max_groups,
        )
        for page_index in selected_pages
    ]

    report = {
        "root": str(root),
        "page_size": IDX_PAGE_SIZE,
        "page_count": page_count,
        "pages": pages,
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_idx_observed_summary(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    db_idx = files["db.idx"].read_bytes()
    db_dic = files["db.dic"].read_bytes()
    db_dat = files["db.dat"].read_bytes()
    dic_fields = parse_db_dic(db_dic)
    dat_records = collect_dat_records(db_dat)
    dat_record_by_start = {record.record_start: record for record in dat_records}
    annotation_maps = build_annotation_maps(dic_fields, dat_records)

    chain_length_counts: dict[int, int] = {}
    anchor_class_counts: dict[str, int] = {}
    anchor_annotation_counts: dict[str, int] = {}
    node_type_counts: dict[str, int] = {}
    node_type_by_payload_class: dict[str, dict[str, int]] = {}
    node_type_by_anchor_class: dict[str, dict[str, int]] = {}
    anchor_class_examples: dict[str, list[dict[str, object]]] = {}

    page_count = len(db_idx) // IDX_PAGE_SIZE
    for page_index in range(page_count):
        parsed = parse_observed_idx_page(
            db_idx,
            page_index,
            annotation_maps,
            max_nodes=args.max_nodes,
            max_groups=args.max_groups,
        )
        for chain in parsed["chains"]:
            anchor_annotation = chain["anchor_annotation"]
            anchor_class = "non_dat"
            if anchor_annotation and anchor_annotation.startswith("dat:"):
                anchor_text = anchor_annotation[4:]
                dat_record = dat_record_by_start.get(chain["anchor_value"])
                if dat_record and dat_record.kind == 0x200:
                    suffix = Path(anchor_text).suffix.lower()
                    if suffix in PLAYLIST_EXTENSIONS:
                        anchor_class = "dat_playlist"
                    else:
                        anchor_class = "dat_audio"
                elif dat_record and dat_record.kind == 0x100:
                    anchor_class = "dat_folder"
                else:
                    anchor_class = "dat_other"
            chain_length_counts[chain["node_count"]] = chain_length_counts.get(chain["node_count"], 0) + 1
            anchor_class_counts[anchor_class] = anchor_class_counts.get(anchor_class, 0) + 1
            if anchor_annotation:
                anchor_annotation_counts[anchor_annotation] = anchor_annotation_counts.get(anchor_annotation, 0) + 1
            examples = anchor_class_examples.setdefault(anchor_class, [])
            if len(examples) < args.example_limit:
                examples.append(
                    {
                        "page_index": parsed["page_index"],
                        "start_rel": chain["start_rel"],
                        "anchor_value_hex": chain["anchor_value_hex"],
                        "anchor_annotation": anchor_annotation,
                        "node_count": chain["node_count"],
                        "payload_texts": chain["payload_texts"],
                    }
                )

        for node in parsed["nodes"]:
            node_type = node["words"][4]
            node_type_counts[node_type] = node_type_counts.get(node_type, 0) + 1
            anchor_annotation = node["word_annotations"][0]
            anchor_class = "non_dat"
            if anchor_annotation and anchor_annotation.startswith("dat:"):
                anchor_text = anchor_annotation[4:]
                anchor_start = int(node["words"][0], 16)
                dat_record = dat_record_by_start.get(anchor_start)
                if dat_record and dat_record.kind == 0x200:
                    suffix = Path(anchor_text).suffix.lower()
                    if suffix in PLAYLIST_EXTENSIONS:
                        anchor_class = "dat_playlist"
                    else:
                        anchor_class = "dat_audio"
                elif dat_record and dat_record.kind == 0x100:
                    anchor_class = "dat_folder"
                else:
                    anchor_class = "dat_other"
            bucket = node_type_by_anchor_class.setdefault(node_type, {})
            bucket[anchor_class] = bucket.get(anchor_class, 0) + 1
            if node["payload_text"]:
                payload_class = classify_payload_text(node["payload_text"])
                bucket = node_type_by_payload_class.setdefault(node_type, {})
                bucket[payload_class] = bucket.get(payload_class, 0) + 1

    report = {
        "root": str(root),
        "page_count": page_count,
        "chain_count": sum(chain_length_counts.values()),
        "chain_length_counts": [{"node_count": k, "count": v} for k, v in sorted(chain_length_counts.items(), key=lambda item: (-item[1], item[0]))[: args.limit]],
        "anchor_class_counts": [{"anchor_class": k, "count": v} for k, v in sorted(anchor_class_counts.items(), key=lambda item: (-item[1], item[0]))],
        "top_anchor_annotations": [{"anchor_annotation": k, "count": v} for k, v in sorted(anchor_annotation_counts.items(), key=lambda item: (-item[1], item[0]))[: args.limit]],
        "node_type_counts": [{"node_type": k, "count": v} for k, v in sorted(node_type_counts.items(), key=lambda item: (-item[1], item[0]))[: args.limit]],
        "node_type_by_payload_class": [
            {"node_type": node_type, "payload_classes": payloads}
            for node_type, payloads in sorted(
                node_type_by_payload_class.items(),
                key=lambda item: (-sum(item[1].values()), item[0]),
            )[: args.limit]
        ],
        "node_type_by_anchor_class": [
            {"node_type": node_type, "anchor_classes": classes}
            for node_type, classes in sorted(
                node_type_by_anchor_class.items(),
                key=lambda item: (-sum(item[1].values()), item[0]),
            )[: args.limit]
        ],
        "anchor_class_examples": anchor_class_examples,
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_dat_tree(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    db_dat = files["db.dat"].read_bytes()
    records = collect_dat_records(db_dat)
    if not args.include_unknown:
        records = [
            record for record in records
            if record.kind in {0x100, 0x200} and 0 < record.object_id < 0xFFFFFFFF
        ]
    records_by_id = {record.object_id: record for record in records}
    children_by_parent: dict[int, list[DatRecord]] = {}
    kind_counts: dict[str, int] = {}
    for record in records:
        children_by_parent.setdefault(record.parent_id, []).append(record)
        kind_counts[kind_name(record.kind)] = kind_counts.get(kind_name(record.kind), 0) + 1

    roots = [
        record for record in records
        if record.parent_id == 0 or record.parent_id not in records_by_id
    ]
    roots.sort(key=lambda item: (item.kind, item.text.lower(), item.object_id))
    rendered_roots = [
        render_dat_tree_node(root_record, children_by_parent, max_depth=args.max_depth, max_children=args.max_children)
        for root_record in roots[:args.max_roots]
    ]

    report = {
        "root": str(root),
        "record_count": len(records),
        "kind_counts": kind_counts,
        "root_count": len(roots),
        "roots": rendered_roots,
    }
    if len(roots) > args.max_roots:
        report["roots_truncated"] = len(roots) - args.max_roots
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_media_cluster(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    db_dat = files["db.dat"].read_bytes()
    db_idx = files["db.idx"].read_bytes()
    db_dic = files["db.dic"].read_bytes()

    dic_fields = parse_db_dic(db_dic)
    dat_records_all = collect_dat_records(db_dat)
    dat_map = {record.record_start: record.text for record in dat_records_all}
    field_map = {field.entry_offset: field.name for field in dic_fields}

    dat_records = dat_records_by_text(db_dat, args.needle)
    clusters = []
    for record in dat_records:
        ref_offsets = aligned_u32_refs(db_idx, record.record_start, endian="big")
        inline_offsets = find_all_occurrences(db_idx, args.needle.encode("utf-16be"))
        page_indices = {offset // IDX_PAGE_SIZE for offset in ref_offsets}
        page_indices.update(offset // IDX_PAGE_SIZE for offset in inline_offsets)

        if args.neighbor_pages:
            expanded: set[int] = set()
            page_count = len(db_idx) // IDX_PAGE_SIZE
            for page_index in page_indices:
                for neighbor in range(page_index - args.neighbor_pages, page_index + args.neighbor_pages + 1):
                    if 0 <= neighbor < page_count:
                        expanded.add(neighbor)
            page_indices = expanded

        pages = []
        for page_index in sorted(page_indices):
            page_offset = page_index * IDX_PAGE_SIZE
            block = db_idx[page_offset:page_offset + IDX_PAGE_SIZE]
            strings, dat_refs, field_refs = collect_idx_page_links(block, page_offset, dat_map, field_map)
            pages.append(
                {
                    "page_index": page_index,
                    "offset": page_offset,
                    "target_ref_offsets": [offset for offset in ref_offsets if offset // IDX_PAGE_SIZE == page_index],
                    "target_inline_offsets": [offset for offset in inline_offsets if offset // IDX_PAGE_SIZE == page_index],
                    "inline_strings": [{"offset": page_offset + hit.offset, "text": hit.text} for hit in strings[: args.max_samples]],
                    "other_dat_refs": [
                        ref for ref in dat_refs
                        if ref["record_start"] != record.record_start
                    ][: args.max_samples],
                    "field_refs": field_refs[: args.max_samples],
                }
            )

        clusters.append(
            {
                "record": asdict(record),
                "idx_ref_offsets": ref_offsets,
                "idx_inline_offsets": inline_offsets,
                "pages": pages,
            }
        )

    report = {
        "root": str(root),
        "needle": args.needle,
        "cluster_count": len(clusters),
        "clusters": clusters,
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_model_export(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    db_dat = files["db.dat"].read_bytes()
    db_idx = files["db.idx"].read_bytes()
    db_dic = files["db.dic"].read_bytes()
    entries = build_normalized_media_entries(db_dat, db_idx, db_dic)
    canonicalized = canonicalize_entries(entries)
    canonical_entries = canonicalized["canonical_entries"]
    shadow_groups = canonicalized["shadow_groups"]

    by_file_name: dict[str, list[NormalizedMediaEntry]] = {}
    by_path: dict[str, list[NormalizedMediaEntry]] = {}
    for entry in entries:
        by_file_name.setdefault(entry.file_name, []).append(entry)
        by_path.setdefault(entry.inferred_path, []).append(entry)

    duplicates_by_name = [
        {
            "file_name": name,
            "count": len(group),
            "entries": [asdict(entry) for entry in group[: args.max_duplicates]],
        }
        for name, group in sorted(by_file_name.items())
        if len(group) > 1
    ]
    duplicates_by_path = [
        {
            "inferred_path": path,
            "count": len(group),
            "entries": [asdict(entry) for entry in group[: args.max_duplicates]],
        }
        for path, group in sorted(by_path.items())
        if len(group) > 1
    ]

    report = {
        "root": str(root),
        "file_entry_count": len(entries),
        "canonical_file_entry_count": len(canonical_entries),
        "entries_with_idx_refs": sum(1 for entry in entries if entry.idx_ref_offsets),
        "entries_with_idx_inline_strings": sum(1 for entry in entries if entry.idx_inline_offsets),
        "duplicate_file_name_count": len(duplicates_by_name),
        "duplicate_inferred_path_count": len(duplicates_by_path),
        "sample_entries": [asdict(entry) for entry in entries[: args.limit]],
        "sample_canonical_entries": [asdict(entry) for entry in canonical_entries[: args.limit]],
        "shadow_group_count": len(shadow_groups),
        "shadow_groups": shadow_groups[: args.max_groups],
        "duplicate_file_names": duplicates_by_name[: args.max_groups],
        "duplicate_inferred_paths": duplicates_by_path[: args.max_groups],
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_rebuild_plan(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    media_dirs = [Path(path).resolve() for path in args.media_dirs]
    inventory_paths = [Path(path).resolve() for path in args.inventory] if args.inventory else None
    plan = build_rebuild_plan_data(
        root,
        media_dirs,
        full_db=args.full_db,
        inventory_paths=inventory_paths,
        max_collisions=args.max_collisions,
    )
    canonical_entries: list[NormalizedMediaEntry] = plan["canonical_entries"]
    source_entries: list[SourceMediaEntry] = plan["source_entries"]
    manifests = plan["selected_sources"]
    planned_additions = plan["planned_additions"]
    exact_path_collisions = plan["exact_path_collisions"]
    name_collisions = plan["name_collisions"]

    report = {
        "root": str(root),
        "selected_sources": manifests,
        "mode": "full-db" if args.full_db else "selected-dirs",
        "canonical_database_entry_count": len(canonical_entries),
        "source_entry_count": len(source_entries),
        "planned_addition_count": len(planned_additions),
        "exact_path_collision_count": len(exact_path_collisions),
        "name_collision_count": len(name_collisions),
        "planned_additions": planned_additions[: args.limit],
        "exact_path_collisions": exact_path_collisions[: args.max_groups],
        "name_collisions": name_collisions[: args.max_groups],
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_write_rebuild_snapshot(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    media_dirs = [Path(path).resolve() for path in args.media_dirs]
    inventory_paths = [Path(path).resolve() for path in args.inventory] if args.inventory else None
    out_dir = Path(args.out_dir).resolve()

    plan = build_rebuild_plan_data(
        root,
        media_dirs,
        full_db=args.full_db,
        inventory_paths=inventory_paths,
        max_collisions=args.max_collisions,
    )
    canonical_entries: list[NormalizedMediaEntry] = plan["canonical_entries"]
    target_library = build_target_library_entries(canonical_entries, plan["planned_additions"])

    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "root": str(root),
        "mode": plan["mode"],
        "selected_sources": plan["selected_sources"],
        "canonical_database_entry_count": len(canonical_entries),
        "planned_addition_count": len(plan["planned_additions"]),
        "exact_path_collision_count": len(plan["exact_path_collisions"]),
        "name_collision_count": len(plan["name_collisions"]),
        "target_library_entry_count": len(target_library),
    }

    write_snapshot_artifacts(out_dir, manifest, canonical_entries, plan, target_library)

    report = {
        "out_dir": str(out_dir),
        **manifest,
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def write_snapshot_artifacts(
    out_dir: Path,
    manifest: dict[str, object],
    canonical_entries: list[NormalizedMediaEntry],
    plan: dict[str, object],
    target_library: list[TargetLibraryEntry],
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    (out_dir / "canonical_entries.json").write_text(
        json.dumps([asdict(entry) for entry in canonical_entries], indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (out_dir / "planned_additions.json").write_text(
        json.dumps(plan["planned_additions"], indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (out_dir / "target_library.json").write_text(
        json.dumps([asdict(entry) for entry in target_library], indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (out_dir / "collisions.json").write_text(
        json.dumps(
            {
                "exact_path_collisions": plan["exact_path_collisions"],
                "name_collisions": plan["name_collisions"],
            },
            indent=2,
            ensure_ascii=False,
        ) + "\n",
        encoding="utf-8",
    )


def command_write_dbdat_prototype(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    media_dirs = [Path(path).resolve() for path in args.media_dirs]
    inventory_paths = [Path(path).resolve() for path in args.inventory] if args.inventory else None
    out_dir = Path(args.out_dir).resolve()

    plan = build_rebuild_plan_data(
        root,
        media_dirs,
        full_db=args.full_db,
        inventory_paths=inventory_paths,
        max_collisions=args.max_collisions,
    )
    target_library = build_target_library_entries(plan["canonical_entries"], plan["planned_additions"])
    records = build_dbdat_prototype_records(root, target_library)
    dbdat_bytes = serialize_dbdat_prototype(records)

    out_dir.mkdir(parents=True, exist_ok=True)
    dbdat_path = out_dir / "db.dat.prototype"
    records_path = out_dir / "db.dat.records.json"
    dbdat_path.write_bytes(dbdat_bytes)
    records_path.write_text(json.dumps([asdict(record) for record in records], indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    reparsed = validated_folder_file_records(dbdat_bytes)
    report = {
        "out_dir": str(out_dir),
        "dbdat_path": str(dbdat_path),
        "records_path": str(records_path),
        "record_count": len(records),
        "serialized_size_bytes": len(dbdat_bytes),
        "reparsed_record_count": len(reparsed),
        "planned_addition_count": len(plan["planned_additions"]),
        "target_library_entry_count": len(target_library),
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_write_idx_prototype(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    media_dirs = [Path(path).resolve() for path in args.media_dirs]
    inventory_paths = [Path(path).resolve() for path in args.inventory] if args.inventory else None
    out_dir = Path(args.out_dir).resolve()

    files = find_db_files(root)
    db_dic = files["db.dic"].read_bytes()
    plan = build_rebuild_plan_data(
        root,
        media_dirs,
        full_db=args.full_db,
        inventory_paths=inventory_paths,
        max_collisions=args.max_collisions,
    )
    target_library = build_target_library_entries(plan["canonical_entries"], plan["planned_additions"])
    dbdat_records = build_dbdat_prototype_records(root, target_library)
    pages, page_summaries = build_idx_prototype_pages(root, target_library, dbdat_records, db_dic)
    idx_bytes = b"".join(pages)

    out_dir.mkdir(parents=True, exist_ok=True)
    idx_path = out_dir / "db.idx.prototype"
    pages_path = out_dir / "db.idx.pages.json"
    idx_path.write_bytes(idx_bytes)
    pages_path.write_text(json.dumps(page_summaries, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    reparsed_pages = parse_idx_prototype_pages(idx_bytes)
    report = {
        "out_dir": str(out_dir),
        "idx_path": str(idx_path),
        "pages_path": str(pages_path),
        "page_count": len(pages),
        "serialized_size_bytes": len(idx_bytes),
        "reparsed_page_count": len(reparsed_pages),
        "target_library_entry_count": len(target_library),
        "planned_addition_count": len(plan["planned_additions"]),
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_write_rebuild_prototype(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    media_dirs = [Path(path).resolve() for path in args.media_dirs]
    inventory_paths = [Path(path).resolve() for path in args.inventory] if args.inventory else None
    out_dir = Path(args.out_dir).resolve()

    files = find_db_files(root)
    db_dic = files["db.dic"].read_bytes()
    plan = build_rebuild_plan_data(
        root,
        media_dirs,
        full_db=args.full_db,
        inventory_paths=inventory_paths,
        max_collisions=args.max_collisions,
    )
    canonical_entries: list[NormalizedMediaEntry] = plan["canonical_entries"]
    target_library = build_target_library_entries(canonical_entries, plan["planned_additions"])
    records = build_dbdat_prototype_records(root, target_library)
    dbdat_bytes = serialize_dbdat_prototype(records)
    pages, page_summaries = build_idx_prototype_pages(root, target_library, records, db_dic)
    idx_bytes = b"".join(pages)

    manifest = {
        "root": str(root),
        "mode": plan["mode"],
        "selected_sources": plan["selected_sources"],
        "canonical_database_entry_count": len(canonical_entries),
        "planned_addition_count": len(plan["planned_additions"]),
        "exact_path_collision_count": len(plan["exact_path_collisions"]),
        "name_collision_count": len(plan["name_collisions"]),
        "target_library_entry_count": len(target_library),
        "dbdat_record_count": len(records),
        "dbdat_serialized_size_bytes": len(dbdat_bytes),
        "idx_page_count": len(pages),
        "idx_serialized_size_bytes": len(idx_bytes),
    }
    write_snapshot_artifacts(out_dir, manifest, canonical_entries, plan, target_library)

    dbdat_path = out_dir / "db.dat.prototype"
    records_path = out_dir / "db.dat.records.json"
    idx_path = out_dir / "db.idx.prototype"
    pages_path = out_dir / "db.idx.pages.json"
    dic_reference_path = out_dir / "db.dic.reference"
    dbdat_path.write_bytes(dbdat_bytes)
    records_path.write_text(json.dumps([asdict(record) for record in records], indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    idx_path.write_bytes(idx_bytes)
    pages_path.write_text(json.dumps(page_summaries, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    dic_reference_path.write_bytes(db_dic)

    reparsed_records = validated_folder_file_records(dbdat_bytes)
    reparsed_pages = parse_idx_prototype_pages(idx_bytes)
    report = {
        "out_dir": str(out_dir),
        "manifest_path": str(out_dir / "manifest.json"),
        "dbdat_path": str(dbdat_path),
        "records_path": str(records_path),
        "idx_path": str(idx_path),
        "pages_path": str(pages_path),
        "dic_reference_path": str(dic_reference_path),
        "record_count": len(records),
        "reparsed_record_count": len(reparsed_records),
        "page_count": len(pages),
        "reparsed_page_count": len(reparsed_pages),
        "planned_addition_count": len(plan["planned_additions"]),
        "target_library_entry_count": len(target_library),
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local tooling for exploring the iRiver E10 database files.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    db_summary = subparsers.add_parser("db-summary", help="Summarize System/db.* and media files under a mounted player root.")
    db_summary.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    db_summary.set_defaults(func=command_db_summary)

    missing_media = subparsers.add_parser("missing-media", help="List audio files present on disk but not visible in db.dat/db.idx strings.")
    missing_media.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    missing_media.add_argument("--media-dir", help="Restrict filesystem comparison to one directory")
    missing_media.set_defaults(func=command_missing_media)

    media_inventory = subparsers.add_parser("media-inventory", help="Probe audio files in one directory with ffprobe.")
    media_inventory.add_argument("media_dir", help="Directory containing media files to inventory")
    media_inventory.add_argument("--out", help="Optional output JSON path")
    media_inventory.set_defaults(func=command_media_inventory)

    source_model = subparsers.add_parser("source-model", help="Build a normalized source-media model from one or more filesystem directories.")
    source_model.add_argument("media_dirs", nargs="*", help="Directories containing source media files")
    source_model.add_argument("--root", help="Mounted player root, required with --full-db")
    source_model.add_argument("--full-db", action="store_true", help="Use ROOT/Music as the source set instead of explicit directories")
    source_model.add_argument("--inventory", action="append", help="Optional inventory JSON files, in the same order as media_dirs")
    source_model.add_argument("--limit", type=int, default=40, help="Maximum entries to include")
    source_model.set_defaults(func=command_source_model)

    record_context = subparsers.add_parser("record-context", help="Find a UTF-16LE string in a db file and dump binary context around it.")
    record_context.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    record_context.add_argument("file_name", choices=["db.dat", "db.idx", "db.dic"])
    record_context.add_argument("needle", help="String to search for")
    record_context.add_argument("--context", type=int, default=64, help="Bytes of context on each side")
    record_context.add_argument("--limit", type=int, default=4, help="Maximum number of matches to include")
    record_context.set_defaults(func=command_record_context)

    u32_context = subparsers.add_parser("u32-context", help="Find a 32-bit integer in a db file and dump binary context around it.")
    u32_context.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    u32_context.add_argument("file_name", choices=["db.dat", "db.idx", "db.dic"])
    u32_context.add_argument("value", help="Integer value, decimal or hex like 0xbc")
    u32_context.add_argument("--endian", choices=["big", "little"], default="big")
    u32_context.add_argument("--context", type=int, default=64, help="Bytes of context on each side")
    u32_context.add_argument("--limit", type=int, default=4, help="Maximum number of matches to include")
    u32_context.add_argument("--aligned-only", action="store_true", help="Only include hits on 4-byte boundaries")
    u32_context.set_defaults(func=command_u32_context)

    media_xref = subparsers.add_parser("media-xref", help="Cross-reference one exact media string across db.dat and db.idx.")
    media_xref.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    media_xref.add_argument("needle", help="Exact media string, e.g. 01 Hello.wma")
    media_xref.add_argument("--context", type=int, default=64, help="Bytes of binary context around each match")
    media_xref.add_argument("--limit", type=int, default=4, help="Maximum number of contexts to include per section")
    media_xref.set_defaults(func=command_media_xref)

    schema_summary = subparsers.add_parser("schema-summary", help="Parse db.dic as a field dictionary and count field references in db.idx.")
    schema_summary.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    schema_summary.add_argument("--limit", type=int, default=80, help="Maximum number of fields to include")
    schema_summary.set_defaults(func=command_schema_summary)

    idx_page_map = subparsers.add_parser("idx-page-map", help="Summarize db.idx page-by-page using inline strings and db.dat/db.dic references.")
    idx_page_map.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    idx_page_map.add_argument("--page", type=int, help="Optional 0-based page index to inspect")
    idx_page_map.add_argument("--max-samples", type=int, default=3, help="Maximum sample strings/refs per page")
    idx_page_map.add_argument("--include-empty", action="store_true", help="Include fully zeroed pages")
    idx_page_map.set_defaults(func=command_idx_page_map)

    idx_observed_page = subparsers.add_parser("idx-observed-page", help="Parse db.idx pages using the observed chained-node layout heuristic.")
    idx_observed_page.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    idx_observed_page.add_argument("--page", type=int, help="Optional 0-based page index to inspect")
    idx_observed_page.add_argument("--max-nodes", type=int, default=64, help="Maximum chained nodes to parse per page")
    idx_observed_page.add_argument("--max-groups", type=int, default=24, help="Maximum anchor groups to summarize per page")
    idx_observed_page.set_defaults(func=command_idx_observed_page)

    idx_observed_summary = subparsers.add_parser("idx-observed-summary", help="Summarize observed chained-node patterns across the full db.idx.")
    idx_observed_summary.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    idx_observed_summary.add_argument("--max-nodes", type=int, default=64, help="Maximum chained nodes to parse per page")
    idx_observed_summary.add_argument("--max-groups", type=int, default=24, help="Maximum anchor groups to summarize per page")
    idx_observed_summary.add_argument("--limit", type=int, default=20, help="Maximum summary rows per section")
    idx_observed_summary.add_argument("--example-limit", type=int, default=5, help="Maximum example chains per anchor class")
    idx_observed_summary.set_defaults(func=command_idx_observed_summary)

    dat_tree = subparsers.add_parser("dat-tree", help="Render the parseable db.dat object records as a parent/child tree.")
    dat_tree.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    dat_tree.add_argument("--max-depth", type=int, default=3, help="Maximum tree depth to render")
    dat_tree.add_argument("--max-children", type=int, default=20, help="Maximum children to include per node")
    dat_tree.add_argument("--max-roots", type=int, default=40, help="Maximum root nodes to include")
    dat_tree.add_argument("--include-unknown", action="store_true", help="Include records outside the currently validated folder/file kinds")
    dat_tree.set_defaults(func=command_dat_tree)

    media_cluster = subparsers.add_parser("media-cluster", help="Group db.idx pages that refer to one db.dat media record.")
    media_cluster.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    media_cluster.add_argument("needle", help="Exact media string, e.g. 01 Hello.wma")
    media_cluster.add_argument("--neighbor-pages", type=int, default=0, help="Include adjacent pages around direct hits")
    media_cluster.add_argument("--max-samples", type=int, default=8, help="Maximum strings/refs to include per page")
    media_cluster.set_defaults(func=command_media_cluster)

    model_export = subparsers.add_parser("model-export", help="Export a normalized media model from validated db.dat/db.idx/db.dic structures.")
    model_export.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    model_export.add_argument("--limit", type=int, default=40, help="Maximum sample entries to include")
    model_export.add_argument("--max-groups", type=int, default=20, help="Maximum duplicate groups to include")
    model_export.add_argument("--max-duplicates", type=int, default=8, help="Maximum entries per duplicate group")
    model_export.set_defaults(func=command_model_export)

    rebuild_plan = subparsers.add_parser("rebuild-plan", help="Compare selected source directories, or the full Music tree, to the canonical E10 database model.")
    rebuild_plan.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    rebuild_plan.add_argument("media_dirs", nargs="*", help="Directories containing source media files to plan for")
    rebuild_plan.add_argument("--full-db", action="store_true", help="Plan against the full ROOT/Music tree instead of explicit directories")
    rebuild_plan.add_argument("--inventory", action="append", help="Optional inventory JSON files, in the same order as media_dirs")
    rebuild_plan.add_argument("--limit", type=int, default=40, help="Maximum planned additions to include")
    rebuild_plan.add_argument("--max-groups", type=int, default=20, help="Maximum collision groups to include")
    rebuild_plan.add_argument("--max-collisions", type=int, default=8, help="Maximum existing matches per collision group")
    rebuild_plan.set_defaults(func=command_rebuild_plan)

    write_rebuild_snapshot = subparsers.add_parser("write-rebuild-snapshot", help="Write a safe rebuild snapshot to an output directory for later db.dat/db.idx generation.")
    write_rebuild_snapshot.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    write_rebuild_snapshot.add_argument("out_dir", help="Output directory for the rebuild snapshot")
    write_rebuild_snapshot.add_argument("media_dirs", nargs="*", help="Directories containing source media files to include")
    write_rebuild_snapshot.add_argument("--full-db", action="store_true", help="Snapshot the full ROOT/Music tree instead of explicit directories")
    write_rebuild_snapshot.add_argument("--inventory", action="append", help="Optional inventory JSON files, in the same order as media_dirs")
    write_rebuild_snapshot.add_argument("--max-collisions", type=int, default=8, help="Maximum existing matches per collision group")
    write_rebuild_snapshot.set_defaults(func=command_write_rebuild_snapshot)

    write_dbdat_prototype = subparsers.add_parser("write-dbdat-prototype", help="Write a first db.dat prototype from the target library model.")
    write_dbdat_prototype.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    write_dbdat_prototype.add_argument("out_dir", help="Output directory for the db.dat prototype")
    write_dbdat_prototype.add_argument("media_dirs", nargs="*", help="Directories containing source media files to include")
    write_dbdat_prototype.add_argument("--full-db", action="store_true", help="Prototype against the full ROOT/Music tree instead of explicit directories")
    write_dbdat_prototype.add_argument("--inventory", action="append", help="Optional inventory JSON files, in the same order as media_dirs")
    write_dbdat_prototype.add_argument("--max-collisions", type=int, default=8, help="Maximum existing matches per collision group")
    write_dbdat_prototype.set_defaults(func=command_write_dbdat_prototype)

    write_idx_prototype = subparsers.add_parser("write-idx-prototype", help="Write a first db.idx prototype from the target library model.")
    write_idx_prototype.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    write_idx_prototype.add_argument("out_dir", help="Output directory for the db.idx prototype")
    write_idx_prototype.add_argument("media_dirs", nargs="*", help="Directories containing source media files to include")
    write_idx_prototype.add_argument("--full-db", action="store_true", help="Prototype against the full ROOT/Music tree instead of explicit directories")
    write_idx_prototype.add_argument("--inventory", action="append", help="Optional inventory JSON files, in the same order as media_dirs")
    write_idx_prototype.add_argument("--max-collisions", type=int, default=8, help="Maximum existing matches per collision group")
    write_idx_prototype.set_defaults(func=command_write_idx_prototype)

    write_rebuild_prototype = subparsers.add_parser("write-rebuild-prototype", help="Write a complete rebuild-prototype bundle with snapshot JSON, db.dat prototype, db.idx prototype, and a db.dic reference copy.")
    write_rebuild_prototype.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    write_rebuild_prototype.add_argument("out_dir", help="Output directory for the rebuild prototype bundle")
    write_rebuild_prototype.add_argument("media_dirs", nargs="*", help="Directories containing source media files to include")
    write_rebuild_prototype.add_argument("--full-db", action="store_true", help="Prototype against the full ROOT/Music tree instead of explicit directories")
    write_rebuild_prototype.add_argument("--inventory", action="append", help="Optional inventory JSON files, in the same order as media_dirs")
    write_rebuild_prototype.add_argument("--max-collisions", type=int, default=8, help="Maximum existing matches per collision group")
    write_rebuild_prototype.set_defaults(func=command_write_rebuild_prototype)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
