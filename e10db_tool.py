#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import struct
import subprocess
import sys
import unicodedata
import zlib
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


AUDIO_EXTENSIONS = {".mp3", ".wma", ".ogg", ".asf", ".wav"}
PLAYLIST_EXTENSIONS = {".plp", ".plx", ".m3u", ".pls", ".pla"}
IDX_PAGE_SIZE = 0x400
IDX_PROTO_MAGIC = b"E10IPX1\x00"
IDX_OBSERVED_HEADER_BYTES = 0x20
IDX_OBSERVED_NODE_BYTES = 24
DB_DAT_PREFIX_BYTES = 16
DB_DAT_RECORD_PADDING_BYTES = 21


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
    target_base_dir: str
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


@dataclass
class TargetMetadataBlob:
    offset: int
    target_path: str
    artist: str
    album: str
    genre: str
    title: str


@dataclass
class ObservedIdxNodeTemplate:
    field_name: str | None
    node_type: int
    payload_kind: str
    payload_role: str | None
    value_mode: str
    literal_value: int | None


@dataclass
class ObservedIdxChainTemplate:
    chain_family: str
    source_page_index: int
    source_anchor_annotation: str | None
    node_templates: list[ObservedIdxNodeTemplate]


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


def normalize_display_text(text: str | None) -> str | None:
    if not text:
        return None
    replacements = {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2013": "-",
        "\u2014": "-",
        "\u2026": "...",
        "\u00a0": " ",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = " ".join(ascii_text.split()).strip()
    return ascii_text or None


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
        0x0: "special",
        0x100: "folder",
        0x200: "file",
        0x400: "playlist",
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
                    "node_start_index": chain_start_index,
                    "node_end_index": len(nodes),
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


def observed_node_field_name(node: dict[str, object]) -> str | None:
    for annotation in node["word_annotations"]:
        if annotation and annotation.startswith("field:"):
            return annotation[6:]
    return None


def observed_chain_family(
    chain_nodes: list[dict[str, object]],
    dat_record_by_start: dict[int, DatRecord],
) -> str:
    if not chain_nodes:
        return "empty"
    anchor_value = int(chain_nodes[0]["words"][0], 16)
    dat_record = dat_record_by_start.get(anchor_value)
    field_names = [observed_node_field_name(node) for node in chain_nodes]
    payload_classes = {
        classify_payload_text(node["payload_text"])
        for node in chain_nodes
        if node["payload_text"]
    }
    if dat_record:
        if dat_record.kind == 0x0:
            return "dat_special_root"
        if dat_record.kind == 0x100:
            return "dat_folder"
        if dat_record.kind == 0x400:
            return "dat_playlist"
        if dat_record.kind == 0x200:
            if "audio_file" in payload_classes:
                return "dat_audio_file"
            if "Title" in field_names or "metadata" in payload_classes:
                return "dat_audio_title"
            return "dat_audio_misc"
        return "dat_other"
    if any(name in {"Artist", "Album", "Genre", "Title", "Duration", "BitRate", "SampleRate"} for name in field_names):
        return "non_dat_metadata"
    return "non_dat_other"


def infer_observed_payload_role(
    field_name: str | None,
    payload_text: str | None,
    chain_family: str,
) -> str | None:
    if field_name:
        return field_name
    if not payload_text:
        return None
    payload_class = classify_payload_text(payload_text)
    if chain_family == "dat_special_root":
        return "RMusic"
    if chain_family == "dat_folder" and payload_class == "folder":
        return "FilePath"
    if chain_family == "dat_audio_file" and payload_class == "audio_file":
        return "FileName"
    if chain_family == "dat_playlist" and payload_class in {"playlist", "audio_file"}:
        return "FileName"
    if chain_family == "dat_audio_title" and payload_class == "metadata":
        return "Title"
    if chain_family == "non_dat_metadata" and payload_class == "metadata":
        return "Title"
    return None


def infer_observed_value_mode(
    field_name: str | None,
    payload_text: str | None,
    value: int,
) -> str:
    if payload_text:
        if stable_idx_text_value(payload_text) == value:
            return "stable_text_crc"
        if value == 0:
            return "zero"
        return "literal"
    if field_name == "Duration":
        return "duration_ms"
    if field_name == "BitRate":
        return "bit_rate_bps"
    if field_name == "SampleRate":
        return "sample_rate_hz"
    if value == 0:
        return "zero"
    return "literal"


def observed_chain_to_template(
    chain_family: str,
    page_index: int,
    anchor_annotation: str | None,
    chain_nodes: list[dict[str, object]],
) -> ObservedIdxChainTemplate:
    node_templates: list[ObservedIdxNodeTemplate] = []
    for node in chain_nodes:
        field_name = observed_node_field_name(node)
        payload_text = node["payload_text"]
        value = int(node["words"][3], 16)
        payload_kind = "text" if payload_text else "empty"
        if not payload_text and field_name in {"Duration", "BitRate", "SampleRate"}:
            payload_kind = "numeric"
        node_templates.append(
            ObservedIdxNodeTemplate(
                field_name=field_name,
                node_type=int(node["words"][4], 16),
                payload_kind=payload_kind,
                payload_role=infer_observed_payload_role(field_name, payload_text, chain_family),
                value_mode=infer_observed_value_mode(field_name, payload_text, value),
                literal_value=value if value else None,
            )
        )
    return ObservedIdxChainTemplate(
        chain_family=chain_family,
        source_page_index=page_index,
        source_anchor_annotation=anchor_annotation,
        node_templates=node_templates,
    )


def observed_chain_template_signature(template: ObservedIdxChainTemplate) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            node.field_name,
            node.node_type,
            node.payload_kind,
            node.payload_role,
            node.value_mode,
            node.literal_value,
        )
        for node in template.node_templates
    )


def observed_chain_template_score(template: ObservedIdxChainTemplate) -> tuple[int, int, int]:
    recognized_nodes = sum(
        1
        for node in template.node_templates
        if node.payload_role or node.payload_kind == "empty"
    )
    text_nodes = sum(1 for node in template.node_templates if node.payload_kind == "text")
    return (recognized_nodes, text_nodes, len(template.node_templates))


def build_observed_idx_template_library(
    db_idx: bytes,
    db_dic: bytes,
    db_dat: bytes,
    *,
    max_nodes: int = 128,
    max_groups: int = 32,
) -> dict[str, object]:
    dic_fields = parse_db_dic(db_dic)
    dat_records = collect_dat_records(db_dat)
    dat_record_by_start = {record.record_start: record for record in dat_records}
    annotation_maps = build_annotation_maps(dic_fields, dat_records)

    variants_by_family: dict[str, dict[tuple[tuple[object, ...], ...], dict[str, object]]] = {}
    page_count = len(db_idx) // IDX_PAGE_SIZE
    for page_index in range(page_count):
        parsed = parse_observed_idx_page(
            db_idx,
            page_index,
            annotation_maps,
            max_nodes=max_nodes,
            max_groups=max_groups,
        )
        for chain in parsed["chains"]:
            chain_nodes = parsed["nodes"][chain["node_start_index"]:chain["node_end_index"]]
            chain_family = observed_chain_family(chain_nodes, dat_record_by_start)
            template = observed_chain_to_template(
                chain_family,
                parsed["page_index"],
                chain["anchor_annotation"],
                chain_nodes,
            )
            signature = observed_chain_template_signature(template)
            family_variants = variants_by_family.setdefault(chain_family, {})
            candidate = family_variants.get(signature)
            if candidate is None:
                family_variants[signature] = {
                    "template": template,
                    "count": 1,
                    "score": observed_chain_template_score(template),
                    "example_payloads": chain["payload_texts"],
                }
            else:
                candidate["count"] += 1

    selected_templates: dict[str, ObservedIdxChainTemplate] = {}
    family_summaries: list[dict[str, object]] = []
    for family_name, variants in sorted(variants_by_family.items()):
        ordered_variants = sorted(
            variants.values(),
            key=lambda item: (item["score"], item["count"]),
            reverse=True,
        )
        selected_templates[family_name] = ordered_variants[0]["template"]
        family_summaries.append(
            {
                "chain_family": family_name,
                "variant_count": len(ordered_variants),
                "selected_template": asdict(ordered_variants[0]["template"]),
                "variants": [
                    {
                        "count": item["count"],
                        "score": list(item["score"]),
                        "example_payloads": item["example_payloads"],
                        "template": asdict(item["template"]),
                    }
                    for item in ordered_variants[:8]
                ],
            }
        )

    compact_library = build_observed_compact_idx_template_library(
        db_idx,
        db_dic,
        db_dat,
    )

    return {
        "page_count": page_count,
        "chain_family_count": len(family_summaries),
        "compact_family_count": compact_library["family_count"],
        "family_count": len(family_summaries) + compact_library["family_count"],
        "selected_templates": selected_templates,
        "family_summaries": family_summaries,
        "selected_compact_templates": compact_library["selected_templates"],
        "compact_family_summaries": compact_library["family_summaries"],
        "compact_metadata_blob_count": compact_library["metadata_blob_count"],
        "compact_metadata_blob_by_offset": compact_library["metadata_blob_by_offset"],
        "compact_metadata_blobs": compact_library["metadata_blobs"],
    }


def decode_utf16be_string_at(data: bytes, offset: int, min_chars: int = 1) -> tuple[str, int] | None:
    if offset < 0 or offset + 1 >= len(data):
        return None
    chars: list[str] = []
    pos = offset
    while pos + 1 < len(data):
        codepoint = decode_utf16_codepoint(data, pos, "big")
        if codepoint == 0:
            break
        ch = chr(codepoint)
        if not is_reasonable_text_char(ch):
            return None
        chars.append(ch)
        pos += 2
    if len(chars) < min_chars or pos + 1 >= len(data) or data[pos:pos + 2] != b"\x00\x00":
        return None
    text = "".join(chars)
    if not text_quality_ok(text):
        return None
    return text, pos + 2


def parse_dbdat_metadata_blob_at(data: bytes, offset: int, max_strings: int = 6) -> dict[str, object] | None:
    if offset < 0 or offset >= len(data):
        return None
    if offset >= 2 and data[offset - 2:offset] != b"\x00\x00":
        return None
    strings: list[str] = []
    pos = offset
    while len(strings) < max_strings:
        decoded = decode_utf16be_string_at(data, pos)
        if decoded is None:
            break
        text, next_pos = decoded
        strings.append(text)
        pos = next_pos
    if len(strings) < 4:
        return None
    if any(len(text) < 2 for text in strings[:4]):
        return None
    if not all(all(is_latinish_char(ch) for ch in text) for text in strings[:4]):
        return None
    if strings[0][0].isalpha() and strings[0][0].islower():
        return None
    blob = {
        "offset": offset,
        "offset_hex": f"0x{offset:06x}",
        "strings": strings,
        "artist": strings[0] if len(strings) > 0 else None,
        "album": strings[1] if len(strings) > 1 else None,
        "genre": strings[2] if len(strings) > 2 else None,
        "title": strings[3] if len(strings) > 3 else None,
        "end_offset": pos,
        "trailer_preview_hex": data[pos:min(len(data), pos + 16)].hex(" "),
    }
    return blob


def observed_compact_slot_tag(block: bytes, raw_offset: int) -> int | None:
    if raw_offset < 4:
        return None
    value = struct.unpack_from(">I", block, raw_offset - 4)[0]
    if value > 0x20:
        return None
    return value


def observed_compact_slot_prefix_char(raw_text: str, trimmed_text: str) -> str | None:
    if not raw_text:
        return None
    if raw_text == trimmed_text:
        return None
    trimmed_index = raw_text.find(trimmed_text)
    if trimmed_index <= 0:
        return raw_text[0]
    return raw_text[:trimmed_index]


def parse_observed_compact_idx_page(
    db_idx: bytes,
    page_index: int,
    dat_record_by_start: dict[int, DatRecord],
    field_map: dict[int, str],
    metadata_blobs: dict[int, dict[str, object]] | None = None,
    db_dat_size: int | None = None,
    max_slots: int = 24,
    max_tail_cells: int = 24,
) -> dict[str, object]:
    metadata_blobs = metadata_blobs or {}
    page_offset = page_index * IDX_PAGE_SIZE
    block = db_idx[page_offset:page_offset + IDX_PAGE_SIZE]
    if len(block) < IDX_PAGE_SIZE:
        raise ValueError(f"page {page_index} is out of range")

    raw_hits = extract_utf16_strings(
        block,
        min_chars=4,
        byteorder="big",
        offset_step=1,
        normalize_leading_noise=False,
    )
    slots: list[dict[str, object]] = []
    candidate_blob_refs: set[int] = set()

    candidate_hits = []
    for hit in raw_hits:
        trimmed_offset, trimmed_text = trim_leading_noise(hit.text, hit.offset)
        if not trimmed_text:
            continue
        tag_value = observed_compact_slot_tag(block, hit.offset)
        prefix = observed_compact_slot_prefix_char(hit.text, trimmed_text)
        if tag_value is None and prefix is None:
            continue
        candidate_hits.append(
            {
                "raw_offset": hit.offset,
                "raw_text": hit.text,
                "trimmed_offset": trimmed_offset,
                "trimmed_text": trimmed_text,
                "tag_value": tag_value,
                "prefix": prefix,
            }
        )

    for index, hit in enumerate(candidate_hits[:max_slots]):
        raw_text = hit["raw_text"]
        raw_text_bytes = raw_text.encode("utf-16be") + b"\x00\x00"
        text_end = hit["raw_offset"] + len(raw_text_bytes)
        if index + 1 < len(candidate_hits):
            next_hit = candidate_hits[index + 1]
            next_start = next_hit["raw_offset"] - 4 if next_hit["tag_value"] is not None else next_hit["raw_offset"]
        else:
            next_start = len(block)
        next_start = max(text_end, min(len(block), next_start))
        tail = block[text_end:next_start]
        tail_cells = []
        for tail_index in range(0, min(len(tail), max_tail_cells * 4), 4):
            cell = tail[tail_index:tail_index + 4]
            if len(cell) < 4:
                break
            be32 = int.from_bytes(cell, "big")
            ref24 = int.from_bytes(cell[:3], "big")
            annotation_kind = "literal"
            annotation = None
            if be32 in field_map:
                annotation_kind = "field_u32"
                annotation = f"field:{field_map[be32]}"
            elif be32 in metadata_blobs:
                blob = metadata_blobs[be32]
                annotation_kind = "metadata_blob_u32"
                annotation = (
                    f"metadata:{blob['artist']} / {blob['album']} / {blob['title']}"
                )
            elif be32 in dat_record_by_start:
                annotation_kind = "dat_u32"
                annotation = f"dat:{dat_record_by_start[be32].text}"
            elif be32 == 0:
                annotation_kind = "zero_u32"
                annotation = "zero"
            elif ref24 in dat_record_by_start:
                annotation_kind = "dat_24"
                annotation = f"dat:{dat_record_by_start[ref24].text}"
            elif 0 < be32 < len(db_idx):
                annotation_kind = "idx_u32"
                annotation = f"idx:0x{be32:08x}"
            elif 0 < ref24 < 0x100000:
                annotation_kind = "raw_24"
                annotation = f"raw24:0x{ref24:06x}"
            if db_dat_size is not None and 0 < be32 < db_dat_size:
                candidate_blob_refs.add(be32)
            tail_cells.append(
                {
                    "tail_offset": page_offset + text_end + tail_index,
                    "u32_hex": f"0x{be32:08x}",
                    "ref24_hex": f"0x{ref24:06x}",
                    "suffix_byte": cell[3],
                    "annotation_kind": annotation_kind,
                    "annotation": annotation,
                }
            )
            if annotation_kind == "metadata_blob_24":
                candidate_blob_refs.add(ref24)

        slots.append(
            {
                "raw_offset": page_offset + hit["raw_offset"],
                "trimmed_offset": page_offset + hit["trimmed_offset"],
                "tag_value": hit["tag_value"],
                "prefix_text": hit["prefix"],
                "text": hit["trimmed_text"],
                "text_class": classify_payload_text(hit["trimmed_text"]),
                "tail_cell_count": len(tail_cells),
                "tail_cells": tail_cells,
                "metadata_blob_ref_count": sum(1 for cell in tail_cells if cell["annotation_kind"] == "metadata_blob_u32"),
                "dat_ref_count": sum(1 for cell in tail_cells if cell["annotation_kind"] in {"dat_u32", "dat_24"}),
                "field_ref_count": sum(1 for cell in tail_cells if cell["annotation_kind"] == "field_u32"),
            }
        )

    return {
        "page_index": page_index,
        "offset": page_offset,
        "slot_count": len(slots),
        "candidate_metadata_blob_ref_count": len(candidate_blob_refs),
        "candidate_metadata_blob_refs": [f"0x{value:08x}" for value in sorted(candidate_blob_refs)],
        "slots": slots,
    }


def compact_slot_template_signature(slot: dict[str, object]) -> tuple[object, ...]:
    return (
        slot["tag_value"],
        slot["text_class"],
        tuple(
            cell["annotation_kind"]
            for cell in slot["tail_cells"][:6]
        ),
    )


def compact_page_template_signature(page: dict[str, object]) -> tuple[tuple[object, ...], ...]:
    return tuple(compact_slot_template_signature(slot) for slot in page["slots"][:10])


def build_observed_compact_idx_template_library(
    db_idx: bytes,
    db_dic: bytes,
    db_dat: bytes,
) -> dict[str, object]:
    dic_fields = parse_db_dic(db_dic)
    dat_records = collect_dat_records(db_dat)
    dat_record_by_start = {record.record_start: record for record in dat_records}
    field_map = {field.entry_offset: field.name for field in dic_fields}
    page_count = len(db_idx) // IDX_PAGE_SIZE

    raw_pages: list[dict[str, object]] = []
    candidate_blob_offsets: dict[int, int] = {}
    for page_index in range(page_count):
        parsed = parse_observed_compact_idx_page(
            db_idx,
            page_index,
            dat_record_by_start,
            field_map,
            metadata_blobs={},
            db_dat_size=len(db_dat),
        )
        if parsed["slot_count"] < 2:
            continue
        raw_pages.append(parsed)
        for ref_hex in parsed["candidate_metadata_blob_refs"]:
            ref24 = int(ref_hex, 16)
            candidate_blob_offsets[ref24] = candidate_blob_offsets.get(ref24, 0) + 1

    metadata_blobs: dict[int, dict[str, object]] = {}
    for offset, ref_count in sorted(candidate_blob_offsets.items()):
        blob = parse_dbdat_metadata_blob_at(db_dat, offset)
        if blob is None:
            continue
        blob["ref_count"] = ref_count
        metadata_blobs[offset] = blob

    variants_by_signature: dict[tuple[tuple[object, ...], ...], dict[str, object]] = {}
    for page in raw_pages:
        annotated = parse_observed_compact_idx_page(
            db_idx,
            page["page_index"],
            dat_record_by_start,
            field_map,
            metadata_blobs=metadata_blobs,
            db_dat_size=len(db_dat),
        )
        useful_slots = [
            slot
            for slot in annotated["slots"]
            if slot["metadata_blob_ref_count"] or slot["dat_ref_count"] or slot["field_ref_count"]
        ]
        if len(useful_slots) < 2:
            continue
        signature = compact_page_template_signature(annotated)
        candidate = variants_by_signature.get(signature)
        template = {
            "source_page_index": annotated["page_index"],
            "slot_count": annotated["slot_count"],
            "slot_templates": [
                {
                    "tag_value": slot["tag_value"],
                    "text_class": slot["text_class"],
                    "metadata_blob_ref_count": slot["metadata_blob_ref_count"],
                    "dat_ref_count": slot["dat_ref_count"],
                    "field_ref_count": slot["field_ref_count"],
                    "example_text": slot["text"],
                }
                for slot in annotated["slots"][:10]
            ],
        }
        if candidate is None:
            variants_by_signature[signature] = {
                "template": template,
                "count": 1,
                "score": (
                    sum(slot["metadata_blob_ref_count"] for slot in useful_slots),
                    len(useful_slots),
                    annotated["slot_count"],
                ),
                "example_page": {
                    "page_index": annotated["page_index"],
                    "slot_count": annotated["slot_count"],
                    "texts": [slot["text"] for slot in annotated["slots"][:8]],
                },
            }
        else:
            candidate["count"] += 1

    ordered_variants = sorted(
        variants_by_signature.values(),
        key=lambda item: (item["score"], item["count"]),
        reverse=True,
    )
    family_summaries = [
        {
            "page_family": "compact_metadata_page",
            "variant_count": len(ordered_variants),
            "selected_template": ordered_variants[0]["template"] if ordered_variants else None,
            "variants": [
                {
                    "count": item["count"],
                    "score": list(item["score"]),
                    "example_page": item["example_page"],
                    "template": item["template"],
                }
                for item in ordered_variants[:8]
            ],
        }
    ] if ordered_variants else []

    metadata_blob_summaries = [
        {
            "offset": blob["offset"],
            "offset_hex": blob["offset_hex"],
            "ref_count": blob["ref_count"],
            "artist": blob["artist"],
            "album": blob["album"],
            "genre": blob["genre"],
            "title": blob["title"],
        }
        for blob in sorted(
            metadata_blobs.values(),
            key=lambda item: (-int(item["ref_count"]), item["offset"]),
        )[:64]
    ]

    selected_templates = {
        "compact_metadata_page": ordered_variants[0]["template"]
        for _ in [0]
        if ordered_variants
    }

    return {
        "page_count": page_count,
        "family_count": len(family_summaries),
        "selected_templates": selected_templates,
        "family_summaries": family_summaries,
        "metadata_blob_count": len(metadata_blobs),
        "metadata_blob_by_offset": metadata_blobs,
        "metadata_blobs": metadata_blob_summaries,
    }


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
                target_base_dir="",
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
            if relative_media_dir in {"", "."}:
                relative_media_dir = media_dir.name
        except ValueError:
            relative_media_dir = f"Music/{media_dir.name}"
        for entry in dir_entries:
            entry.target_base_dir = relative_media_dir
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
        target_path = "/".join(
            part.strip("/")
            for part in [entry.target_base_dir, entry.relative_path]
            if part and part not in {"", "."}
        )
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
    existing_object_id_floor: int = 0,
) -> list[TargetLibraryEntry]:
    entries: list[TargetLibraryEntry] = []
    max_object_id = max(existing_object_id_floor, max((entry.object_id for entry in canonical_entries), default=0))
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

    def infer_planned_artist(target_path: str, source: dict[str, object]) -> str | None:
        path_parts = {part.lower() for part in Path(target_path).parts}
        if "podcast" in path_parts:
            return "Podcast"
        artist = source.get("artist")
        if artist:
            return normalize_display_text(artist)
        album = source.get("album")
        if album:
            return normalize_display_text(album)
        parent_name = Path(target_path).parent.name
        return normalize_display_text(parent_name) or None

    def infer_planned_genre(target_path: str, source: dict[str, object]) -> str | None:
        genre = source.get("genre")
        if genre:
            return normalize_display_text(genre)
        path_parts = {part.lower() for part in Path(target_path).parts}
        if "podcast" in path_parts:
            return "Podcast"
        return None

    for item in planned_additions:
        source = item["source"]
        entries.append(
            TargetLibraryEntry(
                source_kind="planned_addition",
                target_path=item["target_path"],
                file_name=source["file_name"],
                title=normalize_display_text(source.get("title")) or normalize_display_text(Path(source["file_name"]).stem),
                artist=infer_planned_artist(item["target_path"], source),
                album=normalize_display_text(source.get("album")),
                genre=infer_planned_genre(item["target_path"], source),
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


def max_preserved_object_id(root: Path) -> int:
    db_dat = find_db_files(root)["db.dat"].read_bytes()
    return max(
        (
            record.object_id
            for record in collect_preserved_dbdat_records(db_dat)
            if 0 < record.object_id < 0xFFFFFFFF
        ),
        default=0,
    )


def build_existing_path_entry_map(db_dat: bytes, allowed_kinds: set[int]) -> dict[str, DatRecord]:
    records = [record for record in collect_dat_records(db_dat) if record.kind in allowed_kinds]
    records_by_id = {
        record.object_id: record
        for record in records
        if 0 < record.object_id < 0xFFFFFFFF
    }
    path_map: dict[str, DatRecord] = {}
    for record in records:
        _ancestor_ids, _ancestor_names, inferred_path = infer_record_path(record, records_by_id)
        existing = path_map.get(inferred_path)
        if existing is None or record.record_start < existing.record_start:
            path_map[inferred_path] = record
    return path_map


def build_existing_folder_entries(db_dat: bytes) -> dict[str, DatRecord]:
    return build_existing_path_entry_map(db_dat, {0x100})


def build_existing_playlist_entries(db_dat: bytes) -> dict[str, DatRecord]:
    return build_existing_path_entry_map(db_dat, {0x400})


def build_existing_special_root_entries(db_dat: bytes) -> dict[str, DatRecord]:
    path_map: dict[str, DatRecord] = {}
    for record in collect_dat_records(db_dat):
        if record.kind != 0x0:
            continue
        if not (record.text.startswith("/") and record.text.endswith("/")):
            continue
        existing = path_map.get(record.text)
        if existing is None or record.record_start < existing.record_start:
            path_map[record.text] = record
    return path_map


def collect_preserved_dbdat_records(db_dat: bytes) -> list[TargetDatRecord]:
    preserved = [
        record
        for record in collect_dat_records(db_dat)
        if record.kind in {0x100, 0x200, 0x400} or (record.kind == 0x0 and record.text == "/a/")
    ]
    records_by_id = {
        record.object_id: record
        for record in preserved
        if 0 < record.object_id < 0xFFFFFFFF
    }
    target_records: list[TargetDatRecord] = []
    for record in preserved:
        _ancestor_ids, _ancestor_names, inferred_path = infer_record_path(record, records_by_id)
        target_records.append(
            TargetDatRecord(
                record_start=record.record_start,
                object_id=record.object_id,
                parent_id=record.parent_id,
                kind=record.kind,
                text=record.text,
                target_path=inferred_path,
            )
        )
    return target_records


def folder_paths_for_entries(entries: list[TargetLibraryEntry]) -> set[str]:
    folder_path_set: set[str] = set()
    for entry in entries:
        parent = str(Path(entry.target_path).parent).replace("\\", "/")
        if parent in {"", "."}:
            continue
        parts = Path(parent).parts
        for index in range(1, len(parts) + 1):
            folder_path_set.add("/".join(parts[:index]))
    return folder_path_set


def target_entry_object_id(entry: TargetLibraryEntry) -> int:
    if entry.existing_object_id is not None:
        return entry.existing_object_id
    if entry.provisional_object_id is not None:
        return entry.provisional_object_id
    raise ValueError(f"Target entry has no object id: {entry.target_path}")


def align_up(value: int, alignment: int) -> int:
    if alignment <= 1:
        return value
    return ((value + alignment - 1) // alignment) * alignment


def serialize_dat_record_bytes(object_id: int, parent_id: int, kind: int, text: str) -> bytes:
    return (
        struct.pack(">III", object_id, parent_id & 0xFFFFFFFF, kind)
        + text.encode("utf-16le")
        + b"\x00\x00"
        + (b"\x00" * DB_DAT_RECORD_PADDING_BYTES)
    )


def dbdat_prefix_bytes(db_dat: bytes) -> bytes:
    return db_dat[:DB_DAT_PREFIX_BYTES]


def metadata_blob_strings_for_entry(entry: TargetLibraryEntry) -> tuple[str, str, str, str]:
    title = target_title_for_idx(entry) or normalize_display_text(Path(entry.file_name).stem) or "Unknown Title"
    artist = normalize_display_text(entry.artist) or normalize_display_text(entry.album) or "Unknown Artist"
    album = normalize_display_text(entry.album) or normalize_display_text(Path(entry.target_path).parent.name) or "Unknown Album"
    genre = normalize_display_text(entry.genre) or "Unknown"
    return artist, album, genre, title


def serialize_metadata_blob_bytes(artist: str, album: str, genre: str, title: str) -> bytes:
    blob = b"".join(
        pack_utf16be_null_terminated(text)
        for text in [artist, album, genre, title]
    )
    blob += b"\x00" * 16
    blob += b"\x00" * ((4 - (len(blob) % 4)) % 4)
    return blob


def build_dbdat_metadata_blobs(
    root: Path,
    target_library: list[TargetLibraryEntry],
    records: list[TargetDatRecord],
) -> list[TargetMetadataBlob]:
    original_dbdat = find_db_files(root)["db.dat"].read_bytes()
    layout_bytes = serialize_dbdat_prototype(records, base_bytes=original_dbdat)
    start_offset = align_up(
        max(
            (
                record.record_start + len(serialize_dat_record_bytes(record.object_id, record.parent_id, record.kind, record.text))
                for record in records
            ),
            default=0,
        ),
        4,
    )
    free_spans = [list(span) for span in dbdat_zero_spans(layout_bytes, start_offset=start_offset, min_size=4)]
    if not free_spans:
        return []

    record_by_target_path = {
        record.target_path: record
        for record in records
        if record.kind == 0x200
    }
    metadata_blobs: list[TargetMetadataBlob] = []
    for entry in sorted((item for item in target_library if item.source_kind == "planned_addition"), key=lambda item: item.target_path.lower()):
        record = record_by_target_path.get(entry.target_path)
        if record is None:
            continue
        artist, album, genre, title = metadata_blob_strings_for_entry(entry)
        blob_bytes = serialize_metadata_blob_bytes(artist, album, genre, title)
        allocated_offset: int | None = None
        for span in free_spans:
            aligned_start = align_up(span[0], 4)
            if span[1] - aligned_start >= len(blob_bytes):
                allocated_offset = aligned_start
                span[0] = aligned_start + len(blob_bytes)
                break
        if allocated_offset is None:
            raise ValueError(f"db.dat has no free aligned span large enough for metadata blob: {entry.target_path}")
        metadata_blobs.append(
            TargetMetadataBlob(
                offset=allocated_offset,
                target_path=entry.target_path,
                artist=artist,
                album=album,
                genre=genre,
                title=title,
            )
        )
    return metadata_blobs


def dbdat_zero_spans(
    db_dat: bytes,
    *,
    start_offset: int = 0,
    min_size: int = 1,
) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    span_start: int | None = None
    for offset in range(max(0, start_offset), len(db_dat)):
        if db_dat[offset] == 0:
            if span_start is None:
                span_start = offset
        elif span_start is not None:
            if offset - span_start >= min_size:
                spans.append((span_start, offset))
            span_start = None
    if span_start is not None and len(db_dat) - span_start >= min_size:
        spans.append((span_start, len(db_dat)))
    return spans


def build_dbdat_prototype_records(
    root: Path,
    target_library: list[TargetLibraryEntry],
) -> list[TargetDatRecord]:
    db_dat = find_db_files(root)["db.dat"].read_bytes()
    preserved_records = collect_preserved_dbdat_records(db_dat)
    existing_folder_map = {
        record.target_path: record
        for record in preserved_records
        if record.kind == 0x100
    }
    max_existing_folder_id = max((record.object_id for record in preserved_records if 0 < record.object_id < 0xFFFFFFFF), default=0)
    max_existing_target_id = max((target_entry_object_id(entry) for entry in target_library), default=0)
    next_object_id = max(max_existing_folder_id, max_existing_target_id) + 1

    planned_entries = [entry for entry in target_library if entry.source_kind == "planned_addition"]
    folder_path_set = folder_paths_for_entries(planned_entries)

    folder_paths = sorted(folder_path_set, key=lambda path: (path.count("/"), path.lower()))

    folder_ids: dict[str, int] = {}
    for folder_path in folder_paths:
        if folder_path in existing_folder_map:
            folder_ids[folder_path] = existing_folder_map[folder_path].object_id
        else:
            folder_ids[folder_path] = next_object_id
            next_object_id += 1
    new_folder_paths = {
        folder_path
        for folder_path in folder_paths
        if folder_path not in existing_folder_map
    }

    folders_by_parent: dict[str, list[str]] = {}
    files_by_parent: dict[str, list[TargetLibraryEntry]] = {}
    for folder_path in folder_paths:
        parent = str(Path(folder_path).parent).replace("\\", "/")
        if parent == ".":
            parent = ""
        folders_by_parent.setdefault(parent, []).append(folder_path)
    for entry in planned_entries:
        parent = str(Path(entry.target_path).parent).replace("\\", "/")
        if parent == ".":
            parent = ""
        files_by_parent.setdefault(parent, []).append(entry)

    for paths in folders_by_parent.values():
        paths.sort(key=lambda path: (0 if path == "Music" else 1, path.lower()))
    for entries in files_by_parent.values():
        entries.sort(key=lambda entry: entry.target_path.lower())

    records = list(preserved_records)
    reserved_end = max(
        (record.record_start + len(serialize_dat_record_bytes(record.object_id, record.parent_id, record.kind, record.text)) for record in records),
        default=len(dbdat_prefix_bytes(db_dat)),
    )
    pending_specs: list[tuple[int, int, int, str, str]] = []

    def append_record_spec(object_id: int, parent_id: int, kind: int, text: str, target_path: str) -> None:
        pending_specs.append((object_id, parent_id, kind, text, target_path))

    def emit_folder_header(folder_path: str) -> None:
        if folder_path in new_folder_paths:
            folder_name = Path(folder_path).name + "/"
            parent_path = str(Path(folder_path).parent).replace("\\", "/")
            if parent_path == ".":
                parent_path = ""
            parent_id = 0xFFFFFFFF if folder_path == "Music" else folder_ids.get(parent_path, 0)
            append_record_spec(folder_ids[folder_path], parent_id, 0x100, folder_name, folder_path)
        for entry in files_by_parent.get(folder_path, []):
            append_record_spec(target_entry_object_id(entry), folder_ids[folder_path], 0x200, entry.file_name, entry.target_path)

    def emit_folder(folder_path: str) -> None:
        emit_folder_header(folder_path)
        for child_folder in folders_by_parent.get(folder_path, []):
            emit_folder(child_folder)

    for folder_path in folders_by_parent.get("", []):
        emit_folder(folder_path)

    if not pending_specs:
        return records

    free_spans = [list(span) for span in dbdat_zero_spans(db_dat, start_offset=reserved_end, min_size=1)]
    if not free_spans:
        raise ValueError("db.dat has no zero-filled free spans available for planned additions")

    for object_id, parent_id, kind, text, target_path in pending_specs:
        record_bytes = serialize_dat_record_bytes(object_id, parent_id, kind, text)
        record_size = len(record_bytes)
        allocated_offset: int | None = None
        for span in free_spans:
            span_start, span_end = span
            if span_end - span_start >= record_size:
                allocated_offset = span_start
                span[0] = span_start + record_size
                break
        if allocated_offset is None:
            raise ValueError(f"db.dat has no free zero span large enough for {target_path} ({record_size} bytes)")
        records.append(
            TargetDatRecord(
                record_start=allocated_offset,
                object_id=object_id,
                parent_id=parent_id,
                kind=kind,
                text=text,
                target_path=target_path,
            )
        )

    records.sort(key=lambda record: record.record_start)
    return records


def select_idx_extension_dbdat_records(
    all_records: list[TargetDatRecord],
    planned_entries: list[TargetLibraryEntry],
) -> list[TargetDatRecord]:
    planned_paths = {entry.target_path for entry in planned_entries}
    folder_paths = folder_paths_for_entries(planned_entries)
    return [
        record
        for record in all_records
        if record.target_path in planned_paths or record.target_path in folder_paths
    ]


def serialize_dbdat_prototype(
    records: list[TargetDatRecord],
    prefix: bytes = b"",
    base_bytes: bytes | None = None,
    metadata_blobs: list[TargetMetadataBlob] | None = None,
) -> bytes:
    metadata_blobs = metadata_blobs or []
    if base_bytes is None:
        out = prefix + b"".join(
            serialize_dat_record_bytes(record.object_id, record.parent_id, record.kind, record.text)
            for record in records
        )
        if not metadata_blobs:
            return out
        blob_tail = bytearray(out)
        for blob in metadata_blobs:
            blob_bytes = serialize_metadata_blob_bytes(blob.artist, blob.album, blob.genre, blob.title)
            end = blob.offset + len(blob_bytes)
            if end > len(blob_tail):
                blob_tail.extend(b"\x00" * (end - len(blob_tail)))
            blob_tail[blob.offset:end] = blob_bytes
        return bytes(blob_tail)
    out = bytearray(base_bytes)
    for record in records:
        record_bytes = serialize_dat_record_bytes(record.object_id, record.parent_id, record.kind, record.text)
        end = record.record_start + len(record_bytes)
        if end > len(out):
            out.extend(b"\x00" * (end - len(out)))
        out[record.record_start:end] = record_bytes
    for blob in metadata_blobs:
        blob_bytes = serialize_metadata_blob_bytes(blob.artist, blob.album, blob.genre, blob.title)
        end = blob.offset + len(blob_bytes)
        if end > len(out):
            out.extend(b"\x00" * (end - len(out)))
        out[blob.offset:end] = blob_bytes
    return bytes(out)


def target_title_for_idx(entry: TargetLibraryEntry) -> str:
    return entry.title or Path(entry.file_name).stem


def pack_utf16be_null_terminated(text: str) -> bytes:
    return text.encode("utf-16be") + b"\x00\x00"


def stable_idx_text_value(text: str) -> int:
    return zlib.crc32(text.encode("utf-16be")) & 0xFFFFFFFF


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def pad_bytes_to_length(data: bytes, target_length: int, label: str) -> bytes:
    if len(data) > target_length:
        raise ValueError(f"{label} is larger than target length: {len(data)} > {target_length}")
    if len(data) == target_length:
        return data
    return data + (b"\x00" * (target_length - len(data)))


def idx_text_values_for_record(record: TargetDatRecord) -> dict[str, str]:
    values: dict[str, str] = {}
    if record.kind == 0x0:
        values["RMusic"] = record.text
    elif record.kind == 0x100:
        values["FilePath"] = clean_folder_name(record.text) + "/"
    elif record.kind in {0x200, 0x400}:
        values["FileName"] = record.text
    return values


def idx_text_values_for_entry(entry: TargetLibraryEntry) -> dict[str, str]:
    values = {"FileName": entry.file_name}
    title_text = target_title_for_idx(entry)
    if title_text:
        values["Title"] = title_text
    if entry.artist:
        values["Artist"] = entry.artist
    if entry.album:
        values["Album"] = entry.album
    if entry.genre:
        values["Genre"] = entry.genre
    return values


def idx_numeric_values_for_entry(entry: TargetLibraryEntry) -> dict[str, int]:
    values: dict[str, int] = {}
    duration_ms = int((entry.duration_seconds or 0) * 1000)
    if duration_ms:
        values["Duration"] = duration_ms
    if entry.bit_rate_bps:
        values["BitRate"] = entry.bit_rate_bps
    if entry.sample_rate_hz:
        values["SampleRate"] = entry.sample_rate_hz
    return values


def instantiate_observed_chain_template(
    template: ObservedIdxChainTemplate | None,
    *,
    field_offsets: dict[str, int],
    text_values: dict[str, str],
    numeric_values: dict[str, int] | None = None,
) -> list[dict[str, object]]:
    if template is None:
        return []
    numeric_values = numeric_values or {}
    nodes: list[dict[str, object]] = []
    for node_template in template.node_templates:
        field_name = node_template.field_name or node_template.payload_role
        field_entry_offset = field_offsets.get(field_name, 0) if field_name else 0
        if node_template.payload_kind == "empty":
            nodes.append(
                {
                    "field_name": field_name or "",
                    "field_entry_offset": field_entry_offset,
                    "value": node_template.literal_value or 0,
                    "node_type": node_template.node_type,
                    "payload": b"",
                    "payload_text": "",
                }
            )
            continue
        if node_template.payload_kind == "text":
            payload_role = node_template.payload_role or field_name
            if not payload_role:
                continue
            text = text_values.get(payload_role)
            if not text:
                continue
            value = 0 if node_template.value_mode == "zero" else stable_idx_text_value(text)
            nodes.append(
                {
                    "field_name": field_name or "",
                    "field_entry_offset": field_entry_offset,
                    "value": value,
                    "node_type": node_template.node_type,
                    "payload": pack_utf16be_null_terminated(text),
                    "payload_text": text,
                }
            )
            continue
        if node_template.payload_kind == "numeric":
            payload_role = node_template.payload_role or field_name
            if not payload_role:
                continue
            value = numeric_values.get(payload_role)
            if not value:
                continue
            nodes.append(
                {
                    "field_name": field_name or "",
                    "field_entry_offset": field_entry_offset,
                    "value": value,
                    "node_type": node_template.node_type,
                    "payload": b"",
                    "payload_text": "",
                }
            )
    return nodes


def compact_slot_recipe_from_observed_slot(
    slot: dict[str, object],
    slots: list[dict[str, object]],
    page_offset: int,
    source_slot_index: int,
    *,
    fallback_tag_value: int = 0,
) -> dict[str, object]:
    tail_cells: list[dict[str, object]] = []
    for cell in slot["tail_cells"]:
        source_u32 = int(cell["u32_hex"], 16)
        page_local_target_slot_delta = None
        page_local_target_offset = None
        if cell["annotation_kind"] == "idx_u32" and page_offset <= source_u32 < page_offset + IDX_PAGE_SIZE:
            for target_slot_index, target_slot in enumerate(slots):
                target_start = target_slot["raw_offset"]
                if target_slot_index + 1 < len(slots):
                    target_end = slots[target_slot_index + 1]["raw_offset"]
                else:
                    target_end = page_offset + IDX_PAGE_SIZE
                if target_start <= source_u32 < target_end:
                    page_local_target_slot_delta = target_slot_index - source_slot_index
                    page_local_target_offset = source_u32 - target_start
                    break
        tail_cells.append(
            {
                "annotation_kind": cell["annotation_kind"],
                "source_u32": source_u32,
                "page_local_target_slot_delta": page_local_target_slot_delta,
                "page_local_target_offset": page_local_target_offset,
            }
        )
    return {
        "tag_value": slot["tag_value"] if slot["tag_value"] is not None else fallback_tag_value,
        "tail_cells": tail_cells,
        "tail_cell_count": len(tail_cells),
        "source_slot_index": source_slot_index,
        "source_text": slot["text"],
    }


def compact_slot_bytes(
    text: str,
    tail_values: list[int],
    *,
    tag_value: int = 0,
) -> bytes:
    return (
        struct.pack(">I", tag_value & 0xFFFFFFFF)
        + pack_utf16be_null_terminated(text)
        + b"".join(struct.pack(">I", value & 0xFFFFFFFF) for value in tail_values)
    )


def compact_recipe_from_annotation_kinds(
    annotation_kinds: list[str],
    *,
    tag_value: int,
) -> dict[str, object]:
    return {
        "tag_value": tag_value,
        "tail_cells": [
            {
                "annotation_kind": annotation_kind,
                "source_u32": 0,
                "page_local_target_slot_delta": None,
                "page_local_target_offset": None,
            }
            for annotation_kind in annotation_kinds
        ],
        "tail_cell_count": len(annotation_kinds),
        "source_slot_index": None,
        "source_text": "",
    }


def compact_recipe_block_score(slots: list[dict[str, object]], start_index: int) -> int | None:
    if start_index + 5 > len(slots):
        return None
    block = slots[start_index:start_index + 5]
    audio_slot = block[0]
    if audio_slot["text_class"] != "audio_file" or audio_slot["dat_ref_count"] == 0:
        return None
    metadata_slots = block[1:]
    if any(slot["text_class"] != "metadata" or slot["metadata_blob_ref_count"] == 0 for slot in metadata_slots):
        return None
    score = 100
    score += sum(min(slot["metadata_blob_ref_count"], 6) for slot in metadata_slots)
    score += sum(min(slot["tail_cell_count"], 24) for slot in block)
    score -= sum(
        25
        for slot in block
        if slot["tag_value"] == 0 and slot["metadata_blob_ref_count"] == 0 and slot["dat_ref_count"] == 0
    )
    return score


def build_compact_slot_recipes(
    root: Path,
    template_library: dict[str, object] | None = None,
) -> dict[str, dict[str, object]]:
    fallback = {
        "audio_file": compact_recipe_from_annotation_kinds(
            ["dat_u32", "zero_u32", "dat_u32", "zero_u32"],
            tag_value=5,
        ),
        "title": compact_recipe_from_annotation_kinds(
            ["metadata_blob_u32", "zero_u32", "metadata_blob_u32", "zero_u32", "metadata_blob_u32"],
            tag_value=0,
        ),
        "artist": compact_recipe_from_annotation_kinds(
            ["metadata_blob_u32", "zero_u32", "metadata_blob_u32"],
            tag_value=0,
        ),
        "album": compact_recipe_from_annotation_kinds(
            ["metadata_blob_u32", "zero_u32", "metadata_blob_u32"],
            tag_value=0,
        ),
        "genre": compact_recipe_from_annotation_kinds(
            ["metadata_blob_u32", "zero_u32", "metadata_blob_u32"],
            tag_value=0,
        ),
        "_source_page_index": None,
    }
    if not template_library:
        return fallback

    metadata_blobs = template_library.get("compact_metadata_blob_by_offset")
    if metadata_blobs is None:
        return fallback

    files = find_db_files(root)
    db_idx = files["db.idx"].read_bytes()
    db_dic = files["db.dic"].read_bytes()
    db_dat = files["db.dat"].read_bytes()
    dat_record_by_start = {
        record.record_start: record
        for record in collect_dat_records(db_dat)
    }
    field_map = {
        field.entry_offset: field.name
        for field in parse_db_dic(db_dic)
    }
    best_match: tuple[tuple[bool, int, int, int], dict[str, object]] | None = None
    page_count = len(db_idx) // IDX_PAGE_SIZE
    for page_index in range(page_count):
        observed = parse_observed_compact_idx_page(
            db_idx,
            page_index,
            dat_record_by_start,
            field_map,
            metadata_blobs=metadata_blobs,
            db_dat_size=len(db_dat),
            max_slots=32,
            max_tail_cells=24,
        )
        block_scores: dict[int, int] = {}
        best_single_block: tuple[int, int] | None = None
        for start_index in range(max(0, observed["slot_count"] - 4)):
            score = compact_recipe_block_score(observed["slots"], start_index)
            if score is None:
                continue
            block_scores[start_index] = score
            if best_single_block is None or score > best_single_block[1]:
                best_single_block = (start_index, score)
        if best_single_block is None:
            continue
        has_two_block_prefix = 0 in block_scores and 5 in block_scores
        pair_score = (block_scores.get(0, 0) + block_scores.get(5, 0)) if has_two_block_prefix else 0
        start_index = 0 if has_two_block_prefix else best_single_block[0]
        rank = (
            has_two_block_prefix,
            pair_score,
            block_scores.get(start_index, 0),
            -page_index,
        )
        candidate = (rank, {"page_index": page_index, "start_index": start_index, "observed": observed})
        if best_match is None or candidate > best_match:
            best_match = candidate

    if best_match is None:
        return fallback

    observed = best_match[1]["observed"]
    start_index = best_match[1]["start_index"]
    source_slots = observed["slots"]
    role_indices = {
        "audio_file": start_index,
        "title": start_index + 1,
        "artist": start_index + 2,
        "album": start_index + 3,
        "genre": start_index + 4,
    }

    recipes = {
        role: compact_slot_recipe_from_observed_slot(
            source_slots[slot_index],
            source_slots,
            observed["offset"],
            slot_index,
            fallback_tag_value=5 if role == "audio_file" else 0,
        )
        for role, slot_index in role_indices.items()
    }
    recipes["_source_page_index"] = observed["page_index"]
    return recipes


def compact_slot_size(
    text: str,
    recipe: dict[str, object],
) -> int:
    return 4 + len(pack_utf16be_null_terminated(text)) + (len(recipe["tail_cells"]) * 4)


def resolve_compact_slot_tail_values(
    recipe: dict[str, object],
    *,
    blob_offset: int,
    dat_record_start: int,
    field_offset: int,
    page_slots: list[dict[str, object]],
    slot_index: int,
) -> list[int]:
    values: list[int] = []
    for cell in recipe["tail_cells"]:
        annotation_kind = cell["annotation_kind"]
        if annotation_kind == "metadata_blob_u32":
            values.append(blob_offset)
            continue
        if annotation_kind in {"dat_u32", "dat_24"}:
            values.append(dat_record_start)
            continue
        if annotation_kind == "field_u32":
            values.append(field_offset)
            continue
        if annotation_kind == "zero_u32":
            values.append(0)
            continue
        target_slot_delta = cell.get("page_local_target_slot_delta")
        target_offset = cell.get("page_local_target_offset")
        if target_slot_delta is not None and target_offset is not None:
            target_slot_index = slot_index + target_slot_delta
            if 0 <= target_slot_index < len(page_slots):
                target_slot = page_slots[target_slot_index]
                values.append(target_slot["absolute_text_offset"] + target_offset)
                continue
            values.append(0)
            continue
        values.append(cell.get("source_u32", 0))
    return values


def build_compact_metadata_pages(
    root: Path,
    target_library: list[TargetLibraryEntry],
    dbdat_records: list[TargetDatRecord],
    metadata_blobs: list[TargetMetadataBlob],
    db_dic: bytes,
    *,
    page_index_base: int = 0,
    template_library: dict[str, object] | None = None,
) -> tuple[list[bytes], list[dict[str, object]]]:
    field_offsets = {
        field.name: field.entry_offset
        for field in parse_db_dic(db_dic)
    }
    recipes = build_compact_slot_recipes(root, template_library)
    blob_by_target_path = {
        blob.target_path: blob
        for blob in metadata_blobs
    }
    record_by_target_path = {
        record.target_path: record
        for record in dbdat_records
        if record.kind == 0x200
    }

    pages: list[bytes] = []
    page_summaries: list[dict[str, object]] = []
    current_offset = 0
    current_slots: list[dict[str, object]] = []

    def finalize_page(page_index: int) -> None:
        nonlocal current_offset, current_slots
        if current_offset == 0:
            return
        absolute_page_index = page_index_base + page_index
        absolute_page_offset = absolute_page_index * IDX_PAGE_SIZE
        page_bytes = bytearray(IDX_PAGE_SIZE)
        rendered_slots: list[dict[str, object]] = []
        for slot in current_slots:
            slot["absolute_text_offset"] = absolute_page_offset + slot["start_offset"] + 4
        for slot_index, slot in enumerate(current_slots):
            tail_values = resolve_compact_slot_tail_values(
                slot["recipe"],
                blob_offset=slot["blob_offset"],
                dat_record_start=slot["dat_record_start"],
                field_offset=slot["field_offset"],
                page_slots=current_slots,
                slot_index=slot_index,
            )
            slot_bytes = compact_slot_bytes(
                slot["text"],
                tail_values,
                tag_value=slot["recipe"]["tag_value"],
            )
            start = slot["start_offset"]
            page_bytes[start:start + len(slot_bytes)] = slot_bytes
            rendered_slots.append(
                {
                    "field_name": slot["field_name"],
                    "target_path": slot["target_path"],
                    "object_id": slot["object_id"],
                    "blob_offset": slot["blob_offset"],
                    "blob_offset_hex": f"0x{slot['blob_offset']:08x}",
                    "text": slot["text"],
                    "tag_value": slot["recipe"]["tag_value"],
                    "tail_cell_count": len(tail_values),
                    "bytes": len(slot_bytes),
                    "recipe_source_slot_index": slot["recipe"].get("source_slot_index"),
                }
            )
        pages.append(bytes(page_bytes))
        page_summaries.append(
            {
                "page_index": page_index,
                "absolute_page_index": absolute_page_index,
                "page_family": "compact_metadata_page",
                "slot_count": len(rendered_slots),
                "bytes_used": current_offset,
                "template_source_page_index": recipes.get("_source_page_index"),
                "items": rendered_slots,
            }
        )
        current_offset = 0
        current_slots = []

    planned_entries = sorted(
        (entry for entry in target_library if entry.source_kind == "planned_addition"),
        key=lambda entry: entry.target_path.lower(),
    )
    for entry in planned_entries:
        record = record_by_target_path.get(entry.target_path)
        blob = blob_by_target_path.get(entry.target_path)
        if record is None or blob is None:
            continue
        slot_specs: list[tuple[str, str, dict[str, object], int]] = []
        title_text = blob.title
        if title_text:
            slot_specs.append(("Title", title_text, recipes["title"], field_offsets.get("Title", 0)))
        if blob.artist:
            slot_specs.append(("Artist", blob.artist, recipes["artist"], field_offsets.get("Artist", 0)))
        if blob.album:
            slot_specs.append(("Album", blob.album, recipes["album"], field_offsets.get("Album", 0)))
        if blob.genre:
            slot_specs.append(("Genre", blob.genre, recipes["genre"], field_offsets.get("Genre", 0)))
        slot_specs.append(("FileName", entry.file_name, recipes["audio_file"], field_offsets.get("FileName", 0)))

        for field_name, text, recipe, field_offset in slot_specs:
            slot_size = compact_slot_size(text, recipe)
            if current_offset and current_offset + slot_size > IDX_PAGE_SIZE:
                finalize_page(len(pages))
            if slot_size > IDX_PAGE_SIZE:
                raise ValueError(f"One compact metadata slot exceeds page size: {entry.target_path} [{field_name}]")
            current_slots.append(
                {
                    "field_name": field_name,
                    "target_path": entry.target_path,
                    "object_id": record.object_id,
                    "text": text,
                    "recipe": recipe,
                    "blob_offset": blob.offset,
                    "dat_record_start": record.record_start,
                    "field_offset": field_offset,
                    "start_offset": current_offset,
                    "bytes": slot_size,
                }
            )
            current_offset += slot_size

    if current_offset:
        finalize_page(len(pages))
    return pages, page_summaries


def build_idx_addition_pages(
    root: Path,
    planned_entries: list[TargetLibraryEntry],
    dbdat_records: list[TargetDatRecord],
    metadata_blobs: list[TargetMetadataBlob],
    db_dic: bytes,
    *,
    page_index_base: int = 0,
    template_library: dict[str, object] | None = None,
    include_title_chains: bool = False,
) -> tuple[list[bytes], list[dict[str, object]]]:
    include_chain_families = {"dat_folder", "dat_audio_file"}
    if include_title_chains:
        include_chain_families.add("dat_audio_title")
    chain_pages, chain_summaries = build_idx_prototype_pages(
        root,
        planned_entries,
        dbdat_records,
        db_dic,
        page_index_base=page_index_base,
        include_chain_families=include_chain_families,
        template_library=template_library,
    )
    compact_pages, compact_summaries = build_compact_metadata_pages(
        root,
        planned_entries,
        dbdat_records,
        metadata_blobs,
        db_dic,
        page_index_base=page_index_base + len(chain_pages),
        template_library=template_library,
    )
    return chain_pages + compact_pages, chain_summaries + compact_summaries


def build_idx_prototype_pages(
    root: Path,
    target_library: list[TargetLibraryEntry],
    dbdat_records: list[TargetDatRecord],
    db_dic: bytes,
    page_index_base: int = 0,
    include_chain_families: set[str] | None = None,
    metadata_field_order: list[str] | None = None,
    template_library: dict[str, object] | None = None,
) -> tuple[list[bytes], list[dict[str, object]]]:
    field_offsets = {
        field.name: field.entry_offset
        for field in parse_db_dic(db_dic)
    }
    _ = root
    dbdat_by_target_path = {
        record.target_path: record
        for record in dbdat_records
        if record.kind == 0x200
    }
    special_root_records = sorted(
        (record for record in dbdat_records if record.kind == 0x0),
        key=lambda record: (record.record_start, record.text),
    )
    folder_records = sorted(
        (record for record in dbdat_records if record.kind == 0x100),
        key=lambda record: (record.target_path.lower(), record.record_start),
    )
    playlist_records = sorted(
        (record for record in dbdat_records if record.kind == 0x400),
        key=lambda record: (record.target_path.lower(), record.record_start),
    )

    pages: list[bytes] = []
    page_summaries: list[dict[str, object]] = []
    current_chains: list[dict[str, object]] = []
    current_nodes: list[dict[str, object]] = []
    current_offset = IDX_OBSERVED_HEADER_BYTES
    metadata_field_order = metadata_field_order or ["Artist", "Genre", "Album", "Title", "Duration", "BitRate", "SampleRate"]
    selected_templates: dict[str, ObservedIdxChainTemplate] = {}
    if template_library:
        selected_templates = template_library.get("selected_templates", {})

    def make_text_node(field_name: str, node_type: int, text: str) -> dict[str, object]:
        return {
            "field_name": field_name,
            "field_entry_offset": field_offsets.get(field_name, 0),
            "value": stable_idx_text_value(text),
            "node_type": node_type,
            "payload": pack_utf16be_null_terminated(text),
            "payload_text": text,
        }

    def make_numeric_node(field_name: str, node_type: int, value: int) -> dict[str, object]:
        return {
            "field_name": field_name,
            "field_entry_offset": field_offsets.get(field_name, 0),
            "value": value,
            "node_type": node_type,
            "payload": b"",
            "payload_text": "",
        }

    def make_empty_tail_node() -> dict[str, object]:
        # Original pages commonly put a non-payload node after a text-bearing node.
        # Without that delimiter, a single-node chain with next=0 consumes the rest
        # of the page as payload and hides later chains from the firmware/parser.
        return {
            "field_name": "",
            "field_entry_offset": 0,
            "value": 0,
            "node_type": 0,
            "payload": b"",
            "payload_text": "",
        }

    def fallback_record_nodes(record: TargetDatRecord, chain_family: str) -> list[dict[str, object]]:
        if chain_family == "dat_special_root":
            return [make_text_node("RMusic", 0x03, record.text)]
        if chain_family == "dat_folder":
            return [make_text_node("FilePath", 0x05, clean_folder_name(record.text) + "/")]
        if chain_family == "dat_playlist":
            return [make_text_node("FileName", 0x03, record.text)]
        raise ValueError(f"Unhandled record fallback chain family: {chain_family}")

    def record_chain_nodes(record: TargetDatRecord, chain_family: str) -> list[dict[str, object]]:
        nodes = instantiate_observed_chain_template(
            selected_templates.get(chain_family),
            field_offsets=field_offsets,
            text_values=idx_text_values_for_record(record),
        )
        return nodes or fallback_record_nodes(record, chain_family)

    def fallback_entry_chain_nodes(entry: TargetLibraryEntry, chain_family: str) -> list[dict[str, object]]:
        title_text = target_title_for_idx(entry)
        if chain_family == "dat_audio_file":
            return [make_text_node("FileName", 0x03, entry.file_name)]
        if chain_family == "dat_audio_title":
            return [make_text_node("Title", 0x09, title_text)]
        if chain_family == "non_dat_metadata":
            nodes: list[dict[str, object]] = []
            if "Artist" in metadata_field_order and entry.artist:
                nodes.append(make_text_node("Artist", 0x02, entry.artist))
            if "Genre" in metadata_field_order and entry.genre:
                nodes.append(make_text_node("Genre", 0x03, entry.genre))
            if "Album" in metadata_field_order and entry.album:
                nodes.append(make_text_node("Album", 0x04, entry.album))
            if "Title" in metadata_field_order and title_text:
                nodes.append(make_text_node("Title", 0x07, title_text))
            duration_ms = int((entry.duration_seconds or 0) * 1000)
            if "Duration" in metadata_field_order and duration_ms:
                nodes.append(make_numeric_node("Duration", 0x01, duration_ms))
            if "BitRate" in metadata_field_order and entry.bit_rate_bps:
                nodes.append(make_numeric_node("BitRate", 0x01, entry.bit_rate_bps))
            if "SampleRate" in metadata_field_order and entry.sample_rate_hz:
                nodes.append(make_numeric_node("SampleRate", 0x01, entry.sample_rate_hz))
            return nodes
        raise ValueError(f"Unhandled entry fallback chain family: {chain_family}")

    def entry_chain_nodes(entry: TargetLibraryEntry, chain_family: str) -> list[dict[str, object]]:
        nodes = instantiate_observed_chain_template(
            selected_templates.get(chain_family),
            field_offsets=field_offsets,
            text_values=idx_text_values_for_entry(entry),
            numeric_values=idx_numeric_values_for_entry(entry),
        )
        return nodes or fallback_entry_chain_nodes(entry, chain_family)

    def finalize_page(page_index: int) -> None:
        nonlocal current_offset
        if not current_nodes:
            return
        page = bytearray(IDX_PAGE_SIZE)
        absolute_page_index = page_index_base + page_index
        page_base = absolute_page_index * IDX_PAGE_SIZE
        first_node_abs_offset = page_base + current_nodes[0]["start_rel"]
        last_node_abs_offset = page_base + current_nodes[-1]["start_rel"]
        struct.pack_into(">I", page, 0, first_node_abs_offset)
        struct.pack_into(">I", page, 4, last_node_abs_offset)
        struct.pack_into(">I", page, 8, len(current_nodes))
        struct.pack_into(">I", page, 12, len(current_chains))
        struct.pack_into(">I", page, 16, current_chains[0]["anchor_value"])
        struct.pack_into(">I", page, 20, current_chains[-1]["anchor_value"])
        struct.pack_into(">I", page, 24, current_chains[0]["object_id"])
        struct.pack_into(">I", page, 28, current_offset)

        for node in current_nodes:
            base = node["start_rel"]
            next_rel = node["next_rel"]
            next_abs = page_base + next_rel if next_rel else 0
            struct.pack_into(">I", page, base + 0, node["anchor_value"])
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
                "absolute_page_index": absolute_page_index,
                "item_count": len(current_chains),
                "chain_count": len(current_chains),
                "node_count": len(current_nodes),
                "first_node_abs_offset": first_node_abs_offset,
                "last_node_abs_offset": last_node_abs_offset,
                "bytes_used": current_offset,
                "items": [
                    {
                        "chain_family": item["chain_family"],
                        "target_path": item["target_path"],
                        "object_id": item["object_id"],
                        "anchor_value": item["anchor_value"],
                        "anchor_value_hex": f"0x{item['anchor_value']:08x}",
                        "template_source_page_index": item.get("template_source_page_index"),
                        "flags": item["flags"],
                        "node_count": item["node_count"],
                    }
                    for item in current_chains
                ],
            }
        )
        current_chains.clear()
        current_nodes.clear()
        current_offset = IDX_OBSERVED_HEADER_BYTES

    chain_specs: list[dict[str, object]] = []

    for record in special_root_records:
        chain_specs.append(
            {
                "chain_family": "dat_special_root",
                "target_path": record.target_path,
                "object_id": record.object_id,
                "anchor_value": record.record_start,
                "flags": 1,
                "nodes": record_chain_nodes(record, "dat_special_root"),
            }
        )

    for record in folder_records:
        if record.parent_id == 0xFFFFFFFF:
            continue
        folder_name = clean_folder_name(record.text) + "/"
        chain_specs.append(
            {
                "chain_family": "dat_folder",
                "target_path": record.target_path,
                "object_id": record.object_id,
                "anchor_value": record.record_start,
                "flags": 1,
                "nodes": record_chain_nodes(record, "dat_folder"),
            }
        )

    for record in playlist_records:
        chain_specs.append(
            {
                "chain_family": "dat_playlist",
                "target_path": record.target_path,
                "object_id": record.object_id,
                "anchor_value": record.record_start,
                "flags": 1,
                "nodes": record_chain_nodes(record, "dat_playlist"),
            }
        )

    for entry in sorted(target_library, key=lambda item: item.target_path.lower()):
        dbdat_record = dbdat_by_target_path.get(entry.target_path)
        if dbdat_record is None:
            continue
        flags = 1 if entry.source_kind == "existing" else 2
        title_text = target_title_for_idx(entry)

        chain_specs.append(
            {
                "chain_family": "dat_audio_file",
                "target_path": entry.target_path,
                "object_id": dbdat_record.object_id,
                "anchor_value": dbdat_record.record_start,
                "flags": flags,
                "nodes": entry_chain_nodes(entry, "dat_audio_file"),
            }
        )

        if title_text and title_text != entry.file_name:
            chain_specs.append(
                {
                    "chain_family": "dat_audio_title",
                    "target_path": entry.target_path,
                    "object_id": dbdat_record.object_id,
                    "anchor_value": dbdat_record.record_start,
                    "flags": flags,
                    "nodes": entry_chain_nodes(entry, "dat_audio_title"),
                }
            )

        metadata_nodes = entry_chain_nodes(entry, "non_dat_metadata")
        if metadata_nodes:
            chain_specs.append(
                {
                    "chain_family": "non_dat_metadata",
                    "target_path": entry.target_path,
                    "object_id": dbdat_record.object_id,
                    "anchor_value": 0x20000 + dbdat_record.object_id,
                    "flags": flags,
                    "nodes": metadata_nodes,
                }
            )

    if include_chain_families is not None:
        chain_specs = [
            chain
            for chain in chain_specs
            if chain["chain_family"] in include_chain_families
        ]

    chain_specs.sort(key=lambda item: (item["target_path"].lower(), item["chain_family"], item["anchor_value"]))

    for chain in chain_specs:
        if len(chain["nodes"]) == 1 and chain["nodes"][0]["payload"]:
            chain["nodes"] = [*chain["nodes"], make_empty_tail_node()]
        chain_size = sum(IDX_OBSERVED_NODE_BYTES + len(node["payload"]) for node in chain["nodes"])
        if current_chains and current_offset + chain_size > IDX_PAGE_SIZE:
            finalize_page(len(pages))
        if IDX_OBSERVED_HEADER_BYTES + chain_size > IDX_PAGE_SIZE:
            raise ValueError(f"One observed idx prototype chain exceeds page size: {chain['target_path']} [{chain['chain_family']}]")

        chain_start_rel = current_offset
        chain_nodes = chain["nodes"]
        for index, node in enumerate(chain_nodes):
            node["anchor_value"] = chain["anchor_value"]
            node["object_id"] = chain["object_id"]
            node["start_rel"] = current_offset
            node_size = IDX_OBSERVED_NODE_BYTES + len(node["payload"])
            next_rel = current_offset + node_size if index + 1 < len(chain_nodes) else 0
            node["next_rel"] = next_rel
            current_nodes.append(node)
            current_offset += node_size

        current_chains.append(
            {
                "chain_family": chain["chain_family"],
                "target_path": chain["target_path"],
                "object_id": chain["object_id"],
                "anchor_value": chain["anchor_value"],
                "template_source_page_index": (
                    selected_templates.get(chain["chain_family"]).source_page_index
                    if selected_templates.get(chain["chain_family"]) is not None
                    else None
                ),
                "flags": chain["flags"],
                "node_count": len(chain_nodes),
                "start_rel": chain_start_rel,
            }
        )

    if current_nodes:
        finalize_page(len(pages))

    return pages, page_summaries


def trailing_zero_page_indices(db_idx: bytes) -> list[int]:
    page_count = len(db_idx) // IDX_PAGE_SIZE
    zero_pages: list[int] = []
    for page_index in range(page_count - 1, -1, -1):
        block = db_idx[page_index * IDX_PAGE_SIZE:(page_index + 1) * IDX_PAGE_SIZE]
        if any(block):
            break
        zero_pages.append(page_index)
    return list(reversed(zero_pages))


def build_compact_idx_overlay(
    root: Path,
    original_idx: bytes,
    planned_entries: list[TargetLibraryEntry],
    dbdat_records: list[TargetDatRecord],
    metadata_blobs: list[TargetMetadataBlob],
    db_dic: bytes,
    template_library: dict[str, object] | None = None,
) -> tuple[bytes, list[dict[str, object]], list[TargetLibraryEntry]] | None:
    zero_pages = trailing_zero_page_indices(original_idx)
    if not zero_pages:
        return None

    max_pages = len(zero_pages)
    sorted_entries = sorted(planned_entries, key=lambda entry: entry.target_path.lower())
    selected_entries: list[TargetLibraryEntry] = []
    selected_pages: list[bytes] = []
    selected_summaries: list[dict[str, object]] = []

    for count in range(len(sorted_entries), 0, -1):
        candidate_entries = sorted_entries[:count]
        candidate_records = select_idx_extension_dbdat_records(dbdat_records, candidate_entries)
        candidate_blobs = [
            blob
            for blob in metadata_blobs
            if blob.target_path in {entry.target_path for entry in candidate_entries}
        ]
        candidate_pages, candidate_summaries = build_idx_addition_pages(
            root,
            candidate_entries,
            candidate_records,
            candidate_blobs,
            db_dic,
            page_index_base=zero_pages[0],
            template_library=template_library,
        )
        if len(candidate_pages) <= max_pages:
            selected_entries = candidate_entries
            selected_pages = candidate_pages
            selected_summaries = candidate_summaries
            break

    if not selected_pages:
        return None

    idx_bytes = bytearray(original_idx)
    for page_offset, page_bytes in zip(zero_pages, selected_pages):
        start = page_offset * IDX_PAGE_SIZE
        idx_bytes[start:start + IDX_PAGE_SIZE] = page_bytes

    overlay_summary = [
        {
            "mode": "compact_inplace_overlay",
            "overlay_page_indices": zero_pages,
            "overlay_page_count": len(selected_pages),
            "available_zero_page_count": len(zero_pages),
            "included_planned_entry_count": len(selected_entries),
            "omitted_planned_entry_count": len(planned_entries) - len(selected_entries),
            "included_target_paths": [entry.target_path for entry in selected_entries],
        },
        *selected_summaries,
    ]
    return bytes(idx_bytes), overlay_summary, selected_entries


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
        chain_count_header = struct.unpack_from(">I", block, 12)[0]
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
        pages.append(
            {
                "page_index": page_index,
                "item_count": chain_count_header,
                "item_count_header": chain_count_header,
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


def command_idx_compact_page(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    db_idx = files["db.idx"].read_bytes()
    db_dic = files["db.dic"].read_bytes()
    db_dat = files["db.dat"].read_bytes()
    dic_fields = parse_db_dic(db_dic)
    dat_records = collect_dat_records(db_dat)
    dat_record_by_start = {record.record_start: record for record in dat_records}
    field_map = {field.entry_offset: field.name for field in dic_fields}
    compact_library = build_observed_compact_idx_template_library(db_idx, db_dic, db_dat)
    metadata_blobs = compact_library["metadata_blob_by_offset"]

    page_count = len(db_idx) // IDX_PAGE_SIZE
    selected_pages = range(page_count) if args.page is None else [args.page]
    pages = [
        parse_observed_compact_idx_page(
            db_idx,
            page_index,
            dat_record_by_start,
            field_map,
            metadata_blobs=metadata_blobs,
            db_dat_size=len(db_dat),
            max_slots=args.max_slots,
            max_tail_cells=args.max_tail_cells,
        )
        for page_index in selected_pages
    ]

    report = {
        "root": str(root),
        "page_size": IDX_PAGE_SIZE,
        "page_count": page_count,
        "compact_family_count": compact_library["family_count"],
        "compact_metadata_blob_count": compact_library["metadata_blob_count"],
        "pages": pages,
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def summarize_observed_idx(
    db_idx: bytes,
    db_dic: bytes,
    db_dat: bytes,
    *,
    limit: int = 20,
    max_nodes: int = 128,
    max_groups: int = 32,
    example_limit: int = 5,
) -> dict[str, object]:
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
            max_nodes=max_nodes,
            max_groups=max_groups,
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
            if len(examples) < example_limit:
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

    return {
        "page_count": page_count,
        "chain_count": sum(chain_length_counts.values()),
        "chain_length_counts": [{"node_count": k, "count": v} for k, v in sorted(chain_length_counts.items(), key=lambda item: (-item[1], item[0]))[:limit]],
        "anchor_class_counts": [{"anchor_class": k, "count": v} for k, v in sorted(anchor_class_counts.items(), key=lambda item: (-item[1], item[0]))],
        "top_anchor_annotations": [{"anchor_annotation": k, "count": v} for k, v in sorted(anchor_annotation_counts.items(), key=lambda item: (-item[1], item[0]))[:limit]],
        "node_type_counts": [{"node_type": k, "count": v} for k, v in sorted(node_type_counts.items(), key=lambda item: (-item[1], item[0]))[:limit]],
        "node_type_by_payload_class": [
            {"node_type": node_type, "payload_classes": payloads}
            for node_type, payloads in sorted(
                node_type_by_payload_class.items(),
                key=lambda item: (-sum(item[1].values()), item[0]),
            )[:limit]
        ],
        "node_type_by_anchor_class": [
            {"node_type": node_type, "anchor_classes": classes}
            for node_type, classes in sorted(
                node_type_by_anchor_class.items(),
                key=lambda item: (-sum(item[1].values()), item[0]),
            )[:limit]
        ],
        "anchor_class_examples": anchor_class_examples,
    }


def command_idx_observed_summary(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    report = {
        "root": str(root),
        **summarize_observed_idx(
            files["db.idx"].read_bytes(),
            files["db.dic"].read_bytes(),
            files["db.dat"].read_bytes(),
            limit=args.limit,
            max_nodes=args.max_nodes,
            max_groups=args.max_groups,
            example_limit=args.example_limit,
        ),
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_idx_template_summary(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    files = find_db_files(root)
    template_library = build_observed_idx_template_library(
        files["db.idx"].read_bytes(),
        files["db.dic"].read_bytes(),
        files["db.dat"].read_bytes(),
        max_nodes=args.max_nodes,
        max_groups=args.max_groups,
    )
    report = {
        "root": str(root),
        "page_count": template_library["page_count"],
        "family_count": template_library["family_count"],
        "chain_family_count": template_library["chain_family_count"],
        "compact_family_count": template_library["compact_family_count"],
        "compact_metadata_blob_count": template_library["compact_metadata_blob_count"],
        "families": template_library["family_summaries"],
        "compact_families": template_library["compact_family_summaries"],
        "compact_metadata_blobs": template_library["compact_metadata_blobs"],
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def count_summary_entries(items: list[dict[str, object]], key_field: str) -> dict[str, int]:
    return {
        str(item[key_field]): int(item["count"])
        for item in items
    }


def assess_install_safety(
    current_dbdat: bytes,
    current_idx: bytes,
    current_dic: bytes,
    prototype_dbdat: bytes,
    prototype_idx: bytes,
    target_library_entry_count: int | None,
) -> dict[str, object]:
    current_idx_hits = extract_db_idx_strings(current_idx)
    prototype_idx_hits = extract_db_idx_strings(prototype_idx)
    current_observed = summarize_observed_idx(current_idx, current_dic, current_dbdat, example_limit=0)
    prototype_observed = summarize_observed_idx(prototype_idx, current_dic, prototype_dbdat, example_limit=0)
    current_anchor_counts = count_summary_entries(current_observed["anchor_class_counts"], "anchor_class")
    prototype_anchor_counts = count_summary_entries(prototype_observed["anchor_class_counts"], "anchor_class")
    current_audio_name_count = len(database_audio_names(extract_db_dat_strings(current_dbdat), current_idx_hits))
    prototype_audio_name_count = len(database_audio_names(extract_db_dat_strings(prototype_dbdat), prototype_idx_hits))

    issues: list[str] = []

    current_chain_count = int(current_observed["chain_count"])
    prototype_chain_count = int(prototype_observed["chain_count"])
    if current_chain_count and prototype_chain_count < max(1, current_chain_count // 2):
        issues.append(
            "prototype db.idx exposes far fewer observed chains than the current device database "
            f"({prototype_chain_count} vs {current_chain_count})"
        )

    current_dat_audio = current_anchor_counts.get("dat_audio", 0)
    prototype_dat_audio = prototype_anchor_counts.get("dat_audio", 0)
    if current_dat_audio and prototype_dat_audio < max(1, current_dat_audio // 2):
        issues.append(
            "prototype db.idx exposes far fewer audio-anchored chains than the current device database "
            f"({prototype_dat_audio} vs {current_dat_audio})"
        )

    current_non_dat = current_anchor_counts.get("non_dat", 0)
    prototype_non_dat = prototype_anchor_counts.get("non_dat", 0)
    if current_non_dat and prototype_non_dat < max(1, current_non_dat // 2):
        issues.append(
            "prototype db.idx exposes far fewer metadata/non-dat chains than the current device database "
            f"({prototype_non_dat} vs {current_non_dat})"
        )

    if current_idx_hits and len(prototype_idx_hits) < max(1, int(len(current_idx_hits) * 0.65)):
        issues.append(
            "prototype db.idx contains far fewer UTF-16BE strings than the current device database "
            f"({len(prototype_idx_hits)} vs {len(current_idx_hits)})"
        )

    if target_library_entry_count is not None and prototype_audio_name_count < target_library_entry_count:
        issues.append(
            "prototype database advertises fewer audio names than the target library requires "
            f"({prototype_audio_name_count} vs {target_library_entry_count})"
        )

    return {
        "issues": issues,
        "current": {
            "idx_string_count": len(current_idx_hits),
            "audio_name_count": current_audio_name_count,
            "observed_chain_count": current_chain_count,
            "anchor_class_counts": current_anchor_counts,
        },
        "prototype": {
            "idx_string_count": len(prototype_idx_hits),
            "audio_name_count": prototype_audio_name_count,
            "observed_chain_count": prototype_chain_count,
            "anchor_class_counts": prototype_anchor_counts,
        },
    }


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
    target_library = build_target_library_entries(
        canonical_entries,
        plan["planned_additions"],
        existing_object_id_floor=max_preserved_object_id(root),
    )

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
    target_library = build_target_library_entries(
        plan["canonical_entries"],
        plan["planned_additions"],
        existing_object_id_floor=max_preserved_object_id(root),
    )
    records = build_dbdat_prototype_records(root, target_library)
    metadata_blobs = build_dbdat_metadata_blobs(root, target_library, records)
    original_dbdat = find_db_files(root)["db.dat"].read_bytes()
    dbdat_bytes = serialize_dbdat_prototype(records, base_bytes=original_dbdat, metadata_blobs=metadata_blobs)

    out_dir.mkdir(parents=True, exist_ok=True)
    dbdat_path = out_dir / "db.dat.prototype"
    records_path = out_dir / "db.dat.records.json"
    blobs_path = out_dir / "db.dat.metadata_blobs.json"
    dbdat_path.write_bytes(dbdat_bytes)
    records_path.write_text(json.dumps([asdict(record) for record in records], indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    blobs_path.write_text(json.dumps([asdict(blob) for blob in metadata_blobs], indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    reparsed = validated_folder_file_records(dbdat_bytes)
    report = {
        "out_dir": str(out_dir),
        "dbdat_path": str(dbdat_path),
        "records_path": str(records_path),
        "blobs_path": str(blobs_path),
        "record_count": len(records),
        "metadata_blob_count": len(metadata_blobs),
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
    target_library = build_target_library_entries(
        plan["canonical_entries"],
        plan["planned_additions"],
        existing_object_id_floor=max_preserved_object_id(root),
    )
    dbdat_records = build_dbdat_prototype_records(root, target_library)
    metadata_blobs = build_dbdat_metadata_blobs(root, target_library, dbdat_records)
    template_library = build_observed_idx_template_library(
        files["db.idx"].read_bytes(),
        db_dic,
        files["db.dat"].read_bytes(),
    )
    if not plan["planned_additions"]:
        idx_bytes = files["db.idx"].read_bytes()
        pages = [idx_bytes[index:index + IDX_PAGE_SIZE] for index in range(0, len(idx_bytes), IDX_PAGE_SIZE)]
        page_summaries = [{"mode": "preserved_original", "page_count": len(pages)}]
    else:
        original_idx = files["db.idx"].read_bytes()
        original_page_count = len(original_idx) // IDX_PAGE_SIZE
        planned_entries = [entry for entry in target_library if entry.source_kind == "planned_addition"]
        compact_overlay = build_compact_idx_overlay(
            root,
            original_idx,
            planned_entries,
            dbdat_records,
            metadata_blobs,
            db_dic,
            template_library=template_library,
        )
        if compact_overlay is not None:
            idx_bytes, page_summaries, _selected_entries = compact_overlay
            pages = [idx_bytes[index:index + IDX_PAGE_SIZE] for index in range(0, len(idx_bytes), IDX_PAGE_SIZE)]
        else:
            extension_records = select_idx_extension_dbdat_records(dbdat_records, planned_entries)
            extension_blobs = [
                blob
                for blob in metadata_blobs
                if blob.target_path in {entry.target_path for entry in planned_entries}
            ]
            extension_pages, page_summaries = build_idx_addition_pages(
                root,
                planned_entries,
                extension_records,
                extension_blobs,
                db_dic,
                page_index_base=original_page_count,
                template_library=template_library,
            )
            idx_bytes = original_idx + b"".join(extension_pages)
            pages = [idx_bytes[index:index + IDX_PAGE_SIZE] for index in range(0, len(idx_bytes), IDX_PAGE_SIZE)]
            page_summaries = [
                {
                    "mode": "preserved_original_plus_extension",
                    "original_page_count": original_page_count,
                    "extension_page_count": len(extension_pages),
                    "template_family_count": template_library["family_count"],
                },
                *page_summaries,
            ]

    out_dir.mkdir(parents=True, exist_ok=True)
    idx_path = out_dir / "db.idx.prototype"
    pages_path = out_dir / "db.idx.pages.json"
    templates_path = out_dir / "db.idx.templates.json"
    blobs_path = out_dir / "db.dat.metadata_blobs.json"
    idx_path.write_bytes(idx_bytes)
    pages_path.write_text(json.dumps(page_summaries, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    blobs_path.write_text(json.dumps([asdict(blob) for blob in metadata_blobs], indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    templates_path.write_text(
        json.dumps(
            {
                "page_count": template_library["page_count"],
                "family_count": template_library["family_count"],
                "families": template_library["family_summaries"],
                "chain_family_count": template_library["chain_family_count"],
                "compact_family_count": template_library["compact_family_count"],
                "compact_families": template_library["compact_family_summaries"],
                "compact_metadata_blob_count": template_library["compact_metadata_blob_count"],
                "compact_metadata_blobs": template_library["compact_metadata_blobs"],
            },
            indent=2,
            ensure_ascii=False,
        ) + "\n",
        encoding="utf-8",
    )

    reparsed_pages = parse_idx_prototype_pages(idx_bytes)
    report = {
        "out_dir": str(out_dir),
        "idx_path": str(idx_path),
        "pages_path": str(pages_path),
        "templates_path": str(templates_path),
        "blobs_path": str(blobs_path),
        "page_count": len(pages),
        "serialized_size_bytes": len(idx_bytes),
        "reparsed_page_count": len(reparsed_pages),
        "target_library_entry_count": len(target_library),
        "planned_addition_count": len(plan["planned_additions"]),
        "metadata_blob_count": len(metadata_blobs),
        "template_family_count": template_library["family_count"],
        "compact_template_family_count": template_library["compact_family_count"],
        "compact_metadata_blob_count": template_library["compact_metadata_blob_count"],
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
    target_library = build_target_library_entries(
        canonical_entries,
        plan["planned_additions"],
        existing_object_id_floor=max_preserved_object_id(root),
    )
    records = build_dbdat_prototype_records(root, target_library)
    metadata_blobs = build_dbdat_metadata_blobs(root, target_library, records)
    original_dbdat = find_db_files(root)["db.dat"].read_bytes()
    dbdat_bytes = serialize_dbdat_prototype(records, base_bytes=original_dbdat, metadata_blobs=metadata_blobs)
    template_library = build_observed_idx_template_library(
        files["db.idx"].read_bytes(),
        db_dic,
        files["db.dat"].read_bytes(),
    )
    manifest_note: dict[str, object] = {}
    if not plan["planned_additions"]:
        idx_bytes = files["db.idx"].read_bytes()
        pages = [idx_bytes[index:index + IDX_PAGE_SIZE] for index in range(0, len(idx_bytes), IDX_PAGE_SIZE)]
        page_summaries = [{"mode": "preserved_original", "page_count": len(pages)}]
    else:
        original_idx = files["db.idx"].read_bytes()
        original_page_count = len(original_idx) // IDX_PAGE_SIZE
        planned_entries = [entry for entry in target_library if entry.source_kind == "planned_addition"]
        compact_overlay = build_compact_idx_overlay(
            root,
            original_idx,
            planned_entries,
            records,
            metadata_blobs,
            db_dic,
            template_library=template_library,
        )
        if compact_overlay is not None:
            idx_bytes, page_summaries, selected_entries = compact_overlay
            pages = [idx_bytes[index:index + IDX_PAGE_SIZE] for index in range(0, len(idx_bytes), IDX_PAGE_SIZE)]
            if len(selected_entries) != len(planned_entries):
                manifest_note = {
                    "compact_overlay_included_planned_entry_count": len(selected_entries),
                    "compact_overlay_omitted_planned_entry_count": len(planned_entries) - len(selected_entries),
                }
            else:
                manifest_note = {}
        else:
            extension_records = select_idx_extension_dbdat_records(records, planned_entries)
            extension_blobs = [
                blob
                for blob in metadata_blobs
                if blob.target_path in {entry.target_path for entry in planned_entries}
            ]
            extension_pages, page_summaries = build_idx_addition_pages(
                root,
                planned_entries,
                extension_records,
                extension_blobs,
                db_dic,
                page_index_base=original_page_count,
                template_library=template_library,
            )
            idx_bytes = original_idx + b"".join(extension_pages)
            pages = [idx_bytes[index:index + IDX_PAGE_SIZE] for index in range(0, len(idx_bytes), IDX_PAGE_SIZE)]
            page_summaries = [
                {
                    "mode": "preserved_original_plus_extension",
                    "original_page_count": original_page_count,
                    "extension_page_count": len(extension_pages),
                    "template_family_count": template_library["family_count"],
                },
                *page_summaries,
            ]
            manifest_note = {}

    reparsed_records = validated_folder_file_records(dbdat_bytes)
    reparsed_pages = parse_idx_prototype_pages(idx_bytes)

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
        "dbdat_reparsed_record_count": len(reparsed_records),
        "dbdat_serialized_size_bytes": len(dbdat_bytes),
        "dbdat_metadata_blob_count": len(metadata_blobs),
        "idx_page_count": len(pages),
        "idx_reparsed_page_count": len(reparsed_pages),
        "idx_serialized_size_bytes": len(idx_bytes),
        "idx_template_family_count": template_library["family_count"],
        "idx_chain_template_family_count": template_library["chain_family_count"],
        "idx_compact_template_family_count": template_library["compact_family_count"],
        "idx_compact_metadata_blob_count": template_library["compact_metadata_blob_count"],
        **manifest_note,
    }
    write_snapshot_artifacts(out_dir, manifest, canonical_entries, plan, target_library)

    dbdat_path = out_dir / "db.dat.prototype"
    records_path = out_dir / "db.dat.records.json"
    blobs_path = out_dir / "db.dat.metadata_blobs.json"
    idx_path = out_dir / "db.idx.prototype"
    pages_path = out_dir / "db.idx.pages.json"
    templates_path = out_dir / "db.idx.templates.json"
    dic_reference_path = out_dir / "db.dic.reference"
    dbdat_path.write_bytes(dbdat_bytes)
    records_path.write_text(json.dumps([asdict(record) for record in records], indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    blobs_path.write_text(json.dumps([asdict(blob) for blob in metadata_blobs], indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    idx_path.write_bytes(idx_bytes)
    pages_path.write_text(json.dumps(page_summaries, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    templates_path.write_text(
        json.dumps(
            {
                "page_count": template_library["page_count"],
                "family_count": template_library["family_count"],
                "families": template_library["family_summaries"],
                "chain_family_count": template_library["chain_family_count"],
                "compact_family_count": template_library["compact_family_count"],
                "compact_families": template_library["compact_family_summaries"],
                "compact_metadata_blob_count": template_library["compact_metadata_blob_count"],
                "compact_metadata_blobs": template_library["compact_metadata_blobs"],
            },
            indent=2,
            ensure_ascii=False,
        ) + "\n",
        encoding="utf-8",
    )
    dic_reference_path.write_bytes(db_dic)

    report = {
        "out_dir": str(out_dir),
        "manifest_path": str(out_dir / "manifest.json"),
        "dbdat_path": str(dbdat_path),
        "records_path": str(records_path),
        "blobs_path": str(blobs_path),
        "idx_path": str(idx_path),
        "pages_path": str(pages_path),
        "templates_path": str(templates_path),
        "dic_reference_path": str(dic_reference_path),
        "record_count": len(records),
        "metadata_blob_count": len(metadata_blobs),
        "reparsed_record_count": len(reparsed_records),
        "page_count": len(pages),
        "reparsed_page_count": len(reparsed_pages),
        "planned_addition_count": len(plan["planned_additions"]),
        "target_library_entry_count": len(target_library),
        "template_family_count": template_library["family_count"],
        "compact_template_family_count": template_library["compact_family_count"],
        "compact_metadata_blob_count": template_library["compact_metadata_blob_count"],
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_test_install_prototype(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    bundle_dir = Path(args.bundle_dir).resolve()
    files = find_db_files(root)

    manifest_path = bundle_dir / "manifest.json"
    dbdat_proto_path = bundle_dir / "db.dat.prototype"
    idx_proto_path = bundle_dir / "db.idx.prototype"
    dic_reference_path = bundle_dir / "db.dic.reference"
    missing = [
        str(path.name)
        for path in [manifest_path, dbdat_proto_path, idx_proto_path, dic_reference_path]
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError(f"Missing prototype bundle files under {bundle_dir}: {', '.join(missing)}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dbdat_proto = dbdat_proto_path.read_bytes()
    idx_proto = idx_proto_path.read_bytes()
    dic_reference = dic_reference_path.read_bytes()
    current_dbdat = files["db.dat"].read_bytes()
    current_idx = files["db.idx"].read_bytes()
    current_dic = files["db.dic"].read_bytes()

    reparsed_records = validated_folder_file_records(dbdat_proto)
    reparsed_pages = parse_idx_prototype_pages(idx_proto)
    if manifest.get("root") and str(root) != manifest["root"]:
        raise ValueError(f"Bundle root mismatch: manifest has {manifest['root']}, requested root is {root}")
    expected_dbdat_reparsed = manifest.get("dbdat_reparsed_record_count", manifest.get("dbdat_record_count"))
    if expected_dbdat_reparsed is not None and len(reparsed_records) != expected_dbdat_reparsed:
        raise ValueError("db.dat prototype record count does not match manifest")
    expected_idx_reparsed = manifest.get("idx_reparsed_page_count", manifest.get("idx_page_count"))
    if expected_idx_reparsed is not None and len(reparsed_pages) != expected_idx_reparsed:
        raise ValueError("db.idx prototype page count does not match manifest")
    if dic_reference != current_dic and not args.replace_dic:
        raise ValueError("db.dic.reference does not match the current device db.dic; rerun with --replace-dic only if intentional")

    safety_report = assess_install_safety(
        current_dbdat,
        current_idx,
        current_dic,
        dbdat_proto,
        idx_proto,
        manifest.get("target_library_entry_count"),
    )
    if safety_report["issues"] and not args.allow_unsafe_install:
        issue_lines = "\n".join(f"- {issue}" for issue in safety_report["issues"])
        raise ValueError(
            "Refusing unsafe prototype install because the generated database looks much thinner than the current device database:\n"
            f"{issue_lines}\n"
            "Use --allow-unsafe-install only if you explicitly want to override this guard."
        )

    if len(dbdat_proto) > len(current_dbdat) and not args.allow_size_growth:
        raise ValueError(
            f"db.dat prototype is larger than the current device file ({len(dbdat_proto)} > {len(current_dbdat)}). "
            "Rerun with --allow-size-growth only if you explicitly want to test larger database files."
        )
    if len(idx_proto) > len(current_idx) and not args.allow_size_growth:
        raise ValueError(
            f"db.idx prototype is larger than the current device file ({len(idx_proto)} > {len(current_idx)}). "
            "Rerun with --allow-size-growth only if you explicitly want to test larger database files."
        )

    padded_dbdat = dbdat_proto if len(dbdat_proto) > len(current_dbdat) else pad_bytes_to_length(dbdat_proto, len(current_dbdat), "db.dat prototype")
    padded_idx = idx_proto if len(idx_proto) > len(current_idx) else pad_bytes_to_length(idx_proto, len(current_idx), "db.idx prototype")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_dir = Path(args.backup_dir).resolve() if args.backup_dir else Path("/tmp") / f"iriver-e10-backup-{timestamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)

    backup_dbdat = backup_dir / "db.dat.backup"
    backup_idx = backup_dir / "db.idx.backup"
    backup_dic = backup_dir / "db.dic.backup"
    backup_dbdat.write_bytes(current_dbdat)
    backup_idx.write_bytes(current_idx)
    backup_dic.write_bytes(current_dic)

    install_manifest = {
        "root": str(root),
        "bundle_dir": str(bundle_dir),
        "backup_dir": str(backup_dir),
        "timestamp": timestamp,
        "current_sizes": {
            "db.dat": len(current_dbdat),
            "db.idx": len(current_idx),
            "db.dic": len(current_dic),
        },
        "prototype_sizes": {
            "db.dat.prototype": len(dbdat_proto),
            "db.idx.prototype": len(idx_proto),
        },
        "installed_sizes": {
            "db.dat": len(padded_dbdat),
            "db.idx": len(padded_idx),
        },
        "sha256": {
            "current_db.dat": sha256_bytes(current_dbdat),
            "current_db.idx": sha256_bytes(current_idx),
            "current_db.dic": sha256_bytes(current_dic),
            "prototype_db.dat": sha256_bytes(dbdat_proto),
            "prototype_db.idx": sha256_bytes(idx_proto),
            "prototype_db.dic": sha256_bytes(dic_reference),
            "installed_db.dat": sha256_bytes(padded_dbdat),
            "installed_db.idx": sha256_bytes(padded_idx),
        },
        "manifest_counts": {
            "target_library_entry_count": manifest.get("target_library_entry_count"),
            "planned_addition_count": manifest.get("planned_addition_count"),
            "dbdat_record_count": manifest.get("dbdat_record_count"),
            "idx_page_count": manifest.get("idx_page_count"),
        },
        "safety_report": safety_report,
    }
    (backup_dir / "install_manifest.json").write_text(json.dumps(install_manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    dbdat_tmp = files["db.dat"].with_name("db.dat.codex-new")
    idx_tmp = files["db.idx"].with_name("db.idx.codex-new")
    dic_tmp = files["db.dic"].with_name("db.dic.codex-new")
    try:
        dbdat_tmp.write_bytes(padded_dbdat)
        idx_tmp.write_bytes(padded_idx)
        if args.replace_dic:
            dic_tmp.write_bytes(dic_reference)

        dbdat_tmp.replace(files["db.dat"])
        idx_tmp.replace(files["db.idx"])
        if args.replace_dic:
            dic_tmp.replace(files["db.dic"])
    finally:
        for tmp_path in [dbdat_tmp, idx_tmp, dic_tmp]:
            if tmp_path.exists():
                tmp_path.unlink()

    report = {
        "root": str(root),
        "bundle_dir": str(bundle_dir),
        "backup_dir": str(backup_dir),
        "installed": True,
        "replaced_dic": bool(args.replace_dic),
        "record_count": len(reparsed_records),
        "page_count": len(reparsed_pages),
        "safety_report": safety_report,
        "installed_sizes": {
            "db.dat": len(padded_dbdat),
            "db.idx": len(padded_idx),
            "db.dic": len(dic_reference if args.replace_dic else current_dic),
        },
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def command_restore_system_backup(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    backup_dir = Path(args.backup_dir).resolve()
    files = find_db_files(root)
    backup_paths = {
        "db.dat": backup_dir / "db.dat.backup",
        "db.idx": backup_dir / "db.idx.backup",
        "db.dic": backup_dir / "db.dic.backup",
    }
    missing = [name for name, path in backup_paths.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing backup files under {backup_dir}: {', '.join(missing)}")

    tmp_paths = {
        name: files[name].with_name(f"{name}.codex-restore")
        for name in backup_paths
    }
    try:
        for name, backup_path in backup_paths.items():
            tmp_paths[name].write_bytes(backup_path.read_bytes())
        for name, path in tmp_paths.items():
            path.replace(files[name])
    finally:
        for path in tmp_paths.values():
            if path.exists():
                path.unlink()

    report = {
        "root": str(root),
        "backup_dir": str(backup_dir),
        "restored": True,
    }
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


def diff_byte_spans(left: bytes, right: bytes, *, max_spans: int = 32) -> list[dict[str, int]]:
    limit = min(len(left), len(right))
    spans: list[dict[str, int]] = []
    start: int | None = None
    prev = -1
    for offset in range(limit):
        if left[offset] == right[offset]:
            if start is not None:
                spans.append({"start": start, "end": prev + 1, "length": prev + 1 - start})
                if len(spans) >= max_spans:
                    return spans
                start = None
            continue
        if start is None:
            start = offset
        prev = offset
    if start is not None and len(spans) < max_spans:
        spans.append({"start": start, "end": prev + 1, "length": prev + 1 - start})
    if len(left) != len(right) and len(spans) < max_spans:
        tail_start = limit
        tail_end = max(len(left), len(right))
        spans.append({"start": tail_start, "end": tail_end, "length": tail_end - tail_start})
    return spans


def summarize_binary_diff(left: bytes, right: bytes, *, page_size: int | None = None) -> dict[str, object]:
    limit = min(len(left), len(right))
    differing_offsets = [offset for offset in range(limit) if left[offset] != right[offset]]
    report: dict[str, object] = {
        "equal": left == right,
        "left_size": len(left),
        "right_size": len(right),
        "differing_byte_count": len(differing_offsets) + abs(len(left) - len(right)),
        "first_diff_offset": differing_offsets[0] if differing_offsets else (limit if len(left) != len(right) else None),
        "diff_spans": diff_byte_spans(left, right),
    }
    if page_size:
        page_limit = min(len(left), len(right)) // page_size
        changed_pages = [
            page_index
            for page_index in range(page_limit)
            if left[page_index * page_size:(page_index + 1) * page_size] != right[page_index * page_size:(page_index + 1) * page_size]
        ]
        report["changed_page_count"] = len(changed_pages)
        report["changed_pages"] = changed_pages[:64]
    return report


def command_compare_bundle(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    bundle_dir = Path(args.bundle_dir).resolve()
    files = find_db_files(root)

    dbdat_proto_path = bundle_dir / "db.dat.prototype"
    idx_proto_path = bundle_dir / "db.idx.prototype"
    dic_reference_path = bundle_dir / "db.dic.reference"
    missing = [str(path) for path in [dbdat_proto_path, idx_proto_path] if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing bundle files: {', '.join(missing)}")

    report = {
        "root": str(root),
        "bundle_dir": str(bundle_dir),
        "db.dat": summarize_binary_diff(files["db.dat"].read_bytes(), dbdat_proto_path.read_bytes()),
        "db.idx": summarize_binary_diff(files["db.idx"].read_bytes(), idx_proto_path.read_bytes(), page_size=IDX_PAGE_SIZE),
    }
    if dic_reference_path.exists():
        report["db.dic"] = summarize_binary_diff(files["db.dic"].read_bytes(), dic_reference_path.read_bytes())

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

    idx_compact_page = subparsers.add_parser("idx-compact-page", help="Parse db.idx pages using the observed compact metadata/text-slot layout.")
    idx_compact_page.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    idx_compact_page.add_argument("--page", type=int, help="Optional 0-based page index to inspect")
    idx_compact_page.add_argument("--max-slots", type=int, default=24, help="Maximum compact text slots to parse per page")
    idx_compact_page.add_argument("--max-tail-cells", type=int, default=24, help="Maximum 4-byte tail cells to summarize per slot")
    idx_compact_page.set_defaults(func=command_idx_compact_page)

    idx_observed_summary = subparsers.add_parser("idx-observed-summary", help="Summarize observed chained-node patterns across the full db.idx.")
    idx_observed_summary.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    idx_observed_summary.add_argument("--max-nodes", type=int, default=64, help="Maximum chained nodes to parse per page")
    idx_observed_summary.add_argument("--max-groups", type=int, default=24, help="Maximum anchor groups to summarize per page")
    idx_observed_summary.add_argument("--limit", type=int, default=20, help="Maximum summary rows per section")
    idx_observed_summary.add_argument("--example-limit", type=int, default=5, help="Maximum example chains per anchor class")
    idx_observed_summary.set_defaults(func=command_idx_observed_summary)

    idx_template_summary = subparsers.add_parser("idx-template-summary", help="Infer reusable idx templates from both chained-node and compact metadata pages.")
    idx_template_summary.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    idx_template_summary.add_argument("--max-nodes", type=int, default=128, help="Maximum chained nodes to parse per page")
    idx_template_summary.add_argument("--max-groups", type=int, default=32, help="Maximum anchor groups to summarize per page")
    idx_template_summary.set_defaults(func=command_idx_template_summary)

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

    compare_bundle = subparsers.add_parser("compare-bundle", help="Compare a generated prototype bundle to the current database files under one root.")
    compare_bundle.add_argument("root", help="Mounted player root or local extracted root containing System/db.*")
    compare_bundle.add_argument("bundle_dir", help="Prototype bundle directory produced by write-rebuild-prototype")
    compare_bundle.set_defaults(func=command_compare_bundle)

    test_install_prototype = subparsers.add_parser("test-install-prototype", help="Install one rebuild prototype bundle onto the mounted player after backing up the current System/db.* files.")
    test_install_prototype.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    test_install_prototype.add_argument("bundle_dir", help="Prototype bundle directory produced by write-rebuild-prototype")
    test_install_prototype.add_argument("--backup-dir", help="Optional directory for the automatic backup copy")
    test_install_prototype.add_argument("--replace-dic", action="store_true", help="Also replace db.dic with the bundle reference copy if needed")
    test_install_prototype.add_argument("--allow-unsafe-install", action="store_true", help="Override the structural safety guard and install a prototype even if it looks much thinner than the current device database")
    test_install_prototype.add_argument("--allow-size-growth", action="store_true", help="Allow installing db.dat/db.idx prototypes that are larger than the current device files")
    test_install_prototype.set_defaults(func=command_test_install_prototype)

    restore_system_backup = subparsers.add_parser("restore-system-backup", help="Restore System/db.* from a backup directory created by test-install-prototype.")
    restore_system_backup.add_argument("root", help="Mounted player root, e.g. /run/media/nichlas/E10")
    restore_system_backup.add_argument("backup_dir", help="Backup directory created by test-install-prototype")
    restore_system_backup.set_defaults(func=command_restore_system_backup)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
