
"""
universal_response_retriever.py
--------------------------------

A dynamic, all-in-one Python "response retriever" that:
  • Accepts either a web endpoint (URL) or a local/uploaded file path.
  • Automatically detects the content type (JSON, NDJSON, CSV/TSV, Excel, XML, HTML tables, Parquet, gzip).
  • Parses and normalizes the data into a tidy pandas DataFrame.
  • Optionally follows basic pagination (HTTP Link headers or common JSON "next" fields).
  • Provides both:
      - A CLI (flags/args), and
      - An INTERACTIVE terminal mode (prompted wizard).

Dependencies (install as needed):
    pandas, requests
Optional (for more formats / better parsing):
    lxml (for HTML/XML), xmltodict (for XML), pyarrow or fastparquet (for Parquet), openpyxl (Excel .xlsx)

Usage (flags/args CLI):
    python universal_response_retriever.py \
        --source https://api.github.com/repos/pandas-dev/pandas/issues \
        --out issues.parquet --out-format parquet --max-pages 2

Interactive terminal mode (no flags):
    python universal_response_retriever.py
    # or explicitly:
    python universal_response_retriever.py --interactive

Programmatic:
    from universal_response_retriever import retrieve_to_df
    df, meta = retrieve_to_df("https://example.com/data.json")
    print(df.head(), meta)
"""

from __future__ import annotations

import io
import os
import re
import csv
import sys
import gzip
import json
import math
import time
import types
import zipfile
import warnings
import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse, urljoin

# Required core deps
import pandas as pd

# Optional deps
try:
    import requests
except Exception:  # pragma: no cover
    requests = None

try:
    import xmltodict  # type: ignore
except Exception:  # pragma: no cover
    xmltodict = None

try:
    from lxml import etree  # type: ignore
except Exception:  # pragma: no cover
    etree = None


# -----------------------------
# Utilities
# -----------------------------

def _is_url(s: Union[str, os.PathLike]) -> bool:
    try:
        u = urlparse(str(s))
        return u.scheme in ("http", "https")
    except Exception:
        return False


def _guess_content_type_from_extension(suffix: str) -> str:
    suffix = suffix.lower().lstrip(".")
    mapping = {
        "json": "application/json",
        "ndjson": "application/x-ndjson",
        "jsonl": "application/x-ndjson",
        "csv": "text/csv",
        "tsv": "text/tab-separated-values",
        "txt": "text/plain",
        "xml": "application/xml",
        "html": "text/html",
        "htm": "text/html",
        "xls": "application/vnd.ms-excel",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "parquet": "application/x-parquet",
        "feather": "application/x-feather",
        "gz": "application/gzip",
        "zip": "application/zip",
    }
    return mapping.get(suffix, "application/octet-stream")


def _bytes_head(b: bytes, n: int = 16) -> bytes:
    return b[:n] if isinstance(b, (bytes, bytearray)) else b""


def _decode_text_best_effort(b: bytes) -> Tuple[str, str]:
    """Try utf-8 then latin-1. Returns (text, encoding)."""
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return b.decode(enc), enc
        except Exception:
            continue
    # last resort
    return b.decode("utf-8", errors="ignore"), "utf-8"


def _maybe_decompress(b: bytes, content_type: Optional[str], filename: Optional[str]) -> Tuple[bytes, Optional[str], Optional[str], Dict[str, Any]]:
    """
    If bytes look compressed (gzip), decompress and update content-type if needed.
    Returns new_bytes, new_content_type, new_filename, meta_additions
    """
    meta: Dict[str, Any] = {}
    head = _bytes_head(b, 2)
    if head == b"\x1f\x8b":  # gzip magic
        try:
            b = gzip.decompress(b)
            meta["decompressed"] = "gzip"
            if content_type in (None, "", "application/gzip"):
                # We lost the original type—will re-sniff later
                content_type = None
            if filename and filename.endswith(".gz"):
                filename = filename[:-3]
        except Exception as e:
            meta["decompress_error"] = str(e)
    return b, content_type, filename, meta


def _sniff_format(b: bytes, content_type: Optional[str], filename: Optional[str]) -> str:
    """
    Heuristic sniffing of content format.
    Returns one of: json, ndjson, csv, tsv, excel, xml, html, parquet, zip, text, unknown
    """
    # Prefer MIME type when present
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if "json" in ct and "ndjson" not in ct:
            return "json"
        if "x-ndjson" in ct or "ndjson" in ct:
            return "ndjson"
        if "csv" in ct:
            return "csv"
        if "tab-separated-values" in ct or "tsv" in ct:
            return "tsv"
        if "xml" in ct:
            return "xml"
        if "html" in ct:
            return "html"
        if "parquet" in ct:
            return "parquet"
        if "zip" in ct:
            return "zip"
        if "excel" in ct or "spreadsheetml" in ct:
            return "excel"
        if "text/plain" in ct:
            # fallthrough to content sniff
            pass

    # Extension hint
    if filename:
        ext = Path(filename).suffix.lower().lstrip(".")
        if ext in ("json",):
            return "json"
        if ext in ("ndjson", "jsonl"):
            return "ndjson"
        if ext in ("csv", "tsv"):
            return ext
        if ext in ("xls", "xlsx"):
            return "excel"
        if ext in ("xml",):
            return "xml"
        if ext in ("htm", "html"):
            return "html"
        if ext in ("parquet",):
            return "parquet"
        if ext in ("zip",):
            return "zip"
        if ext in ("gz",):
            # will have been decompressed above usually
            pass

    head4 = _bytes_head(b, 4)
    head8 = _bytes_head(b, 8)
    head16 = _bytes_head(b, 16)

    # Parquet magic bytes: PAR1 (start and end)
    if head4 == b"PAR1":
        return "parquet"

    # ZIP-based formats (xlsx, docx, etc.)
    if head4 == b"PK\x03\x04" or head4 == b"PK\x05\x06" or head4 == b"PK\x07\x08":
        # Try to peek for Excel marker files
        try:
            with zipfile.ZipFile(io.BytesIO(b)) as zf:
                names = {n.lower() for n in zf.namelist()}
                if "[content_types].xml" in names or any(n.startswith("xl/") for n in names):
                    return "excel"
        except Exception:
            return "zip"

    # XML/HTML detection
    sample = _bytes_head(b, 2048).decode("utf-8", errors="ignore").strip()
    if sample.startswith("<"):
        if re.search(r"<\s*html", sample, flags=re.I):
            return "html"
        if "<?xml" in sample or re.search(r"<\s*\w+[^>]*>", sample):
            return "xml"

    # JSON-ish detection
    trimmed = sample.lstrip()
    if trimmed.startswith("{") or trimmed.startswith("[" ):
        # Could be JSON; detect NDJSON by line structure
        lines = sample.splitlines()
        if len(lines) > 1 and all(
            (ln.strip().startswith("{") or ln.strip().startswith("[")) for ln in lines[:50] if ln.strip()
        ):
            # Ambiguous; prefer 'json' when it begins with { or [
            return "json"
        return "json"

    # Delimited text detection (csv/tsv)
    if "," in sample and "\n" in sample:
        return "csv"
    if "\t" in sample and "\n" in sample:
        return "tsv"

    # Fallbacks
    return "text"


@dataclass
class FetchMeta:
    source: str
    url: Optional[str] = None
    filename: Optional[str] = None
    content_type: Optional[str] = None
    detected_format: Optional[str] = None
    encoding: Optional[str] = None
    decompressed: Optional[str] = None
    http_status: Optional[int] = None
    headers: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


# -----------------------------
# Fetcher
# -----------------------------

def _make_dummy_response_meta(path: Path) -> FetchMeta:
    return FetchMeta(
        source=str(path),
        url=None,
        filename=str(path.name),
        content_type=_guess_content_type_from_extension(path.suffix),
        http_status=None,
        headers={},
    )


def fetch_bytes(
    source: Union[str, os.PathLike],
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Union[Dict[str, Any], bytes]] = None,
    json_body: Optional[Any] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 60,
    verify: bool = True,
    allow_redirects: bool = True,
) -> Tuple[bytes, FetchMeta]:
    """
    Fetch bytes from a URL or read a local file.
    Returns (content_bytes, FetchMeta).
    """
    meta = FetchMeta(source=str(source))

    if _is_url(source):
        if requests is None:
            raise RuntimeError("requests is required to fetch URLs. Please install requests.")
        sess = requests.Session()
        if headers:
            sess.headers.update(headers)
        resp = sess.request(
            method=method.upper(),
            url=str(source),
            params=params,
            data=data,
            json=json_body,
            timeout=timeout,
            verify=verify,
            allow_redirects=allow_redirects,
        )
        meta.http_status = resp.status_code
        meta.content_type = (resp.headers.get("content-type") or "").split(";")[0].lower()
        meta.headers = dict(resp.headers)
        meta.url = resp.url
        resp.raise_for_status()
        b = resp.content
        filename = None
        cd = resp.headers.get("content-disposition", "")
        m = re.search(r'filename="?([^"]+)"?', cd)
        if m:
            filename = m.group(1)
        meta.filename = filename
    else:
        p = Path(source)
        if not p.exists():
            raise FileNotFoundError(f"File not found: {p}")
        meta = _make_dummy_response_meta(p)
        with open(p, "rb") as f:
            b = f.read()

    # Decompress if needed
    b, content_type_override, filename_override, add_meta = _maybe_decompress(b, meta.content_type, meta.filename)
    meta.__dict__.update(add_meta)
    if content_type_override:
        meta.content_type = content_type_override
    if filename_override:
        meta.filename = filename_override

    # Detect format
    fmt = _sniff_format(b, meta.content_type, meta.filename)
    meta.detected_format = fmt

    # For text-like formats, record guessed encoding
    if fmt in ("json", "ndjson", "csv", "tsv", "xml", "html", "text"):
        _, enc = _decode_text_best_effort(b)
        meta.encoding = enc

    return b, meta


# -----------------------------
# JSON helpers
# -----------------------------

def _iter_paths_for_lists_of_dicts(obj: Any, prefix: Optional[List[str]] = None) -> List[Tuple[List[str], int]]:
    """
    Return list of (path, length) where path points to a list of dicts.
    """
    if prefix is None:
        prefix = []
    out: List[Tuple[List[str], int]] = []

    if isinstance(obj, list) and all(isinstance(x, dict) for x in obj):
        out.append((prefix[:], len(obj)))
        # Explore inside elements as well, in case there are deeper lists
        for el in obj:
            out.extend(_iter_paths_for_lists_of_dicts(el, prefix))
    elif isinstance(obj, dict):
        for k, v in obj.items():
            out.extend(_iter_paths_for_lists_of_dicts(v, prefix + [str(k)]))
    return out


def _choose_record_path(obj: Any, prefer: Optional[Union[str, Sequence[str]]] = None) -> Optional[List[str]]:
    """
    Heuristically choose the best record_path for json_normalize.
    - If prefer is provided, try to match it.
    - Else pick the longest list-of-dicts.
    """
    candidates = _iter_paths_for_lists_of_dicts(obj)
    if not candidates:
        return None

    if prefer:
        if isinstance(prefer, str):
            prefer = [s for s in re.split(r"[./]+", prefer) if s]
        for path, _ in candidates:
            if path[-len(prefer):] == list(prefer):
                return path

    # Choose path with maximum length (row count),
    # tie-breaking by depth (prefer deeper paths for richer rows).
    candidates.sort(key=lambda t: (t[1], len(t[0])), reverse=True)
    return candidates[0][0]


def _flatten_mixed_df(df: pd.DataFrame, max_depth: int = 2, explode_lists: bool = True) -> pd.DataFrame:
    """
    Iteratively flatten columns containing dicts/lists.
    - Dicts are expanded into columns (dot notation).
    - Lists of dicts are exploded and flattened.
    - Lists of primitives are exploded if explode_lists=True.
    """
    depth = 0
    while depth < max_depth:
        object_cols = [c for c in df.columns if df[c].dtype == "object"]
        changed = False

        for col in object_cols:
            # Expand dict-like
            mask_dict = df[col].apply(lambda x: isinstance(x, dict))
            if mask_dict.any():
                expanded = pd.json_normalize(df.loc[mask_dict, col]).add_prefix(f"{col}.")
                df = df.join(expanded, how="left")
                df.loc[mask_dict, col] = None
                changed = True

            # Handle list-like
            mask_list = df[col].apply(lambda x: isinstance(x, list))
            if mask_list.any():
                if explode_lists:
                    df = df.explode(col, ignore_index=True)
                    # After explode, if elements are dicts, flatten them
                    mask_dict2 = df[col].apply(lambda x: isinstance(x, dict))
                    if mask_dict2.any():
                        expanded2 = pd.json_normalize(df.loc[mask_dict2, col]).add_prefix(f"{col}.")
                        df = df.join(expanded2, how="left")
                        df.loc[mask_dict2, col] = None
                    changed = True
                else:
                    # Keep lists as JSON strings
                    df.loc[mask_list, col] = df.loc[mask_list, col].apply(json.dumps)

        if not changed:
            break
        depth += 1
    return df


def _coerce_common_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Try to coerce common primitive types nicely:
      - booleans from yes/no/true/false/1/0 strings
      - datetimes when columns look like dates or ISO datetimes
      - numerics where appropriate
    """
    date_like_name = re.compile(r"(date|time|timestamp|created|updated|at)$", re.I)
    true_set = {"true", "yes", "y", "1", "t"}
    false_set = {"false", "no", "n", "0", "f"}

    for col in df.columns:
        s = df[col]
        if s.dtype == "object":
            # Try boolean coercion
            vals = s.dropna().astype(str).str.strip().str.lower().unique()[:20]
            if set(vals).issubset(true_set | false_set) and len(vals) > 0:
                df[col] = s.astype(str).str.strip().str.lower().map(
                    lambda x: True if x in true_set else False if x in false_set else pd.NA
                )
                continue

            # Try datetime coercion (by name hint or ISO-ish content)
            sample = s.dropna().astype(str).head(50)
            iso_like = sample.str.match(r"\d{4}-\d{2}-\d{2}(?:[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)?").all()
            if date_like_name.search(str(col)) or iso_like:
                try:
                    df[col] = pd.to_datetime(s, errors="coerce", utc=True)
                    continue
                except Exception:
                    pass

            # Try numeric
            try:
                df[col] = pd.to_numeric(s, errors="ignore")
            except Exception:
                pass
    return df


def _snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    def snake(s: str) -> str:
        s = re.sub(r"[^\w]+", "_", str(s)).strip("_")
        s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
        s = re.sub(r"_+", "_", s)
        return s.lower()
    df = df.rename(columns=lambda c: snake(c))
    return df


# -----------------------------
# Parsers
# -----------------------------

def parse_json_to_df(
    b: bytes,
    prefer_record_path: Optional[Union[str, Sequence[str]]] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    text, _ = _decode_text_best_effort(b)
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}") from e

    meta: Dict[str, Any] = {}
    if isinstance(obj, list):
        # Best-case: list of records
        df = pd.json_normalize(obj)
        meta["record_path"] = None
    elif isinstance(obj, dict):
        # Heuristically pick a list-of-dicts path
        path = _choose_record_path(obj, prefer_record_path)
        meta["record_path"] = path
        if path:
            # json_normalize with record_path
            df = pd.json_normalize(obj, record_path=path, errors="ignore")
            # Optionally attach top-level scalars as metadata columns
            top_scalars = {k: v for k, v in obj.items() if not isinstance(v, (list, dict))}
            if top_scalars:
                for k, v in top_scalars.items():
                    df[f"__meta__.{k}"] = v
        else:
            # Flatten dict into one row
            df = pd.json_normalize(obj)
    else:
        # Any other top-level type: wrap and normalize
        df = pd.json_normalize({"value": obj})

    df = _flatten_mixed_df(df, max_depth=2, explode_lists=True)
    return df, meta


def parse_ndjson_to_df(b: bytes) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    text, _ = _decode_text_best_effort(b)
    rows = []
    for i, line in enumerate(text.splitlines()):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid NDJSON at line {i+1}: {e}") from e
    df = pd.json_normalize(rows)
    df = _flatten_mixed_df(df, max_depth=2, explode_lists=True)
    return df, {}


def parse_csv_like_to_df(b: bytes, sep: Optional[str] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    text, enc = _decode_text_best_effort(b)
    # Let pandas detect if sep=None
    try:
        df = pd.read_csv(io.StringIO(text), sep=sep, engine="python")
    except Exception as e:
        # As a fallback, try common separators
        for s in [",", "\t", ";", "|"]:
            try:
                df = pd.read_csv(io.StringIO(text), sep=s, engine="python")
                break
            except Exception:
                df = None  # type: ignore
        if df is None:
            raise e
    return df, {"encoding": enc}


def parse_excel_to_df(b: bytes) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    bio = io.BytesIO(b)
    try:
        xls = pd.ExcelFile(bio)
    except Exception as e:
        raise ValueError(f"Could not open Excel file: {e}") from e

    frames = []
    for name in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=name)
            df["__sheet__"] = name
            frames.append(df)
        except Exception:
            continue
    if not frames:
        raise ValueError("No readable sheets found in Excel file.")
    df_all = pd.concat(frames, ignore_index=True, sort=False)
    return df_all, {"sheets": xls.sheet_names}


def parse_xml_to_df(b: bytes) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if xmltodict is not None:
        text, _ = _decode_text_best_effort(b)
        obj = xmltodict.parse(text)
        # Reuse JSON logic
        df, meta = parse_json_to_df(json.dumps(obj).encode("utf-8"))
        meta["xml_parser"] = "xmltodict"
        return df, meta

    # Fallback to ElementTree with a simple heuristics:
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(b)
    except Exception as e:
        raise ValueError(f"Invalid XML: {e}") from e

    # Find repeated child tags to define "rows"
    def find_best_row_path(el: ET.Element, path: List[str]) -> Tuple[List[str], int]:
        counts: Dict[str, int] = {}
        for child in el:
            counts[child.tag] = counts.get(child.tag, 0) + 1
        # pick the tag that repeats the most
        if counts:
            best_tag = max(counts.items(), key=lambda kv: kv[1])[0]
            best_child = [c for c in el if c.tag == best_tag][0]
            sub_path, n = find_best_row_path(best_child, path + [best_tag])
            return sub_path, max(n, counts[best_tag])
        return path, 1

    best_path, _ = find_best_row_path(root, [root.tag])

    # Collect rows under best_path
    def iter_at_path(el: ET.Element, path: List[str]) -> Iterable[ET.Element]:
        if not path:
            yield el
        else:
            tag = path[0]
            for child in el.findall(tag):
                yield from iter_at_path(child, path[1:])

    rows = []
    for node in iter_at_path(root, best_path[1:]):  # skip root tag
        row: Dict[str, Any] = {}
        # attributes
        for k, v in node.attrib.items():
            row[f"@{k}"] = v
        # child elements
        for ch in node:
            if list(ch):  # has children
                # flatten one level
                for gch in ch:
                    key = f"{ch.tag}.{gch.tag}"
                    val = (gch.text or "").strip()
                    if key in row:
                        # make it a list
                        if not isinstance(row[key], list):
                            row[key] = [row[key]]
                        row[key].append(val)
                    else:
                        row[key] = val
            else:
                row[ch.tag] = (ch.text or "").strip()
        # text content
        if (node.text or "").strip():
            row["#text"] = (node.text or "").strip()
        rows.append(row)

    df = pd.DataFrame(rows)
    df = _flatten_mixed_df(df, max_depth=2, explode_lists=True)
    return df, {"xml_parser": "elementtree", "best_row_path": "/".join(best_path)}


def parse_html_tables_to_df(b: bytes) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    text, _ = _decode_text_best_effort(b)
    try:
        tables = pd.read_html(text)  # requires lxml or html5lib
    except Exception as e:
        raise ValueError(f"No HTML tables found or failed to parse: {e}") from e
    if not tables:
        raise ValueError("No HTML tables found in document.")
    # pick the largest table by rows*cols
    tables.sort(key=lambda df: (df.shape[0] * df.shape[1]), reverse=True)
    return tables[0], {"num_tables": len(tables)}


def parse_parquet_to_df(b: bytes) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    bio = io.BytesIO(b)
    try:
        df = pd.read_parquet(bio)  # requires pyarrow or fastparquet
    except Exception as e:
        raise ValueError(f"Failed to read Parquet (pyarrow/fastparquet required): {e}") from e
    return df, {}


# -----------------------------
# Main normalization entry
# -----------------------------

def normalize_df(
    df: pd.DataFrame,
    coerce_types: bool = True,
    snake_case: bool = True,
    max_depth: int = 2,
    explode_lists: bool = True,
) -> pd.DataFrame:
    df2 = df.copy()
    df2 = _flatten_mixed_df(df2, max_depth=max_depth, explode_lists=explode_lists)
    if coerce_types:
        df2 = _coerce_common_types(df2)
    if snake_case:
        df2 = _snake_case_columns(df2)
    # Ensure deduplicated columns
    df2 = df2.loc[:, ~df2.columns.duplicated()]
    return df2


def retrieve_to_df(
    source: Union[str, os.PathLike],
    *,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Union[Dict[str, Any], bytes]] = None,
    json_body: Optional[Any] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 60,
    verify: bool = True,
    prefer_record_path: Optional[Union[str, Sequence[str]]] = None,
    # normalization flags
    coerce_types: bool = True,
    snake_case_cols: bool = True,
    max_depth: int = 2,
    explode_lists: bool = True,
    # pagination
    paginate: bool = True,
    max_pages: int = 1,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Fetch, parse and normalize a source into a DataFrame.
    Returns (df, meta)
    """
    all_frames: List[pd.DataFrame] = []
    meta_agg: Dict[str, Any] = {"pages": []}

    next_url: Optional[str] = str(source)
    page_count = 0

    while next_url and page_count < max_pages:
        b, meta = fetch_bytes(
            next_url,
            method=method,
            params=params,
            data=data,
            json_body=json_body,
            headers=headers,
            timeout=timeout,
            verify=verify,
        )
        fmt = meta.detected_format
        page_info = {"source": meta.source, "url": meta.url, "format": fmt, "status": meta.http_status}
        meta_agg["pages"].append(page_info)

        if fmt == "json":
            df, submeta = parse_json_to_df(b, prefer_record_path=prefer_record_path)
        elif fmt == "ndjson":
            df, submeta = parse_ndjson_to_df(b)
        elif fmt == "csv":
            df, submeta = parse_csv_like_to_df(b, sep=",")
        elif fmt == "tsv":
            df, submeta = parse_csv_like_to_df(b, sep="\t")
        elif fmt == "excel":
            df, submeta = parse_excel_to_df(b)
        elif fmt == "xml":
            df, submeta = parse_xml_to_df(b)
        elif fmt == "html":
            df, submeta = parse_html_tables_to_df(b)
        elif fmt == "parquet":
            df, submeta = parse_parquet_to_df(b)
        elif fmt in ("text", "unknown"):
            # Try a last-ditch CSV or NDJSON parse based on content
            text, _ = _decode_text_best_effort(b)
            if "\n" in text and ("," in text or "\t" in text or ";" in text):
                df, submeta = parse_csv_like_to_df(b, sep=None)
            elif any(ln.strip().startswith("{") for ln in text.splitlines()[:10]):
                df, submeta = parse_ndjson_to_df(b)
            else:
                df = pd.DataFrame({"text": text.splitlines()})
                submeta = {}
        else:
            raise ValueError(f"Unsupported or unrecognized format: {fmt}")

        # Normalize
        df = normalize_df(
            df,
            coerce_types=coerce_types,
            snake_case=snake_case_cols,
            max_depth=max_depth,
            explode_lists=explode_lists,
        )
        all_frames.append(df)

        # Handle simple pagination if enabled and we fetched from HTTP
        if paginate and _is_url(source) and meta.headers:
            next_url = _extract_next_url(meta, b)
        else:
            next_url = None

        page_count += 1

    if not all_frames:
        return pd.DataFrame(), meta_agg

    df_final = pd.concat(all_frames, ignore_index=True, sort=False)
    return df_final, meta_agg


# -----------------------------
# Pagination helpers
# -----------------------------

def _parse_link_header(link_header: str) -> Dict[str, str]:
    # RFC 5988: <url>; rel="next", <url>; rel="prev", ...
    out: Dict[str, str] = {}
    for part in link_header.split(","):
        part = part.strip()
        m = re.match(r'<([^>]+)>\s*;\s*rel="([^"]+)"', part)
        if m:
            url, rel = m.group(1), m.group(2)
            out[rel] = url
    return out


def _extract_next_url(meta: FetchMeta, body_bytes: bytes) -> Optional[str]:
    # 1) Link header
    link = meta.headers.get("Link") or meta.headers.get("link")
    if link:
        links = _parse_link_header(link)
        if "next" in links:
            return links["next"]

    # 2) JSON body fields commonly used
    try:
        text, _ = _decode_text_best_effort(body_bytes)
        obj = json.loads(text)
        if isinstance(obj, dict):
            for key in ("next", "next_url", "nextPageUrl", "next_page_url", "nextLink", "links"):
                if key in obj and isinstance(obj[key], str) and obj[key].strip():
                    return obj[key]
                if key == "links" and isinstance(obj[key], dict):
                    for k2 in ("next", "next_url"):
                        v = obj[key].get(k2)
                        if isinstance(v, str) and v.strip():
                            return v
    except Exception:
        pass

    return None


# -----------------------------
# CLI (flags) and Interactive (prompts)
# -----------------------------

def _coerce_blank_to_none(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    if isinstance(s, str) and s.strip() == "":
        return None
    return s


def _try_parse_dictish(s: Optional[str]) -> Optional[Dict[str, Any]]:
    """Parse a JSON or Python-literal dict string. Returns None for blank."""
    s = _coerce_blank_to_none(s)
    if s is None:
        return None
    try:
        return json.loads(s)
    except Exception:
        try:
            v = ast.literal_eval(s)
            if isinstance(v, dict):
                return v
        except Exception:
            pass
    raise ValueError("Could not parse as JSON or Python dict.")


def _cli():
    import argparse

    p = argparse.ArgumentParser(description="Universal Response Retriever → tidy DataFrame")
    p.add_argument("--interactive", action="store_true", help="Run in interactive prompt mode.")
    p.add_argument("--source", required=False, help="URL or local/ uploaded file path")
    p.add_argument("--method", default="GET", help="HTTP method for URL sources (default: GET)")
    p.add_argument("--headers", default=None, help="JSON dict of HTTP headers")
    p.add_argument("--params", default=None, help="JSON dict of query params")
    p.add_argument("--data", default=None, help="JSON dict or raw string for request body")
    p.add_argument("--json", dest="json_body", default=None, help="JSON dict for request body")
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--no-verify", action="store_true", help="Disable TLS verification")
    p.add_argument("--max-pages", type=int, default=1, help="Follow pagination up to N pages")
    p.add_argument("--no-paginate", action="store_true", help="Disable pagination even if hints exist")
    p.add_argument("--prefer-record-path", default=None, help="Hint path for nested JSON arrays (e.g., 'data.items')")
    p.add_argument("--no-coerce", action="store_true", help="Disable type coercions")
    p.add_argument("--no-snake", action="store_true", help="Keep original column names")
    p.add_argument("--max-depth", type=int, default=2, help="Max depth for flattening nested objects")
    p.add_argument("--no-explode", action="store_true", help="Do not explode list columns")
    p.add_argument("--out", default=None, help="Output file path (csv, parquet, json)")
    p.add_argument("--out-format", default=None, choices=["csv", "parquet", "json"], help="Force output format")
    args = p.parse_args()

    # If interactive flag is provided OR no --source given, go interactive
    if args.interactive or not args.source:
        return _interactive_cli()

    # Normalize blanks
    args.out = _coerce_blank_to_none(args.out)
    args.out_format = _coerce_blank_to_none(args.out_format)
    args.prefer_record_path = _coerce_blank_to_none(args.prefer_record_path)

    headers = _try_parse_dictish(args.headers)
    params = _try_parse_dictish(args.params)

    # `data` can be JSON or raw string
    data = args.data
    if data:
        data = _coerce_blank_to_none(data)
        if data and data.strip().startswith("{"):
            try:
                data = json.loads(data)
            except Exception:
                pass

    json_body = _try_parse_dictish(args.json_body)

    df, meta = retrieve_to_df(
        args.source,
        method=args.method,
        params=params,
        data=data,
        json_body=json_body,
        headers=headers,
        timeout=args.timeout,
        verify=not args.no_verify,
        prefer_record_path=args.prefer_record_path,
        coerce_types=not args.no_coerce,
        snake_case_cols=not args.no_snake,
        max_depth=args.max_depth,
        explode_lists=not args.no_explode,
        paginate=not args.no_paginate,
        max_pages=args.max_pages,
    )

    # Display preview
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.width", 160)
    print(df.head(20))
    print("\n--- Metadata ---")
    print(json.dumps(meta, indent=2, default=str))

    # Output
    if args.out:
        out_fmt = args.out_format or (Path(args.out).suffix.lower().lstrip(".") or "csv")
        if out_fmt == "csv":
            df.to_csv(args.out, index=False)
        elif out_fmt == "parquet":
            try:
                df.to_parquet(args.out, index=False)
            except Exception as e:
                sys.stderr.write(f"Parquet write failed (pyarrow/fastparquet required): {e}\n")
                sys.stderr.write("Falling back to CSV...\n")
                df.to_csv(args.out if str(args.out).endswith(".csv") else str(args.out) + ".csv", index=False)
        elif out_fmt == "json":
            df.to_json(args.out, orient="records", lines=False, date_format="iso")
        else:
            raise ValueError(f"Unsupported out-format: {out_fmt}")
        print(f"\nSaved output → {args.out}")


def _interactive_cli():
    print("\nUniversal Response Retriever — Interactive Mode")
    print("Press Enter to accept defaults. Type 'q' to quit any time.\n")

    def ask(prompt: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
        d = f" [{default}]" if default is not None else ""
        while True:
            value = input(f"{prompt}{d}: ").strip()
            if value.lower() == "q":
                sys.exit(0)
            if value == "" and default is not None:
                return default
            if value == "" and not required:
                return None
            if value != "":
                return value
            print("Please enter a value or press Enter for default.")

    def ask_yes_no(prompt: str, default: bool = False) -> bool:
        d = "Y/n" if default else "y/N"
        while True:
            v = input(f"{prompt} ({d}): ").strip().lower()
            if v == "q":
                sys.exit(0)
            if v == "" and default is not None:
                return default
            if v in ("y", "yes"):
                return True
            if v in ("n", "no"):
                return False
            print("Please answer y or n.")

    def parse_dictish(name: str) -> Optional[Dict[str, Any]]:
        s = ask(f"Enter {name} as JSON or Python dict (or leave blank)", default=None, required=False)
        if s in (None, ""):
            return None
        try:
            return json.loads(s)
        except Exception:
            try:
                v = ast.literal_eval(s)
                if isinstance(v, dict):
                    return v
            except Exception:
                pass
        print(f"Could not parse {name}. Try again or leave blank.")
        return parse_dictish(name)

    # Gather inputs
    source = ask("Source URL or local file path", required=True)

    method = ask("HTTP method for URL sources", default="GET") or "GET"
    headers = parse_dictish("headers")
    params = parse_dictish("query params")
    body_choice = ask("Provide a request body? type 'json' for JSON, 'raw' for raw string, or leave blank", default=None)
    data = None
    json_body = None
    if body_choice:
        if body_choice.lower() == "json":
            json_body = parse_dictish("JSON body") or None
        elif body_choice.lower() == "raw":
            data = ask("Enter raw body string", default=None, required=False)

    prefer_record_path = ask("Prefer record path for nested JSON (e.g., data.items)", default=None)
    max_pages_str = ask("Max pages to follow (pagination)", default="1")
    try:
        max_pages = max(1, int(max_pages_str or "1"))
    except Exception:
        max_pages = 1

    no_paginate = not ask_yes_no("Enable pagination if hints exist?", default=True)

    # Normalization toggles
    coerce_types = ask_yes_no("Coerce common types (booleans/datetimes/numerics)?", default=True)
    snake_case = ask_yes_no("Snake-case column names?", default=True)
    explode_lists = ask_yes_no("Explode list columns?", default=True)
    max_depth_str = ask("Max depth for flattening", default="2")
    try:
        max_depth = max(0, int(max_depth_str or "2"))
    except Exception:
        max_depth = 2

    # Output
    save_out = ask_yes_no("Save output to a file?", default=False)
    out = None
    out_format = None
    if save_out:
        out = ask("Output file path", default="out.csv", required=True)
        # Infer format if extension present
        ext = Path(out).suffix.lower().lstrip(".")
        if ext in ("csv", "parquet", "json"):
            out_format = ext
        else:
            out_format = ask("Output format (csv/parquet/json)", default="csv")

    # Execute
    print("\nFetching & normalizing...")
    try:
        df, meta = retrieve_to_df(
            source,
            method=method,
            params=params,
            data=data,
            json_body=json_body,
            headers=headers,
            prefer_record_path=prefer_record_path,
            coerce_types=coerce_types,
            snake_case_cols=snake_case,
            max_depth=max_depth,
            explode_lists=explode_lists,
            paginate=not no_paginate,
            max_pages=max_pages,
        )
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

    # Preview
    print("\n--- Result Preview ---")
    print(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
    with pd.option_context("display.max_columns", 80, "display.width", 200):
        print(df.head(20))

    # Meta
    print("\n--- Fetch Meta ---")
    try:
        print(json.dumps(meta, indent=2, default=str))
    except Exception:
        print(str(meta))

    # Save
    if out:
        print("\nSaving output...")
        try:
            if out_format == "csv":
                df.to_csv(out, index=False)
            elif out_format == "parquet":
                try:
                    df.to_parquet(out, index=False)
                except Exception as e:
                    sys.stderr.write(f"Parquet write failed (pyarrow/fastparquet required): {e}\n")
                    sys.stderr.write("Falling back to CSV...\n")
                    df.to_csv(out if str(out).endswith(".csv") else str(out) + ".csv", index=False)
            elif out_format == "json":
                df.to_json(out, orient="records", lines=False, date_format="iso")
            else:
                print(f"Unknown format '{out_format}', writing CSV instead.")
                df.to_csv(out if str(out).endswith(".csv") else str(out) + ".csv", index=False)
            print(f"Saved → {out}")
        except Exception as e:
            print(f"Failed to save file: {e}")

    print("\nDone.\n")


if __name__ == "__main__":
    # If no args OR explicit --interactive is present anywhere → use interactive mode.
    if len(sys.argv) == 1 or "--interactive" in sys.argv[1:]:
        _interactive_cli()
    else:
        _cli()
