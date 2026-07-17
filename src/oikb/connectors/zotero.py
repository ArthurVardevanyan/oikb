"""Zotero connector - sync PDF attachment text from a Zotero library.

Auth via ZOTERO_LIBRARY_ID, ZOTERO_API_KEY, and optional ZOTERO_LIBRARY_TYPE.
Source syntax: zotero:<collection>%%<subcollection>. Use bare zotero: for all
top-level collections.
"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from typing import Any

from oikb.connectors import BaseConnector, ManifestEntry

HIERARCHY_SEP = "%%"


class ZoteroConnector(BaseConnector):
    """Sync PDF attachments in Zotero collections as extracted .txt files."""

    def __init__(
        self,
        hierarchy: str | None = None,
        library_id: str | None = None,
        library_type: str | None = None,
        api_key: str | None = None,
    ):
        try:
            from pyzotero import zotero
        except ImportError as exc:
            raise ImportError(
                "Zotero connector requires pyzotero and pymupdf. "
                "Install with: pip install oikb[zotero]"
            ) from exc

        library_id = library_id or os.environ.get("ZOTERO_LIBRARY_ID", "")
        api_key = api_key or os.environ.get("ZOTERO_API_KEY", "")
        if not library_id or not api_key:
            raise ValueError(
                "Zotero credentials required. Set ZOTERO_LIBRARY_ID and ZOTERO_API_KEY."
            )

        self.hierarchy = hierarchy
        self._zot = zotero.Zotero(
            library_id,
            library_type or os.environ.get("ZOTERO_LIBRARY_TYPE", "user"),
            api_key,
        )
        self._files: dict[tuple[str, str], str] = {}
        self._text_cache: dict[str, str] = {}

    def build_manifest(self) -> list[ManifestEntry]:
        collections = self._zot.everything(self._zot.collections())
        roots = self._roots(collections)
        if self.hierarchy:
            root = self._find(collections, None, self.hierarchy.split(HIERARCHY_SEP))
            if root is None:
                raise ValueError(f"Zotero collection path not found: {self.hierarchy}")
            work = [(root, "")]
        else:
            work = [(root, root["name"]) for root in roots]

        entries: list[ManifestEntry] = []
        for root, path in work:
            self._walk_collection(root["key"], collections, path, entries)
        entries.sort(key=lambda entry: entry.display_path)
        return entries

    def read_file(self, path: str, filename: str) -> bytes:
        attachment_key = self._files.get((path, filename))
        if not attachment_key:
            raise FileNotFoundError(f"Unknown Zotero file: {path}/{filename}")
        return self._attachment_text(attachment_key).encode("utf-8")

    def _walk_collection(
        self,
        collection_key: str,
        collections: list[dict[str, Any]],
        path: str,
        entries: list[ManifestEntry],
    ) -> None:
        for item in self._zot.everything(self._zot.collection_items(collection_key)):
            self._add_item(item, path, entries)

        for child in collections:
            data = child.get("data", {})
            if data.get("parentCollection") != collection_key:
                continue
            child_path = f"{path}/{data['name']}" if path else data["name"]
            self._walk_collection(child["key"], collections, child_path, entries)

    def _add_item(self, item: dict[str, Any], path: str, entries: list[ManifestEntry]) -> None:
        data = item.get("data", {})
        if data.get("itemType") == "attachment":
            return

        attachments = [
            child
            for child in self._zot.everything(self._zot.children(item["key"]))
            if self._is_pdf_attachment(child)
        ]
        for index, attachment in enumerate(attachments):
            filename = self._unique_filename(
                path,
                self._filename(data.get("title", "Untitled"), index),
            )
            attachment_key = attachment["key"]
            self._files[(path, filename)] = attachment_key
            entries.append(
                ManifestEntry(
                    filename=filename,
                    path=path,
                    checksum=self._checksum(item, attachment),
                    size=0,
                )
            )

    @staticmethod
    def _roots(collections: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {"key": item["key"], "name": item["data"]["name"]}
            for item in collections
            if not item.get("data", {}).get("parentCollection")
        ]

    def _find(
        self,
        collections: list[dict[str, Any]],
        parent_key: str | None,
        parts: list[str],
    ) -> dict[str, Any] | None:
        if not parts:
            return None
        for item in collections:
            data = item.get("data", {})
            parent = data.get("parentCollection")
            if parent != parent_key or data.get("name") != parts[0]:
                continue
            if len(parts) == 1:
                return {"key": item["key"], "name": data["name"]}
            return self._find(collections, item["key"], parts[1:])
        return None

    @staticmethod
    def _is_pdf_attachment(item: dict[str, Any]) -> bool:
        data = item.get("data", {})
        if data.get("itemType") != "attachment":
            return False
        content_type = data.get("contentType", "")
        title = data.get("title", "")
        path = data.get("path", "")
        return (
            content_type == "application/pdf"
            or title.lower().endswith(".pdf")
            or path.lower().endswith(".pdf")
        )

    @staticmethod
    def _filename(title: str, index: int) -> str:
        name = re.sub(r"<[^>]+>", "", title)
        name = re.sub(r'[<>:"/\\|?*]', "_", name).strip() or "Untitled"
        suffix = f"_{index + 1}" if index else ""
        return f"{name[:180]}{suffix}.txt"

    def _unique_filename(self, path: str, filename: str) -> str:
        if (path, filename) not in self._files:
            return filename
        stem, _, ext = filename.rpartition(".")
        index = 2
        while (path, f"{stem}_{index}.{ext}") in self._files:
            index += 1
        return f"{stem}_{index}.{ext}"

    @staticmethod
    def _checksum(item: dict[str, Any], attachment: dict[str, Any]) -> str:
        item_version = item.get("version", item.get("data", {}).get("version", 0))
        attachment_version = attachment.get("version", attachment.get("data", {}).get("version", 0))
        raw = f"{item['key']}:{item_version}:{attachment['key']}:{attachment_version}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def _attachment_text(self, attachment_key: str) -> str:
        if attachment_key in self._text_cache:
            return self._text_cache[attachment_key]

        try:
            text = self._zot.fulltext_item(attachment_key).get("content", "")
            if text:
                self._text_cache[attachment_key] = text
                return text
        except Exception:
            pass

        try:
            import fitz
        except ImportError as exc:
            raise ImportError(
                "pymupdf is required for Zotero PDF extraction. "
                "Install with: pip install oikb[zotero]"
            ) from exc

        pdf_bytes = self._zot.file(attachment_key)
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name
        try:
            doc = fitz.open(tmp_path)
            text = "".join(page.get_text() for page in doc)
            doc.close()
            self._text_cache[attachment_key] = text
            return text
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def parse_zotero_source(source: str) -> dict[str, str | None]:
    hierarchy = source.removeprefix("zotero:")
    return {"hierarchy": hierarchy or None}
