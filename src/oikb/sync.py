"""Sync orchestrator — diff → cleanup → mkdir → upload."""

from __future__ import annotations

import fnmatch
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable

import click
import httpx
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from oikb.client import OikbClient
from oikb.connectors import BaseConnector, ManifestEntry, SourceFileUnavailable
from oikb.history import SyncHistory

# ── Verification tuning ─────────────────────────────────────────────
# Verify budget: max seconds to poll uploaded file statuses.
# Set via env; default 90s keeps within the 120s httpx timeout.
OIKB_VERIFY_BUDGET: float = float(os.environ.get("OIKB_VERIFY_BUDGET", "90"))
OIKB_VERIFY_INTERVAL: float = float(os.environ.get("OIKB_VERIFY_INTERVAL", "2"))
OIKB_MAX_TRANSIENT_ATTEMPTS: int = int(os.environ.get("OIKB_MAX_TRANSIENT_ATTEMPTS", "3"))
OIKB_TRANSIENT_RETRY_AFTER: int = int(os.environ.get("OIKB_TRANSIENT_RETRY_AFTER", "86400"))

# ── Duplicate-content detection ─────────────────────────────────────
_DUPLICATE_PATTERNS = ("Duplicate content", "duplicate content")

# Stderr console for progress output (keeps stdout clean for piping).
_console = Console(stderr=True)


@dataclass
class SyncResult:
    """Summary of a completed sync operation."""

    added: int = 0
    modified: int = 0
    deleted: int = 0
    unmodified: int = 0
    dirs_created: int = 0
    dirs_removed: int = 0
    skipped: int = 0
    errors: list[str] | None = None
    warnings: list[str] | None = None

    @property
    def total_changes(self) -> int:
        return self.added + self.modified + self.deleted

    def summary(self) -> str:
        parts = []
        if self.added:
            parts.append(f"{self.added} added")
        if self.modified:
            parts.append(f"{self.modified} modified")
        if self.deleted:
            parts.append(f"{self.deleted} deleted")
        if self.unmodified:
            parts.append(f"{self.unmodified} unchanged")
        if self.skipped:
            parts.append(f"{self.skipped} skipped")
        if self.dirs_created:
            parts.append(f"{self.dirs_created} dirs created")
        if self.dirs_removed:
            parts.append(f"{self.dirs_removed} dirs removed")
        return ", ".join(parts) if parts else "nothing to do"


class SyncCancelled(Exception):
    """Raised when a running sync is asked to stop."""


def parse_size(value: str | int | None) -> int | None:
    """Parse a human-readable size string to bytes.

    Examples: '50mb' → 52428800, '1gb' → 1073741824, '500kb' → 512000.
    Returns None if value is None or empty.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)

    value = value.strip().lower()
    multipliers = {"b": 1, "kb": 1024, "mb": 1024 ** 2, "gb": 1024 ** 3}

    for suffix, mult in sorted(multipliers.items(), key=lambda x: -len(x[0])):
        if value.endswith(suffix):
            return int(float(value[: -len(suffix)].strip()) * mult)

    return int(value)


def build_manifest_filter(
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    max_size: int | None = None,
) -> Callable[[list[ManifestEntry]], list[ManifestEntry]] | None:
    """Build a filter function from glob include/exclude patterns and size limit.

    Returns None if no filtering is needed.
    """
    if not include and not exclude and max_size is None:
        return None

    def _filter(entries: list[ManifestEntry]) -> list[ManifestEntry]:
        result = []
        for entry in entries:
            path = entry.display_path
            if include and not any(fnmatch.fnmatch(path, p) for p in include):
                continue
            if exclude and any(fnmatch.fnmatch(path, p) for p in exclude):
                continue
            if max_size is not None and entry.size > max_size:
                click.echo(
                    click.style(
                        f"  ⚠ Skipping {path} ({_fmt_size(entry.size)}) "
                        f"— exceeds max-size ({_fmt_size(max_size)})",
                        fg="yellow",
                    ),
                    err=True,
                )
                continue
            result.append(entry)
        return result

    return _filter


def _fmt_size(n: int) -> str:
    """Format bytes as a human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _is_duplicate_error(error_msg: str) -> bool:
    """Return True if the error is the duplicate-content guard."""
    lower = error_msg.lower()
    return any(p.lower() in lower for p in _DUPLICATE_PATTERNS)


def run_sync(
    client: OikbClient,
    connector: BaseConnector,
    kb_id: str,
    dry_run: bool = False,
    verbose: bool = False,
    quiet: bool = False,
    manifest_filter: Callable[[list[ManifestEntry]], list[ManifestEntry]] | None = None,
    concurrency: int = 1,
    cancel_requested: Callable[[], bool] | None = None,
) -> SyncResult:
    """Execute a full incremental sync.

    Steps:
      1. Build manifest from connector
      2. Apply optional manifest filter
      3. POST manifest to /sync/diff
      4. Cleanup stale files (delete before upload)
      5. Create missing directories
      6. Upload added + modified files
      7. Verify background linkage; reap failures
    """
    result = SyncResult()
    result.errors = []
    result.warnings = []

    try:
        return _run_sync_inner(
            client, connector, kb_id, dry_run, verbose, quiet,
            manifest_filter, concurrency, result, cancel_requested,
        )
    finally:
        connector.close()


def _run_sync_inner(
    client: OikbClient,
    connector: BaseConnector,
    kb_id: str,
    dry_run: bool,
    verbose: bool,
    quiet: bool,
    manifest_filter: Callable[[list[ManifestEntry]], list[ManifestEntry]] | None,
    concurrency: int,
    result: SyncResult,
    cancel_requested: Callable[[], bool] | None,
) -> SyncResult:
    """Inner sync logic, separated for clean connector cleanup."""
    show_progress = not quiet and not dry_run

    def check_stop() -> None:
        if cancel_requested and cancel_requested():
            raise SyncCancelled("sync cancelled")

    # ── 1. Build manifest ──────────────────────────────────────
    check_stop()
    if show_progress:
        with _console.status("[bold blue]Scanning source..."):
            manifest = connector.build_manifest()
        _console.print(f"  [dim]{len(manifest)} files found[/dim]")
    else:
        if verbose:
            click.echo("Scanning source...", err=True)
        manifest = connector.build_manifest()
        if verbose:
            click.echo(f"  {len(manifest)} files found", err=True)

    # ── 2. Apply filter ────────────────────────────────────────
    check_stop()
    if manifest_filter:
        manifest = manifest_filter(manifest)
        if show_progress:
            _console.print(f"  [dim]{len(manifest)} files after filtering[/dim]")
        elif verbose:
            click.echo(f"  {len(manifest)} files after filtering", err=True)

    if not manifest:
        if not quiet:
            click.echo("Source is empty — nothing to sync.", err=True)
        return result

    # ── 3. Compute diff ────────────────────────────────────────
    check_stop()
    if show_progress:
        with _console.status("[bold blue]Computing diff..."):
            diff = client.sync_diff(kb_id, [e.to_dict() for e in manifest])
    else:
        if verbose:
            click.echo("Computing diff...", err=True)
        diff = client.sync_diff(kb_id, [e.to_dict() for e in manifest])

    added: list[dict[str, Any]] = diff.get("added", [])
    modified: list[dict[str, Any]] = diff.get("modified", [])
    deleted: list[dict[str, Any]] = diff.get("deleted", [])
    unmodified_count: int = diff.get("unmodified_count", 0)
    mkdir: list[str] = diff.get("mkdir", [])
    rmdir: list[str] = diff.get("rmdir", [])
    directory_map: dict[str, str] = diff.get("directory_map", {})

    result.unmodified = unmodified_count

    if show_progress:
        parts = []
        if added:
            parts.append(f"[green]+{len(added)}[/green]")
        if modified:
            parts.append(f"[yellow]~{len(modified)}[/yellow]")
        if deleted:
            parts.append(f"[red]-{len(deleted)}[/red]")
        if unmodified_count:
            parts.append(f"[dim]{unmodified_count} unchanged[/dim]")
        _console.print(f"  Diff: {', '.join(parts)}" if parts else "  [dim]Nothing to do[/dim]")

    # ── Dry run: just print what would happen ──────────────────
    if dry_run:
        result.added = len(added)
        result.modified = len(modified)
        result.deleted = len(deleted)
        result.dirs_created = len(mkdir)
        result.dirs_removed = len(rmdir)

        if added:
            click.echo(click.style("+ Added:", fg="green"))
            for f in added:
                _echo_file_entry(f, "+", "green")

        if modified:
            click.echo(click.style("~ Modified:", fg="yellow"))
            for f in modified:
                _echo_file_entry(f, "~", "yellow")

        if deleted:
            click.echo(click.style("- Deleted:", fg="red"))
            for f in deleted:
                _echo_file_entry(f, "-", "red")

        if mkdir:
            click.echo(click.style("📁 Dirs to create:", fg="cyan"))
            for d in mkdir:
                click.echo(f"  + {d}")

        if rmdir:
            click.echo(click.style("📁 Dirs to remove:", fg="cyan"))
            for d in rmdir:
                click.echo(f"  - {d}")

        return result

    # Nothing to do?
    if not added and not modified and not deleted and not mkdir and not rmdir:
        return result

    # ── 4. Cleanup stale files ─────────────────────────────────
    stale_file_ids = [
        *[d["file_id"] for d in deleted],
        *[m["stale_file_id"] for m in modified],
    ]

    if stale_file_ids or rmdir:
        check_stop()
        if show_progress:
            with _console.status(f"[bold blue]Cleaning up {len(stale_file_ids)} stale files..."):
                client.sync_cleanup(kb_id, stale_file_ids, rmdir if rmdir else None)
        else:
            if verbose:
                click.echo(
                    f"Cleaning up {len(stale_file_ids)} files, {len(rmdir)} dirs...",
                    err=True,
                )
            client.sync_cleanup(kb_id, stale_file_ids, rmdir if rmdir else None)
        result.deleted = len(deleted)
        result.dirs_removed = len(rmdir)

    # ── 5. Create missing directories ──────────────────────────
    for dir_path in mkdir:
        check_stop()
        segments = dir_path.split("/")
        name = segments[-1]
        parent_path = "/".join(segments[:-1])
        parent_id = directory_map.get(parent_path)

        if verbose:
            click.echo(f"  mkdir {dir_path}", err=True)

        resp = client.create_directory(kb_id, name, parent_id)
        directory_map[dir_path] = resp.get("id", "")
        result.dirs_created += 1

    # ── 6. Upload files ────────────────────────────────────────
    manifest_by_key = {(e.path, e.filename): e for e in manifest}

    files_to_upload = [
        *[(a, "added") for a in added],
        *[(m, "modified") for m in modified],
    ]

    if files_to_upload:
        # ── 6a. Skip cached permanent failures ───────────────────
        history = SyncHistory()
        try:
            cache = history.get_failures(kb_id)
        except Exception:
            cache = {}

        # (path, filename, checksum) -> cache entry
        _cache_map: dict[tuple[str, str, str], dict[str, Any]] = cache

        files_to_upload = [
            (entry, ct)
            for entry, ct in files_to_upload
        ]
        _filtered: list[tuple[dict, str]] = []
        for entry, ct in files_to_upload:
            path = entry.get("path", "")
            filename = entry["filename"]
            manifest_entry = manifest_by_key.get((path, filename))
            checksum = manifest_entry.checksum if manifest_entry else ""
            key = (path, filename, checksum)
            cached = _cache_map.get(key)
            if cached is None:
                _filtered.append((entry, ct))
                continue
            kind = cached["kind"]
            if kind == "permanent":
                result.skipped += 1
                if verbose:
                    click.echo(
                        f"  [dim]Skipped (permanent failure): {path}/{filename}[/dim]",
                        err=True,
                    )
                continue
            if kind == "transient" and cached["attempts"] >= OIKB_MAX_TRANSIENT_ATTEMPTS:
                elapsed = time.time() - cached["last_seen"]
                if elapsed < OIKB_TRANSIENT_RETRY_AFTER:
                    result.skipped += 1
                    if verbose:
                        click.echo(
                            f"  [dim]Skipped (transient, retrying tomorrow): {path}/{filename}[/dim]",
                            err=True,
                        )
                    continue
            _filtered.append((entry, ct))

        files_to_upload = _filtered

        if files_to_upload:
            # ── 6b. Upload phase ───────────────────────────────────
            # Track (file_id, path, filename, checksum, change_type)
            _upload_tracker: list[tuple[str, str, str, str, str]] = []

            def _upload_one(
                i: int, entry: dict, change_type: str, progress: Progress | None, task_id: Any,
            ) -> tuple[str, str, str, str, str | None]:
                """Upload a single file with retry.

                Returns (change_type, path, filename, checksum, file_id_or_error).
                file_id is the id from the upload response, or None on failure.
                """
                filename = entry["filename"]
                path = entry.get("path", "")
                display = f"{path}/{filename}" if path else filename

                if verbose and not progress:
                    click.echo(f"  [{i}/{len(files_to_upload)}] {display}", err=True)

                check_stop()
                manifest_entry = manifest_by_key.get((path, filename))
                if not manifest_entry:
                    return ("error", path, filename, "", f"File not in manifest: {display}")

                file_id: str | None = None
                last_err: Exception | None = None
                for attempt in range(3):
                    check_stop()
                    try:
                        content = connector.read_file(path, filename)
                        check_stop()
                        directory_id = directory_map.get(path) if path else None
                        resp = client.upload_file(
                            file_content=content,
                            filename=filename,
                            kb_id=kb_id,
                            file_hash=manifest_entry.checksum,
                            directory_id=directory_id,
                        )
                        file_id = resp.get("id", "")
                        if progress is not None:
                            progress.update(task_id, advance=1, description=f"[cyan]{display}[/cyan]")
                        return (change_type, path, filename, manifest_entry.checksum, file_id)
                    except SourceFileUnavailable as e:
                        message = f"{display}: {e}"
                        if progress is not None:
                            progress.update(task_id, advance=1, description=f"[yellow]⚠ {display}[/yellow]")
                        else:
                            click.echo(click.style(f"  ⚠ {message}", fg="yellow"), err=True)
                        return ("warning", path, filename, "", message)
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code >= 500 and attempt < 2:
                            time.sleep(2 ** attempt)
                            check_stop()
                            last_err = e
                            continue
                        last_err = e
                        break
                    except SyncCancelled:
                        raise
                    except Exception as e:
                        last_err = e
                        break

                if progress is not None:
                    progress.update(task_id, advance=1, description=f"[red]✗ {display}[/red]")
                else:
                    click.echo(click.style(f"  ✗ {display}: {last_err}", fg="red"), err=True)
                return ("error", path, filename, "", f"{display}: {last_err}")

            def _tally_upload(outcome: tuple[str, str, str, str, str | None]) -> None:
                """Update result counters from an upload outcome."""
                kind, path, filename, checksum, file_id = outcome
                if kind == "added":
                    result.added += 1
                elif kind == "modified":
                    result.modified += 1
                elif kind == "warning":
                    result.warnings.append(f"{path}/{filename}: {file_id}")
                else:
                    result.errors.append(f"{path}/{filename}: {file_id}")
                if file_id is not None:
                    _upload_tracker.append((file_id, path, filename, checksum, kind))

            if show_progress:
                progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[bold blue]Uploading"),
                    BarColumn(bar_width=30),
                    MofNCompleteColumn(),
                    TextColumn("•"),
                    TextColumn("{task.description}"),
                    TextColumn("•"),
                    TimeElapsedColumn(),
                    console=_console,
                    transient=True,
                )
                with progress:
                    task_id = progress.add_task("", total=len(files_to_upload))

                    if concurrency > 1 and len(files_to_upload) > 1:
                        with ThreadPoolExecutor(max_workers=concurrency) as pool:
                            futures = {
                                pool.submit(_upload_one, i, entry, ct, progress, task_id): (entry, ct)
                                for i, (entry, ct) in enumerate(files_to_upload, 1)
                            }
                            for future in as_completed(futures):
                                _tally_upload(future.result())
                    else:
                        for i, (entry, change_type) in enumerate(files_to_upload, 1):
                            _tally_upload(_upload_one(i, entry, change_type, progress, task_id))
            else:
                # Quiet or daemon mode — no progress bar.
                if concurrency > 1 and len(files_to_upload) > 1:
                    with ThreadPoolExecutor(max_workers=concurrency) as pool:
                        futures = {
                            pool.submit(_upload_one, i, entry, ct, None, None): (entry, ct)
                            for i, (entry, ct) in enumerate(files_to_upload, 1)
                        }
                        for future in as_completed(futures):
                            _tally_upload(future.result())
                else:
                    for i, (entry, change_type) in enumerate(files_to_upload, 1):
                        _tally_upload(_upload_one(i, entry, change_type, None, None))

            # ── 6c. Verification phase ─────────────────────────────
            if _upload_tracker:
                _verify_uploads(
                    client, kb_id, _upload_tracker, result, verbose, quiet,
                    show_progress, concurrency, _console, check_stop,
                )
    else:
        if not quiet:
            click.echo("[dim]Nothing to upload.[/dim]", err=True)

    return result


def _verify_uploads(
    client: OikbClient,
    kb_id: str,
    uploads: list[tuple[str, str, str, str, str]],
    result: SyncResult,
    verbose: bool,
    quiet: bool,
    show_progress: bool,
    concurrency: int,
    console: Console,
    check_stop: Callable[[], None],
) -> None:
    """Poll status for uploaded files and reap failures."""
    history = SyncHistory()
    try:
        _failure_cache = history.get_failures(kb_id)
    except Exception:
        _failure_cache = {}

    deadline = time.time() + OIKB_VERIFY_BUDGET
    uploaded_ids = [(file_id, path, filename, checksum, ct)
                    for file_id, path, filename, checksum, ct in uploads]

    if show_progress:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]Verifying"),
            BarColumn(bar_width=20),
            MofNCompleteColumn(),
            TextColumn("•"),
            TextColumn("{task.description}"),
            TextColumn("•"),
            TimeElapsedColumn(),
            console=console,
            transient=True,
        )
        with progress:
            task_id = progress.add_task("", total=len(uploaded_ids))
            _poll_loop(client, kb_id, uploaded_ids, progress, task_id,
                       deadline, concurrency, check_stop, result, history,
                       _failure_cache, verbose)
    else:
        _poll_loop(client, kb_id, uploaded_ids, None, None, deadline,
                   concurrency, check_stop, result, history, _failure_cache, verbose)


def _poll_loop(
    client: OikbClient,
    kb_id: str,
    uploads: list[tuple[str, str, str, str, str]],
    progress: Progress | None,
    task_id: Any,
    deadline: float,
    concurrency: int,
    check_stop: Callable[[], None],
    result: SyncResult,
    history: SyncHistory,
    _failure_cache: dict[tuple[str, str, str], dict[str, Any]],
    verbose: bool = False,
) -> None:
    """Concurrent polling loop for file linkage status."""
    interval = OIKB_VERIFY_INTERVAL

    while True:
        check_stop()
        remaining = [(fid, p, fn, cs, ct) for fid, p, fn, cs, ct in uploads
                     if fid is not None]
        if not remaining:
            break
        if time.time() >= deadline:
            if verbose:
                click.echo(
                    f"  [dim]Verify budget exhausted ({OIKB_VERIFY_BUDGET}s); "
                    f"{len(remaining)} files still processing[/dim]",
                    err=True,
                )
            break

        if concurrency > 1 and len(remaining) > 1:
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures: dict[Any, tuple[str, str, str, str, str]] = {}
                for file_id, path, filename, checksum, ct in remaining:
                    futures[pool.submit(client.get_file_status, file_id)] = \
                        (file_id, path, filename, checksum, ct)
                for future in as_completed(futures):
                    file_id, path, filename, checksum, ct = futures[future]
                    try:
                        status_resp = future.result()
                    except Exception as exc:
                        status_resp = {"status": "failed", "error": str(exc)}
                    _handle_status(
                        client, status_resp, file_id, path, filename,
                        checksum, ct, result, history, kb_id,
                        _failure_cache, verbose,
                    )
                    if progress is not None:
                        progress.update(task_id, advance=1)
        else:
            for file_id, path, filename, checksum, ct in remaining:
                try:
                    status_resp = client.get_file_status(file_id)
                except Exception as exc:
                    status_resp = {"status": "failed", "error": str(exc)}
                _handle_status(
                    client, status_resp, file_id, path, filename,
                    checksum, ct, result, history, kb_id,
                    _failure_cache, verbose,
                )
                if progress is not None:
                    progress.update(task_id, advance=1)

        # Sleep until next poll or deadline.
        remaining_time = deadline - time.time()
        if remaining_time > 0:
            time.sleep(min(interval, remaining_time))


def _handle_status(
    client: OikbClient,
    status_resp: dict[str, Any],
    file_id: str,
    path: str,
    filename: str,
    checksum: str,
    change_type: str,
    result: SyncResult,
    history: SyncHistory,
    kb_id: str,
    _failure_cache: dict[tuple[str, str, str], dict[str, Any]],
    verbose: bool = False,
) -> None:
    """Process a single file status response."""
    key = (path, filename, checksum)
    status = status_resp.get("status", "")

    if status == "completed":
        # Linkage succeeded. Clear any transient failure record.
        try:
            history.clear_failure(kb_id, path, filename, checksum)
        except Exception:
            pass
        _failure_cache.pop(key, None)
        return

    if status == "failed":
        # Fetch the actual error message.
        error_msg = status_resp.get("error", "")
        try:
            file_info = client.get_file(file_id)
            error_msg = file_info.get("data", {}).get("error", error_msg)
        except Exception:
            pass

        if not error_msg:
            error_msg = f"Linkage failed (status={status})"

        # Classify and record.
        if _is_duplicate_error(error_msg):
            kind = "permanent"
        else:
            kind = "transient"

        cached = _failure_cache.get(key, {})
        attempts = cached.get("attempts", 0) + 1

        # For permanent failures, delete the orphan immediately.
        if kind == "permanent":
            try:
                client.delete_file(file_id)
            except Exception:
                pass
            # Decrement the counter that was incremented by _tally_upload.
            if change_type == "added":
                result.added = max(0, result.added - 1)
            elif change_type == "modified":
                result.modified = max(0, result.modified - 1)
            result.errors.append(f"{path}/{filename}: {error_msg}")
            if verbose:
                click.echo(
                    f"  [red]✗ {path}/{filename}: {error_msg}[/red]", err=True,
                )
            history.record_failure(kb_id, path, filename, checksum,
                                   file_id, error_msg, kind)
            _failure_cache[key] = {
                "kind": kind, "attempts": attempts, "last_seen": time.time(),
            }

        else:
            # Transient: track attempts.
            if attempts >= OIKB_MAX_TRANSIENT_ATTEMPTS:
                try:
                    client.delete_file(file_id)
                except Exception:
                    pass
                if change_type == "added":
                    result.added = max(0, result.added - 1)
                elif change_type == "modified":
                    result.modified = max(0, result.modified - 1)
                result.errors.append(f"{path}/{filename}: {error_msg}")
                if verbose:
                    click.echo(
                        f"  [red]✗ {path}/{filename}: {error_msg}[/red]", err=True,
                    )
                history.record_failure(kb_id, path, filename, checksum,
                                       file_id, error_msg, kind)
                _failure_cache[key] = {
                    "kind": kind, "attempts": attempts, "last_seen": time.time(),
                }
            else:
                _failure_cache[key] = {
                    "kind": kind, "attempts": attempts, "last_seen": time.time(),
                }
        return
