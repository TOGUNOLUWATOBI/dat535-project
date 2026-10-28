"""Persistent stage checkpoints with file verification and exclusive snapshot locks.

SQLite stores metadata, not transport records. Outputs and failed attempts are
never overwritten. A process crash releases flock; a later run records the
interrupted attempt and starts a new directory for that stage.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
from uuid import uuid4


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""): value.update(block)
    return value.hexdigest()


def identity(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def output_inventory(output):
    root = Path(output)
    manifest = root / "metadata/manifest.json"
    data = json.loads(manifest.read_text())
    if data.get("status") != "complete": raise RuntimeError(f"Output has no complete manifest: {manifest}")
    paths = [manifest] + sorted(root.rglob("*.parquet"))
    if not data.get("tables") or len(paths) == 1: raise RuntimeError("Stage produced no Parquet tables")
    for table in data["tables"].values():
        table_root = (root / table["path"]).resolve()
        if not table_root.is_relative_to(root.resolve()) or not table_root.is_dir():
            raise RuntimeError("Manifest table path is missing or outside output")
        if not list(table_root.glob("*.parquet")): raise RuntimeError(f"No saved Parquet files: {table_root}")
    return [{"path": str(p.relative_to(root)), "bytes": p.stat().st_size, "sha256": digest(p)} for p in paths]


class Checkpoints:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS snapshots (
              snapshot_key TEXT PRIMARY KEY, snapshot_date TEXT NOT NULL,
              providers TEXT NOT NULL, source_manifest TEXT NOT NULL,
              source_fingerprint TEXT NOT NULL, registered_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS stage_attempts (
              attempt_id TEXT PRIMARY KEY, snapshot_key TEXT NOT NULL REFERENCES snapshots(snapshot_key),
              stage TEXT NOT NULL, input_key TEXT NOT NULL, implementation TEXT NOT NULL,
              status TEXT NOT NULL, output_path TEXT NOT NULL, started_at TEXT NOT NULL,
              finished_at TEXT, host TEXT NOT NULL, pid INTEGER NOT NULL,
              manifest_sha256 TEXT, inventory_json TEXT, error TEXT);
            CREATE INDEX IF NOT EXISTS stage_lookup ON stage_attempts
              (snapshot_key,stage,input_key,implementation,status);
            CREATE TABLE IF NOT EXISTS checkpoint_events (
              event_id INTEGER PRIMARY KEY, attempt_id TEXT NOT NULL,
              timestamp TEXT NOT NULL, event TEXT NOT NULL, detail TEXT);
        """)
        self.db.commit()

    def close(self): self.db.close()

    def register(self, selection):
        fingerprint = identity({"date": selection["snapshot_date"], "providers": sorted(selection["providers"]),
            "files": sorted((f["source_member"], f["source_xml_sha256"])
                            for f in selection["files"] + selection["stop_files"])})
        key = fingerprint
        self.db.execute("INSERT OR IGNORE INTO snapshots VALUES (?,?,?,?,?,?)", (
            key, selection["snapshot_date"], ",".join(sorted(selection["providers"])),
            selection["source_manifest"], fingerprint, now()))
        self.db.commit()
        return key

    @contextmanager
    def lock(self, snapshot_key):
        directory = self.path.parent / (self.path.name + ".locks")
        directory.mkdir(exist_ok=True)
        with (directory / snapshot_key).open("a") as handle:
            try: fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise RuntimeError("Another process is processing this snapshot with this checkpoint registry") from error
            try: yield
            finally: fcntl.flock(handle, fcntl.LOCK_UN)

    def execute(self, snapshot_key, stage, input_key, implementation, destination, operation):
        """Call under lock(snapshot_key). Reuse only byte-verified complete outputs."""
        rows = self.db.execute("""SELECT * FROM stage_attempts WHERE snapshot_key=? AND stage=?
            AND input_key=? AND implementation=? AND status='complete' ORDER BY finished_at DESC""",
            (snapshot_key, stage, input_key, implementation)).fetchall()
        for row in rows:
            try:
                if output_inventory(row["output_path"]) != json.loads(row["inventory_json"]):
                    raise RuntimeError("Saved output files no longer match their checkpoint hashes")
            except (OSError, ValueError, KeyError, RuntimeError) as error:
                self.db.execute("UPDATE stage_attempts SET status='invalidated',error=? WHERE attempt_id=?",
                                (str(error), row["attempt_id"]))
                self.db.execute("INSERT INTO checkpoint_events(attempt_id,timestamp,event,detail) VALUES (?,?,?,?)",
                                (row["attempt_id"], now(), "integrity_failed", str(error)))
                self.db.commit()
                continue
            self.db.execute("INSERT INTO checkpoint_events(attempt_id,timestamp,event) VALUES (?,?,?)",
                            (row["attempt_id"], now(), "reused"))
            self.db.commit()
            print(f"checkpoint: reuse {stage} -> {row['output_path']}", flush=True)
            return Path(row["output_path"]), row["manifest_sha256"], True
        # With the exclusive lock held, older running entries belong to an interrupted process.
        self.db.execute("""UPDATE stage_attempts SET status='interrupted',finished_at=?,error=?
            WHERE snapshot_key=? AND status='running'""", (now(), "Process ended before completing the stage", snapshot_key))
        attempt = uuid4().hex
        output = Path(destination).resolve() / (stage + "-" + attempt[:12])
        self.db.execute("""INSERT INTO stage_attempts
            (attempt_id,snapshot_key,stage,input_key,implementation,status,output_path,started_at,host,pid)
            VALUES (?,?,?,?,?,'running',?,?,?,?)""", (
                attempt, snapshot_key, stage, input_key, implementation, str(output), now(), socket.gethostname(), os.getpid()))
        self.db.commit()
        try:
            operation(output)
            inventory = output_inventory(output)
            sha = digest(output / "metadata/manifest.json")
            self.db.execute("""UPDATE stage_attempts SET status='complete',finished_at=?,
                manifest_sha256=?,inventory_json=? WHERE attempt_id=?""", (now(), sha, json.dumps(inventory), attempt))
            self.db.commit()
            return output, sha, False
        except BaseException as error:
            self.db.execute("UPDATE stage_attempts SET status='failed',finished_at=?,error=? WHERE attempt_id=?",
                            (now(), f"{type(error).__name__}: {error}", attempt))
            self.db.commit()
            raise

    def status(self):
        return [dict(r) for r in self.db.execute("""SELECT s.snapshot_date,s.providers,a.stage,a.status,
            a.attempt_id,a.started_at,a.finished_at,a.output_path,a.error
            FROM snapshots s JOIN stage_attempts a USING(snapshot_key)
            ORDER BY s.snapshot_date,a.started_at""")]


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Show persistent pipeline stage checkpoints")
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    if not args.state.is_file(): parser.error("Checkpoint database does not exist")
    state = Checkpoints(args.state)
    try: print(json.dumps(state.status(), indent=2))
    finally: state.close()


if __name__ == "__main__": main()
