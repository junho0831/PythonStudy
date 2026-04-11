from __future__ import annotations

from pathlib import Path, PurePosixPath


def build_relative_path(remote_path: str, root_path: str) -> str:
    remote = PurePosixPath(remote_path)
    root = PurePosixPath(root_path)
    try:
        return remote.relative_to(root).as_posix()
    except ValueError:
        return remote.as_posix().lstrip("/")


def build_local_path(work_dir: Path, source_root: str, remote_path: str) -> Path:
    relative = PurePosixPath(build_relative_path(remote_path, source_root))
    return work_dir.joinpath(*relative.parts)


def make_rbi_path(source_remote_path: str) -> PurePosixPath:
    parts = list(PurePosixPath(source_remote_path).parts)
    ruip_index = next((idx for idx, part in enumerate(parts) if part.lower() == "ruip"), None)
    if ruip_index is None:
        raise ValueError(f"RUIP 세그먼트를 찾을 수 없습니다: {source_remote_path}")
    tail_parts = parts[ruip_index:]
    tail_parts[0] = "ruip"
    return (PurePosixPath("rbi") / PurePosixPath(*tail_parts)).with_suffix(".png")
