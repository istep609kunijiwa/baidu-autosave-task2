#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Safe, opt-in mirror deletion state and reconciliation."""

import json
import os
import posixpath
from pathlib import Path

from config_utils import share_config_state_key
from storage_rules import is_safe_relative_target_path


MIRROR_STATE_FILE = Path(__file__).resolve().parent / ".transfershare_mirror_state.json"
MIRROR_SCHEMA_VERSION = 1
DEFAULT_MAX_DELETES_PER_RUN = 100


def _truthy(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _max_deletes_per_run():
    raw_value = os.getenv("TRANSFERSHARE_MIRROR_DELETE_MAX_FILES", str(DEFAULT_MAX_DELETES_PER_RUN))
    try:
        return max(0, int(raw_value))
    except (TypeError, ValueError):
        return DEFAULT_MAX_DELETES_PER_RUN


def mirror_delete_enabled(config):
    return _truthy(config.get("mirror_delete") or os.getenv("TRANSFERSHARE_MIRROR_DELETE"))


def load_mirror_state(path=MIRROR_STATE_FILE):
    path = Path(path)
    if not path.exists():
        return {"schema_version": MIRROR_SCHEMA_VERSION, "subscriptions": {}}
    with path.open("r", encoding="utf-8") as stream:
        state = json.load(stream)
    if not isinstance(state, dict) or not isinstance(state.get("subscriptions", {}), dict):
        raise ValueError("镜像删除状态格式无效")
    state.setdefault("schema_version", MIRROR_SCHEMA_VERSION)
    state.setdefault("subscriptions", {})
    return state


def save_mirror_state(state, path=MIRROR_STATE_FILE):
    path = Path(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp_path.open("w", encoding="utf-8") as stream:
        json.dump(state, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.write("\n")
    os.chmod(tmp_path, 0o600)
    os.replace(tmp_path, path)


def _result_records(result):
    if isinstance(result.get("results"), list):
        return result["results"]
    return [result]


def _snapshot_files(record):
    files = {}
    for item in record.get("source_files", []):
        path = str(item.get("path") or "").replace("\\", "/").lstrip("/")
        if not path or not is_safe_relative_target_path(path):
            continue
        files[path] = {"md5": item.get("md5"), "missing_count": 0}
    return files


def _scan_missing_local_files(storage, save_dir, missing_paths):
    relative_dirs = {
        "" if posixpath.dirname(path) in ("", ".") else posixpath.dirname(path)
        for path in missing_paths
    }
    return storage._scan_local_files_dict(save_dir, None, relative_dirs)


def reconcile_mirror_deletions(storage, logger, config, result, state_path=MIRROR_STATE_FILE):
    """Reconcile complete source snapshots and remove confirmed missing files.

    A path must be absent in two consecutive complete snapshots. Before removal,
    the destination MD5 must still equal the source MD5 recorded in the baseline.
    """
    if not mirror_delete_enabled(config):
        return {"enabled": False, "deleted": 0, "pending": 0}

    if (
        os.getenv("GITHUB_ACTIONS") == "true"
        and os.getenv("TRANSFERSHARE_MIRROR_STATE_ENABLED", "").lower() != "true"
    ):
        logger.warning(
            "镜像删除已启用，但未配置 TRANSFERSHARE_STATE_KEY；为避免丢失清单，本次不删除"
        )
        return {"enabled": True, "deleted": 0, "pending": 0, "state_disabled": True}

    try:
        state = load_mirror_state(state_path)
    except Exception as exc:
        logger.warning(f"读取镜像删除状态失败，为避免误删，本次跳过: {exc}")
        return {"enabled": True, "deleted": 0, "pending": 0, "state_error": True}

    configs_by_key = {
        share_config_state_key(item): item for item in config.get("share_configs", [])
    }
    complete_records = [
        item
        for item in _result_records(result)
        if item.get("success") and item.get("source_snapshot_complete") and item.get("state_key")
    ]

    # Protect paths still present in any subscription writing to the same directory.
    protected = set()
    for record in complete_records:
        save_dir = str(record.get("save_dir") or "").rstrip("/")
        for path in _snapshot_files(record):
            protected.add((save_dir, path))

    max_deletes = _max_deletes_per_run()
    deleted = 0
    pending = 0
    changed = False

    for record in complete_records:
        state_key = record["state_key"]
        share_config = configs_by_key.get(state_key)
        if not share_config:
            logger.warning("镜像删除无法匹配本次订阅配置，已跳过该链接")
            continue
        save_dir = str(record.get("save_dir") or share_config.get("save_dir") or "").rstrip("/")
        if not save_dir:
            logger.warning("镜像删除缺少保存目录，已跳过该链接")
            continue

        current_files = _snapshot_files(record)
        subscriptions = state["subscriptions"]
        previous = subscriptions.get(state_key)
        if previous is None:
            subscriptions[state_key] = {"save_dir": save_dir, "files": current_files}
            changed = True
            logger.info(f"镜像删除已建立初始清单，共 {len(current_files)} 个文件；首次运行不删除")
            continue

        previous_files = previous.get("files", {}) if isinstance(previous, dict) else {}
        missing_paths = sorted(set(previous_files) - set(current_files))
        if not current_files and previous_files:
            logger.warning("共享端完整快照为空，为避免异常清空，本次不确认或删除历史文件")
            continue

        local_files = (
            _scan_missing_local_files(storage, save_dir, missing_paths) if missing_paths else {}
        )
        next_files = dict(current_files)
        confirmed = []
        for path in missing_paths:
            previous_item = previous_files.get(path) or {}
            missing_count = int(previous_item.get("missing_count", 0)) + 1
            source_md5 = previous_item.get("md5")
            normalized_path = storage.path_service.normalize_path(path)
            local_md5 = local_files.get(normalized_path)

            if (save_dir, path) in protected:
                continue
            if local_md5 is None:
                continue
            if not source_md5 or local_md5 != source_md5:
                logger.warning(f"镜像删除保留已变更文件: {path}")
                continue
            if missing_count < 2:
                next_files[path] = {"md5": source_md5, "missing_count": missing_count}
                pending += 1
                continue
            confirmed.append((path, source_md5))

        remaining_capacity = max_deletes - deleted
        if len(confirmed) > remaining_capacity:
            logger.warning(
                f"本次待镜像删除 {len(confirmed)} 个文件，超过剩余安全上限 {remaining_capacity}；本轮全部保留"
            )
            for path, source_md5 in confirmed:
                next_files[path] = {"md5": source_md5, "missing_count": 2}
                pending += 1
        else:
            normalized_save_dir = storage.path_service.normalize_path(save_dir).rstrip("/")
            for path, source_md5 in confirmed:
                remote_path = posixpath.join(normalized_save_dir, path)
                try:
                    storage.client.remove(remote_path)
                    deleted += 1
                    logger.info(f"镜像删除完成: {remote_path}")
                except Exception as exc:
                    next_files[path] = {"md5": source_md5, "missing_count": 2}
                    pending += 1
                    logger.warning(f"镜像删除失败，将在下次重试: {remote_path} - {exc}")

        subscriptions[state_key] = {"save_dir": save_dir, "files": next_files}
        changed = True
        if deleted:
            storage._clear_local_files_cache(save_dir)

    if changed:
        save_mirror_state(state, state_path)
    if complete_records:
        logger.info(f"镜像删除对账完成: 已删除 {deleted} 个，待再次确认或重试 {pending} 个")
    return {"enabled": True, "deleted": deleted, "pending": pending}


__all__ = [
    "MIRROR_STATE_FILE",
    "load_mirror_state",
    "mirror_delete_enabled",
    "reconcile_mirror_deletions",
    "save_mirror_state",
]
