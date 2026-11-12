#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import posixpath
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from env_utils import read_positive_int_env
from storage_errors import classify_storage_error
from storage_metrics import emit_storage_metric
from storage_rules import extract_file_info, should_exclude_folder, should_include_folder
from utils import handle_error_and_notify


# 百度分享目录列表分页 size。官方接口实测可支持到 ~1000。
# 默认保持 100 兼容现有测试断言；用户可通过环境变量调高来减少多页扫描的网络往返。
_DEFAULT_SHARED_DIR_PAGE_SIZE = read_positive_int_env("TRANSFERSHARE_SHARED_PAGE_SIZE", 100)
# 大页短页探测开关。当用户主动调大 page_size 后，若服务端静默截断（例如
# 请求 size=1000 但只返回 100），默认的 "len < page_size 即停止" 会导致漏扫。
# 设为 1 后，遇到非空短页继续请求下一页直到出现空页；默认 0 关闭保持旧行为。
_DEFAULT_SHARED_DIR_PAGE_PROBE_ON_SHORT = (
    read_positive_int_env("TRANSFERSHARE_SHARED_PAGE_PROBE_ON_SHORT", 0) >= 1
)
# 同时枚举多少个独立共享目录。默认 1 保持兼容；GitHub Actions 对大型分享使用 2，
# 在不过度触发百度限流的前提下重叠目录列表请求的网络等待。
_DEFAULT_SHARED_DIR_CONCURRENCY = read_positive_int_env("TRANSFERSHARE_SHARED_SCAN_CONCURRENCY", 1)


class SharedPathService:
    SHARED_DIR_PAGE_SIZE = _DEFAULT_SHARED_DIR_PAGE_SIZE
    SHARED_DIR_PAGE_PROBE_ON_SHORT = _DEFAULT_SHARED_DIR_PAGE_PROBE_ON_SHORT
    SHARED_DIR_CONCURRENCY = _DEFAULT_SHARED_DIR_CONCURRENCY
    PROGRESS_DIR_INTERVAL = 20
    PROGRESS_PAGE_INTERVAL = 20
    PROGRESS_FILE_INTERVAL = 500

    def __init__(self, client, wechat_notifier=None):
        self.client = client
        self.wechat_notifier = wechat_notifier

    def load_shared_paths(self, share_url, pwd=None):
        self.client.access_shared(share_url, pwd)
        return self.client.shared_paths(shared_url=share_url)

    @staticmethod
    def _normalize_selected_path(path):
        selected_path = str(path or "").strip().replace("\\", "/")
        if not selected_path or selected_path == "/":
            return ""
        if not selected_path.startswith("/"):
            selected_path = f"/{selected_path}"
        return posixpath.normpath(selected_path)

    @staticmethod
    def _canonical_shared_path(path):
        """Normalize Baidu share paths before comparing them.

        Baidu may return the same share mount path both with and without a
        leading slash. Keeping comparisons relative also avoids leaking the
        internal ``sharelink0-*`` mount directory into the destination.
        """
        normalized = str(path or "").strip().replace("\\", "/")
        if not normalized or normalized == "/":
            return ""
        normalized = posixpath.normpath(f"/{normalized.lstrip('/')}")
        return "" if normalized == "/" else normalized.lstrip("/")

    def select_shared_path(self, shared_paths, selected_path):
        """Return the single share entry selected by a Baidu ``#list/path`` fragment."""
        paths = list(shared_paths or [])
        target_path = self._normalize_selected_path(selected_path)
        if not target_path:
            return paths
        if not paths:
            raise ValueError("分享链接为空，无法定位指定子目录")

        shared_root = self._resolve_shared_root(paths)
        candidates = [target_path]
        if shared_root and not (
            target_path == shared_root or target_path.startswith(f"{shared_root}/")
        ):
            candidates.append(posixpath.join(shared_root, target_path.lstrip("/")))

        normalized_candidates = []
        for candidate in candidates:
            normalized = self._normalize_selected_path(candidate)
            if normalized and normalized not in normalized_candidates:
                normalized_candidates.append(normalized)

        for shared_path in paths:
            path = self._normalize_selected_path(getattr(shared_path, "path", ""))
            if path in normalized_candidates:
                return [shared_path]

        first_path = paths[0]
        uk = first_path.uk
        share_id = first_path.share_id
        bdstoken = first_path.bdstoken
        for candidate in normalized_candidates:
            parent_path = posixpath.dirname(candidate) or "/"
            for child in self.iter_shared_dir_children(parent_path, uk, share_id, bdstoken):
                child_path = self._normalize_selected_path(child["path"])
                if child_path == candidate:
                    return [child["raw"]]

        raise ValueError(f"分享链接指定的子目录不存在或无权访问: {target_path}")

    @classmethod
    def _resolve_shared_root(cls, shared_paths):
        root_candidates = []
        for shared_path in shared_paths or []:
            raw_path = getattr(shared_path, "path", "")
            if not raw_path:
                continue

            normalized_path = cls._canonical_shared_path(raw_path)
            if not normalized_path:
                continue

            parent_path = posixpath.dirname(normalized_path)
            if parent_path:
                root_candidates.append(parent_path)
            elif getattr(shared_path, "is_dir", False):
                root_candidates.append(normalized_path)

        if not root_candidates:
            return ""

        try:
            shared_root = posixpath.commonpath(root_candidates)
        except ValueError:
            return ""

        if not shared_root or shared_root == ".":
            return ""
        return f"/{shared_root.strip('/')}"

    @classmethod
    def _trim_shared_root(cls, shared_file_path, shared_root=""):
        if not shared_file_path:
            return ""

        normalized_path = cls._canonical_shared_path(shared_file_path)
        if not normalized_path:
            return ""

        normalized_root = cls._canonical_shared_path(shared_root)
        if normalized_root:
            root_prefix = f"{normalized_root}/"
            if normalized_path == normalized_root:
                return ""
            if normalized_path.startswith(root_prefix):
                return normalized_path[len(root_prefix) :]

        return normalized_path

    @classmethod
    def _new_scan_stats(cls):
        return {
            "dirs": 0,
            "files": 0,
            "pages": 0,
            "skipped_dirs": 0,
            "last_report_dirs": 0,
            "last_report_files": 0,
            "last_report_pages": 0,
        }

    @classmethod
    def _should_report_progress(cls, stats):
        return (
            stats["dirs"] - stats["last_report_dirs"] >= cls.PROGRESS_DIR_INTERVAL
            or stats["files"] - stats["last_report_files"] >= cls.PROGRESS_FILE_INTERVAL
            or stats["pages"] - stats["last_report_pages"] >= cls.PROGRESS_PAGE_INTERVAL
        )

    @classmethod
    def _report_scan_progress(cls, progress_callback, stats):
        if not progress_callback:
            return
        progress_callback(
            "info",
            "共享文件扫描进度："
            f"已扫描 {stats['dirs']} 个目录 / {stats['pages']} 页，"
            f"发现 {stats['files']} 个文件，跳过 {stats['skipped_dirs']} 个目录",
        )
        stats["last_report_dirs"] = stats["dirs"]
        stats["last_report_files"] = stats["files"]
        stats["last_report_pages"] = stats["pages"]

    @classmethod
    def _normalize_shared_file_info(cls, shared_file, shared_root=""):
        if hasattr(shared_file, "_asdict"):
            shared_file_dict = shared_file._asdict()
        elif isinstance(shared_file, dict):
            shared_file_dict = dict(shared_file)
        else:
            shared_file_dict = {
                "server_filename": os.path.basename(getattr(shared_file, "path", "")),
                "fs_id": getattr(shared_file, "fs_id", ""),
                "path": getattr(shared_file, "path", ""),
                "size": getattr(shared_file, "size", 0),
                "isdir": 1 if getattr(shared_file, "is_dir", False) else 0,
                "md5": getattr(shared_file, "md5", None),
            }

        file_info = extract_file_info(shared_file_dict)
        if file_info:
            shared_file_path = getattr(shared_file, "path", file_info.get("path", ""))
            file_info["path"] = cls._trim_shared_root(shared_file_path, shared_root)
        return file_info

    @staticmethod
    def _normalize_shared_child(shared_file):
        if isinstance(shared_file, dict):
            path = shared_file.get("path") or shared_file.get("server_filename", "")
            is_dir = shared_file.get("is_dir") or shared_file.get("isdir") == 1
            is_file = shared_file.get("is_file") or shared_file.get("isdir") == 0
            fs_id = shared_file.get("fs_id", "")
            md5 = shared_file.get("md5")
        else:
            path = getattr(shared_file, "path", "")
            is_dir = getattr(shared_file, "is_dir", False)
            is_file = getattr(shared_file, "is_file", not is_dir)
            fs_id = getattr(shared_file, "fs_id", "")
            md5 = getattr(shared_file, "md5", None)

        name = os.path.basename(str(path).rstrip("/"))
        return {
            "raw": shared_file,
            "fs_id": fs_id,
            "path": path,
            "name": name,
            "is_dir": bool(is_dir),
            "is_file": bool(is_file),
            "md5": md5,
        }

    def _iter_shared_dir_pages(self, dir_path, uk, share_id, bdstoken):
        page = 1
        page_size = self.SHARED_DIR_PAGE_SIZE
        probe_on_short = getattr(
            self, "SHARED_DIR_PAGE_PROBE_ON_SHORT", _DEFAULT_SHARED_DIR_PAGE_PROBE_ON_SHORT
        )
        last_yielded_page = 0
        total_pages_yielded = 0
        last_page_count = 0
        last_page_key = None

        try:
            while True:
                sub_paths = self.client.list_shared_paths(
                    dir_path, uk, share_id, bdstoken, page=page, size=page_size
                )
                if isinstance(sub_paths, list):
                    sub_files = sub_paths
                elif isinstance(sub_paths, dict):
                    sub_files = sub_paths.get("list", [])
                else:
                    break

                page_key = tuple(
                    getattr(item, "fs_id", getattr(item, "path", item)) for item in sub_files
                )
                if not sub_files:
                    last_yielded_page = page
                    total_pages_yielded += 1
                    last_page_count = 0
                    yield page, sub_files
                    break
                if page_key == last_page_key:
                    break
                last_page_key = page_key
                last_yielded_page = page
                total_pages_yielded += 1
                last_page_count = len(sub_files)
                yield page, sub_files
                if last_page_count < page_size and not probe_on_short:
                    break
                page += 1
        finally:
            emit_storage_metric(
                "shared_dir_pages",
                dir_path=dir_path,
                page_size=page_size,
                pages=total_pages_yielded,
                last_page=last_yielded_page,
                last_page_count=last_page_count,
            )

    def iter_shared_dir_children(self, path, uk, share_id, bdstoken):
        dir_path = getattr(path, "path", path)
        for _, sub_files in self._iter_shared_dir_pages(dir_path, uk, share_id, bdstoken):
            for sub_file in sub_files:
                yield self._normalize_shared_child(sub_file)

    def list_shared_dir_children(self, path, uk, share_id, bdstoken):
        return list(self.iter_shared_dir_children(path, uk, share_id, bdstoken))

    def _list_shared_dir_pages(self, dir_path, uk, share_id, bdstoken):
        return list(self._iter_shared_dir_pages(dir_path, uk, share_id, bdstoken))

    @staticmethod
    def _normalize_dir_path_for_compare(path):
        normalized = str(getattr(path, "path", path) or "").rstrip("/")
        return normalized or "/"

    def _skip_forbidden_shared_child_dir(
        self,
        exc,
        dir_path,
        root_dir_path,
        progress_callback,
        stats,
    ):
        """Skip an inaccessible child without hiding an invalid share root."""

        if classify_storage_error(exc).kind != "share_forbidden":
            return False
        if self._normalize_dir_path_for_compare(dir_path) == self._normalize_dir_path_for_compare(
            root_dir_path
        ):
            return False

        stats["skipped_dirs"] += 1
        emit_storage_metric(
            "shared_dir_skipped",
            dir_path=dir_path,
            reason="share_forbidden",
        )
        if progress_callback:
            progress_callback(
                "warning",
                f"已跳过禁止分享的子目录，继续扫描其他目录: {dir_path}",
            )
        return True

    def _iter_shared_dir_files_concurrent(
        self,
        path,
        uk,
        share_id,
        bdstoken,
        folder_filter,
        shared_root,
        progress_callback,
        stats,
        exclude_folder_filter,
        concurrency,
    ):
        """Traverse independent shared directories with bounded concurrency.

        All filtering, progress accounting, and yielding stay on the caller thread;
        workers only perform paginated directory-list requests.  This avoids races in
        callbacks while overlapping the dominant network latency for large trees.
        """

        root_dir_path = getattr(path, "path", path)
        pending_dirs = deque([path])
        futures = {}
        executor = ThreadPoolExecutor(
            max_workers=concurrency,
            thread_name_prefix="transfershare-shared-scan",
        )

        def submit_available():
            while pending_dirs and len(futures) < concurrency:
                current_path_obj = pending_dirs.popleft()
                dir_path = getattr(current_path_obj, "path", current_path_obj)
                stats["dirs"] += 1
                if self._should_report_progress(stats):
                    self._report_scan_progress(progress_callback, stats)
                future = executor.submit(
                    self._list_shared_dir_pages,
                    dir_path,
                    uk,
                    share_id,
                    bdstoken,
                )
                futures[future] = dir_path

        submit_available()
        try:
            while futures:
                # Futures are inserted in discovery order.  Waiting for the oldest
                # submitted directory keeps filtering/deduplication deterministic,
                # while the other directory requests continue running in parallel.
                future = next(iter(futures))
                dir_path = futures.pop(future)
                try:
                    pages = future.result()
                except Exception as exc:
                    if self._skip_forbidden_shared_child_dir(
                        exc,
                        dir_path,
                        root_dir_path,
                        progress_callback,
                        stats,
                    ):
                        submit_available()
                        continue
                    handle_error_and_notify(
                        exc,
                        f"获取共享目录文件时发生异常\n目录路径: {dir_path}",
                        self.wechat_notifier,
                        None,
                        collect=True,
                    )
                    raise

                for page, sub_files in pages:
                    stats["pages"] += 1
                    if self._should_report_progress(stats):
                        self._report_scan_progress(progress_callback, stats)
                    if not sub_files:
                        if page == 1 and progress_callback:
                            progress_callback("info", f"共享目录为空: {dir_path}")
                        continue

                    for sub_file in sub_files:
                        child = self._normalize_shared_child(sub_file)
                        if child["is_dir"]:
                            folder_name = child["name"]
                            if should_exclude_folder(folder_name, exclude_folder_filter):
                                stats["skipped_dirs"] += 1
                            elif should_include_folder(folder_name, folder_filter):
                                pending_dirs.append(child["raw"])
                            else:
                                stats["skipped_dirs"] += 1
                            continue

                        file_info = self._normalize_shared_file_info(child["raw"], shared_root)
                        if file_info:
                            stats["files"] += 1
                            if self._should_report_progress(stats):
                                self._report_scan_progress(progress_callback, stats)
                            yield file_info
                submit_available()
        finally:
            for future in futures:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)

    def iter_shared_dir_files(
        self,
        path,
        uk,
        share_id,
        bdstoken,
        folder_filter=None,
        shared_root="",
        progress_callback=None,
        stats=None,
        exclude_folder_filter=None,
    ):
        if stats is None:
            stats = self._new_scan_stats()

        root_dir_path = getattr(path, "path", path)

        if not self.client:
            handle_error_and_notify(
                ValueError("客户端未初始化或初始化失败"),
                "获取共享目录文件失败: 客户端不可用",
                self.wechat_notifier,
                None,
                collect=False,
            )
            return

        concurrency = max(1, int(getattr(self, "SHARED_DIR_CONCURRENCY", 1)))
        if concurrency > 1:
            yield from self._iter_shared_dir_files_concurrent(
                path,
                uk,
                share_id,
                bdstoken,
                folder_filter,
                shared_root,
                progress_callback,
                stats,
                exclude_folder_filter,
                concurrency,
            )
            return

        stack = [(path, None, None)]
        while stack:
            current_path_obj, page_iter, file_iter = stack.pop()
            dir_path = getattr(current_path_obj, "path", current_path_obj)
            try:
                if page_iter is None:
                    stats["dirs"] += 1
                    if self._should_report_progress(stats):
                        self._report_scan_progress(progress_callback, stats)
                    page_iter = iter(self._iter_shared_dir_pages(dir_path, uk, share_id, bdstoken))

                while True:
                    if file_iter is None:
                        page, sub_files = next(page_iter)
                        stats["pages"] += 1
                        if self._should_report_progress(stats):
                            self._report_scan_progress(progress_callback, stats)
                        if not sub_files:
                            if page == 1 and progress_callback:
                                progress_callback("info", f"共享目录为空: {dir_path}")
                            break
                        file_iter = iter(sub_files)

                    try:
                        sub_file = next(file_iter)
                    except StopIteration:
                        file_iter = None
                        continue

                    child = self._normalize_shared_child(sub_file)
                    if child["is_dir"]:
                        folder_name = child["name"]
                        if should_exclude_folder(folder_name, exclude_folder_filter):
                            stats["skipped_dirs"] += 1
                        elif should_include_folder(folder_name, folder_filter):
                            stack.append((current_path_obj, page_iter, file_iter))
                            stack.append((child["raw"], None, None))
                            break
                        else:
                            stats["skipped_dirs"] += 1
                    else:
                        file_info = self._normalize_shared_file_info(child["raw"], shared_root)
                        if file_info:
                            stats["files"] += 1
                            if self._should_report_progress(stats):
                                self._report_scan_progress(progress_callback, stats)
                            yield file_info
            except StopIteration:
                continue
            except Exception as exc:
                if self._skip_forbidden_shared_child_dir(
                    exc,
                    dir_path,
                    root_dir_path,
                    progress_callback,
                    stats,
                ):
                    continue
                handle_error_and_notify(
                    exc,
                    f"获取共享目录文件时发生异常\n目录路径: {dir_path}",
                    self.wechat_notifier,
                    None,
                    collect=True,
                )
                raise

    def list_shared_dir_files(
        self,
        path,
        uk,
        share_id,
        bdstoken,
        folder_filter=None,
        shared_root="",
        progress_callback=None,
        stats=None,
        exclude_folder_filter=None,
    ):
        return list(
            self.iter_shared_dir_files(
                path,
                uk,
                share_id,
                bdstoken,
                folder_filter,
                shared_root,
                progress_callback,
                stats,
                exclude_folder_filter=exclude_folder_filter,
            )
        )

    def iter_shared_files(
        self,
        shared_paths,
        folder_filter=None,
        progress_callback=None,
        exclude_folder_filter=None,
    ):
        if not shared_paths:
            return

        uk = shared_paths[0].uk
        share_id = shared_paths[0].share_id
        bdstoken = shared_paths[0].bdstoken
        shared_root = self._resolve_shared_root(shared_paths)
        stats = self._new_scan_stats()

        if progress_callback:
            progress_callback("info", f"开始获取共享文件列表，共 {len(shared_paths)} 个入口")

        for index, path in enumerate(shared_paths, 1):
            if path.is_dir:
                folder_name = os.path.basename(path.path.rstrip("/"))
                if should_exclude_folder(folder_name, exclude_folder_filter):
                    stats["skipped_dirs"] += 1
                elif should_include_folder(folder_name, folder_filter):
                    if progress_callback:
                        progress_callback(
                            "info",
                            f"开始扫描共享入口 {index}/{len(shared_paths)}: {path.path}",
                        )
                    yield from self.iter_shared_dir_files(
                        path,
                        uk,
                        share_id,
                        bdstoken,
                        folder_filter,
                        shared_root,
                        progress_callback,
                        stats,
                        exclude_folder_filter=exclude_folder_filter,
                    )
                else:
                    stats["skipped_dirs"] += 1
                continue

            file_info = self._normalize_shared_file_info(path, shared_root)
            if file_info:
                stats["files"] += 1
                if self._should_report_progress(stats):
                    self._report_scan_progress(progress_callback, stats)
                yield file_info

        if progress_callback:
            progress_callback(
                "info",
                f"共享文件列表获取完成：扫描 {stats['dirs']} 个目录 / {stats['pages']} 页，"
                f"发现 {stats['files']} 个文件，跳过 {stats['skipped_dirs']} 个目录",
            )

    def list_shared_files(
        self,
        shared_paths,
        folder_filter=None,
        progress_callback=None,
        exclude_folder_filter=None,
    ):
        return list(
            self.iter_shared_files(
                shared_paths,
                folder_filter,
                progress_callback,
                exclude_folder_filter=exclude_folder_filter,
            )
        )
