import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from config_utils import share_config_state_key
from mirror_state import load_mirror_state, reconcile_mirror_deletions


class MirrorStateTests(unittest.TestCase):
    def setUp(self):
        self.config_item = {
            "share_url": "https://pan.baidu.com/s/abc12345",
            "save_dir": "/dest",
        }
        self.config = {"mirror_delete": "true", "share_configs": [self.config_item]}
        self.state_key = share_config_state_key(self.config_item)
        self.storage = Mock()
        self.storage.path_service.normalize_path.side_effect = lambda value, **_: str(value).strip(
            "/"
        )
        self.storage._scan_local_files_dict.return_value = {"old.txt": "same-md5"}
        self.logger = Mock()

    def _result(self, files):
        return {
            "success": True,
            "results": [
                {
                    "success": True,
                    "state_key": self.state_key,
                    "save_dir": "/dest",
                    "source_snapshot_complete": True,
                    "source_files": files,
                }
            ],
        }

    def test_missing_file_requires_two_complete_absent_snapshots(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "mirror.json"
            env = {
                "GITHUB_ACTIONS": "false",
                "TRANSFERSHARE_MIRROR_DELETE_MAX_FILES": "100",
            }
            with patch.dict(os.environ, env, clear=False):
                first = reconcile_mirror_deletions(
                    self.storage,
                    self.logger,
                    self.config,
                    self._result(
                        [
                            {"path": "old.txt", "md5": "same-md5"},
                            {"path": "keep.txt", "md5": "keep-md5"},
                        ]
                    ),
                    state_path,
                )
                second = reconcile_mirror_deletions(
                    self.storage,
                    self.logger,
                    self.config,
                    self._result([{"path": "keep.txt", "md5": "keep-md5"}]),
                    state_path,
                )
                third = reconcile_mirror_deletions(
                    self.storage,
                    self.logger,
                    self.config,
                    self._result([{"path": "keep.txt", "md5": "keep-md5"}]),
                    state_path,
                )

            self.assertEqual(0, first["deleted"])
            self.assertEqual(1, second["pending"])
            self.assertEqual(1, third["deleted"])
            self.storage.client.remove.assert_called_once_with("dest/old.txt")
            state = load_mirror_state(state_path)
            self.assertEqual(
                {"keep.txt": {"md5": "keep-md5", "missing_count": 0}},
                state["subscriptions"][self.state_key]["files"],
            )

    def test_changed_destination_file_is_never_deleted(self):
        self.storage._scan_local_files_dict.return_value = {"old.txt": "user-changed-md5"}
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "mirror.json"
            with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=False):
                reconcile_mirror_deletions(
                    self.storage,
                    self.logger,
                    self.config,
                    self._result(
                        [
                            {"path": "old.txt", "md5": "same-md5"},
                            {"path": "keep.txt", "md5": "keep-md5"},
                        ]
                    ),
                    state_path,
                )
                reconcile_mirror_deletions(
                    self.storage,
                    self.logger,
                    self.config,
                    self._result([{"path": "keep.txt", "md5": "keep-md5"}]),
                    state_path,
                )
                reconcile_mirror_deletions(
                    self.storage,
                    self.logger,
                    self.config,
                    self._result([{"path": "keep.txt", "md5": "keep-md5"}]),
                    state_path,
                )

            self.storage.client.remove.assert_not_called()

    def test_incomplete_or_failed_result_does_not_change_state(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "mirror.json"
            with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=False):
                summary = reconcile_mirror_deletions(
                    self.storage,
                    self.logger,
                    self.config,
                    {"success": False, "results": [{"success": False}]},
                    state_path,
                )

            self.assertEqual(0, summary["deleted"])
            self.assertFalse(state_path.exists())
            self.storage.client.remove.assert_not_called()


if __name__ == "__main__":
    unittest.main()
