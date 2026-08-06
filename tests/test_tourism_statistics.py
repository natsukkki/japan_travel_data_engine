import hashlib
import unittest
from unittest.mock import Mock, patch

import pendulum
from botocore.exceptions import ClientError

from ingestion.batch.tourism_statistics import (
    ARTIFACT_PREFIX,
    STATE_SOURCE,
    STATE_VERSION,
    Artifact,
    check_artifacts_upload,
    discover_artifacts,
    get_artifact_key,
    get_state,
    run_tourism_statistics_ingestion,
    update_artifact_state,
)
from ingestion.config import Settings


def initial_state() -> dict:
    return {
        "version": STATE_VERSION,
        "source": STATE_SOURCE,
        "last_checked_at": None,
        "artifacts": {},
    }


class MissingStateClient:
    def get_object(self, **_kwargs):
        raise ClientError(
            {"Error": {"Code": "NoSuchKey"}},
            "GetObject",
        )


class TourismStatisticsTests(unittest.TestCase):
    def test_discover_artifacts_keeps_only_required_publications(self) -> None:
        html = """
        <html><body>
          <a href="/files/final-2024.xlsx">2024年 確定値 集計結果</a>
          <a href="/files/second-2026-5.xlsx">2026年5月 第2次速報 集計結果</a>
          <a href="/files/first-2026-5.xlsx">2026年5月 第1次速報 集計結果</a>
          <a href="/files/final-2023.xlsx">2023年 確定値 集計結果</a>
          <a href="/files/final-2025.pdf">2025年 確定値 集計結果</a>
          <a href="/files/other.xlsx">2025年 確定値 概要</a>
        </body></html>
        """

        artifacts = discover_artifacts(html)

        self.assertEqual(
            [artifact.logical_key for artifact in artifacts],
            ["final:2024", "second_preliminary:2026-05"],
        )
        self.assertTrue(artifacts[0].source_url.endswith("/files/final-2024.xlsx"))

    def test_discover_artifacts_rejects_conflicting_links(self) -> None:
        html = """
        <a href="/files/a.xlsx">2024年 確定値 集計結果</a>
        <a href="/files/b.xlsx">2024年 確定値 集計結果</a>
        """

        with self.assertRaisesRegex(ValueError, "разные Excel-файлы"):
            discover_artifacts(html)

    def test_missing_state_returns_initial_state(self) -> None:
        self.assertEqual(
            get_state(MissingStateClient(), "raw-batch"),
            initial_state(),
        )

    def test_new_changed_and_unchanged_artifacts(self) -> None:
        artifact = Artifact(
            logical_key="final:2024",
            release_type="final",
            year=2024,
            month=None,
            source_url="https://example.com/final.xlsx",
        )
        state = initial_state()

        self.assertTrue(check_artifacts_upload(state, artifact, "new-sha"))

        state["artifacts"][artifact.logical_key] = {"sha256": "saved-sha"}
        self.assertFalse(check_artifacts_upload(state, artifact, "saved-sha"))
        self.assertTrue(check_artifacts_upload(state, artifact, "changed-sha"))

    def test_artifact_keys_include_release_period_and_checksum(self) -> None:
        final = Artifact("final:2024", "final", 2024, None, "https://example.com/a")
        preliminary = Artifact(
            "second_preliminary:2026-05",
            "second_preliminary",
            2026,
            5,
            "https://example.com/b",
        )

        self.assertEqual(
            get_artifact_key(final, "abc"),
            f"{ARTIFACT_PREFIX}/final/2024/abc.xlsx",
        )
        self.assertEqual(
            get_artifact_key(preliminary, "def"),
            f"{ARTIFACT_PREFIX}/second_preliminary/2026/05/def.xlsx",
        )

    @patch(
        "ingestion.batch.tourism_statistics.pendulum.now",
        return_value=pendulum.datetime(2026, 8, 6, tz="UTC"),
    )
    def test_update_artifact_state_records_uploaded_version(self, _mock_now) -> None:
        state = initial_state()
        artifact = Artifact(
            "final:2024",
            "final",
            2024,
            None,
            "https://example.com/final.xlsx",
        )

        update_artifact_state(state, artifact, "raw/final.xlsx", "abc")

        self.assertEqual(
            state["artifacts"]["final:2024"],
            {
                "sha256": "abc",
                "source_url": "https://example.com/final.xlsx",
                "object_key": "raw/final.xlsx",
                "updated_at": "2026-08-06T00:00:00Z",
            },
        )

    @patch("ingestion.batch.tourism_statistics.upload_json_to_minio")
    @patch("ingestion.batch.tourism_statistics.upload_bytes_to_minio")
    @patch("ingestion.batch.tourism_statistics.download_artifact")
    @patch("ingestion.batch.tourism_statistics.discover_artifacts")
    @patch("ingestion.batch.tourism_statistics.fetch_source_page", return_value="<html></html>")
    @patch("ingestion.batch.tourism_statistics.get_state")
    @patch("ingestion.batch.tourism_statistics.create_minio_client")
    def test_complete_run_uploads_changed_and_skips_unchanged(
        self,
        mock_create_client,
        mock_get_state,
        _mock_fetch_page,
        mock_discover,
        mock_download,
        mock_upload_bytes,
        mock_upload_json,
    ) -> None:
        unchanged = Artifact(
            "final:2024",
            "final",
            2024,
            None,
            "https://example.com/final-2024.xlsx",
        )
        changed = Artifact(
            "second_preliminary:2026-05",
            "second_preliminary",
            2026,
            5,
            "https://example.com/second-2026-05.xlsx",
        )
        unchanged_content = b"PK-unchanged"
        changed_content = b"PK-changed"
        state = initial_state()
        state["artifacts"][unchanged.logical_key] = {
            "sha256": hashlib.sha256(unchanged_content).hexdigest()
        }

        mock_get_state.return_value = state
        mock_discover.return_value = [unchanged, changed]
        mock_download.side_effect = [unchanged_content, changed_content]
        settings = Settings("http://minio:9000", "user", "password", "raw-batch")

        summary = run_tourism_statistics_ingestion(settings)

        self.assertEqual(
            summary,
            {
                "discovered_objects": 2,
                "uploaded_objects": 1,
                "skipped_objects": 1,
            },
        )
        mock_create_client.assert_called_once_with(settings)
        mock_upload_bytes.assert_called_once()
        self.assertEqual(mock_upload_bytes.call_args.args[2].split("/")[-1], hashlib.sha256(changed_content).hexdigest() + ".xlsx")
        self.assertEqual(mock_upload_json.call_count, 2)
        self.assertIn(changed.logical_key, state["artifacts"])


if __name__ == "__main__":
    unittest.main()
