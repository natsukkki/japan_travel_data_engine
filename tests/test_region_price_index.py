import hashlib
import unittest
from unittest.mock import Mock, patch

import pendulum
from botocore.exceptions import ClientError

from ingestion.batch.region_price_index import (
    ARTIFACT_PREFIX,
    STATE_SOURCE,
    STATE_VERSION,
    XLS_MAGIC,
    Artifact,
    YearPage,
    check_artifact_upload,
    discover_artifact,
    discover_year_pages,
    download_artifact,
    get_state,
    run_region_price_index_ingestion,
    update_artifact_state,
    validate_state,
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


class SuccessfulResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


class RecordingSession:
    def __init__(self, responses: list[SuccessfulResponse]) -> None:
        self.responses = responses
        self.calls: list[dict] = []

    def get(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0)


class RegionPriceIndexTests(unittest.TestCase):
    def test_missing_state_returns_initial_state(self) -> None:
        self.assertEqual(
            get_state(MissingStateClient(), "raw-batch"),
            initial_state(),
        )

    def test_validate_state_rejects_invalid_artifacts_container(self) -> None:
        state = initial_state()
        state["artifacts"] = []

        with self.assertRaisesRegex(ValueError, "artifacts"):
            validate_state(state)

    def test_discover_year_pages_uses_query_parameter_not_link_text(self) -> None:
        html = """
        <a href="/en/stat-search/files?layout=datalist&amp;cycle=7&amp;toukei=00200571&amp;tstat=000001067253&amp;year=20240"> </a>
        <a href="/en/stat-search/files?layout=datalist&amp;cycle=7&amp;toukei=00200571&amp;tstat=000001067253&amp;year=20230">2023</a>
        <a href="/en/stat-search/files?layout=dataset&amp;cycle=7&amp;toukei=00200571&amp;tstat=000001067253&amp;year=20250">2025</a>
        """

        pages = discover_year_pages(html)

        self.assertEqual([page.year for page in pages], [2024])

    def test_discover_year_pages_rejects_empty_result(self) -> None:
        with self.assertRaisesRegex(ValueError, "не найдены публикации"):
            discover_year_pages("<html></html>")

    def test_discover_artifact_scopes_excel_to_target_table(self) -> None:
        html = """
        <article>
          <a href="/en/stat-search/files?stat_infid=wrong">Other table</a>
          <a href="/en/stat-search/file-download?statInfId=wrong&amp;fileKind=4"
             data-file_type="EXCEL_Report"><span>EXCEL<br>Report</span></a>
        </article>
        <article>
          <a href="/en/stat-search/files?layout=datalist&amp;stat_infid=target">
            Regional Difference Index of Consumer Prices by Ten Major Groups
            (All Japan = 100) - Japan, Districts, Prefectures, Capital cities
            and ordinance-designated cities
          </a>
          <a href="/en/stat-search/file-download?statInfId=target&amp;fileKind=4"
             data-file_type="EXCEL_Report"><span>EXCEL<br>Report</span></a>
        </article>
        """
        year_page = YearPage(2024, "https://www.e-stat.go.jp/en/year")

        artifact = discover_artifact(html, year_page)

        self.assertEqual(artifact.year, 2024)
        self.assertEqual(
            artifact.source_url,
            "https://www.e-stat.go.jp/en/stat-search/"
            "file-download?statInfId=target&fileKind=4",
        )

    def test_discover_artifact_reports_missing_target_table(self) -> None:
        year_page = YearPage(2024, "https://www.e-stat.go.jp/en/year")

        with self.assertRaisesRegex(ValueError, "найдено: 0"):
            discover_artifact("<html></html>", year_page)

    def test_download_artifact_accepts_real_xls_signature(self) -> None:
        content = XLS_MAGIC + b"-content"
        session = RecordingSession([SuccessfulResponse(content)])
        artifact = Artifact(2024, "https://example.com/file.xls")

        result = download_artifact(session, artifact)

        self.assertEqual(result, content)
        self.assertEqual(session.calls[0]["url"], artifact.source_url)
        self.assertEqual(session.calls[0]["timeout"], (10, 120))

    def test_download_artifact_rejects_non_xls_content(self) -> None:
        session = RecordingSession([SuccessfulResponse(b"<html>Error</html>")])
        artifact = Artifact(2024, "https://example.com/file.xls")

        with self.assertRaisesRegex(ValueError, "не является XLS"):
            download_artifact(session, artifact)

    def test_new_changed_and_unchanged_artifacts(self) -> None:
        artifact = Artifact(2024, "https://example.com/file.xls")
        state = initial_state()

        self.assertTrue(check_artifact_upload(state, artifact, "new-sha"))
        state["artifacts"]["2024"] = {"sha256": "saved-sha"}
        self.assertFalse(check_artifact_upload(state, artifact, "saved-sha"))
        self.assertTrue(check_artifact_upload(state, artifact, "changed-sha"))

    @patch(
        "ingestion.batch.region_price_index.pendulum.now",
        return_value=pendulum.datetime(2026, 8, 9, tz="UTC"),
    )
    def test_update_artifact_state_records_uploaded_version(self, _mock_now) -> None:
        state = initial_state()
        artifact = Artifact(2024, "https://example.com/file.xls")

        update_artifact_state(
            state,
            artifact,
            "prices/regional_price_index/2024/abc.xls",
            "abc",
        )

        self.assertEqual(
            state["artifacts"]["2024"],
            {
                "sha256": "abc",
                "source_url": "https://example.com/file.xls",
                "object_key": "prices/regional_price_index/2024/abc.xls",
                "updated_at": "2026-08-09T00:00:00Z",
            },
        )

    @patch(
        "ingestion.batch.region_price_index.pendulum.now",
        return_value=pendulum.datetime(2026, 8, 9, tz="UTC"),
    )
    @patch("ingestion.batch.region_price_index.upload_json_to_minio")
    @patch("ingestion.batch.region_price_index.upload_bytes_to_minio")
    @patch("ingestion.batch.region_price_index.download_artifact")
    @patch("ingestion.batch.region_price_index.discover_artifact")
    @patch("ingestion.batch.region_price_index.discover_year_pages")
    @patch("ingestion.batch.region_price_index.fetch_source_page")
    @patch("ingestion.batch.region_price_index.get_state")
    @patch("ingestion.batch.region_price_index.create_minio_client")
    def test_complete_run_uploads_changed_and_skips_unchanged(
        self,
        mock_create_client,
        mock_get_state,
        mock_fetch_page,
        mock_discover_years,
        mock_discover_artifact,
        mock_download,
        mock_upload_bytes,
        mock_upload_json,
        _mock_now,
    ) -> None:
        unchanged = Artifact(2024, "https://example.com/2024.xls")
        changed = Artifact(2025, "https://example.com/2025.xls")
        unchanged_content = XLS_MAGIC + b"-unchanged"
        changed_content = XLS_MAGIC + b"-changed"
        unchanged_sha = hashlib.sha256(unchanged_content).hexdigest()
        changed_sha = hashlib.sha256(changed_content).hexdigest()
        state = initial_state()
        state["artifacts"]["2024"] = {"sha256": unchanged_sha}

        mock_get_state.return_value = state
        mock_fetch_page.return_value = "<html></html>"
        mock_discover_years.return_value = [
            YearPage(2024, "https://example.com/year/2024"),
            YearPage(2025, "https://example.com/year/2025"),
        ]
        mock_discover_artifact.side_effect = [unchanged, changed]
        mock_download.side_effect = [unchanged_content, changed_content]
        settings = Settings("http://minio:9000", "user", "password", "raw-batch")

        summary = run_region_price_index_ingestion(settings)

        self.assertEqual(
            summary,
            {
                "discovered_objects": 2,
                "uploaded_objects": 1,
                "skipped_objects": 1,
            },
        )
        mock_create_client.assert_called_once_with(settings)
        mock_upload_bytes.assert_called_once_with(
            client=mock_create_client.return_value,
            bucket="raw-batch",
            object_key=(
                f"{ARTIFACT_PREFIX}/2025/{changed_sha}.xls"
            ),
            data=changed_content,
            content_type="application/vnd.ms-excel",
        )
        self.assertEqual(mock_upload_json.call_count, 2)
        self.assertEqual(state["last_checked_at"], "2026-08-09T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
