import hashlib
import unittest
from unittest.mock import patch

import pendulum
import requests
from botocore.exceptions import ClientError

from ingestion.batch.jma_sakura import (
    ARTIFACT_PREFIX,
    ARTIFACTS,
    STATE_SOURCE,
    STATE_VERSION,
    Artifact,
    check_artifact_upload,
    download_artifact,
    get_state,
    run_jma_sakura_ingestion,
    update_artifact_state,
    validate_artifact,
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


def csv_content(title: str, value: str = "324") -> bytes:
    return (
        f"4,{title}\r\n"
        f"番号,地点名,2025,rm\r\n"
        f"662,東京,{value},8\r\n"
    ).encode("shift_jis")


class MissingStateClient:
    def get_object(self, **_kwargs):
        raise ClientError(
            {"Error": {"Code": "NoSuchKey"}},
            "GetObject",
        )


class SuccessfulResponse:
    def __init__(
        self,
        content: bytes,
        content_type: str = "text/csv; charset=Shift_JIS",
    ) -> None:
        self.content = content
        self.headers = {"Content-Type": content_type}

    def raise_for_status(self) -> None:
        return None


class RecordingSession:
    def __init__(self, response: SuccessfulResponse) -> None:
        self.response = response
        self.calls: list[dict] = []

    def get(self, **kwargs) -> SuccessfulResponse:
        self.calls.append(kwargs)
        return self.response


class FailingSession:
    def get(self, **_kwargs):
        raise requests.ConnectionError("connection failed")


class JmaSakuraTests(unittest.TestCase):
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

    def test_download_artifact_returns_original_bytes(self) -> None:
        content = csv_content("さくらの開花")
        session = RecordingSession(SuccessfulResponse(content))

        result = download_artifact(
            session,
            "https://example.com/flowering.csv",
        )

        self.assertEqual(result, content)
        self.assertEqual(
            session.calls,
            [
                {
                    "url": "https://example.com/flowering.csv",
                    "timeout": (3.05, 27),
                }
            ],
        )

    def test_download_artifact_wraps_request_error(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Не удалось скачать CSV"):
            download_artifact(
                FailingSession(),
                "https://example.com/flowering.csv",
            )

    def test_download_artifact_rejects_non_csv_response(self) -> None:
        session = RecordingSession(
            SuccessfulResponse(b"<html></html>", "text/html"),
        )

        with self.assertRaisesRegex(ValueError, "не в формате CSV"):
            download_artifact(
                session,
                "https://example.com/flowering.csv",
            )

    def test_download_artifact_rejects_empty_response(self) -> None:
        session = RecordingSession(SuccessfulResponse(b" \r\n"))

        with self.assertRaisesRegex(ValueError, "пустой CSV"):
            download_artifact(
                session,
                "https://example.com/flowering.csv",
            )

    def test_validate_artifact_accepts_expected_structure(self) -> None:
        artifact = ARTIFACTS[0]

        validate_artifact(
            artifact,
            csv_content(artifact.expected_title),
        )

    def test_validate_artifact_rejects_invalid_encoding(self) -> None:
        with self.assertRaisesRegex(ValueError, "Shift_JIS"):
            validate_artifact(ARTIFACTS[0], b"\x81")

    def test_validate_artifact_rejects_unexpected_title(self) -> None:
        with self.assertRaisesRegex(ValueError, "ожидался показатель"):
            validate_artifact(
                ARTIFACTS[0],
                csv_content("さくらの満開"),
            )

    def test_validate_artifact_rejects_missing_data_rows(self) -> None:
        content = (
            "4,さくらの開花\r\n"
            "番号,地点名,2025,rm\r\n"
        ).encode("shift_jis")

        with self.assertRaisesRegex(ValueError, "строк с наблюдениями"):
            validate_artifact(ARTIFACTS[0], content)

    def test_validate_artifact_rejects_missing_year_columns(self) -> None:
        content = (
            "4,さくらの開花\r\n"
            "番号,地点名\r\n"
            "662,東京\r\n"
        ).encode("shift_jis")

        with self.assertRaisesRegex(ValueError, "столбцы с годами"):
            validate_artifact(ARTIFACTS[0], content)

    def test_new_changed_and_unchanged_artifacts(self) -> None:
        artifact = ARTIFACTS[0]
        state = initial_state()

        self.assertTrue(check_artifact_upload(state, artifact, "new-sha"))

        state["artifacts"][artifact.logical_key] = {
            "sha256": "saved-sha"
        }
        self.assertFalse(
            check_artifact_upload(state, artifact, "saved-sha")
        )
        self.assertTrue(
            check_artifact_upload(state, artifact, "changed-sha")
        )

    def test_check_artifact_upload_rejects_invalid_saved_state(self) -> None:
        artifact = ARTIFACTS[0]
        state = initial_state()
        state["artifacts"][artifact.logical_key] = []

        with self.assertRaisesRegex(ValueError, "Некорректный state"):
            check_artifact_upload(state, artifact, "new-sha")

    @patch(
        "ingestion.batch.jma_sakura.pendulum.now",
        return_value=pendulum.datetime(2026, 8, 11, tz="UTC"),
    )
    def test_update_artifact_state_records_uploaded_version(
        self,
        _mock_now,
    ) -> None:
        state = initial_state()
        artifact = ARTIFACTS[0]

        update_artifact_state(
            state,
            artifact,
            "events/sakura/jma/flowering/abc.csv",
            "abc",
        )

        self.assertEqual(
            state["artifacts"][artifact.logical_key],
            {
                "sha256": "abc",
                "source_url": artifact.source_url,
                "object_key": "events/sakura/jma/flowering/abc.csv",
                "updated_at": "2026-08-11T00:00:00Z",
            },
        )

    @patch(
        "ingestion.batch.jma_sakura.pendulum.now",
        return_value=pendulum.datetime(2026, 8, 11, tz="UTC"),
    )
    @patch("ingestion.batch.jma_sakura.upload_json_to_minio")
    @patch("ingestion.batch.jma_sakura.upload_bytes_to_minio")
    @patch("ingestion.batch.jma_sakura.download_artifact")
    @patch("ingestion.batch.jma_sakura.get_state")
    @patch("ingestion.batch.jma_sakura.create_minio_client")
    def test_complete_run_uploads_changed_and_skips_unchanged(
        self,
        mock_create_client,
        mock_get_state,
        mock_download,
        mock_upload_bytes,
        mock_upload_json,
        _mock_now,
    ) -> None:
        flowering_content = csv_content("さくらの開花", "324")
        full_bloom_content = csv_content("さくらの満開", "330")
        flowering_sha = hashlib.sha256(flowering_content).hexdigest()
        full_bloom_sha = hashlib.sha256(full_bloom_content).hexdigest()
        state = initial_state()
        state["artifacts"]["flowering"] = {
            "sha256": flowering_sha
        }

        mock_get_state.return_value = state
        mock_download.side_effect = [
            flowering_content,
            full_bloom_content,
        ]
        settings = Settings(
            "http://minio:9000",
            "user",
            "password",
            "raw-batch",
        )

        summary = run_jma_sakura_ingestion(settings)

        self.assertEqual(
            summary,
            {
                "discovered_objects": 2,
                "uploaded_objects": 1,
                "skipped_objects": 1,
            },
        )
        mock_create_client.assert_called_once_with(settings=settings)
        mock_upload_bytes.assert_called_once_with(
            client=mock_create_client.return_value,
            bucket="raw-batch",
            object_key=(
                f"{ARTIFACT_PREFIX}/full_bloom/{full_bloom_sha}.csv"
            ),
            data=full_bloom_content,
            content_type="text/csv; charset=shift_jis",
        )
        self.assertEqual(mock_upload_json.call_count, 2)
        self.assertEqual(
            state["last_checked_at"],
            "2026-08-11T00:00:00Z",
        )
        self.assertIn("full_bloom", state["artifacts"])


if __name__ == "__main__":
    unittest.main()
