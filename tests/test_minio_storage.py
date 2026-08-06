import json
import unittest

from ingestion.storage.minio import upload_bytes_to_minio, upload_json_to_minio


class RecordingClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def put_object(self, **kwargs) -> None:
        self.calls.append(kwargs)


class MinioStorageTests(unittest.TestCase):
    def test_upload_bytes_uses_content_type(self) -> None:
        client = RecordingClient()

        upload_bytes_to_minio(
            client,
            "raw-batch",
            "tourism/file.xlsx",
            b"PK-content",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0]["Body"], b"PK-content")
        self.assertEqual(
            client.calls[0]["ContentType"],
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def test_upload_json_serializes_unicode(self) -> None:
        client = RecordingClient()

        upload_json_to_minio(
            client,
            "raw-batch",
            "state/example.json",
            {"status": "загружено"},
        )

        self.assertEqual(
            json.loads(client.calls[0]["Body"]),
            {"status": "загружено"},
        )

    def test_upload_bytes_rejects_empty_data(self) -> None:
        with self.assertRaisesRegex(ValueError, "не содержит данные"):
            upload_bytes_to_minio(
                RecordingClient(),
                "raw-batch",
                "empty.xlsx",
                b"",
                "application/octet-stream",
            )


if __name__ == "__main__":
    unittest.main()
