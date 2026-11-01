import json
from io import BytesIO
import unittest
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.response import StreamingBody
from urllib3.exceptions import ProtocolError

from http_stream import make_http_stream

from storage.minio import (
    create_minio_client,
    load_bytes_from_minio,
    load_json_from_minio,
    upload_bytes_to_minio,
    upload_json_to_minio,
)


class RecordingClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def put_object(self, **kwargs) -> None:
        self.calls.append(kwargs)


class MinioStorageTests(unittest.TestCase):
    @patch("storage.minio.boto3.client")
    def test_create_client_accepts_explicit_connection_parameters(self, mock_client) -> None:
        result = create_minio_client(
            endpoint="http://minio:9000",
            access_key="test-user",
            secret_key="test-password",
        )

        self.assertIs(result, mock_client.return_value)
        mock_client.assert_called_once_with(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="test-user",
            aws_secret_access_key="test-password",
        )

    def test_load_bytes_preserves_binary_content_and_closes_stream(self) -> None:
        content = b"PK\x00\xff-excel-content"
        raw_stream = BytesIO(content)
        client = Mock()
        client.get_object.return_value = {
            "Body": StreamingBody(raw_stream, len(content))
        }

        result = load_bytes_from_minio(client, "raw-batch", "tourism/file.xlsx")

        self.assertEqual(result, content)
        self.assertTrue(raw_stream.closed)
        client.get_object.assert_called_once_with(
            Bucket="raw-batch", Key="tourism/file.xlsx"
        )

    def test_load_bytes_rejects_incomplete_download_and_closes_stream(self) -> None:
        raw_stream = make_http_stream(b"PK", content_length=10)
        client = Mock()
        client.get_object.return_value = {"Body": StreamingBody(raw_stream, 10)}

        with self.assertRaises(ProtocolError):
            load_bytes_from_minio(client, "raw-batch", "tourism/file.xlsx")

        self.assertTrue(raw_stream.closed)

    def test_missing_bytes_fail_but_missing_json_returns_none(self) -> None:
        for code in ("NoSuchKey", "NoSuchObject", "404"):
            with self.subTest(code=code):
                error = ClientError({"Error": {"Code": code}}, "GetObject")
                client = Mock()
                client.get_object.side_effect = error

                with self.assertRaises(RuntimeError) as caught:
                    load_bytes_from_minio(client, "raw-batch", "tourism/file.xlsx")

                self.assertIs(caught.exception.__cause__, error)
                self.assertIsNone(
                    load_json_from_minio(client, "raw-batch", "state/example.json")
                )

    def test_read_access_errors_are_not_treated_as_missing_objects(self) -> None:
        for read_object in (load_bytes_from_minio, load_json_from_minio):
            with self.subTest(reader=read_object.__name__):
                error = ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
                client = Mock()
                client.get_object.side_effect = error

                with self.assertRaises(RuntimeError) as caught:
                    read_object(client, "raw-batch", "documents/example.json")

                self.assertIs(caught.exception.__cause__, error)

    def test_connection_errors_fail_for_both_readers(self) -> None:
        for read_object in (load_bytes_from_minio, load_json_from_minio):
            with self.subTest(reader=read_object.__name__):
                error = EndpointConnectionError(endpoint_url="http://minio:9000")
                client = Mock()
                client.get_object.side_effect = error

                with self.assertRaises(RuntimeError) as caught:
                    read_object(client, "raw-batch", "documents/example.json")

                self.assertIs(caught.exception.__cause__, error)

    def test_load_json_deserializes_content_and_closes_stream(self) -> None:
        data = {"status": "загружено", "items": [1, 2]}
        content = json.dumps(data, ensure_ascii=False).encode("utf-8")
        body = BytesIO(content)
        client = Mock()
        client.get_object.return_value = {"Body": StreamingBody(body, len(content))}

        result = load_json_from_minio(client, "raw-batch", "documents/example.json")

        self.assertEqual(result, data)
        self.assertTrue(body.closed)

    def test_load_json_rejects_incomplete_download_and_closes_stream(self) -> None:
        body = make_http_stream(b"{}", content_length=10)
        client = Mock()
        client.get_object.return_value = {"Body": StreamingBody(body, 10)}

        with self.assertRaises(ProtocolError):
            load_json_from_minio(client, "raw-batch", "documents/example.json")

        self.assertTrue(body.closed)

    def test_invalid_json_fails_and_closes_stream(self) -> None:
        for content in (b"not json", b"\xff"):
            with self.subTest(content=content):
                body = BytesIO(content)
                client = Mock()
                client.get_object.return_value = {
                    "Body": StreamingBody(body, len(content))
                }

                with self.assertRaises(ValueError):
                    load_json_from_minio(client, "raw-batch", "documents/example.json")

                self.assertTrue(body.closed)

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
