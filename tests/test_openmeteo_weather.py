import json
import unittest
from unittest.mock import patch

import pendulum
from botocore.exceptions import ClientError

from ingestion.batch.openmeteo_weather import (
    STATE_KEY,
    SOURCE_NAME,
    STATE_VERSION,
    WEATHER_MODEL,
    WEATHER_METRICS,
    Region,
    determine_start_date,
    fetch_weather,
    get_state,
    split_date_range_by_month,
    update_region_state,
    validate_weather_response,
)
from ingestion.storage.minio import upload_json_to_minio


def initial_state() -> dict:
    return {
        "version": STATE_VERSION,
        "source": SOURCE_NAME,
        "model": WEATHER_MODEL,
        "regions": {},
    }


class MissingStateClient:
    def get_object(self, **_kwargs):
        raise ClientError(
            {"Error": {"Code": "NoSuchKey"}},
            "GetObject",
        )


class RecordingClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)


class SuccessfulResponse:
    def __init__(self, data: dict) -> None:
        self.data = data

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.data


class RecordingSession:
    def __init__(self, data: dict) -> None:
        self.data = data
        self.calls: list[dict] = []

    def get(self, **kwargs):
        self.calls.append(kwargs)
        return SuccessfulResponse(self.data)


class OpenMeteoWeatherTests(unittest.TestCase):
    def test_first_run_starts_at_initial_date(self) -> None:
        state = initial_state()

        start_date = determine_start_date(state, "JP-TOKYO")

        self.assertEqual(start_date, pendulum.date(2024, 1, 1))

    def test_existing_region_starts_on_next_day(self) -> None:
        state = initial_state()
        state["regions"]["JP-TOKYO"] = {
            "last_successful_date": "2026-07-31"
        }

        start_date = determine_start_date(state, "JP-TOKYO")

        self.assertEqual(start_date, pendulum.date(2026, 8, 1))

    def test_range_is_split_on_month_boundary(self) -> None:
        intervals = split_date_range_by_month(
            pendulum.date(2026, 7, 31),
            pendulum.date(2026, 8, 2),
        )

        self.assertEqual(
            intervals,
            [
                (
                    pendulum.date(2026, 7, 31),
                    pendulum.date(2026, 7, 31),
                ),
                (
                    pendulum.date(2026, 8, 1),
                    pendulum.date(2026, 8, 2),
                ),
            ],
        )

    def test_range_is_empty_when_there_are_no_new_dates(self) -> None:
        intervals = split_date_range_by_month(
            pendulum.date(2026, 8, 2),
            pendulum.date(2026, 8, 1),
        )

        self.assertEqual(intervals, [])

    def test_weather_response_with_all_dates_and_metrics_is_valid(self) -> None:
        dates = ["2026-07-30", "2026-07-31"]
        data = {
            "daily": {
                "time": dates,
                **{metric: [1.0, 2.0] for metric in WEATHER_METRICS},
            }
        }

        validate_weather_response(
            data,
            pendulum.date(2026, 7, 30),
            pendulum.date(2026, 7, 31),
        )

    def test_weather_response_rejects_a_missing_date(self) -> None:
        data = {
            "daily": {
                "time": ["2026-07-30"],
                **{metric: [1.0] for metric in WEATHER_METRICS},
            }
        }

        with self.assertRaisesRegex(ValueError, "Даты ответа"):
            validate_weather_response(
                data,
                pendulum.date(2026, 7, 30),
                pendulum.date(2026, 7, 31),
            )

    def test_weather_response_rejects_null_metric_values(self) -> None:
        data = {
            "daily": {
                "time": ["2026-07-30"],
                **{metric: [1.0] for metric in WEATHER_METRICS},
            }
        }
        data["daily"]["precipitation_sum"] = [None]

        with self.assertRaisesRegex(ValueError, "содержит null"):
            validate_weather_response(
                data,
                pendulum.date(2026, 7, 30),
                pendulum.date(2026, 7, 30),
            )

    def test_fetch_weather_sends_the_expected_api_parameters(self) -> None:
        response_data = {
            "daily": {
                "time": ["2026-07-30"],
                **{metric: [1.0] for metric in WEATHER_METRICS},
            }
        }
        session = RecordingSession(response_data)
        region = Region("JP-TOKYO", "Tokyo", "Tokyo", 35.68, 139.69)

        data = fetch_weather(
            session,
            region,
            pendulum.date(2026, 7, 30),
            pendulum.date(2026, 7, 30),
        )

        self.assertEqual(data, response_data)
        self.assertEqual(len(session.calls), 1)
        parameters = session.calls[0]["params"]
        self.assertEqual(parameters["models"], "ecmwf_ifs")
        self.assertEqual(parameters["timezone"], "Asia/Tokyo")
        self.assertEqual(parameters["wind_speed_unit"], "ms")
        self.assertEqual(parameters["daily"], WEATHER_METRICS)

    def test_missing_state_returns_initial_state(self) -> None:
        state = get_state(MissingStateClient(), "raw-batch")

        self.assertEqual(state, initial_state())

    def test_upload_serializes_json_and_uses_requested_key(self) -> None:
        client = RecordingClient()

        upload_json_to_minio(
            client,
            "raw-batch",
            STATE_KEY,
            {"source": "openmeteo_weather"},
        )

        self.assertEqual(len(client.calls), 1)
        call = client.calls[0]
        self.assertEqual(call["Bucket"], "raw-batch")
        self.assertEqual(call["Key"], STATE_KEY)
        self.assertEqual(call["ContentType"], "application/json")
        self.assertEqual(
            json.loads(call["Body"]),
            {"source": "openmeteo_weather"},
        )

    @patch(
        "ingestion.batch.openmeteo_weather.pendulum.now",
        return_value=pendulum.datetime(2026, 8, 3, tz="UTC"),
    )
    def test_region_state_is_created_and_replaced(self, _mock_now) -> None:
        state = initial_state()

        update_region_state(
            state,
            "JP-TOKYO",
            pendulum.date(2026, 7, 31),
            "weather/JP-TOKYO/2026/07/2026-07-01-2026-07-31.json",
        )
        update_region_state(
            state,
            "JP-TOKYO",
            pendulum.date(2026, 8, 2),
            "weather/JP-TOKYO/2026/08/2026-08-01-2026-08-02.json",
        )

        self.assertEqual(
            state["regions"]["JP-TOKYO"]["last_successful_date"],
            "2026-08-02",
        )
        self.assertEqual(
            state["regions"]["JP-TOKYO"]["last_object_key"],
            "weather/JP-TOKYO/2026/08/2026-08-01-2026-08-02.json",
        )


if __name__ == "__main__":
    unittest.main()
