"""Exercise real ingestion against byte-backed storage and a simulated HTTP API."""

import io
import json
import unittest
from collections import Counter
from datetime import date, timedelta
from unittest.mock import patch

import pendulum
import requests
from botocore.exceptions import ClientError, EndpointConnectionError

from ingestion.batch import openmeteo_weather as weather
from ingestion.config import Settings


def dates_between(start, end):
    start, end = date.fromisoformat(str(start)), date.fromisoformat(str(end))
    return [(start + timedelta(days=i)).isoformat()
            for i in range((end - start).days + 1)]


def payload(start, end, value=1.0):
    dates = dates_between(start, end)
    return {"daily": {"time": dates, **{
        metric: [value] * len(dates) for metric in weather.WEATHER_METRICS
    }}}


class MemoryStorage:
    """Store serialized bytes, with one-shot failures before/after a chosen PUT."""

    def __init__(self):
        self.objects = {}
        self.counts = Counter()
        self.failure = None
        self.read_error = None

    def get_object(self, *, Bucket, Key):
        if self.read_error:
            raise self.read_error
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, *, Bucket, Key, Body, ContentType):
        kind = "state" if Key == weather.STATE_KEY else "raw"
        self.counts[kind] += 1
        fail = self.failure and self.failure[:2] == (kind, self.counts[kind])
        after = fail and self.failure[2] == "after"
        if not fail or after:
            self.objects[Key] = bytes(Body)
        if fail:
            self.failure = None
            raise EndpointConnectionError(endpoint_url="http://test.invalid")

    def state(self):
        raw = self.objects.get(weather.STATE_KEY)
        return json.loads(raw) if raw else {"regions": {}}


class WeatherRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.storage = MemoryStorage()
        self.regions = [weather.Region("JP-TOKYO", "Tokyo", "Tokyo", 35.68, 139.69)]
        self.settings = Settings("http://test.invalid", "test", "test", "raw-batch")
        self.requests = []

    def run_ingestion(self, today, value=1.0, transform=None, api_error=None):
        def respond(**kwargs):
            params = kwargs["params"]
            self.requests.append(dict(params))
            if api_error:
                raise api_error
            data = payload(params["start_date"], params["end_date"], value)
            if transform:
                transform(data)
            response = requests.Response()
            response.status_code = 200
            response._content = json.dumps(data).encode()
            return response

        with (
            patch.object(weather, "create_minio_client", return_value=self.storage),
            patch.object(weather, "load_regions", return_value=self.regions),
            patch.object(weather.pendulum, "today", return_value=today) as clock,
            patch.object(weather.requests.Session, "get", side_effect=respond),
        ):
            result = weather.run_weather_ingestion(self.settings)
            clock.assert_called_once_with("Asia/Tokyo")
            return result

    def assert_history(self, end):
        expected = Counter(dates_between("2024-01-01", end))
        for region in self.regions:
            observed = Counter()
            for key, raw in self.storage.objects.items():
                if key.startswith(f"weather/{region.region_code}/"):
                    self.assertRegex(key, r"^weather/[^/]+/\d{4}/\d{2}/\d{4}-\d{2}-\d{2}\.json$")
                    data = json.loads(raw)["daily"]
                    observed.update(data["time"])
                    for metric in weather.WEATHER_METRICS:
                        self.assertEqual(len(data[metric]), len(data["time"]))
            self.assertEqual(observed, expected)  # Multiplicities catch duplicates too.
            progress = self.storage.state()["regions"][region.region_code]
            self.assertEqual(progress["last_successful_date"], end)
            last = json.loads(self.storage.objects[progress["last_object_key"]])
            self.assertEqual(last["daily"]["time"][-1], end)

    def test_all_write_failure_points_retry_days_later(self):
        for kind, timing in [("raw", "before"), ("raw", "after"),
                             ("state", "before"), ("state", "after")]:
            with self.subTest(kind=kind, timing=timing):
                self.storage = MemoryStorage()
                self.requests = []
                self.storage.failure = (kind, 1, timing)
                with self.assertRaises(RuntimeError):
                    self.run_ingestion(pendulum.datetime(2024, 1, 4, tz="Asia/Tokyo"))
                committed = kind == "state" and timing == "after"
                self.assertEqual(bool(self.storage.state()["regions"]), committed)
                raw_key = "weather/JP-TOKYO/2024/01/2024-01-01.json"
                self.assertEqual(raw_key in self.storage.objects,
                                 kind == "state" or timing == "after")
                self.run_ingestion(pendulum.datetime(2024, 1, 7, tz="Asia/Tokyo"), value=2.0)
                self.assert_history("2024-01-06")
                self.assertEqual(self.requests[-1]["start_date"],
                                 "2024-01-04" if committed else "2024-01-01")
                original = json.loads(self.storage.objects[raw_key])["daily"]
                self.assertEqual(original["temperature_2m_mean"],
                                 [1.0] * 3 if committed else [2.0] * 6)

    def test_retry_crosses_month_boundary(self):
        self.storage.failure = ("state", 1, "before")
        with self.assertRaises(RuntimeError):
            self.run_ingestion(pendulum.datetime(2024, 1, 30, tz="Asia/Tokyo"))
        self.run_ingestion(pendulum.datetime(2024, 2, 4, tz="Asia/Tokyo"), value=2.0)
        self.assert_history("2024-02-03")
        self.assertEqual(self.storage.counts["raw"], 3)

    def test_resume_after_several_committed_months(self):
        self.storage.failure = ("state", 3, "before")
        with self.assertRaises(RuntimeError):
            self.run_ingestion(pendulum.datetime(2024, 4, 4, tz="Asia/Tokyo"))
        self.assertEqual(self.storage.state()["regions"]["JP-TOKYO"]["last_successful_date"],
                         "2024-02-29")
        january = self.storage.objects["weather/JP-TOKYO/2024/01/2024-01-01.json"]
        before_retry = len(self.requests)
        self.run_ingestion(pendulum.datetime(2024, 4, 7, tz="Asia/Tokyo"), value=2.0)
        self.assertEqual(self.requests[before_retry]["start_date"], "2024-03-01")
        self.assertEqual(self.storage.objects["weather/JP-TOKYO/2024/01/2024-01-01.json"], january)
        self.assert_history("2024-04-06")

    def test_multiple_regions_share_one_attempt_end_date(self):
        self.regions = weather.load_regions()
        self.run_ingestion(pendulum.datetime(2024, 2, 3, tz="Asia/Tokyo"))
        self.assert_history("2024-02-02")

    def test_invalid_responses_do_not_advance_progress(self):
        mutations = {
            "missing_date": lambda d: d["daily"]["time"].pop(),
            "duplicate_date": lambda d: d["daily"]["time"].__setitem__(1, "2024-01-01"),
            "reordered_dates": lambda d: d["daily"]["time"].reverse(),
            "short_metric": lambda d: d["daily"][weather.WEATHER_METRICS[0]].pop(),
            "missing_metric": lambda d: d["daily"].pop(weather.WEATHER_METRICS[0]),
            "null_metric": lambda d: d["daily"][weather.WEATHER_METRICS[0]].__setitem__(0, None),
        }
        for name, mutation in mutations.items():
            with self.subTest(name=name):
                self.storage = MemoryStorage()
                with self.assertRaises(ValueError):
                    self.run_ingestion(pendulum.datetime(2024, 1, 4), transform=mutation)
                self.assertEqual(self.storage.objects, {})
                self.run_ingestion(pendulum.datetime(2024, 1, 7))
                self.assert_history("2024-01-06")

    def test_api_failure_preserves_existing_progress(self):
        self.run_ingestion(pendulum.datetime(2024, 1, 4))
        before = dict(self.storage.objects)
        with self.assertRaises(RuntimeError):
            self.run_ingestion(pendulum.datetime(2024, 1, 7), api_error=requests.Timeout())
        self.assertEqual(self.storage.objects, before)
        self.run_ingestion(pendulum.datetime(2024, 1, 9))
        self.assert_history("2024-01-08")

    def test_state_read_errors_do_not_start_ingestion(self):
        for error in [EndpointConnectionError(endpoint_url="http://test.invalid"),
                      ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject"),
                      ClientError({"Error": {"Code": "NoSuchBucket"}}, "GetObject")]:
            with self.subTest(error=type(error).__name__, detail=str(error)):
                self.storage.read_error = error
                with self.assertRaises(RuntimeError):
                    self.run_ingestion(pendulum.datetime(2024, 1, 4))
                self.assertEqual(self.requests, [])
                self.assertEqual(self.storage.objects, {})

    def test_corrupt_state_is_rejected_without_writes(self):
        base = {"version": weather.STATE_VERSION, "source": weather.SOURCE_NAME,
                "model": weather.WEATHER_MODEL, "regions": {}}
        invalid = [b"{broken", b"\xff", json.dumps({**base, "regions": None}).encode()]
        for value in [None, {}, {"last_successful_date": "bad-date"}]:
            invalid.append(json.dumps({**base, "regions": {"JP-TOKYO": value}}).encode())
        for raw in invalid:
            with self.subTest(raw=raw):
                self.storage.objects = {weather.STATE_KEY: raw}
                with self.assertRaises(ValueError):
                    self.run_ingestion(pendulum.datetime(2024, 1, 4))
                self.assertEqual(self.storage.objects, {weather.STATE_KEY: raw})
                self.assertEqual(self.requests, [])


if __name__ == "__main__":
    unittest.main()
