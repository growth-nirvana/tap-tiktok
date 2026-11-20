"""Daily reports: date windows, paging, chunked metrics, TikTok's error codes."""

import datetime
import json
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_tiktok.client import tiktok_failure
from tap_tiktok.streams import (
    AdsBasicDataMetricsByDayStream,
    AdsInAppEventMetricsByDayStream,
    AdsMetricsByDayStream,
    CampaignsStream,
    IN_APP_EVENT_METRICS,
)
from tap_tiktok.tap import TapTikTok

TODAY = datetime.datetime.now(datetime.timezone.utc).date()


def _tap(**config: Any) -> TapTikTok:
    return TapTikTok(config={"access_token": "tok", "advertiser_ids": "111", "start_date": "2025-01-01", **config})


def _response(url: str, body: Dict[str, Any], status: int = 200) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body).encode()
    response.request = requests.Request("GET", url).prepare()
    response.url = url
    return response


class FakeTikTok:
    """Answers report/integrated/get: rows per (start_date, metrics), split in pages."""

    def __init__(self, rows_for, page_size: int = 2) -> None:
        self.rows_for = rows_for
        self.page_size = page_size
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, prepared, context):
        query = {k: v[0] for k, v in parse_qs(urlparse(prepared.url).query).items()}
        self.calls.append(query)
        rows = self.rows_for(query)
        page = int(query.get("page", 1))
        total_page = max(1, -(-len(rows) // self.page_size))
        chunk = rows[(page - 1) * self.page_size : page * self.page_size]
        body = {"code": 0, "message": "OK", "data": {"list": chunk, "page_info": {"page": page, "total_page": total_page}}}
        return _response(prepared.url, body)


def _run(stream: AdsMetricsByDayStream, fake: FakeTikTok) -> List[dict]:
    stream.request_decorator = lambda func: fake  # type: ignore[method-assign]
    return [stream.post_process(row, {"advertiser_id": "111"}) for row in stream.request_records({"advertiser_id": "111"})]


def test_windows_are_contiguous_and_at_most_31_days():
    windows = AdsMetricsByDayStream.windows(datetime.date(2025, 1, 1), datetime.date(2025, 3, 5))
    assert windows[0] == (datetime.date(2025, 1, 1), datetime.date(2025, 1, 31))
    assert windows[-1][1] == datetime.date(2025, 3, 5)
    for (a_start, a_end), (b_start, _) in zip(windows, windows[1:]):
        assert b_start == a_end + datetime.timedelta(days=1)
    assert all((end - start).days <= 30 for start, end in windows)


def test_lookback_restarts_before_the_bookmark_but_never_before_start_date():
    stream = AdsBasicDataMetricsByDayStream(tap=_tap(lookback=28))
    stream.get_starting_timestamp = lambda context: datetime.datetime.now(datetime.timezone.utc)  # bookmark: today
    start, end = stream._range({"advertiser_id": "111"})
    assert start == TODAY - datetime.timedelta(days=28) and end == TODAY
    recent = AdsBasicDataMetricsByDayStream(tap=_tap(lookback=28, start_date=TODAY.isoformat()))
    recent.get_starting_timestamp = lambda context: datetime.datetime.now(datetime.timezone.utc)
    assert recent._range({"advertiser_id": "111"})[0] == TODAY


def test_end_date_bounds_a_re_pull():
    stream = AdsBasicDataMetricsByDayStream(tap=_tap(start_date="2025-01-01", end_date="2025-01-10", lookback=0))
    stream.get_starting_timestamp = lambda context: datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
    assert stream._range({"advertiser_id": "111"}) == (datetime.date(2025, 1, 1), datetime.date(2025, 1, 10))


def test_every_window_and_page_is_read_even_after_an_empty_window():
    stream = AdsBasicDataMetricsByDayStream(tap=_tap(start_date="2025-01-01", end_date="2025-03-05", lookback=0))
    stream.get_starting_timestamp = lambda context: datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)

    def rows_for(query):
        if query["start_date"] == "2025-01-01":
            return []  # an empty first window must not end the stream
        return [{"dimensions": {"ad_id": str(i), "stat_time_day": query["start_date"] + " 00:00:00"}, "metrics": {"spend": "1"}} for i in range(5)]

    fake = FakeTikTok(rows_for)
    records = _run(stream, fake)
    assert len(records) == 10  # two non-empty windows x 5 rows, 3 pages each
    assert [c["start_date"] for c in fake.calls if c["page"] == "1"] == ["2025-01-01", "2025-02-01", "2025-03-04"]
    assert {c["page_size"] for c in fake.calls} == {"1000"}
    assert all(r["advertiser_id"] == "111" for r in records)


def test_in_app_metrics_are_asked_in_two_halves_and_joined_across_pages():
    stream = AdsInAppEventMetricsByDayStream(tap=_tap(start_date=TODAY.isoformat(), lookback=0))
    stream.get_starting_timestamp = lambda context: datetime.datetime.now(datetime.timezone.utc)

    def rows_for(query):
        metrics = json.loads(query["metrics"])
        ids = ["1", "2", "3"] if metrics[0] == IN_APP_EVENT_METRICS[0] else ["3", "2", "1"]  # another order per half
        return [{"dimensions": {"ad_id": i, "stat_time_day": "d"}, "metrics": {m: i for m in metrics}} for i in ids]

    records = _run(stream, FakeTikTok(rows_for))
    assert sorted(r["ad_id"] for r in records) == ["1", "2", "3"]
    for record in records:
        assert all(record[m] == record["ad_id"] for m in IN_APP_EVENT_METRICS)


def test_tiktok_errors_on_http_200_are_raised_not_skipped():
    url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/?advertiser_id=111"
    assert tiktok_failure("s", _response(url, {"code": 0, "data": {}})) is None
    revoked = tiktok_failure("s", _response(url, {"code": 40105, "message": "Access token is incorrect or has been revoked.", "request_id": "r1"}))
    assert isinstance(revoked, FatalAPIError) and "invalid oauth access token" in str(revoked)
    assert "advertiser 111" in str(revoked) and "r1" in str(revoked) and "tok" not in str(revoked).replace("token", "")
    assert isinstance(tiktok_failure("s", _response(url, {"code": 40100, "message": "Too many requests"})), RetriableAPIError)
    assert isinstance(tiktok_failure("s", _response(url, {"code": 51000, "message": "busy"})), RetriableAPIError)
    assert isinstance(tiktok_failure("s", _response(url, {"code": 40001, "message": "No permission"})), FatalAPIError)


def test_stream_validate_response_raises_on_a_tiktok_refusal():
    stream = CampaignsStream(tap=_tap())
    with pytest.raises(FatalAPIError, match="code 40001"):
        stream.validate_response(_response("https://x/?advertiser_id=111", {"code": 40001, "message": "No permission"}))
