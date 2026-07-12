"""Tests for DataQualityChecker.

Tests the quality report data model and the _interval_to_ms helper.
SQL-based checks are tested via integration tests (require ClickHouse).
"""

from hypeedge.storage.data_quality import QualityReport, _interval_to_ms


class TestQualityReport:
    def test_has_issues_when_gaps(self) -> None:
        report = QualityReport(table="candles", coin="BTC", gaps=[(1000, 2000)])
        assert report.has_issues is True

    def test_has_issues_when_duplicates(self) -> None:
        report = QualityReport(table="candles", coin="BTC", duplicate_count=5)
        assert report.has_issues is True

    def test_has_issues_when_anomalies(self) -> None:
        report = QualityReport(table="l2_book", coin="ETH", anomaly_count=1)
        assert report.has_issues is True

    def test_no_issues_when_clean(self) -> None:
        report = QualityReport(table="candles", coin="BTC", total_rows=1000)
        assert report.has_issues is False

    def test_multiple_gaps(self) -> None:
        report = QualityReport(
            table="candles",
            coin="BTC",
            gaps=[(1000, 2000), (3000, 5000)],
            duplicate_count=3,
            anomaly_count=1,
        )
        assert len(report.gaps) == 2
        assert report.has_issues is True


class TestIntervalToMs:
    def test_common_intervals(self) -> None:
        assert _interval_to_ms("1m") == 60_000
        assert _interval_to_ms("5m") == 300_000
        assert _interval_to_ms("1h") == 3_600_000
        assert _interval_to_ms("1d") == 86_400_000

    def test_unknown_interval_defaults_to_1m(self) -> None:
        assert _interval_to_ms("unknown") == 60_000
