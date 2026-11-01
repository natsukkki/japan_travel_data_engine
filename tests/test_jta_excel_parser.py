"""Tests for the Excel-only JTA parser; no MinIO or database access."""

from io import BytesIO
import json
from pathlib import Path
import tempfile
import unittest

from openpyxl import Workbook

from processing.spark.jta_excel_parser import parse_jta_workbook


def workbook_bytes(
    months: list[int],
    foreign_column: int = 17,
    prefecture_codes: list[int] | None = None,
) -> bytes:
    workbook = Workbook()
    workbook.remove(workbook.active)
    for month in months:
        sheet = workbook.create_sheet(f"第2表({month}月)")
        sheet.cell(4, 2, "延べ\n宿泊者数\n1)")
        sheet.cell(4, foreign_column, "うち\n外国人延べ\n宿泊者数\n1)")
        codes = range(1, 48) if prefecture_codes is None else prefecture_codes
        for row, code in enumerate(codes, start=8):
            sheet.cell(row, 1, f" {code:02d}県{code}")
            sheet.cell(row, 2, 1000 + code)
            sheet.cell(row, foreign_column, 100 + code)
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


class JtaExcelParserTests(unittest.TestCase):
    def test_preliminary_reads_prefectures_and_dynamic_foreign_column(self) -> None:
        rows = parse_jta_workbook(
            workbook_bytes([6], foreign_column=20),
            release_type="second_preliminary", year=2026, month=6,
        )
        self.assertEqual(len(rows), 8)
        self.assertEqual(
            {row["prefecture_code"]: row["region_code"] for row in rows},
            {
                1: "JP-HOKKAIDO", 13: "JP-TOKYO", 26: "JP-KYOTO", 27: "JP-OSAKA",
                29: "JP-NARA", 34: "JP-HIROSHIMA", 40: "JP-FUKUOKA", 47: "JP-OKINAWA",
            },
        )
        self.assertEqual(rows[0], {
            "prefecture_code": 1, "region_code": "JP-HOKKAIDO",
            "year": 2026, "month": 6,
            "total_guest_nights": 1001, "foreign_guest_nights": 101,
        })

    def test_final_reads_twelve_months_without_annual_total(self) -> None:
        rows = parse_jta_workbook(
            workbook_bytes(list(range(1, 13))), release_type="final", year=2025,
        )
        self.assertEqual(len(rows), 12 * 8)
        self.assertEqual({row["month"] for row in rows}, set(range(1, 13)))
        expected_keys = {
            (code, month)
            for code in (1, 13, 26, 27, 29, 34, 40, 47)
            for month in range(1, 13)
        }
        self.assertEqual(
            {(row["prefecture_code"], row["month"]) for row in rows},
            expected_keys,
        )

    def test_selection_uses_the_supplied_regions_config(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "regions.json"
            path.write_text(json.dumps([
                {"region_code": "CUSTOM-TOKYO", "prefecture_code": 13},
                {"region_code": "CUSTOM-OKINAWA", "prefecture_code": 47},
            ]), encoding="utf-8")

            rows = parse_jta_workbook(
                workbook_bytes([6]),
                release_type="second_preliminary",
                year=2026,
                month=6,
                regions_path=path,
            )

        self.assertEqual([row["prefecture_code"] for row in rows], [13, 47])
        self.assertEqual(
            [row["region_code"] for row in rows],
            ["CUSTOM-TOKYO", "CUSTOM-OKINAWA"],
        )

    def test_rejects_invalid_regions_config(self) -> None:
        invalid_configs = (
            [], {}, [None], [{}],
            [{"prefecture_code": True}],
            [{"prefecture_code": "13"}],
            [{"prefecture_code": 0}],
            [{"prefecture_code": 48}],
            [
                {"prefecture_code": 13, "region_code": "JP-TOKYO"},
                {"prefecture_code": 13, "region_code": "JP-KYOTO"},
            ],
            [{"prefecture_code": 13}],
            [{"prefecture_code": 13, "region_code": None}],
            [{"prefecture_code": 13, "region_code": 13}],
            [{"prefecture_code": 13, "region_code": ""}],
            [{"prefecture_code": 13, "region_code": "   "}],
        )
        content = workbook_bytes([6])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "regions.json"
            for config in invalid_configs:
                with self.subTest(config=config):
                    path.write_text(json.dumps(config), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        parse_jta_workbook(
                            content,
                            release_type="second_preliminary",
                            year=2026,
                            month=6,
                            regions_path=path,
                        )

    def test_still_rejects_missing_source_prefectures(self) -> None:
        for missing_code in (2, 13):
            with self.subTest(missing_code=missing_code):
                codes = [code for code in range(1, 48) if code != missing_code]
                with self.assertRaisesRegex(ValueError, "46 из 47"):
                    parse_jta_workbook(
                        workbook_bytes([6], prefecture_codes=codes),
                        release_type="second_preliminary", year=2026, month=6,
                    )

    def test_still_rejects_duplicate_source_prefectures(self) -> None:
        with self.assertRaisesRegex(ValueError, "неверный код префектуры"):
            parse_jta_workbook(
                workbook_bytes([6], prefecture_codes=[*range(1, 48), 13]),
                release_type="second_preliminary", year=2026, month=6,
            )

    def test_rejects_missing_month(self) -> None:
        with self.assertRaisesRegex(ValueError, "Неверный набор"):
            parse_jta_workbook(workbook_bytes([1]), release_type="final", year=2025)

    def test_rejects_foreign_count_above_total(self) -> None:
        workbook = Workbook()
        workbook.remove(workbook.active)
        sheet = workbook.create_sheet("第2表(6月)")
        sheet.cell(4, 2, "延べ宿泊者数")
        sheet.cell(4, 17, "外国人延べ宿泊者数")
        sheet.cell(8, 1, "01北海道")
        sheet.cell(8, 2, 10)
        sheet.cell(8, 17, 11)
        output = BytesIO()
        workbook.save(output)
        with self.assertRaisesRegex(ValueError, "больше общего"):
            parse_jta_workbook(output.getvalue(), release_type="second_preliminary", year=2026, month=6)


if __name__ == "__main__":
    unittest.main()
