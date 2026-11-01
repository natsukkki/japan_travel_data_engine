"""Read prefecture-level guest nights from JTA accommodation workbooks.

This module interprets Excel and maps selected prefectures to project regions.
Publication version selection, DDS region ID mapping, validation across files,
and DDS writes belong to the ETL job.
"""

from __future__ import annotations

from io import BytesIO
import json
from pathlib import Path
import re
from typing import Literal, TypedDict

from openpyxl import load_workbook


ReleaseType = Literal["final", "second_preliminary"]
SHEET_PATTERN = re.compile(r"^第2表\((\d{1,2})月\)$")
PREFECTURE_PATTERN = re.compile(r"^\s*(\d{2})(\S.+?)\s*$")
REGIONS_PATH = (
    Path(__file__).resolve().parents[2]
    / "ingestion" / "batch" / "config" / "regions.json"
)


class TourismRow(TypedDict):
    prefecture_code: int
    region_code: str
    year: int
    month: int
    total_guest_nights: int
    foreign_guest_nights: int


def load_region_codes_by_prefecture(path: Path = REGIONS_PATH) -> dict[int, str]:
    """Map selected prefecture codes to region codes using the shared config."""

    with path.open(encoding="utf-8") as stream:
        regions = json.load(stream)

    if not isinstance(regions, list) or not regions:
        raise ValueError("Справочник регионов должен содержать непустой список")

    region_codes_by_prefecture: dict[int, str] = {}
    for index, region in enumerate(regions, start=1):
        if not isinstance(region, dict):
            raise ValueError(f"Регион №{index} должен быть объектом JSON")

        code = region.get("prefecture_code")
        if isinstance(code, bool) or not isinstance(code, int) or not 1 <= code <= 47:
            raise ValueError(
                f"У региона №{index} prefecture_code должен быть целым числом от 1 до 47"
            )
        if code in region_codes_by_prefecture:
            raise ValueError(f"Повторяющийся prefecture_code в справочнике: {code}")

        region_code = region.get("region_code")
        if not isinstance(region_code, str) or not region_code.strip():
            raise ValueError(
                f"У региона №{index} region_code должен быть непустой строкой"
            )
        region_codes_by_prefecture[code] = region_code.strip()

    return region_codes_by_prefecture


def _count(value: object, sheet_name: str, row_number: int) -> int:
    """Reject missing, marked, fractional, and negative source values."""
    if isinstance(value, bool):
        raise ValueError(f"{sheet_name}, строка {row_number}: некорректное число {value!r}")
    if isinstance(value, int):
        result = value
    elif isinstance(value, float) and value.is_integer():
        result = int(value)
    elif isinstance(value, str) and re.fullmatch(r"\d{1,3}(?:,\d{3})*|\d+", value.strip()):
        result = int(value.strip().replace(",", ""))
    else:
        raise ValueError(f"{sheet_name}, строка {row_number}: некорректное число {value!r}")
    if result < 0:
        raise ValueError(f"{sheet_name}, строка {row_number}: отрицательное число")
    return result


def parse_jta_workbook(
    content: bytes,
    *,
    release_type: ReleaseType,
    year: int,
    month: int | None = None,
    regions_path: Path = REGIONS_PATH,
) -> list[TourismRow]:
    """Extract monthly JTA rows with region codes from the shared config.

    ``year`` and ``month`` are taken from the ingestion artifact metadata, not
    inferred from sheet titles. A final workbook has twelve monthly sheets;
    a second preliminary workbook has exactly the requested month's sheet.
    Validate all 47 source prefectures, but return only those in regions_path.
    Include the configured region_code instead of the source prefecture name.
    """
    if release_type not in ("final", "second_preliminary"):
        raise ValueError(f"Неизвестный тип публикации: {release_type}")
    if not 2000 <= year <= 2100:
        raise ValueError(f"Некорректный год: {year}")
    if release_type == "second_preliminary" and (month is None or not 1 <= month <= 12):
        raise ValueError("Для second_preliminary нужен месяц от 1 до 12")
    if release_type == "final" and month is not None:
        raise ValueError("Для final месяц задаётся листами книги")

    region_codes_by_prefecture = load_region_codes_by_prefecture(path=regions_path)

    workbook = load_workbook(BytesIO(content), read_only=True, data_only=True)
    try:
        sheets = {
            int(match.group(1)): sheet
            for sheet in workbook
            if (match := SHEET_PATTERN.fullmatch(sheet.title))
        }
        expected_months = set(range(1, 13)) if release_type == "final" else {month}
        if set(sheets) != expected_months:
            raise ValueError(
                f"Неверный набор месячных листов 第2表: {sorted(sheets)}; "
                f"ожидалось {sorted(expected_months)}"
            )

        result: list[TourismRow] = []
        for sheet_month in sorted(sheets):
            sheet = sheets[sheet_month]
            header = next(sheet.iter_rows(min_row=4, max_row=4, values_only=True))
            if "延べ宿泊者数" not in str(header[1]).replace("\n", "").replace(" ", ""):
                raise ValueError(f"{sheet.title}: не найден заголовок общего числа ночёвок")
            foreign_columns = [
                index
                for index, value in enumerate(header)
                if value is not None
                and "外国人延べ宿泊者数" in re.sub(r"\s+", "", str(value))
            ]
            if len(foreign_columns) != 1:
                raise ValueError(f"{sheet.title}: не найден единственный столбец иностранных ночёвок")
            foreign_column = foreign_columns[0]

            seen: set[int] = set()
            for row_number, values in enumerate(sheet.iter_rows(min_row=8, values_only=True), start=8):
                label = values[0]
                match = PREFECTURE_PATTERN.fullmatch(label) if isinstance(label, str) else None
                if match is None:
                    continue
                prefecture_code = int(match.group(1))
                if not 1 <= prefecture_code <= 47 or prefecture_code in seen:
                    raise ValueError(f"{sheet.title}, строка {row_number}: неверный код префектуры")
                seen.add(prefecture_code)
                total = _count(values[1], sheet.title, row_number)
                foreign = _count(values[foreign_column], sheet.title, row_number)
                if foreign > total:
                    raise ValueError(f"{sheet.title}, строка {row_number}: иностранных ночёвок больше общего числа")
                if prefecture_code not in region_codes_by_prefecture:
                    continue
                result.append({
                    "prefecture_code": prefecture_code,
                    "region_code": region_codes_by_prefecture[prefecture_code],
                    "year": year,
                    "month": sheet_month,
                    "total_guest_nights": total,
                    "foreign_guest_nights": foreign,
                })
            if seen != set(range(1, 48)):
                raise ValueError(f"{sheet.title}: найдено {len(seen)} из 47 префектур")
        return result
    finally:
        workbook.close()
