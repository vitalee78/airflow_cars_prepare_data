# scripts/cars/common/parsing_utils.py

import logging
import re
import sys

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def get_bs4_util(url: str, headers: dict = None, timeout: int = 10) -> BeautifulSoup:
    """Загружает HTML по URL и возвращает BeautifulSoup объект"""
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'
        return BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        logger.error(f"Ошибка при загрузке {url}: {e}")
        sys.exit(1)


def get_field_util(pattern: str, text: str, cast=str):
    """Извлекает значение по регулярному выражению и приводит к типу"""
    match = re.search(pattern, text)
    if match:
        try:
            return cast(match.group(1))
        except (ValueError, TypeError):
            return None
    return None


def should_skip_by_year(year: int | None, min_year: int, title: str = "Без названия") -> bool:
    """
    Проверяет, нужно ли пропустить запись из-за слишком старого года.

    :param year: год выпуска (может быть None)
    :param min_year: минимально допустимый год
    :param title: название авто для логирования
    :return: True — если нужно пропустить
    """
    if year is not None and year < min_year:
        logger.info(f"Пропущен лот с годом {year} (< {min_year}): {title}")
        print(f"Пропущен лот с годом {year} (< {min_year}): {title}")
        return True
    return False


def clean_equipment(text: str) -> str | None:
    """
    Очищает строку equipment:
    - Если есть 'e:HEV Z.PLaY package' (в любом регистре) → возвращает None.
    - Если есть слово 'Honda', но НЕ в самом начале → обрезает строку до него.
    - Иначе возвращает исходную строку.
    """
    if not text:
        return None

    # Паттерн для обнаружения стоп-слов/фраз
    stop_pattern = re.compile(
        r'[а-яА-ЯёЁ,]|'
        r'\b[Hh]onda\b|'  # Honda (в любом месте)
        r'\.[pP][lL][aA][yY]\s+[pP]ackage|'  # .PLaY package
        r'\.{2,}\s*[pP]ackage',  # .... package и т.п.
        re.IGNORECASE
    )

    match = stop_pattern.search(text)
    if match:
        cleaned = text[:match.start()].rstrip(' .')
        return cleaned if cleaned else None
    else:
        return text.rstrip(' .')
