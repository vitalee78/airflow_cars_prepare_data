# scripts/cars/common/parsing_utils.py

import logging
import re
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
        raise


def get_field_util(pattern: str, text: str, cast=str):
    """Извлекает значение по регулярному выражению и приводит к типу"""
    match = re.search(pattern, text)
    if match:
        try:
            return cast(match.group(1))
        except (ValueError, TypeError):
            return None
    return None