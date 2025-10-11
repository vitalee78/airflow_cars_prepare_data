# parsers/base_parser.py

import re
import logging

from bs4 import BeautifulSoup

from scripts.cars.common.parser_utils import get_bs4_util

logger = logging.getLogger(__name__)


class BaseParser:
    def get_bs4_from_url(self, url: str) -> BeautifulSoup:
        return get_bs4_util(url, headers=self.HEADERS)

    def check_first_page_and_get_soup(self, base_url):
        """
        Загружает первую страницу, проверяет наличие результатов.
        Возвращает (soup, should_stop, result_dict)
        - soup: BeautifulSoup объект (если есть)
        - should_stop: bool — нужно ли прекратить парсинг
        - result_dict: dict — результат для возврата (если should_stop=True)
        """
        try:
            soup = self.get_bs4_from_url(base_url)
        except Exception as e:
            logger.error(f"Не удалось загрузить первую страницу: {e}")
            return None, True, {
                "total_lots": 0,
                "status": "error",
                "message": f"Ошибка загрузки первой страницы: {e}"
            }

        # Проверка на отсутствие результатов
        no_results = (
            soup.find('h3', string=re.compile(r'.*не найдены.*', re.IGNORECASE)) or
            soup.find(string=re.compile(r'Всего найдено:\s*0'))
        )

        if no_results:
            logger.info("Нет результатов по заданным фильтрам. Парсинг завершён.")
            return soup, True, {
                "total_lots": 0,
                "status": "success",
                "message": "Нет лотов по фильтрам"
            }

        # Проверка на отсутствие карточек лотов
        articles = soup.find_all('article', class_='lot-teaser')
        if not articles and not no_results:
            logger.warning("Лоты не найдены, но явного '0 найдено' нет. Продолжаем парсинг...")

        return soup, False, None