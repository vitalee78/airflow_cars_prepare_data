# scripts/parser_lots.py

import logging
import re
from datetime import datetime
from random import uniform
from time import sleep
from typing import Tuple

import pandas as pd

from scripts.cars.common.base_parser import BaseParser
from scripts.cars.common.parser_utils import get_field_util, should_skip_by_year
from scripts.cars.lots.loader_lots import LotsLoader

logger = logging.getLogger(__name__)


class ParserCars(BaseParser):
    def __init__(self,
                 airflow_mode: bool = True,
                 brand_model: str = "honda/vezel",
                 option_cars: str = "rate-4=4&rate-4-5=4.5&year-from=2020&year-to=2023",
                 batch_size: int = 20,
                 min_year: int = 2010
                 ):
        self.MIN_YEAR = min_year
        self.BATCH_SIZE = batch_size
        self.airflow_mode = airflow_mode
        self.BASE_URL = "https://tokidoki.su"
        self.SECTION_PATH = f"/stat/{brand_model}/?{option_cars}"
        self.HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }

    def get_pagination_from_url(self, base_section_url: str) -> int:
        """Получает максимальный номер страницы из пагинации на первой странице"""
        try:
            soup = self.get_bs4_from_url(base_section_url)
            pagination_ul = soup.find("ul", class_="pagination")

            if not pagination_ul:
                return 1

            max_page = 1
            for link in pagination_ul.find_all("a", href=True):
                href = link["href"]
                match = re.search(r'/page-(\d+)/', href)
                if match:
                    page_num = int(match.group(1))
                    max_page = max(max_page, page_num)
            return max_page
        except Exception as e:
            logger.warning(f"Не удалось определить пагинацию, используем 1 страницу: {e}")
            return 1

    def parse_tokidoki_and_save(self):
        base_url = self.BASE_URL + self.SECTION_PATH

        soup, should_stop, result = self.check_first_page_and_get_soup(base_url)
        if should_stop:
            return result

        total_pages = self.get_pagination_from_url(base_url)

        loader = LotsLoader(airflow_mode=self.airflow_mode)
        batch = []
        parsed_count = 0

        for page in range(1, total_pages + 1):
            sleep(uniform(0.5, 2.0))

            if page == 1:
                url = base_url
            else:
                if '?' in self.SECTION_PATH:
                    path_part, params_part = self.SECTION_PATH.split('?', 1)
                    url = f"{self.BASE_URL.strip()}{path_part.rstrip('/')}/page-{page}/?{params_part}"
                else:
                    url = f"{self.BASE_URL.strip()}{self.SECTION_PATH.rstrip('/')}/page-{page}/"

            logger.info(f"Парсинг страницы {page}/{total_pages}: {url}")

            try:
                soup = self.get_bs4_from_url(url)
                articles = soup.find_all('article', class_='lot-teaser')

                if not articles:
                    logger.warning(f"На странице {page} не найдено лотов")
                    continue

                for article in articles:
                    try:
                        parsed = self.parse_info(article)
                        if not parsed or 'brand' not in parsed:
                            continue

                        title_elem = parsed.get('brand') + ' ' + parsed.get('model')

                        if should_skip_by_year(parsed.get('year'), self.MIN_YEAR, title_elem):
                            continue

                        lot_id, lot_date = self.pars_info_lot(article)
                        if not lot_id:
                            continue

                        # Добавляем в parsed
                        parsed['source_lot_id'] = lot_id
                        try:
                            parsed['lot_date'] = datetime.strptime(lot_date, '%d.%m.%Y').date()
                        except ValueError:
                            parsed['lot_date'] = None

                        price_meta = article.find('meta', itemprop='price')
                        if not (price_meta and price_meta.get('content')):
                            continue
                        try:
                            cost = float(price_meta['content'])
                            parsed['cost'] = cost
                        except (ValueError, TypeError):
                            continue

                        a_tag = article.find('a', href=True)
                        if not a_tag:
                            logging.error("Не найден тег <a> с href в статье. Невозможно извлечь id_car.")
                            raise ValueError("Обязательное поле 'id_car' не может быть извлечено: отсутствует ссылка.")

                        href = a_tag['href']
                        parsed['link_source'] = (self.BASE_URL.strip() + a_tag['href']) if a_tag else None
                        id_car = get_field_util(r'/(\d+)/?$', href, cast=int)
                        if id_car is None:
                            logging.error(f"Не удалось извлечь id_car из href: {href}")
                            raise ValueError(f"Обязательное поле 'id_car' не найдено в ссылке: {href}")

                        parsed['id_car'] = id_car

                        batch.append(parsed)
                        print(parsed)
                        parsed_count += 1

                        # Сохраняем батч
                        if len(batch) >= self.BATCH_SIZE:
                            df_batch = pd.DataFrame(batch)
                            loader.save_lots_to_db(df_batch)
                            batch = []  # очищаем

                    except Exception as e:
                        logger.error(f"Ошибка при обработке лота: {e}")
                        continue

            except Exception as e:
                logger.error(f"Ошибка на странице {page}: {e}")
                continue

        # Сохраняем остаток
        if batch:
            df_batch = pd.DataFrame(batch)
            loader.save_lots_to_db(df_batch)

        logger.info(f"Завершён парсинг. Сохранено {parsed_count} лотов.")
        return {
            "total_lots": parsed_count,
            "status": "success",
            "message": "Парсинг завершён успешно"
        }

    def parse_info(self, article) -> dict:
        info_div = article.find("div", class_="lot-teaser__info")
        if not info_div:
            return {}

        data = {}
        title_elem = info_div.find("div", class_='lot-teaser__info__title')
        if not title_elem:
            return {}

        info_title = title_elem.get_text(strip=True)
        parts = info_title.split(maxsplit=1)
        if len(parts) < 2:
            return {}
        data['brand'] = parts[0].capitalize()
        data['model'] = parts[1].upper()

        info_sub_title = info_div.find('div', class_='lot-teaser__info__sub-title').text
        data['equipment'] = str(info_sub_title)

        info_list = info_div.find("div", class_='lot-teaser__info__list')
        if not info_list:
            return data

        for li in info_list.select('li'):
            text = li.get_text(strip=True)
            if ':' not in text:
                continue
            key, value = text.split(':', 1)
            key = key.strip()
            value = value.strip()

            if key == "Кузов":
                data['carbody'] = str(value)
            elif key == "Пробег":
                try:
                    data['mileage'] = int(value.replace(' ', ''))
                except ValueError:
                    pass
            elif key in ["Обьем", "Объем", "Объём"]:
                try:
                    # Убираем "см³" или другие символы
                    vol = re.sub(r'[^\d]', '', value)
                    if vol:
                        data['engine_volume'] = int(vol)
                except (ValueError, TypeError):
                    pass
            elif key == "Год":
                try:
                    data['year'] = int(value)
                except ValueError:
                    pass
            elif key == "Оценка":
                try:
                    data['rate'] = str(value.replace(',', '.'))
                except ValueError:
                    pass

        return data

    def pars_info_lot(self, article) -> Tuple[str, str]:
        lot_div = article.find("div", class_="lot-teaser__lot")
        if not lot_div:
            return "", ""

        lines = [s.strip() for s in lot_div.stripped_strings]

        lot_number = ""
        lot_date = ""

        # Найдём лот
        for line in lines:
            if line.startswith("Лот "):
                match = re.search(r'Лот\s+(\d+)', line)
                if match:
                    lot_number = match.group(1)

        # Дата — последняя строка, если она подходит под формат
        if lines and re.fullmatch(r'\d{2}\.\d{2}\.\d{4}', lines[-1]):
            lot_date = lines[-1]

        return lot_number, lot_date

    def get_field(self, pattern, text, cast=str):
        return get_field_util(pattern, text, cast)


if __name__ == '__main__':
    # Для локального запуска
    parser = ParserCars(airflow_mode=False)
    parser.parse_tokidoki_and_save()
