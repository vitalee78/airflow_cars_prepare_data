import logging
import pickle
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.cars.common.db_postgres import get_engine

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore")

# Конфиг аукциона
AUCTION_CONFIG = {
    'start_price_ratio': 0.7,
    'min_start_price': 100000,
    'max_start_price': 5000000,
    'start_price_source': 'market',
    'filters': {
        'min_year': None,  # или 2020
        'max_year': None,  # или 2023
        'top': None  # количество записей по брендам
    }
}

class Predict:
    def __init__(self, airflow_mode: bool = True):
        self.airflow_mode = airflow_mode

    def predict_auction_prices(self, config=None, save_to_db=False, show_display=False):
        """Прогнозирование цен для будущих аукционов"""
        if config:
            AUCTION_CONFIG.update(config)

        engine = get_engine(airflow_mode=self.airflow_mode)

        # Загружаем модель
        model_path = Path("../models/car_price_model_v4.pkl")
        try:
            if not model_path.exists():
                # Пробуем загрузить предыдущую версию
                model_path = Path("../models/car_price_model_v3.pkl")
                if not model_path.exists():
                    logger.error(f"Модель не найдена: {model_path}")
        except FileNotFoundError as e:
            logger.error(f"Not found model ", e)
            sys.exit(1)

        with open(model_path, "rb") as f:
            model_data = pickle.load(f)

        model = model_data['model']
        encoder = model_data.get('encoder')
        numerical_features = model_data.get('numerical_features', [])
        categorical_features = model_data.get('categorical_features', [])
        all_feature_columns = model_data.get('all_feature_columns', [])

        print(f"=== ЗАГРУЗКА ДАННЫХ ===")
        print(f"Модель: {len(all_feature_columns)} признаков")
        print(f"Числовые признаки: {len(numerical_features)}")
        print(f"Категориальные признаки: {len(categorical_features)}")

        # Загружаем данные для прогноза
        connection = engine.raw_connection()
        try:
            df = pd.read_sql("""
                  SELECT 
                        f.*,
                        b.brand,
                        m.model,
                        cb.carbody,
                        fac.link_source,
                        CONCAT(b.brand, ' ', m.model, ' ', f.year_release::text, ' (', cb.carbody, ')') as car_info
                    FROM mart.v_ml_car_price_features f
                    LEFT JOIN ref.ref_brands b ON f.id_brand = b.id_brand
                    LEFT JOIN ref.ref_models m ON f.id_model = m.id_model
                    LEFT JOIN ref.ref_carbodies cb ON f.id_carbody = cb.id_carbody
                    LEFT JOIN fact.f_auction_cars fac ON fac.id_car = f.id_car
                    WHERE f.data_type = 'prediction'
                      AND f.auction_date > CURRENT_DATE;
            """, connection)
        finally:
            connection.close()

        if df.empty:
            print("Нет будущих лотов для прогнозирования")
            return

        print(f"Загружено лотов: {len(df)}")

        # Подготавливаем признаки
        X_pred = self.prepare_prediction_features(df, numerical_features, categorical_features, encoder, all_feature_columns)

        # Прогнозируем
        predictions = model.predict(X_pred)

        # Формируем результаты
        df['predicted_price'] = predictions
        df['prediction_date'] = pd.Timestamp.now()

        # Рассчитываем стартовые цены
        df = self.calculate_start_prices(df, AUCTION_CONFIG)

        # Рассчитываем доверие
        df['prediction_confidence'] = self.calculate_confidence(predictions, df)

        # Сохраняем в БД если нужно
        if save_to_db:
            self.save_predictions_to_db(df, engine)

        # Применяем фильтр года из конфига
        if show_display:
            filters = AUCTION_CONFIG.get('filters', {})
            self.display_results(df,
                            min_year=filters.get('min_year'),
                            max_year=filters.get('max_year'),
                            top_count=filters.get('top'))

        return df


    def fix_zero_prices(self, df):
        """Исправление нулевых и отсутствующих стартовых цен"""

        if len(df) == 0:
            return df

        # Заменяем нули и NaN на осмысленные значения
        zero_price_mask = (df['start_price'].isna()) | (df['start_price'] <= 0)

        if zero_price_mask.any():
            logger.info(f"Обнаружено {zero_price_mask.sum()} лотов с нулевой или отсутствующей стартовой ценой")

            # Безопасно вычисляем медианную рыночную цену
            market_prices = pd.to_numeric(df['market_avg_price_3m'], errors='coerce').dropna()
            if len(market_prices) > 0:
                median_market_price = market_prices.median()
                replacement_price = median_market_price * 0.7
                df.loc[zero_price_mask, 'start_price'] = replacement_price
                logger.info(f"Заменяем нулевые цены на {replacement_price:,.0f} руб (70% от медианной рыночной)")
            else:
                # Если нет рыночных данных, используем прогнозируемые цены
                predicted_prices = pd.to_numeric(df['predicted_price'], errors='coerce').dropna()
                if len(predicted_prices) > 0:
                    median_predicted = predicted_prices.median()
                    replacement_price = median_predicted * 0.7
                    df.loc[zero_price_mask, 'start_price'] = replacement_price
                    logger.info(f"Заменяем нулевые цены на {replacement_price:,.0f} руб (70% от медианной прогнозируемой)")
                else:
                    # Запасной вариант
                    df.loc[zero_price_mask, 'start_price'] = 100000
                    logger.info("Заменяем нулевые цены на 100,000 руб (значение по умолчанию)")

        return df


    def ensure_numeric_types(self, df):
        """Убедиться, что числовые колонки имеют правильный тип"""
        numeric_columns = ['mileage', 'market_avg_price_3m', 'start_price', 'predicted_price']

        for col in numeric_columns:
            if col in df.columns:
                # Преобразуем в numeric, ошибки заменяем на NaN, затем заполняем 0
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df


    def calculate_price_difference(self, results):
        """Безопасное вычисление разницы в процентах"""
        if 'start_price' in results.columns and 'predicted_price' in results.columns:
            # Убедимся, что колонки числовые
            start_price = pd.to_numeric(results['start_price'], errors='coerce').fillna(0)
            predicted_price = pd.to_numeric(results['predicted_price'], errors='coerce').fillna(0)

            # Вычисляем разницу в процентах, избегая деления на ноль
            with np.errstate(divide='ignore', invalid='ignore'):
                price_diff_pct = np.where(
                    start_price > 0,
                    (predicted_price - start_price) / start_price * 100,
                    0
                )

            results['price_difference_pct'] = np.round(price_diff_pct, 1)
        else:
            results['price_difference_pct'] = 0

        return results


    def calculate_start_prices(self, df, config):
        """Расчет стартовых цен согласно конфигурации"""

        ratio = config['start_price_ratio']
        min_price = config['min_start_price']
        max_price = config['max_start_price']
        source = config['start_price_source']

        if source == 'market':
            # От рыночной цены
            base_prices = pd.to_numeric(df['market_avg_price_3m'], errors='coerce').fillna(0)
            df['start_price'] = (base_prices * ratio).clip(min_price, max_price)
            df['start_price_source'] = 'market_based'

        elif source == 'predicted':
            # От прогнозируемой цены
            base_prices = pd.to_numeric(df['predicted_price'], errors='coerce').fillna(0)
            df['start_price'] = (base_prices * ratio).clip(min_price, max_price)
            df['start_price_source'] = 'predicted_based'

        elif source == 'fixed':
            # Фиксированная цена для всех
            df['start_price'] = min_price
            df['start_price_source'] = 'fixed_price'

        # Статистика
        logger.info(f"  Рассчитано стартовых цен: {len(df)}")
        logger.info(f"  Средняя стартовая цена: {df['start_price'].mean():,.0f} руб")

        return df


    def prepare_prediction_features(self, df, numerical_features, categorical_features, encoder, all_feature_columns):
        """Подготовка признаков для прогнозирования"""

        # Числовые признаки
        X_numerical = df[numerical_features].copy()

        # Заполняем пропущенные значения в числовых признаках
        for col in numerical_features:
            if col in X_numerical.columns:
                # Преобразуем в numeric, ошибки заменяем на NaN, затем заполняем 0
                X_numerical[col] = pd.to_numeric(X_numerical[col], errors='coerce').fillna(0)
            else:
                # Если колонка отсутствует, создаем с нулевыми значениями
                X_numerical[col] = 0

        # One-hot encoding для категориальных признаков
        if encoder and categorical_features:
            try:
                # Проверяем наличие всех категориальных признаков
                for col in categorical_features:
                    if col not in df.columns:
                        logger.info(f"Создаем отсутствующий категориальный признак: {col}")
                        df[col] = 0

                # Преобразуем категориальные признаки
                X_categorical = encoder.transform(df[categorical_features])

                # Создаем DataFrame с правильными названиями колонок
                categorical_columns = []
                for i, feature in enumerate(categorical_features):
                    for category in encoder.categories_[i][1:]:  # пропускаем первую категорию
                        categorical_columns.append(f"{feature}_{category}")

                X_categorical_df = pd.DataFrame(X_categorical, columns=categorical_columns, index=df.index)

            except Exception as e:
                logger.error(f"Ошибка при кодировании категориальных признаков: {e}")
                # Создаем пустой DataFrame с нужными колонками
                categorical_columns = []
                for feature in categorical_features:
                    if encoder and hasattr(encoder, 'categories_'):
                        idx = categorical_features.index(feature)
                        for category in encoder.categories_[idx][1:]:
                            categorical_columns.append(f"{feature}_{category}")

                X_categorical_df = pd.DataFrame(0, columns=categorical_columns, index=df.index)
        else:
            # Если нет encoder, создаем пустые колонки
            X_categorical_df = pd.DataFrame(index=df.index)
            for col in all_feature_columns:
                if col not in numerical_features and any(col.startswith(cat) for cat in categorical_features):
                    X_categorical_df[col] = 0

        # Объединяем все признаки
        X_processed = pd.concat([X_numerical, X_categorical_df], axis=1)

        # Убеждаемся, что все данные числовые
        X_processed = X_processed.astype(np.float64)

        logger.info(f"Подготовлено признаков: {X_processed.shape[1]}")
        return X_processed


    def safe_min(self, series):
        """Безопасное вычисление минимума"""
        if len(series) == 0:
            return 0
        series_clean = pd.to_numeric(series, errors='coerce').dropna()
        if len(series_clean) == 0:
            return 0
        return series_clean.min()


    def safe_max(self, series):
        """Безопасное вычисление максимума"""
        if len(series) == 0:
            return 0
        series_clean = pd.to_numeric(series, errors='coerce').dropna()
        if len(series_clean) == 0:
            return 0
        return series_clean.max()


    def safe_mean(self, series):
        """Безопасное вычисление среднего"""
        if len(series) == 0:
            return 0
        series_clean = pd.to_numeric(series, errors='coerce').dropna()
        if len(series_clean) == 0:
            return 0
        return series_clean.mean()


    def safe_median(self, series):
        """Безопасное вычисление медианы"""
        if len(series) == 0:
            return 0
        series_clean = pd.to_numeric(series, errors='coerce').dropna()
        if len(series_clean) == 0:
            return 0
        return series_clean.median()


    def calculate_confidence(self, predictions, df):
        """Расчет доверительной оценки прогноза"""
        confidence_scores = []

        for i, pred in enumerate(predictions):
            try:
                # Безопасно получаем рыночную цену
                market_price = df.iloc[i].get('market_avg_price_3m')
                listing_count = df.iloc[i].get('market_listing_count_3m', 0)

                # Преобразуем в numeric на случай, если это строки
                market_price = pd.to_numeric(market_price, errors='coerce')
                listing_count = pd.to_numeric(listing_count, errors='coerce')

                # Проверяем валидность market_price
                if market_price is not None and market_price > 0 and not np.isnan(market_price):
                    price_diff_ratio = abs(pred - market_price) / market_price
                    price_confidence = max(0, 1 - price_diff_ratio)
                else:
                    price_confidence = 0.3

                # Учитываем количество наблюдений на рынке
                listing_count = listing_count if not np.isnan(listing_count) else 0
                count_confidence = min(1.0, (listing_count or 0) / 100)

                # Общая уверенность
                overall_confidence = (price_confidence * 0.7 + count_confidence * 0.3)
                confidence_scores.append(round(overall_confidence, 2))

            except (TypeError, ValueError, ZeroDivisionError) as e:
                # В случае ошибки устанавливаем низкое доверие
                confidence_scores.append(0.1)

        return confidence_scores


    def display_results(self, df, min_year=None, max_year=None, top_count=None):
        """Компактный вывод по брендам с фильтрацией по году выпуска"""

        df = self.ensure_numeric_types(df)
        df = self.calculate_price_difference(df)

        # Применяем фильтрацию по году выпуска
        if min_year is not None:
            df = df[df['year_release'] >= min_year]
            logger.info(f"Фильтр: год выпуска от {min_year}")

        if max_year is not None:
            df = df[df['year_release'] <= max_year]
            logger.info(f"Фильтр: год выпуска до {max_year}")

        profitable_df = df[df['price_difference_pct'] > 0]

        print(f"\n=== ВЫГОДНЫЕ ПРЕДЛОЖЕНИЯ ПО БРЕНДАМ ===")

        # Статистика по годам
        if not profitable_df.empty:
            min_year_actual = profitable_df['year_release'].min()
            max_year_actual = profitable_df['year_release'].max()
            print(f"Диапазон годов в выборке: {min_year_actual}-{max_year_actual}")

        for brand, group in profitable_df.groupby('brand'):
            top_N = group.nlargest(top_count, 'price_difference_pct')

            print(f"\n{brand.upper()} ({len(top_N)} лучших):")

            for i, (idx, row) in enumerate(top_N.iterrows(), 1):
                # Формируем информацию
                model_year = f"{row.get('model', 'N/A')} {row.get('year_release', 'N/A')}"
                carbody = row.get('carbody', '')
                if carbody:
                    model_info = f"{model_year} ({carbody})"
                else:
                    model_info = model_year

                # Стоимости
                start_price = f"{row['start_price']:,.0f}"
                predicted_price = f"{row['predicted_price']:,.0f}"
                profit_info = f"+{row['price_difference_pct']:.1f}%"
                confidence = f"{row.get('prediction_confidence', 0):.0%}"

                # Выводим
                print(f"  {i}. {model_info}")
                print(f"     Старт: {start_price:>10} руб -> Прогноз: {predicted_price:>10} руб ({profit_info})")
                print(f"     Доверие: {confidence}")

                # Пробег если есть
                mileage = row.get('mileage', 0)
                if mileage and mileage > 0:
                    print(f"     Пробег: {mileage:,.0f} км")

                # Ссылка
                link_source = row.get('link_source')
                if link_source and pd.notna(link_source):
                    print(f"     Ссылка: {link_source}")

                print()  # пустая строка между автомобилями

        # Общая статистика
        if not profitable_df.empty:
            print(f"\n=== СТАТИСТИКА ===")
            print(f"Всего выгодных предложений: {len(profitable_df)}")
            print(f"Средняя выгода: +{profitable_df['price_difference_pct'].mean():.1f}%")
            print(f"Медианная выгода: +{profitable_df['price_difference_pct'].median():.1f}%")


    def save_predictions_to_db(self, df, engine):
        try:
            # Используем raw connection для полного контроля
            connection = engine.raw_connection()
            try:
                cursor = connection.cursor()

                # Очищаем таблицу
                cursor.execute("TRUNCATE TABLE mart.ml_predictions")
                connection.commit()
                logger.info("✅ Таблица ml_predictions очищена")

                # Подготавливаем данные для сохранения
                save_data = []

                for _, row in df.iterrows():
                    prediction_record = (
                        row.get('id_car'),
                        row.get('auction_date'),
                        row.get('brand'),
                        row.get('model'),
                        row.get('year_release'),
                        row.get('carbody'),
                        row.get('mileage'),
                        row.get('start_price'),
                        row.get('predicted_price'),
                        row.get('price_difference_pct', 0),
                        row.get('prediction_confidence', 0),
                        row.get('link_source'),
                        pd.Timestamp.now()
                    )
                    save_data.append(prediction_record)

                # Вставляем данные через executemany
                insert_query = """
                        INSERT INTO mart.ml_predictions 
                        (id_car, auction_date, brand, model, year_release, carbody, mileage, 
                         start_price, predicted_price, price_difference_pct, prediction_confidence, 
                         link_source, prediction_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                cursor.executemany(insert_query, save_data)
                connection.commit()

                logger.info(f"✅ Новые прогнозы сохранены в БД: {len(save_data)} записей")

            finally:
                connection.close()

        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении прогнозов в БД: {e}")
            sys.exit(1)


    def load_current_predictions(self, brand=None, min_confidence=0.0):
        """Загрузка текущих прогнозов из БД"""

        engine = get_engine(airflow_mode=self.airflow_mode)

        query = """
            SELECT 
                brand, model, year_release, carbody, mileage,
                start_price, predicted_price, price_difference_pct,
                prediction_confidence, link_source, prediction_date
            FROM mart.ml_predictions 
            WHERE 1=1
        """

        params = []

        if brand:
            query += " AND brand = %s"
            params.append(brand)

        if min_confidence > 0:
            query += " AND prediction_confidence >= %s"
            params.append(min_confidence)

        query += " ORDER BY price_difference_pct DESC"

        try:
            df = pd.read_sql(query, engine, params=params)
            logger.info(f"Загружено {len(df)} текущих прогнозов из БД")
            return df
        except Exception as e:
            logger.error(f"Ошибка при загрузке прогнозов из БД: {e}")
            return pd.DataFrame()


if __name__ == "__main__":
    config = {
        'filters': {'min_year': 2016, 'max_year': 2018, 'top': 3}
    }
    predict = Predict(airflow_mode=False)
    predict.predict_auction_prices(config, save_to_db=True, show_display=True)
