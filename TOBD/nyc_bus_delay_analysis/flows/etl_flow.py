import os
import datetime
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text
from dask_pipeline.transform import aggregate_by_stop

# Настройки подключения и переменные окружения.
# Если переменные не заданы, используем значения по умолчанию для локальной разработки.
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "bus_delay_pass")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "nyc_bus")
DASK_ADDR = os.getenv("DASK_SCHEDULER_ADDRESS", "tcp://dask-scheduler:8786")

DB_CONN_STR = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

# Эта функция нужна для работы внутри Dask (map_partitions).
# Она решает специфичную проблему данных: иногда время указано как "24:xx" вместо "00:xx" следующего дня.
def parse_strange_time(df_partition):
    # Dask передает сюда кусок датафрейма (партицию).
    
    # Сначала приводим время записи к нормальному datetime
    recorded_dt = pd.to_datetime(df_partition['RecordedAtTime'])
    
    # ScheduledArrivalTime может содержать NaN, заменяем их на полночь, чтобы не упасть при конвертации
    sched_str = df_partition['ScheduledArrivalTime'].astype(str).fillna('00:00:00')
    
    # Ищем строки, где время начинается с 24 часов (специфика датасета)
    is_24_hour = sched_str.str.startswith('24')
    
    # Нормализуем такие случаи: меняем "24:..." на "00:..."
    normalized_time = sched_str.copy()
    mask_24 = normalized_time.str.startswith('24')
    # Используем loc, чтобы pandas не ругался на SettingWithCopy
    normalized_time.loc[mask_24] = '00' + normalized_time.loc[mask_24].str[2:]
    
    # Склеиваем дату (из RecordedAtTime) и нормализованное время, чтобы получить полный timestamp
    date_str = recorded_dt.dt.date.astype(str)
    time_to_parse = date_str + ' ' + normalized_time
    
    # Отбрасываем те строки, которые изначально были NaN (мы их заменили на 00:00:00 выше)
    mask_not_nan = normalized_time != '00:00:00'
    
    # Готовим колонку для результата
    scheduled_dt = pd.Series(pd.NaT, index=df_partition.index, dtype='datetime64[ns]')
    
    if mask_not_nan.any():
        # Парсим только валидные строки. format='mixed' позволяет pandas самому разобраться в деталях формата
        base_datetime = pd.to_datetime(time_to_parse[mask_not_nan], errors='coerce', format='mixed')
        
        # Самое важное: если исходно было "24:xx", значит это уже следующий день. Добавляем сутки.
        result_datetime = base_datetime + pd.Timedelta(days=1) * is_24_hour[mask_not_nan].astype(int)
        scheduled_dt.loc[mask_not_nan] = result_datetime
    
    df_partition['ScheduledDateTime'] = scheduled_dt
    
    return df_partition


# Задача Prefect для чтения данных
@task(retries=3, retry_delay_seconds=10, name="Extract Data")
def extract_data():
    logger = get_run_logger()
    logger.info("Connecting to Dask Cluster...")
    
    # Читаем все CSV файлы из примонтированной директории
    csv_pattern = "/app/data/*.csv"
    
    # Явно задаем типы данных, чтобы Dask не тратил время на их угадывание (и не ошибался)
    dtypes = {
        'DirectionRef': 'object',
        'PublishedLineName': 'object',
        'OriginName': 'object',
        'DestinationName': 'object',
        'VehicleRef': 'object',
        'NextStopPointName': 'object',
        'ArrivalProximityText': 'object',
        'DistanceFromStop': 'float64'
    }
    
    try:
        # blocksize="64MB" — оптимальный размер для разбиения файлов на чанки
        ddf = dd.read_csv(csv_pattern, dtype=dtypes, blocksize="64MB", on_bad_lines='skip')
        logger.info(f"Data extracted lazily. Partitions: {ddf.npartitions}")
        return ddf
    except Exception as e:
        logger.error(f"Failed to extract data: {e}")
        raise

@task(name="Clean Times")
def clean_times(ddf: dd.DataFrame):
    logger = get_run_logger()
    logger.info("Cleaning time formats and fixing Midnight Crossing...")
    
    # Конвертируем основные временные колонки
    ddf['ExpectedArrivalTime'] = dd.to_datetime(ddf['ExpectedArrivalTime'], errors='coerce')
    ddf['RecordedAtTime'] = dd.to_datetime(ddf['RecordedAtTime'])

    # Подготавливаем метаданные для map_partitions. 
    # Dask должен знать структуру выходного датафрейма до начала вычислений.
    meta = ddf._meta.copy()
    for col in ['RecordDate', 'CleanScheduledStr', 'ScheduledDateTime']:
        if col in meta.columns:
            meta = meta.drop(columns=[col])
    meta['ScheduledDateTime'] = pd.to_datetime([])
    
    # Применяем нашу функцию парсинга "24:xx" ко всем партициям параллельно
    ddf_clean = ddf.map_partitions(parse_strange_time, meta=meta)
    
    # Логика обработки переходов через полночь
    # Вычисляем разницу между расписанием и моментом записи
    time_diff = ddf_clean['ScheduledDateTime'] - ddf_clean['RecordedAtTime']
    
    # Ситуация 1: Сейчас начало дня (00:10), а расписание указывает на конец дня (23:50).
    # Разница > 12 часов. Значит, рейс относился к вчерашнему дню.
    mask_prev_day = time_diff > pd.Timedelta(hours=12)
    
    # Ситуация 2: Сейчас конец дня (23:50), а расписание уже на завтра (00:10).
    # Разница < -12 часов. Значит, рейс относится к завтрашнему дню.
    mask_next_day = time_diff < pd.Timedelta(hours=-12)
    
    # Корректируем даты: вычитаем или прибавляем день в зависимости от маски
    ddf_clean['ScheduledDateTime'] = ddf_clean['ScheduledDateTime'] \
                                     - (mask_prev_day.astype(int) * pd.Timedelta(days=1)) \
                                     + (mask_next_day.astype(int) * pd.Timedelta(days=1))
                                     
    return ddf_clean

@task(name="Calculate Delay")
def calculate_delay(ddf: dd.DataFrame):
    logger = get_run_logger()
    logger.info("Calculating delay in seconds...")
    
    # Внимание: формула задержки взята строго из ТЗ.
    # Delay = Scheduled - Expected.
    # Обычно считают наоборот (Actual - Scheduled), но здесь требование: 
    # "Отрицательное значение = автобус опаздывает".
    ddf['delay_seconds'] = (ddf['ScheduledDateTime'] - ddf['ExpectedArrivalTime']).dt.total_seconds()
    
    # Убираем строки, где посчитать задержку не удалось
    ddf = ddf.dropna(subset=['delay_seconds'])
    
    return ddf

@task(name="Aggregate Data")
def aggregate_data(ddf: dd.DataFrame):
    logger = get_run_logger()
    logger.info("Aggregating data by Route and Hour...")
    
    # Выделяем час для группировки
    ddf['hour'] = ddf['ScheduledDateTime'].dt.hour
    
    # Группируем по маршруту и часу.
    # Dask ленивый, эти операции пока только планируются.
    agg_ddf = ddf.groupby(['PublishedLineName', 'hour']).agg(
        avg_delay=('delay_seconds', 'mean'),
        total_trips=('VehicleRef', 'count'),
        # Медиану пока не считаем, в распределенном режиме это дорогая операция
    ).reset_index()
    
    logger.info("Triggering COMPUTE to fetch results from cluster to local memory...")
    # .compute() запускает реальные вычисления на кластере и возвращает результат
    # в виде обычного pandas DataFrame в локальную память.
    result_df = agg_ddf.compute()
    
    logger.info(f"Aggregation complete. Rows: {len(result_df)}")
    return result_df

@task(retries=3, name="Load to Postgres")
def load_to_postgres(df: pd.DataFrame, table_name: str = "route_delays"):
    logger = get_run_logger()
    if df.empty:
        logger.warning("DataFrame is empty, skipping load.")
        return

    logger.info(f"Loading {len(df)} rows to PostgreSQL table '{table_name}'...")
    
    engine = create_engine(DB_CONN_STR)
    
    # Пишем в базу. if_exists='replace' перезатирает таблицу — подходит для идемпотентности или тестов.
    # Для продакшена возможно стоит использовать 'append'.
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
    logger.info("Successfully loaded data.")

# Описание самого пайплайна (Flow)
@flow(name="NYC Bus ETL Flow", log_prints=True)
def etl_flow():
    # Создаем клиент Dask внутри потока, чтобы управлять его жизненным циклом
    logger = get_run_logger()
    logger.info(f"Connecting to Dask Scheduler at {DASK_ADDR}")

    # Используем контекстный менеджер: клиент закроется автоматически при выходе из блока
    with Client(DASK_ADDR) as client:
        logger.info(f"Dask Dashboard: {client.dashboard_link}")
        
        # Шаг 1: Извлечение (ленивое чтение)
        ddf = extract_data()
        
        # Шаг 2: Трансформация (очистка + расчет метрик)
        ddf_clean = clean_times(ddf)
        ddf_calculated = calculate_delay(ddf_clean)
        
        # Шаг 3: Агрегация (здесь происходит compute и данные прилетают в pandas)
        agg_df = aggregate_data(ddf_calculated)
        
        # Шаг 4: Загрузка агрегатов в БД
        load_to_postgres(agg_df)

    # Дополнительная агрегация (из внешней библиотеки), сохраняем в отдельную таблицу
    agg_stops = aggregate_by_stop(ddf_calculated)
    load_to_postgres(agg_stops, table_name="stop_delays")

if __name__ == "__main__":
    # Локальный запуск flow как долгоживущего сервиса.
    # Он сам зарегистрирует деплоймент и будет ждать наступления времени запуска.
    etl_flow.serve(
        name="nyc-bus-daily-etl-manual",
        tags=["etl", "dask", "nyc", "manual"],
        interval=3600  # Интервал повтора в секундах (если нужно частое выполнение)
    )