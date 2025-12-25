import dask.dataframe as dd
import pandas as pd
import numpy as np

def fix_scheduled_time(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Чистит данные: исправляет некорректный формат '24:xx:xx' и синхронизирует даты.
    """
    # Сначала приводим колонки к типу datetime, где это возможно стандартными средствами
    ddf['RecordedAtTime'] = dd.to_datetime(ddf['RecordedAtTime'])
    ddf['ExpectedArrivalTime'] = dd.to_datetime(ddf['ExpectedArrivalTime'], errors='coerce')
    
    # Эта функция будет применяться к каждому куску данных (партиции) отдельно.
    # Dask работает лениво, поэтому мы упаковываем логику обработки строк внутрь этой функции.
    def parse_partition(df):
        # ... (Код из нашего успешного решения выше) ...
        # Здесь мы боремся с форматом '24:00', который Pandas не понимает.
        # Заменяем 24:xx на 00:xx и ставим флаг, что это уже следующий день.
        sched_str = df['ScheduledArrivalTime'].astype(str).fillna('00:00:00')
        is_24 = sched_str.str.startswith('24')
        
        norm_time = sched_str.copy()
        mask_24 = norm_time.str.startswith('24')
        norm_time.loc[mask_24] = '00' + norm_time.loc[mask_24].str[2:]
        
        # Собираем полную дату из Recorded-даты и Scheduled-времени
        date_str = df['RecordedAtTime'].dt.date.astype(str)
        # errors='coerce' превратит мусор в NaT, чтобы не сломать пайплайн
        base_dt = pd.to_datetime(date_str + ' ' + norm_time, errors='coerce')
        
        # Если время было '24:xx', прибавляем 1 день к дате
        final_dt = base_dt + pd.Timedelta(days=1) * is_24.astype(int)
        
        df['ScheduledDateTime'] = final_dt
        return df

    # Dask должен знать структуру данных на выходе до начала вычислений.
    # Создаем пустой шаблон метаданных с новой колонкой.
    meta = ddf._meta.copy()
    meta['ScheduledDateTime'] = pd.to_datetime([])
    
    # Применяем функцию ко всем партициям
    ddf = ddf.map_partitions(parse_partition, meta=meta)

    # Логика "Перехода через полночь" (Midnight Crossing)
    # Бывает, что автобус записан в 23:55 (вчера), а по расписанию он в 00:05 (сегодня).
    # Или наоборот. Если разница между датами более 12 часов — значит, день определен неверно.
    
    time_diff = ddf['ScheduledDateTime'] - ddf['RecordedAtTime']
    
    # Если расписание убежало вперед на >12ч -> значит оно относится к вчерашнему дню
    mask_prev = time_diff > pd.Timedelta(hours=12)
    # Если расписание отстало на >12ч -> значит оно относится к завтрашнему дню
    mask_next = time_diff < pd.Timedelta(hours=-12)
    
    # Корректируем дату расписания (векторизированный сдвиг дней)
    ddf['ScheduledDateTime'] = ddf['ScheduledDateTime'] \
                               - (mask_prev.astype(int) * pd.Timedelta(days=1)) \
                               + (mask_next.astype(int) * pd.Timedelta(days=1))
                               
    return ddf

def calculate_delay(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Вычисляет задержку в секундах согласно ТЗ.
    """
    # Важный момент бизнес-логики:
    # Отрицательное значение = опоздание. 
    # Формула: Scheduled (по плану) - Expected (ожидаемое/фактическое).
    # Пример: План 12:00, Факт 12:10. Результат: -600 секунд (опоздал).
    
    ddf['delay_seconds'] = (ddf['ScheduledDateTime'] - ddf['ExpectedArrivalTime']).dt.total_seconds()
    
    # Фильтруем аномальные выбросы (ошибки GPS или данных).
    # Оставляем только те записи, где задержка в пределах ±5 часов (18000 сек).
    ddf = ddf[(ddf['delay_seconds'] > -18000) & (ddf['delay_seconds'] < 18000)]
    return ddf

def aggregate_by_route(ddf: dd.DataFrame) -> pd.DataFrame:
    """
    Готовит данные для графиков: группировка по Маршруту и Часу.
    """
    ddf['hour'] = ddf['ScheduledDateTime'].dt.hour
    
    # Ленивая группировка (пока ничего не вычисляется)
    agg = ddf.groupby(['PublishedLineName', 'hour']).agg(
        avg_delay=('delay_seconds', 'mean'),
        total_trips=('VehicleRef', 'count')
    ).reset_index()
    
    # .compute() запускает реальный расчет на кластере и возвращает обычный Pandas DataFrame
    return agg.compute()

def aggregate_by_stop(ddf: dd.DataFrame) -> pd.DataFrame:
    """
    Готовит данные для карты: группировка по Остановкам.
    Поскольку у нас нет словаря координат остановок, мы вычисляем их приблизительно.
    """
    # Группируем по названию остановки, к которой едет автобус
    agg = ddf.groupby(['NextStopPointName']).agg(
        avg_delay=('delay_seconds', 'mean'),
        # Лайфхак: берем среднюю координату всех автобусов, направляющихся к этой остановке.
        # Это даст достаточно точную точку на карте.
        lat=('VehicleLocation.Latitude', 'mean'), 
        lon=('VehicleLocation.Longitude', 'mean'),
        count=('VehicleRef', 'count')
    ).reset_index()
    
    return agg.compute()