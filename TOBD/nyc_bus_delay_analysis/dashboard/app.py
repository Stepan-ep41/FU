import streamlit as st
import pandas as pd
import plotly.express as px
import os
from sqlalchemy import create_engine
import folium
from streamlit_folium import st_folium

# Настраиваем параметры страницы и подключение к базе данных.
# Переменные окружения берутся из Docker-контейнера.
st.set_page_config(page_title="NYC Bus Analytics", layout="wide")

DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "bus_delay_pass")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres") # Хост "postgres" доступен внутри сети Docker
DB_NAME = os.getenv("POSTGRES_DB", "nyc_bus")

# Если переменной нет (локальный запуск без Docker), можно переключиться на localhost
# if not os.getenv("POSTGRES_HOST"):
#     DB_HOST = "localhost"

CONN_STR = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

# Функция загрузки данных с кэшированием.
# Streamlit перезапускает весь скрипт при каждом клике, поэтому без кэша (ttl=300 сек)
# мы бы "ложили" базу одинаковыми запросами.
@st.cache_data(ttl=300)
def load_data(query):
    engine = create_engine(CONN_STR)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

# Отрисовка заголовков и вводной части интерфейса
st.title("🚌 NYC Bus Delay Explorer")
st.markdown("Аналитика задержек общественного транспорта Нью-Йорка")

# Fail-fast: сразу проверяем, жива ли база, чтобы не рисовать пустой интерфейс с ошибками
try:
    engine = create_engine(CONN_STR)
    engine.connect()
except Exception as e:
    st.error(f"Ошибка подключения к БД: {e}")
    st.stop()

# Настройка боковой панели (сайдбара) для фильтрации
st.sidebar.header("Фильтры")

# Сначала получаем список всех доступных маршрутов для выпадающего списка
routes_df = load_data("SELECT DISTINCT \"PublishedLineName\" FROM route_delays ORDER BY \"PublishedLineName\"")
if not routes_df.empty:
    selected_route = st.sidebar.selectbox("Выберите маршрут", routes_df["PublishedLineName"])
else:
    st.warning("Нет данных в БД.")
    st.stop()

# Основная область аналитики: детализация по выбранному маршруту
st.header(f"Анализ маршрута: {selected_route}")

# Забираем почасовую статистику только для выбранного автобуса
df_route = load_data(f"""
    SELECT hour, avg_delay, total_trips 
    FROM route_delays 
    WHERE "PublishedLineName" = '{selected_route}'
    ORDER BY hour
""")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Средняя задержка по часам")
    if not df_route.empty:
        # Визуальная коррекция:
        # В базе "опоздание" — это отрицательное число (напр. -600 сек).
        # Для графика людям привычнее: "Столбик вверх = Опоздание".
        # Поэтому умножаем на -1 и переводим секунды в минуты.
        df_route['delay_min'] = df_route['avg_delay'] * (-1) / 60
        
        fig = px.bar(df_route, x='hour', y='delay_min', 
                     title="Минуты опоздания (avg)",
                     labels={'delay_min': 'Минуты ( >0 опоздание)', 'hour': 'Час суток'})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Нет данных для графика.")

with col2:
    st.subheader("Топ-5 самых проблемных маршрутов")
    # Сравниваем наш маршрут с "худшими" по всему городу.
    # Сортируем по возрастанию (ASC), так как самое большое опоздание — это самое маленькое число (напр. -1000).
    top5_df = load_data("""
        SELECT "PublishedLineName", AVG(avg_delay) as global_avg 
        FROM route_delays 
        GROUP BY "PublishedLineName" 
        ORDER BY global_avg ASC 
        LIMIT 5
    """)
    
    if not top5_df.empty:
        top5_df['Avg Delay (min)'] = top5_df['global_avg'] * (-1) / 60
        st.table(top5_df[["PublishedLineName", "Avg Delay (min)"]])

# Блок с гео-аналитикой: отображаем проблемные остановки на карте
st.header("Карта задержек по остановкам")
st.markdown("Красные точки — сильные задержки, Зеленые — всё ок.")

# Пытаемся загрузить данные из таблицы остановок. Она создается отдельным шагом в ETL.
try:
    stops_df = load_data(f"""
        SELECT "NextStopPointName", lat, lon, avg_delay 
        FROM stop_delays 
        WHERE "NextStopPointName" IS NOT NULL
        LIMIT 500
    """)
    
    if not stops_df.empty:
        # Инициализируем карту с центром в Нью-Йорке
        m = folium.Map(location=[40.7128, -74.0060], zoom_start=11)
        
        for _, row in stops_df.iterrows():
            # Раскрашиваем маркеры:
            # avg_delay < -600 (меньше минус 10 минут) -> Опаздывает сильно (Красный)
            # avg_delay около 0 -> Зеленый
            delay_sec = row['avg_delay']
            color = "green"
            if delay_sec < -600: color = "red"
            elif delay_sec < -300: color = "orange"
            
            folium.CircleMarker(
                location=[row['lat'], row['lon']],
                radius=5,
                color=color,
                fill=True,
                fill_opacity=0.7,
                tooltip=f"{row['NextStopPointName']}: {int(delay_sec)}s"
            ).add_to(m)
            
        # Рендерим карту Folium внутри Streamlit
        st_folium(m, width=700, height=500)
    else:
        st.warning("Таблица stop_delays пуста или не существует.")

except Exception:
    st.warning("Таблица stop_delays еще не создана ETL-пайплайном.")