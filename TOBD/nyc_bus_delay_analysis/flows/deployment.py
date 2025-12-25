from flows.etl_flow import etl_flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

# Собираем конфигурацию для деплоя. 
# Эта конструкция связывает код (flow) с расписанием и инфраструктурой.
deployment = Deployment.build_from_flow(
    flow=etl_flow,
    name="nyc-bus-daily-etl",
    version="1.0",
    # Задаем жесткое расписание: старт ровно в 3 часа ночи по времени NY.
    schedule=CronSchedule(cron="0 3 * * *", timezone="America/New_York"),
    # Имя очереди задач. Агент (воркер) должен быть настроен на прослушивание именно этой очереди ("default").
    work_queue_name="default",
    tags=["etl", "dask", "nyc"],
    # Здесь можно прокинуть аргументы в функцию etl_flow. 
    # Оставляем словарь пустым, чтобы использовались значения по умолчанию, прописанные в самой функции.
    parameters={}
)

if __name__ == "__main__":
    # Регистрируем деплоймент на сервере Prefect (или в локальной базе данных).
    deployment.apply()
    print("Deployment applied! Start the agent to execute runs.")