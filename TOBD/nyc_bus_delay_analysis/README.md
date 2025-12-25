0. Убедиться, что на Вашем устройстве установлен Docker (https://www.docker.com/)
1. Запустить приложение Docker и дождаться статуса "Engine running"
2. Перейти в директорию nyc_bus_delay_analysis в консоли (cd <Ваш_путь>)
3. Первый запуск выполняется командой docker-compose up –build -d
4. Запустить Prefect с помощью docker-compose exec prefect-agent python -m flows.etl_flow (далее только эта команда при запуске)
5. Перейти по ссылке http://localhost:4200 в Prefect flow, перейти во вкладку "Deployments", нажать на три вертикальных точки, нажать в выпадающем списке "quick run"
6. Ссылки:\\
6.1. http://localhost:4200 - просмотр историй запуска Prefect\\
6.2. http://localhost:8787 - просмотр нагрузки на процессор\\
6.3. http://localhost:8501 - streamlit dashboard с отображением результатов\\

