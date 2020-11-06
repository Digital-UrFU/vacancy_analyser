# Демонстрационная версия сервиса Data Lake востребованных рынком компетенций цифровой экономики

Набор инструментов для сбора данных о востребованных компетенциях цифровой экономики. Создан Уральским федеральным университетом в проекте разработки модели цифрового университета.

## Зачем это нужно

Сейчас образовательные программы в россйиских университетах строятся по принципу "что знаем, то и преподаем". Мы хотим изменить подход к созданию образовательных программ, чтобы студенты изучали цифровые технологии, которые востребованы работодателями. Первый шаг на этом пути: изнать, что именно нужно работодателям. Именно для этого разнаботан наш инструмент - он позволяет загружать данные о востребованных компетенциях из Интернет на регулярной основе.

## Что есть в демонстрационной версии

В состав входят инструменты для загрузки актуальных и исторических данных о вакансиях. Данные скачиваются с сайта hh.ru, используя публичный API.

После скачивания данных записываются в два хранилища:
- Реляционную базу данных PostgreSQL.
- Распределенную файловую систему HDFS в формате Parquet для анализа с помощью Hadoop и PySpark (в демонстрационной версии используется один узел). 

База данных и сервисы Hadoop разворачиваются вместе с набором инструментов с помощью docker-compose.

В состав входит Jupyter Notebook с установленным PySpark, позволяющий разрабатывать алгоритмы обработки данных на языке Python.

Для построения языковой модели система однократно скачивает все статьи с сайта habr.com.

## Установка

Рекомендуется зарегистрировать доменное имя, чтобы браузер считал HTTPS-соединения защищёнными.

1. `apt update && apt install -y docker-compose`
2. `git clone https://github.com/Digital-UrFU/vacancy_analyser.git && cd vacancy_analyser`
3. `./first_time_setup.sh` # будут сгенерированы пароли, их необходимо записать. Скрипт предоставит возможность ввести доменное имя
4. `docker-compose up -d`
5. `docker-compose restart apache`

## Адреса сервисов

Пароли ко всем сервисам выводятся при установке, после выполнения `./first_time_setup.sh`

1. Jupyter с PySpark доступен по адресу https://ваш-хост/
2. К Postgres можно подключиться командой `psql -h ваш-хост -U vacancy`
3. Веб-интерфейс HDFS доступен по адресу https://yourhost:4430/

## Каталоги с данными

1. ./data # данные о вакансиях в формате csv, обновляются раз в неделю.
2. ./hist_data # исторические данные о вакансиях в формате csv, система делает 1 запрос к API в секунду.
3. ./habr_data # статьи с habr.com в формате csv.
