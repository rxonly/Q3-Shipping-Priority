# Отчёт по практической №1 - Lakehouse (MinIO + Lakekeeper + Trino)

## 1. Вариант и масштаб

| Поле | Значение |
|---|---|
| ФИО (как в VK Education) | Толмачев Михаил Сергеевич |
| Вариант (запрос) | Q3 «Shipping Priority» |
| Масштаб | sf1 |
| Почему выбрал такой масштаб | sf1 ≈ 6 млн строк в lineitem, 1.5 млн в orders, 150 тыс. в customer. Достаточно, чтобы разница между кодеками и форматами была видна в МБ и секундах, и при этом всё считается на ноутбуке за минуты. sf0.01 слишком мал (файлы по килобайтам, замеры шумят), sf10+ упрётся в RAM одного контейнера Trino. |

## 2. Поднятые сервисы

| Сервис | Роль в стеке | Почему именно он |
|---|---|---|
| MinIO | S3-совместимое объектное хранилище, сюда физически пишутся parquet/orc | Открытый код, полная совместимость с S3 API (Trino/Spark/Superset работают без доработок), один контейнер, веб-консоль для просмотра файлов. Альтернатива HDFS тяжелее в развёртывании, настоящий S3 стоит денег. |
| PostgreSQL 16 | БД метаданных Lakekeeper | Lakekeeper хранит в ней состояние каталога (таблицы, снапшоты, warehouse). Надёжная ACID-БД, нужная только для метаданных, сами данные в ней не лежат. |
| Lakekeeper | Iceberg REST Catalog | Реализует открытый REST-протокол Iceberg: каталог не привязан к движку, к нему можно подключить Trino, Spark, Doris. Лёгкий (Rust), в отличие от Hive Metastore не тянет за собой Hadoop. |
| Trino 476 | SQL-движок | Iceberg-коннектор с REST-каталогом, встроенный генератор TPC-H (`tpch.sf1`) - не нужно качать данные, ANSI SQL, быстрые MPP-джойны. |
| Superset (профиль `bi`) | BI/дашборд поверх Gold, часть 2 | Открытый код, коннектор к Trino, быстрая сборка чартов. |

### Скрипты запуска

```bash
docker compose up -d                 # MinIO, PostgreSQL, Lakekeeper (+migrate), Trino
bash scripts/setup_catalog.sh        # bootstrap Lakekeeper + warehouse "lakehouse" на бакете MinIO
docker ps                            # проверка: minio, pg, lakekeeper, trino в Up
docker compose --profile bi up -d --build superset   # часть 2
```

Бакет `lakehouse` создаёт одноразовый контейнер `minio-init`; миграции БД - `lakekeeper-migrate`. Конфиг каталога Trino - `trino/catalog/lakehouse.properties` (Iceberg REST → Lakekeeper, S3 → MinIO, path-style), `tpch.properties` - источник данных.

## 3. Разбор запроса Q3

| Таблица TPC-H | Нужные колонки | Что запрос с ними делает |
|---|---|---|
| customer | custkey, mktsegment | custkey - джойн с orders; mktsegment - фильтр `= 'BUILDING'` |
| orders | orderkey, custkey, orderdate, shippriority | custkey/orderkey - джойны; orderdate - фильтр `< 1995-03-15`; orderdate и shippriority - вывод и группировка |
| lineitem | orderkey, extendedprice, discount, shipdate | orderkey - джойн; shipdate - фильтр `> 1995-03-15`; extendedprice, discount - агрегат `sum(extendedprice*(1-discount))` |

Остальные колонки (name, address, comment и т.д.) не нужны, поэтому в Bronze не загружаются. Колоночный формат тогда читает меньше байт.

## 4. Медальон

Скрипты: `sql/01_bronze.sql`, `02_silver.sql`, `03_gold.sql`.
Строки: `SELECT count(*) FROM …`; вес: `SHOW STATS` или консоль MinIO → бакет `lakehouse`.

### Bronze

| Таблица | Колонки | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---|---|---|---|---|---|
| bronze.customer | custkey, name, nationkey, acctbal, mktsegment | 150 000 | 1.0 | PARQUET | ZSTD | нет | Маленькая таблица, партиции только плодили бы мелкие файлы. |
| bronze.orders | orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, shippriority | 1 500 000 | 13.6 | PARQUET | ZSTD | нет | Данные «как есть», без преобразований; без партиций, чтобы Bronze оставался простым снимком источника. |
| bronze.lineitem | orderkey, linenumber, extendedprice, discount, shipdate | 6 001 215 | 39.6 | PARQUET | ZSTD | нет | Самая большая таблица; колоночный Parquet читает только 4 нужных колонки, ZSTD даёт хороший коэффициент сжатия. |

Parquet выбран потому, что запрос аналитический (много строк, мало колонок, агрегаты): колоночное хранение и статистика min/max по row group позволяют пропускать данные.

### Silver

| Таблица | Колонки | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---|---|---|---|---|---|
| silver.order_lines | orderkey, linenumber, extendedprice, discount, line_revenue, shipdate, orderdate, shippriority, mktsegment | 30 519 | 0.4 | PARQUET | ZSTD | month(orderdate) | Применены фильтры Q3 (BUILDING, orderdate/shipdate), нормализация сегмента (`upper(trim)`), проверка discount ∈ [0,1], обогащение `line_revenue`. Партиция по месяцу orderdate: диапазон дат ограничен, фильтр `orderdate < 1995-03-15` отсекает партиции целиком. |

### Gold

| Таблица | Колонки | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---|---|---|---|---|---|
| gold.shipping_priority_top10 | orderkey, revenue, orderdate, shippriority | 10 | <0.01 | PARQUET | ZSTD | нет | 10 строк; витрина читается BI-инструментом целиком, партиционировать нечего. |

## 5. Тесты производительности

Прогон: `bash scripts/bench.sh` (создаёт схемы `bench_<формат>_<кодек>`, замеряет время записи, среднее по 3 запускам Q3 и размер данных, пишет `benchmark_results.csv`). Время запроса также видно в `system.runtime.queries`.

| Формат | Сжатие | Время записи, с | Время запроса Q3, с | Размер Bronze, МБ |
|---|---|---|---|---|
| PARQUET | NONE | 4.9 | 1.63 | 136.9 |
| PARQUET | SNAPPY | 4.5 | 1.24 | 78.7 |
| PARQUET | GZIP | 7.4 | 1.42 | 54.6 |
| PARQUET | ZSTD | 3.9 | 1.19 | 54.2 |
| ORC | NONE | 4.5 | 1.14 | 139.4 |
| ORC | SNAPPY | 3.5 | 1.04 | 82.2 |
| ORC | GZIP | 5.0 | 1.19 | 58.0 |
| ORC | ZSTD | 3.8 | 1.09 | 59.3 |

Время записи - три CTAS (customer, orders, lineitem), время запроса - среднее из 3 запусков Q3 по Bronze-таблицам соответствующей схемы, размер - сумма файлов трёх таблиц.

**Вывод.** Без сжатия Parquet весит 136.9 МБ и даёт самый медленный запрос (1.63 с): данных читается больше, а CPU на распаковку экономится слабо, поэтому сжатие здесь ускоряет запрос, а не замедляет. Среди Parquet-вариантов лучший - ZSTD: самый маленький размер (54.2 МБ, в 2.5 раза меньше, чем без сжатия), самая быстрая запись (3.9 с) и самый быстрый запрос (1.19 с). GZIP сжимает почти так же (54.6 МБ), но пишет почти вдвое дольше (7.4 с против 3.9 с) и запрос у него медленнее (1.42 с), то есть выигрыша в размере нет, а по CPU он дороже. SNAPPY быстро пишет (4.5 с), но файлы на 45% больше, чем у ZSTD (78.7 против 54.2 МБ). Сжатие критично для размера: ORC без сжатия весит 139.4 МБ, а с ZSTD - 59.3 МБ, то есть в 2.4 раза меньше. ORC в этом тесте запрашивается чуть быстрее Parquet (1.04-1.19 с против 1.19-1.42 с), но разница около 0.1-0.2 с на sf1 при одной машине и трёх запусках находится в пределах шума, а по размеру лучший Parquet (54.2 МБ) компактнее лучшего ORC (58.0 МБ у GZIP). Для Bronze выбран Parquet + ZSTD как лучший баланс размера и скорости.

## 6. Часть 2

### 6.1. Понять и объяснить

| Вопрос | Ответ |
|---|---|
| Зачем нужны большие данные и lakehouse? | Объёмы и разнообразие данных растут быстрее, чем масштабируется одна БД. Lakehouse хранит данные в дешёвом объектном хранилище в открытых форматах (Parquet) и добавляет поверх табличный слой Iceberg: ACID-транзакции, схему, снапшоты. Хранение и вычисления разделены, данные не заперты в одном движке (Trino, Spark, Doris читают одни и те же таблицы), нет дублирования между «озером» и «хранилищем». |
| Schema evolution на Silver | `sql/04_evolution_timetravel.sql`: `ADD COLUMN discount_pct`, затем `RENAME COLUMN` на `discount_percent`. Iceberg ведёт колонки по id, поэтому переименование меняет только метаданные, parquet-файлы не переписываются, старые снапшоты остаются читаемыми. |
| Восстановление из снапшота | см. ниже |

```sql
-- список снапшотов
SELECT snapshot_id, committed_at, operation FROM lakehouse.silver."order_lines$snapshots" ORDER BY committed_at;
-- чтение на момент снапшота
SELECT count(*) FROM lakehouse.silver.order_lines FOR VERSION AS OF <snapshot_id>;
-- откат таблицы
CALL lakehouse.system.rollback_to_snapshot('silver', 'order_lines', <snapshot_id>);
```

### 6.2. Инструмент: Apache Superset

| Инструмент | Почему выбрал |
|---|---|
| Superset | Открытый код, готовый коннектор к Trino, быстро строится чарт по Bronze/Gold, не нужно писать код. Для задачи (отчёт по Q3) BI-слой подходит лучше, чем второй SQL-движок. |

Развёртывание: `docker compose --profile bi up -d --build superset` (образ из `superset/Dockerfile` с драйвером `trino`).
Первичная настройка (один раз): `docker exec -it superset superset fab create-admin --username admin --firstname a --lastname a --email a@a.a --password admin && docker exec superset superset db upgrade && docker exec superset superset init`.

Действия в UI: http://localhost:8088 → Settings → Database Connections → + Database → Trino, SQLAlchemy URI `trino://admin@trino:8080/lakehouse` → SQL Lab → `SELECT mktsegment, count(*) FROM bronze.customer GROUP BY 1` - видны данные Bronze из MinIO.

## 7. Выводы

Медальон разделяет ответственность: Bronze хранит сырые данные, Silver - очищенный и обогащённый факт, Gold - маленькую витрину под конкретный отчёт. Колоночный Parquet вместе с Iceberg подходит для аналитики: читаются только нужные колонки, метаданные дают снапшоты и безопасную эволюцию схемы. Сжатие меняет размер и CPU-нагрузку, поэтому кодек выбирался по замерам, а не по привычке (ожидаемо ZSTD). Открытый стек MinIO + Lakekeeper + Trino заменяет облачный lakehouse без привязки к вендору.
