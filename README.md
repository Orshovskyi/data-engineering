# Data Engineering — лабораторні роботи (Kafka, Kafka Streams, Iceberg)

Монорепозиторій з Maven-модулями та Docker Compose для лабораторних №3–5.

## Вимоги

- [Docker](https://docs.docker.com/get-docker/) і Docker Compose v2
- [JDK 21](https://adoptium.net/) і [Maven 3.9+](https://maven.apache.org/) — для локальної збірки Java
- Для скрипта налаштування Polaris: `curl`, `jq`

## Структура проєкту

| Шлях | Опис |
|------|------|
| `pom.xml` | Багатомодульний батьківський POM |
| `producer/` | **Лаб. №3** — Java-продюсер: CSV → JSON у Kafka (`Topic1`, `Topic2`) |
| `streams/` | **Лаб. №4** — Spring Boot + Kafka Streams, агрегації → топики `lab4-*` |
| `trino/catalog/iceberg.properties` | **Лаб. №5** — каталог Trino для Iceberg через Polaris REST |
| `scripts/polaris-setup.sh` | Ініціалізація каталогу Polaris і RBAC |
| `Divvy_Trips_2019_Q4.csv` | Джерело даних для продюсера (у корені репозиторію) |
| `docker-compose.yml` | Повний стек: Kafka + озерне сховище |

## Швидкий старт (Docker)

```bash
docker compose up --build
```

Після старту виконайте налаштування Polaris (один раз або після очищення томів):

```bash
./scripts/polaris-setup.sh
```

## Порти та веб-інтерфейси

| Служба | URL | Примітки |
|--------|-----|----------|
| Trino | http://localhost:8080 | SQL / UI |
| Kafka UI | http://localhost:18080 | Порт **18080** (8080 зайнятий Trino) |
| MinIO Console | http://localhost:9001 | Логін: `admin`, пароль: `password` |
| MinIO S3 API | http://localhost:9000 | |
| Polaris REST | http://localhost:8181 | OAuth / каталог Iceberg |
| Kafka (хост) | `localhost:9092`, `localhost:9093` | До брокерів з машини розробника |

## Лабораторна №3 — продюсер

- Читає `Divvy_Trips_2019_Q4.csv`, публікує кожен рядок як JSON у **Topic1** і **Topic2**.
- Змінні середовища в `docker-compose` (сервіс `producer`): `MAX_RECORDS` (0 = усі рядки), `CSV_FILE`, `KAFKA_BOOTSTRAP_SERVERS`.

Локальна збірка:

```bash
mvn -pl producer -am -DskipTests package
java -jar producer/target/kafka-lab-producer-1.0.0.jar
```

## Лабораторна №4 — Kafka Streams

- Читає **Topic1**, рахує метрики по днях, пише в:

  - `lab4-avg-duration-by-day`
  - `lab4-trip-count-by-day`
  - `lab4-top-start-station-by-day`
  - `lab4-top3-stations-by-day`

- Образ `streams` зібраний на **Debian JRE** (не Alpine) — потрібно для RocksDB у Kafka Streams.

Локально:

```bash
mvn -pl streams -am -DskipTests package
export SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093
java -jar streams/target/kafka-lab-streams-1.0.0.jar
```

Налаштування топиків: `streams/src/main/resources/application.yml` (`lab4.kafka.*`).

## Лабораторна №5 — Iceberg / Polaris / MinIO / Trino

1. Підніміть стек: `docker compose up -d`.
2. Запустіть `./scripts/polaris-setup.sh` (створює каталог `polariscatalog`, мінімальні права для `root` через REST API Polaris **1.x** — шляхи з дефісами: `catalog-roles`, `principal-roles`).
3. Підключення до Trino:

```bash
docker compose exec -it trino trino --server localhost:8080 --catalog iceberg
```

Приклад (схема як namespace у Polaris):

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.db;
CREATE TABLE iceberg.db.customers (
  customer_id BIGINT,
  first_name VARCHAR,
  last_name VARCHAR,
  email VARCHAR
);
```

Дані Iceberg зберігаються в MinIO (бакет `warehouse`); метадані — через Polaris.

### Скидання середовища

```bash
docker compose down -v
```

Також можна видалити локальну теку `minio_data/` (вона в `.gitignore`).

## Збірка з кореня

```bash
mvn clean package -DskipTests
```

## Усунення несправностей

- **Polaris unhealthy** у старих інструкціях: перевірка йде на `8182/q/health/live`, а не на `/healthcheck` порту 8181.
- **Trino не створює таблицю (403)** — переконайтеся, що `./scripts/polaris-setup.sh` відпрацював без помилок на кроках grant/link (коректні URL з `catalog-roles` / `principal-roles`).
- **Два продюсери підряд** — у Topic1 може накопичитися подвійна кількість повідомлень; для «чистого» прогону використовуйте `docker compose down -v` і знову `up`.

## Ліцензія та дані

CSV **Divvy Trips** — відкриті дані Divvy Bikes (Chicago); використовуйте відповідно до їхніх умов походження даних.
