"""
env_consumer.py
Beehive Monitoring — Topic 2: hive_environment

Consumes hive environment sensor data from Kafka topic 'hive_environment'
and stores each record in MySQL for Grafana dashboard visualisation.

Run AFTER: python env_stream.py --delay 0.1   (to feed data into Kafka)
Usage:     python env_consumer.py
"""

import json
import os
import time
from datetime import datetime, timezone

import mysql.connector
from kafka import KafkaConsumer


MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
DB_NAME = "sda_course_env"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "hive_environment"
GROUP_ID = os.getenv("KAFKA_GROUP", "env-consumer-group")

PURGE_INTERVAL = 100000
RETENTION_DAYS = 36500

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS hive_readings (
        id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
        ts                      DATETIME       NOT NULL,
        hive_id                 VARCHAR(20)    NOT NULL,
        hive_temp_c             DOUBLE         NULL,
        hive_humidity_pct       DOUBLE         NULL,
        co2_ppm                 DOUBLE         NULL,
        outside_temp_c          DOUBLE         NULL,
        outside_humidity_pct    DOUBLE         NULL,
        brood_temp_ok           TINYINT(1)     DEFAULT 1,
        humidity_alert          TINYINT(1)     DEFAULT 0,
        co2_alert               TINYINT(1)     DEFAULT 0,
        thermoregulation_active TINYINT(1)     DEFAULT 0,
        temp_trend_1h           DOUBLE         DEFAULT 0,
        apiary_id               VARCHAR(50)    DEFAULT 'APIARY-NORTH',
        latitude                DOUBLE         DEFAULT 40.7128,
        longitude               DOUBLE         DEFAULT -74.006,
        ingested_at             TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_ts (ts),
        INDEX idx_hive (hive_id),
        INDEX idx_hive_ts (hive_id, ts)
    )
    """,
]

INSERT_SQL = """
INSERT INTO hive_readings (
    ts, hive_id, hive_temp_c, hive_humidity_pct, co2_ppm,
    outside_temp_c, outside_humidity_pct,
    brood_temp_ok, humidity_alert, co2_alert,
    thermoregulation_active, temp_trend_1h,
    apiary_id, latitude, longitude
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s,
    %s, %s, %s,
    %s, %s,
    %s, %s, %s
)
"""

PURGE_SQL = "DELETE FROM hive_readings WHERE ts < NOW() - INTERVAL %s DAY"


def parse_ts(ts_str):
    """Convert ISO 8601 timestamp string to MySQL DATETIME format."""
    ts_str = ts_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(ts_str)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def setup_mysql():
    """Create database, table, and Grafana read-only user."""
    db = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
    )
    cursor = db.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cursor.execute(f"USE {DB_NAME}")
    for stmt in SCHEMA_STATEMENTS:
        cursor.execute(stmt)
    cursor.execute(
        "CREATE USER IF NOT EXISTS 'grafana'@'%' IDENTIFIED BY 'grafana'"
    )
    cursor.execute(f"GRANT SELECT ON {DB_NAME}.* TO 'grafana'@'%'")
    db.commit()
    return db, cursor


def format_record(record, index):
    """Format a record for terminal display."""
    ts = record.get("timestamp", "?")
    hid = record.get("hive_id", "?")
    temp = record.get("hive_temp_c")
    humid = record.get("hive_humidity_pct")
    co2 = record.get("co2_ppm")
    brood_ok = record.get("brood_temp_ok", True)
    hum_alert = record.get("humidity_alert", False)
    co2_alert = record.get("co2_alert", False)
    trend = record.get("temp_trend_1h", 0.0)

    any_alert = (not brood_ok) or hum_alert or co2_alert

    temp_str = f"{temp:>5.1f}C" if temp is not None else "  N/A "
    humid_str = f"{humid:>5.1f}%" if humid is not None else "  N/A "
    co2_str = f"{co2:>5.0f}ppm" if co2 is not None else "  N/A  "

    brood_tag = "ok " if brood_ok else "!! "
    hum_tag = "ok " if not hum_alert else "!! "
    co2_tag = "ok " if not co2_alert else "!! "

    flag = "ALERT" if any_alert else "    "
    trend_str = f"{trend:>+5.2f}"

    return (
        f"{flag} [{index:05d}] {ts}  {hid}  "
        f"{temp_str}  {humid_str}  {co2_str}  "
        f"T:{brood_tag} H:{hum_tag} C:{co2_tag}  dT:{trend_str}C/h"
    )


# ── MySQL setup ───────────────────────────────────────────────────────────────
db, cursor = setup_mysql()

# ── Kafka consumer ────────────────────────────────────────────────────────────
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=GROUP_ID,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Environment consumer started")
print(f"   Topic : {TOPIC}")
print(f"   MySQL : {DB_NAME}")
print("=" * 95)

count = 0

try:
    for message in consumer:
        record = message.value
        try:
            ts_dt = parse_ts(record["timestamp"])
            cursor.execute(
                INSERT_SQL,
                (
                    ts_dt,
                    record["hive_id"],
                    record.get("hive_temp_c"),
                    record.get("hive_humidity_pct"),
                    record.get("co2_ppm"),
                    record.get("outside_temp_c"),
                    record.get("outside_humidity_pct"),
                    1 if record.get("brood_temp_ok", True) else 0,
                    1 if record.get("humidity_alert", False) else 0,
                    1 if record.get("co2_alert", False) else 0,
                    1 if record.get("thermoregulation_active", False) else 0,
                    record.get("temp_trend_1h", 0.0),
                    record.get("apiary_id", "APIARY-NORTH"),
                    record.get("latitude", 40.7128),
                    record.get("longitude", -74.006),
                ),
            )
            db.commit()
            consumer.commit()

            count += 1
            print(format_record(record, count))

            if count % PURGE_INTERVAL == 0:
                cursor.execute(PURGE_SQL, (RETENTION_DAYS,))
                db.commit()
                print(f"  ... purged records older than {RETENTION_DAYS} days "
                      f"({cursor.rowcount} rows removed)")

        except mysql.connector.Error as e:
            db.rollback()
            print(f"  MySQL error: {e}")
        except (KeyError, ValueError) as e:
            print(f"  Bad record: {e}")

except KeyboardInterrupt:
    print(f"\nStopped after {count} records.")
finally:
    consumer.close()
    cursor.close()
    db.close()
    print("Done.")
