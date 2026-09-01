"""
env_producer.py
Beehive Monitoring — Topic 2: hive_environment
Streams each record from hive_environment.json to the terminal.

Run AFTER: python generate_hive_env.py
Usage:  python env_producer.py
        python env_producer.py --delay 0.1
        python env_producer.py --hive HIVE-003
        python env_producer.py --loop
"""

import argparse
import json
import sys
import time
from pathlib import Path

DATA_FILE = Path(__file__).parent / "output" / "hive_environment.json"


def format_record(record, index):
    ts = record["timestamp"]
    hid = record["hive_id"]
    temp = record["hive_temp_c"]
    humid = record["hive_humidity_pct"]
    co2 = record["co2_ppm"]
    brood_ok = record["brood_temp_ok"]
    hum_alert = record["humidity_alert"]
    co2_alert = record["co2_alert"]
    trend = record["temp_trend_1h"]

    any_alert = (not brood_ok) or hum_alert or co2_alert

    temp_str = f"{temp:>5.1f}C" if temp is not None else "  N/A "
    humid_str = f"{humid:>5.1f}%" if humid is not None else "  N/A "
    co2_str = f"{co2:>5.0f}ppm" if co2 is not None else "  N/A  "

    brood_tag = "ok " if brood_ok else "!! "
    hum_tag = "ok " if not hum_alert else "!! "
    co2_tag = "ok " if not co2_alert else "!! "

    flag = "ALERT" if any_alert else "    "
    trend_str = f"{trend:>+5.2f}"

    return (f"{flag} [{index:05d}] {ts}  {hid}  "
            f"{temp_str}  {humid_str}  {co2_str}  "
            f"T:{brood_tag} H:{hum_tag} C:{co2_tag}  dT:{trend_str}C/h")


def stream(args):
    if not DATA_FILE.exists():
        print(f"Data file not found: {DATA_FILE}")
        print("Run: python generate_hive_env.py  first.")
        sys.exit(1)

    kafka_producer = None
    kafka_topic = args.topic
    if args.kafka:
        from kafka import KafkaProducer
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=args.broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            kafka_producer.partitions_for(kafka_topic)
            print(f"Kafka connected  ->  {kafka_topic} @ {args.broker}")
        except Exception as e:
            print(f"Kafka unavailable: {e}")
            print("Continuing terminal-only mode.\n")
            kafka_producer = None

    print(f"Streaming {DATA_FILE.name}  |  delay={args.delay}s"
          + (f"  |  hive={args.hive}" if args.hive else "")
          + (f"  |  loop=on" if args.loop else ""))
    print("-" * 95)

    count = 0
    try:
        while True:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    record = json.loads(line)

                    if args.hive and record["hive_id"] != args.hive:
                        continue

                    count += 1
                    print(format_record(record, count))

                    if kafka_producer:
                        kafka_producer.send(kafka_topic, value=record)

                    time.sleep(args.delay)

            if not args.loop:
                break

            print("\n-- restarting from beginning --\n")

    except KeyboardInterrupt:
        print(f"\nStopped after {count} records.")

    if kafka_producer:
        kafka_producer.flush()
        kafka_producer.close()

    print("Done.")


def main():
    parser = argparse.ArgumentParser(
        description="Stream hive_environment data to terminal"
    )
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Seconds between records (default: 1.0)")
    parser.add_argument("--hive", type=str, default=None,
                        help="Filter to one hive_id, e.g. HIVE-003")
    parser.add_argument("--loop", action="store_true",
                        help="Restart from beginning when file ends")
    parser.add_argument("--kafka", action="store_true",
                        help="Also send records to Kafka topic")
    parser.add_argument("--topic", type=str, default="hive_environment",
                        help="Kafka topic name (default: hive_environment)")
    parser.add_argument("--broker", type=str, default="localhost:9092",
                        help="Kafka broker address (default: localhost:9092)")
    args = parser.parse_args()
    stream(args)


if __name__ == "__main__":
    main()
