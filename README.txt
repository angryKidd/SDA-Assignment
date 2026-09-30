================================================================================
  BEEHIVE MONITORING SYSTEM - IoT Streaming Data Pipeline
  SDA Assignment 3 | Student: Manas Jaiswal (065091)
  Industry: Agriculture / Beekeeping
================================================================================

PROJECT DESCRIPTION
-------------------
This project implements a real-time beehive monitoring system using IoT sensors
and a streaming data pipeline. Five beehives in an apiary are equipped with
environmental sensors that continuously measure internal temperature, humidity,
and CO2 levels. A custom Python generator produces realistic synthetic sensor
data -- including diurnal cycles, a queenless hive event, rain windows, and
sensor dropouts. The data is streamed to Kafka by a producer script, consumed
into MySQL, and visualised in a Grafana dashboard with eight panels covering
alert counts, time series, and per-hive averages. The system enables beekeepers
to detect critical colony states such as queen loss, high humidity, and CO2
buildup, allowing timely intervention to prevent colony collapse.


ARCHITECTURE
------------
  [Generator] --> [Producer] --> [Kafka Topic] --> [Consumer] --> [MySQL]
                                                                   |
                                                              [Grafana Dashboard]

  1. generate_hive_env.py   Produces 50,400 JSONL records (5 hives x 7 days)
  2. env_producer.py        Streams records to terminal and/or Kafka
  3. env_consumer.py        Reads Kafka, writes rows into MySQL table
  4. Grafana                Reads MySQL via read-only user, renders 8 panels


HOW IT WORKS
------------
1. DATA GENERATOR (generate_hive_env.py)
   - Generates 7 days of synthetic sensor data for 5 hives
   - Records are produced every 1 minute (50,400 total records)
   - Injects realistic events:
     * Queenless hive (HIVE-003): temperature drifts from 34.5C to ambient
       over 48 hours as the colony loses its ability to thermoregulate
     * Rain events: 3 rain windows across the 7 days that suppress foraging
       and raise CO2 levels
     * Sensor dropouts: ~0.1% of records have null values for realism
   - Output: output/hive_environment.json (one JSON object per line)

2. PRODUCER (env_producer.py)
   - Reads the generated JSON file line by line
   - Prints each record to the terminal in a formatted one-line summary
   - Optionally sends each record to a Kafka topic
   - Stops after a configurable number of records (default: 1000)

3. CONSUMER (env_consumer.py)
   - Consumes records from Kafka topic 'hive_environment'
   - Creates database sda_course_env and table hive_readings if missing
   - Creates a read-only MySQL user 'grafana'/'grafana' for Grafana
   - Inserts each record into hive_readings (17 columns)
   - Commits offsets only after successful MySQL insert
   - Configurable consumer group via KAFKA_GROUP env var

4. GRAFANA DASHBOARD
   - Dashboard JSON: dashboard_env.json (import into Grafana)
   - Datasource: MySQL, database sda_course_env, user grafana/grafana
   - 8 panels:
     * Total Sensor Readings (stat)
     * Temperature Alerts
     * CO2 Alert Count
     * Hive Population Coverage
     * Hive Temperature Over Time
     * CO2 Levels Over Time
     * Humidity Over Time
     * Average Metrics by Hive
   - Screenshot: dashboard_screenshot.png


HOW TO RUN
----------
Prerequisites: Docker (MySQL 3306, Kafka 9092), Python 3.8+, Grafana

Step 1: Install dependencies
  > pip install -r requirements.txt

Step 2: Generate the synthetic data
  > python generate_hive_env.py

Step 3: Start the producer (streams to terminal + Kafka, 1000 records)
  > python env_producer.py --kafka --delay 0

  Useful options:
    --delay 0.1          Seconds between records (default: 1.0)
    --limit 1000         Stop after N records (default: 1000)
    --hive HIVE-003      Show only one hive
    --loop               Restart from beginning when file ends
    --topic hive_environment   Kafka topic (default: hive_environment)
    --broker localhost:9092     Kafka broker address

Step 4: Start the consumer (reads Kafka -> writes MySQL)
  > python env_consumer.py

  Environment variables (optional):
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD
    KAFKA_BROKER
    KAFKA_GROUP   (consumer group id, default: env-consumer-group)

Step 5: Import the dashboard into Grafana
  - Grafana URL: http://localhost:3000 (admin/admin)
  - Create datasource: type MySQL, host localhost:3306,
    database sda_course_env, user grafana, password grafana
  - Import dashboard_env.json (Dashboards -> New -> Import)


DATA FORMAT (JSONL)
-------------------
Each line in output/hive_environment.json is a JSON object:

  Field                    Type     Description
  ----------------------   ------   ------------------------------------------
  timestamp                string   UTC timestamp (ISO 8601)
  hive_id                  string   Hive identifier (HIVE-001 to HIVE-005)
  hive_temp_c              float    Internal hive temperature (20-42 C)
  hive_humidity_pct        float    Internal hive humidity (30-90%)
  co2_ppm                  float    CO2 concentration (300-6000 ppm)
  outside_temp_c           float    Ambient outdoor temperature (-10 to 45 C)
  outside_humidity_pct     float    Ambient outdoor humidity (10-100%)
  brood_temp_ok            boolean  True if brood temp is 33-36 C
  humidity_alert           boolean  True if humidity >80% or <40%
  co2_alert                boolean  True if CO2 >2500 ppm
  thermoregulation_active  boolean  True if bees are actively fanning
  temp_trend_1h            float    Rolling 1-hour temperature trend (C/hr)
  apiary_id                string   Apiary identifier
  latitude                 float    Apiary GPS latitude
  longitude                float    Apiary GPS longitude


MYSQL SCHEMA (hive_readings)
----------------------------
  id                       BIGINT AUTO_INCREMENT PRIMARY KEY
  ts                       DATETIME       (sensor timestamp)
  hive_id                  VARCHAR(20)
  hive_temp_c              DOUBLE
  hive_humidity_pct        DOUBLE
  co2_ppm                  DOUBLE
  outside_temp_c           DOUBLE
  outside_humidity_pct     DOUBLE
  brood_temp_ok            TINYINT(1)
  humidity_alert           TINYINT(1)
  co2_alert                TINYINT(1)
  thermoregulation_active  TINYINT(1)
  temp_trend_1h            DOUBLE
  apiary_id                VARCHAR(50)
  latitude                 DOUBLE
  longitude                DOUBLE
  ingested_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  Indexes: idx_ts, idx_hive, idx_hive_ts


BUSINESS INSIGHT
----------------
The Grafana dashboard turns raw hive sensor streams into actionable
beekeeping intelligence. Across the 7-day window, the Total Sensor
Readings panel confirms full pipeline throughput (thousands of
ingested rows), while the Temperature Alerts and CO2 Alert Count
panels immediately surface colonies under stress. The most critical
finding is the queenless event on HIVE-003: its temperature over
time drifts from a stable brood-nest 34.5C down toward ambient as
the colony loses thermoregulation, humidity rises above the 80%
chalkbrood-risk threshold, and brood_temp_ok flips to false. A
beekeeper seeing this pattern on the dashboard would requeen HIVE-003
within 24-48 hours instead of discovering a dead colony weeks later.
CO2 spikes align with the three injected rain windows, when bees
cluster indoors and ventilation drops -- a useful operational cue to
check hive entrance blocks. The Hive Population Coverage and Average
Metrics by Hive panels let the apiary manager compare all five
colonies side by side, prioritising interventions by severity.
Overall, streaming monitoring reduces manual inspection frequency,
enables earlier disease and queen-loss detection, and protects honey
yield by preventing colony collapse.


REQUIREMENTS
------------
  Python 3.8+
  numpy                    (generator)
  kafka-python             (producer + consumer)
  mysql-connector-python   (consumer -> MySQL)
  Docker                   (MySQL, Kafka, Zookeeper)
  Grafana                  (dashboard)

Install:
  > pip install -r requirements.txt


FILES IN THIS PROJECT
---------------------
  generate_hive_env.py       Synthetic data generator
  env_producer.py            Producer (terminal + Kafka)
  env_consumer.py            Consumer (Kafka -> MySQL)
  dashboard_env.json         Grafana dashboard definition
  dashboard_screenshot.png   Dashboard screenshot
  requirements.txt           Python dependencies
  output/
    hive_environment.json    Generated sensor data (50,400 records)


REALISTIC EVENTS IN THE DATA
-----------------------------
1. Queenless Hive (HIVE-003):
   - Starts on day 3 of the 7-day period
   - Internal temperature gradually drops from 34.5C to ambient
   - humidity rises above 80% (chalkbrood risk)
   - brood_temp_ok becomes False

2. Rain Events:
   - Day 1: 06:00-14:00
   - Day 4: 14:00-20:00
   - Day 6: 00:00-12:00
   - During rain: outside humidity spikes, CO2 rises

3. Sensor Dropouts:
   - ~0.1% of records have null values for one random field

4. Diurnal Cycles:
   - Temperature follows a sinusoidal day/night pattern
   - CO2 is higher at night (bees clustered, less ventilation)

================================================================================
