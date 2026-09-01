================================================================================
  BEEHIVE MONITORING SYSTEM - IoT Streaming Data Pipeline
================================================================================

PROJECT DESCRIPTION
-------------------
This project implements a real-time beehive monitoring system using IoT sensors
and a streaming data pipeline. Five beehives in an apiary are equipped with
environmental sensors that continuously measure internal temperature, humidity,
and CO2 levels. A custom Python generator produces realistic synthetic sensor
data -- including diurnal cycles, seasonal trends, a queenless hive event, rain
windows, and sensor dropouts -- based on published apicultural research. The
data is streamed to the terminal by a producer script at a configurable rate,
simulating a live sensor feed, with optional Kafka integration for downstream
consumer processing. The system enables beekeepers to detect critical colony
states such as queen loss, high humidity, and CO2 buildup, allowing timely
intervention to prevent colony collapse.


HOW IT WORKS
------------
The system has two main components:

1. DATA GENERATOR (generate_hive_env.py)
   - Generates 7 days of synthetic sensor data for 5 hives
   - Records are produced every 1 minute (50,400 total records)
   - Each record simulates temperature, humidity, and CO2 readings
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
   - Simulates a live sensor feed with configurable delay between records
   - Optionally sends each record to a Kafka topic for downstream processing
   - Supports filtering to a single hive and infinite loop mode


DATA FORMAT (JSONL)
-------------------
Each line in hive_environment.json is a JSON object with these fields:

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


HOW TO RUN
----------
Step 1: Generate the synthetic data
  > python generate_hive_env.py

Step 2: Stream the data to terminal
  > python env_producer.py

Useful options:
  --delay 0.1          Stream faster (0.1 seconds between records)
  --hive HIVE-003      Show only one hive (the queenless one)
  --loop               Restart from beginning when file ends
  --kafka              Also send records to Kafka topic
  --topic hive_environment   Kafka topic name (default: hive_environment)
  --broker localhost:9092     Kafka broker address


FILES IN THIS PROJECT
---------------------
  generate_hive_env.py       Synthetic data generator script
  env_producer.py            Producer script for streaming to terminal
  output/
    hive_environment.json    Generated sensor data (50,400 records)


REQUIREMENTS
------------
  Python 3.8+
  numpy (for random number generation)
  kafka-python (only needed if using --kafka flag)

Install dependencies:
  > pip install numpy
  > pip install kafka-python   (optional, for Kafka mode)


REALISTIC EVENTS IN THE DATA
-----------------------------
1. Queenless Hive (HIVE-003):
   - Starts on day 3 of the 7-day period
   - Internal temperature gradually drops from 34.5C to ambient temperature
     as the colony loses its ability to thermoregulate without a queen
   - humidity rises above 80% (chalkbrood risk)
   - brood_temp_ok becomes False

2. Rain Events:
   - Day 1: 06:00-14:00
   - Day 4: 14:00-20:00
   - Day 6: 00:00-12:00
   - During rain: outside humidity spikes, CO2 rises (bees cluster inside)

3. Sensor Dropouts:
   - ~0.1% of records have null values for one random field
   - Simulates real-world sensor reliability issues

4. Diurnal Cycles:
   - Temperature follows a sinusoidal day/night pattern
   - CO2 is higher at night (bees clustered, less ventilation)
   - Hive humidity is inversely correlated with temperature


================================================================================
