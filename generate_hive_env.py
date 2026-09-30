"""
Synthetic Data Generator — Topic 2: hive_environment
Beehive IoT monitoring (temp, humidity, CO2)
Outputs one JSON record per line for Kafka consumer testing.
"""

import json
import math
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

# ─── CONFIGURATION ───────────────────────────────────────────────────────────

APIARY_ID = "APIARY-NORTH"
APIARY_LAT = 40.7128
APIARY_LON = -74.0060
HIVE_IDS = ["HIVE-001", "HIVE-002", "HIVE-003", "HIVE-004", "HIVE-005"]

# Date range — 7 days in spring (swarm season)
START_DATE = datetime(2026, 4, 1, tzinfo=timezone.utc)
DAYS = 7
INTERVAL_SEC = 60  # one record per minute

# Temperate-zone climate baselines (latitude ~40°N in April)
SPRING_BASE_TEMP_C = 12.0        # mean outside temp early April
SPRING_AMPLITUDE_C = 8.0         # seasonal swing ±8°C
DIURNAL_OUTSIDE_AMP_C = 7.0      # day/night outside swing
DIURNAL_OUTSIDE_PHASE = 0.0      # peak at ~14:00

# Brood nest targets (queenright)
BROOD_TEMP_TARGET_C = 34.5
BROOD_TEMP_STD_C = 0.8           # tight regulation

# Hive humidity base
HIVE_HUMIDITY_BASE = 58.0
HIVE_HUMIDITY_AMP = 8.0          # inversely with temp

# CO2 baseline
CO2_BASE_PPM = 550.0
CO2_DAY_AMPLITUDE_PPM = 150.0    # lower during day (ventilation)

OUTSIDE_HUMIDITY_BASE = 65.0
OUTSIDE_HUMIDITY_AMP = 15.0

# Sensor noise levels (std dev)
TEMP_NOISE_STD = 0.4             # °C
HUMIDITY_NOISE_STD = 1.5         # %
CO2_NOISE_STD = 30.0             # ppm
OUTSIDE_TEMP_NOISE_STD = 0.6

# Queenless event: one hive, starts day 3, lasts 4 days
QUEENLESS_HIVE = "HIVE-003"
QUEENLESS_START_DAY = 3          # 0-indexed day
QUEENLESS_DURATION_DAYS = 4

# Rain events: (day_index, start_hour, duration_hours)
RAIN_EVENTS = [
    (1, 6, 8),    # day 1, 06:00–14:00
    (4, 14, 6),   # day 4, 14:00–20:00
    (6, 0, 12),   # day 6, midnight–noon
]

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_FILE = OUTPUT_DIR / "hive_environment.json"

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def day_of_year(dt: datetime) -> int:
    return dt.timetuple().tm_yday

def hour_of_day(dt: datetime) -> float:
    return dt.hour + dt.minute / 60.0

def is_raining(day_idx: int, hour: float) -> bool:
    """Check if a given day+hour falls inside a rain event window."""
    for rday, rstart, rdur in RAIN_EVENTS:
        if day_idx == rday and rstart <= hour < rstart + rdur:
            return True
    return False

def rain_intensity(hour: float, start: float, duration: float) -> float:
    """Returns mm/hr rain intensity; peaks in the middle of the event."""
    mid = start + duration / 2.0
    spread = duration / 2.5
    return max(0.0, 12.0 * math.exp(-0.5 * ((hour - mid) / spread) ** 2))

def diurnal_cycle(hour: float, amplitude: float, phase_shift: float = 0.0,
                  peak_hour: float = 14.0) -> float:
    """Sinusoidal diurnal pattern. Peak at peak_hour (default 14:00)."""
    angle = 2.0 * math.pi * (hour - peak_hour + phase_shift) / 24.0
    return amplitude * math.cos(angle)

def seasonal_temp(day_idx: int) -> float:
    """Sinusoidal seasonal trend peaking mid-July (day ~200)."""
    doy = START_DATE.timetuple().tm_yday + day_idx
    angle = 2.0 * math.pi * (doy - 200) / 365.0
    return SPRING_BASE_TEMP_C + SPRING_AMPLITUDE_C * math.cos(angle)

def clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))

# ─── SIGNAL GENERATORS ───────────────────────────────────────────────────────

def outside_temp(day_idx: int, hour: float) -> float:
    base = seasonal_temp(day_idx)
    diurnal = diurnal_cycle(hour, DIURNAL_OUTSIDE_AMP_C, peak_hour=14.0)
    noise = np.random.normal(0, OUTSIDE_TEMP_NOISE_STD)
    return base + diurnal + noise

def outside_humidity(day_idx: int, hour: float, temp: float) -> float:
    """Inverse relationship with temperature."""
    base = OUTSIDE_HUMIDITY_BASE
    diurnal = -diurnal_cycle(hour, OUTSIDE_HUMIDITY_AMP, peak_hour=14.0)
    temp_effect = -(temp - seasonal_temp(day_idx)) * 1.2
    noise = np.random.normal(0, 2.0)
    return clamp(base + diurnal + temp_effect + noise, 10.0, 100.0)

def hive_temp(day_idx: int, hour: float, ambient_temp: float,
              is_queenless: bool) -> float:
    """Queenright: tight 33–36°C regulation. Queenless: drifts toward ambient."""
    if is_queenless:
        # Gradual loss of thermoregulation over first 24h, then tracks ambient
        elapsed_hours = day_idx * 24 + hour
        qloss_start = QUEENLESS_START_DAY * 24
        elapsed_since_loss = max(0.0, elapsed_hours - qloss_start)
        # Blend factor: 0 = full regulation, 1 = fully ambient
        blend = clamp(elapsed_since_loss / 48.0, 0.0, 1.0)
        target = BROOD_TEMP_TARGET_C * (1.0 - blend) + ambient_temp * blend
        # Add more variance as regulation degrades
        noise_std = BROOD_TEMP_STD_C + blend * 4.0
    else:
        target = BROOD_TEMP_TARGET_C
        noise_std = BROOD_TEMP_STD_C

    # Small diurnal nudge (bees are slightly more active during day)
    diurnal_nudge = diurnal_cycle(hour, 0.5, peak_hour=12.0)
    noise = np.random.normal(0, noise_std)
    return clamp(target + diurnal_nudge + noise, 20.0, 42.0)

def hive_humidity(day_idx: int, hour: float, hive_temp_val: float,
                  is_queenless: bool) -> float:
    base = HIVE_HUMIDITY_BASE
    # Inverse with hive temp
    temp_effect = -(hive_temp_val - BROOD_TEMP_TARGET_C) * 2.0
    # Queenless hives tend to have higher humidity (less fanning)
    qloss_effect = 8.0 if is_queenless else 0.0
    # Night humidity is higher
    night_effect = -diurnal_cycle(hour, 5.0, peak_hour=14.0)
    noise = np.random.normal(0, HUMIDITY_NOISE_STD)
    return clamp(base + temp_effect + qloss_effect + night_effect + noise,
                 30.0, 90.0)

def hive_co2(day_idx: int, hour: float, is_queenless: bool) -> float:
    """CO2 higher at night (clustered bees, less ventilation), lower during day."""
    base = CO2_BASE_PPM
    # Diurnal: lower during day when bees are active / fanning
    diurnal = -diurnal_cycle(hour, CO2_DAY_AMPLITUDE_PPM, peak_hour=14.0)
    # Queenless colonies cluster more, slightly higher CO2
    qloss_effect = 200.0 if is_queenless else 0.0
    noise = np.random.normal(0, CO2_NOISE_STD)
    return clamp(base + diurnal + qloss_effect + noise, 300.0, 6000.0)

def compute_temp_trend(current_temp: float, history: list) -> float:
    """Rolling 1-hour trend from recent history (60 records)."""
    if len(history) < 60:
        return 0.0
    old_temp = history[-60]
    return round(current_temp - old_temp, 2)

# ─── MAIN GENERATION ─────────────────────────────────────────────────────────

def generate():
    OUTPUT_DIR.mkdir(exist_ok=True)
    total_minutes = DAYS * 24 * 60
    records = []

    # Pre-compute rain schedule as set of (day, hour_start, hour_end)
    rain_windows = {}
    for rday, rstart, rdur in RAIN_EVENTS:
        for h in range(rstart, rstart + rdur):
            rain_windows.setdefault(rday, set()).add(h)

    for hive_idx, hive_id in enumerate(HIVE_IDS):
        # Per-hive temperature history for trend calculation
        temp_history = []
        # One hive goes queenless
        queenless = (hive_id == QUEENLESS_HIVE)

        print(f"Generating {hive_id} ({'queenless' if queenless else 'queenright'}) ...")

        for minute_idx in range(total_minutes):
            dt = START_DATE + timedelta(minutes=minute_idx)
            day_idx = minute_idx // (24 * 60)
            hour = hour_of_day(dt)

            # --- Ambient conditions ---
            amb_temp = outside_temp(day_idx, hour)
            amb_hum = outside_humidity(day_idx, hour, amb_temp)

            # --- Rain check ---
            raining = is_raining(day_idx, hour)
            rain_mm = 0.0
            if raining:
                rstart_next = None
                for rd, rs, rdur in RAIN_EVENTS:
                    if rd == day_idx and rs <= hour < rs + rdur:
                        rstart_next = rs
                        rain_mm = rain_intensity(hour, rs, rdur)
                        break

            # If raining, ambient humidity goes up, temp goes slightly down
            if raining:
                amb_hum = clamp(amb_hum + 15.0, 40.0, 100.0)
                amb_temp -= 2.0

            # --- Hive internal conditions ---
            ht = hive_temp(day_idx, hour, amb_temp, queenless)
            hh = hive_humidity(day_idx, hour, ht, queenless)
            co2 = hive_co2(day_idx, hour, queenless)

            # Rain suppresses foraging → CO2 rises slightly (bees clustered)
            if raining:
                co2 = clamp(co2 + 300.0, 300.0, 6000.0)

            temp_history.append(ht)

            # --- Derived fields ---
            brood_ok = 33.0 <= ht <= 36.0
            humidity_alert = hh > 80.0 or hh < 40.0
            co2_alert = co2 > 2500.0

            # Thermoregulation active when bees are fanning (daytime, warm)
            thermo_active = (6.0 <= hour <= 20.0) and (amb_temp > 18.0)
            if queenless:
                # Queenless colonies fan less effectively
                thermo_active = thermo_active and (random.random() > 0.4)

            temp_trend = compute_temp_trend(ht, temp_history)

            record = {
                "timestamp": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "hive_id": hive_id,
                "hive_temp_c": round(ht, 2),
                "hive_humidity_pct": round(hh, 2),
                "co2_ppm": round(co2, 1),
                "outside_temp_c": round(amb_temp, 2),
                "outside_humidity_pct": round(amb_hum, 2),
                "brood_temp_ok": brood_ok,
                "humidity_alert": humidity_alert,
                "co2_alert": co2_alert,
                "thermoregulation_active": thermo_active,
                "temp_trend_1h": temp_trend,
                "apiary_id": APIARY_ID,
                "latitude": APIARY_LAT,
                "longitude": APIARY_LON,
            }

            # Inject ~0.1% sensor dropouts for realism
            if random.random() < 0.001:
                field = random.choice(["hive_temp_c", "hive_humidity_pct", "co2_ppm"])
                record[field] = None

            records.append(record)

        print(f"  => {len([r for r in records if r['hive_id'] == hive_id])} records")

    # --- Write output ---
    print(f"\nWriting {len(records)} records to {OUTPUT_FILE} ...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, default=str) + "\n")

    size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    print(f"Done. File size: {size_mb:.1f} MB")

    # --- Quick stats ---
    print("\n--- Quick Validation ---")
    temps = [r["hive_temp_c"] for r in records if r["hive_temp_c"] is not None]
    humids = [r["hive_humidity_pct"] for r in records if r["hive_humidity_pct"] is not None]
    co2s = [r["co2_ppm"] for r in records if r["co2_ppm"] is not None]

    for hid in HIVE_IDS:
        hrecs = [r for r in records if r["hive_id"] == hid and r["hive_temp_c"] is not None]
        hts = [r["hive_temp_c"] for r in hrecs]
        queenless_tag = " (queenless)" if hid == QUEENLESS_HIVE else ""
        print(f"  {hid}{queenless_tag}: temp mean={np.mean(hts):.1f}°C  "
              f"std={np.std(hts):.2f}°C  "
              f"min={np.min(hts):.1f}°C  max={np.max(hts):.1f}°C")

    rain_count = 0
    for r in records:
        dt_r = datetime.fromisoformat(r["timestamp"].rstrip("Z")).replace(tzinfo=None)
        day_idx_r = (dt_r - START_DATE.replace(tzinfo=None)).days
        hour_r = hour_of_day(dt_r)
        if is_raining(day_idx_r, hour_r):
            rain_count += 1
    print(f"  Rain-influenced records: ~{rain_count}")
    null_count = sum(1 for r in records
                     if r["hive_temp_c"] is None or r["hive_humidity_pct"] is None
                     or r["co2_ppm"] is None)
    print(f"  Sensor dropouts (nulls): {null_count}")


if __name__ == "__main__":
    generate()
