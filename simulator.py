#!/usr/bin/env python3
import os, time, random, json
import numpy as np
from datetime import datetime, timezone
from dotenv import load_dotenv
from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo

def daily_cycle(t_seconds, amplitude=0.6):
    return amplitude * np.sin(2*np.pi * (t_seconds % 86400) / 86400.0)

def generate_sample(node_id, anomaly_prob, state):
    t = time.time()
    ph_base   = 7.2 + daily_cycle(t, 0.15) + np.random.normal(0, 0.05)
    turb_base = max(0.0, np.random.normal(0.6, 0.3) + max(0, daily_cycle(t, 0.3)))
    temp_base = 20.0 + daily_cycle(t, 3.0) + np.random.normal(0, 0.2)
    cond_base = np.clip(np.random.normal(300, 80), 0, 2000)

    if random.random() < anomaly_prob:
        which = random.choice(["ph_low","ph_high","turb_spike","cond_high"])
        if which == "ph_low":
            ph_base = np.clip(ph_base - np.random.uniform(2.0, 3.0), 4.0, 10.0)
        elif which == "ph_high":
            ph_base = np.clip(ph_base + np.random.uniform(2.0, 3.0), 4.0, 10.0)
        elif which == "turb_spike":
            turb_base = np.clip(turb_base + np.random.uniform(10, 80), 0, 100.0)
        elif which == "cond_high":
            cond_base = np.clip(cond_base + np.random.uniform(700, 1400), 0, 2000.0)
        state["last_anomaly"] = which
    else:
        state["last_anomaly"] = None

    payload = {
    "node_id": node_id,
    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "pH": float(round(np.clip(ph_base, 4.0, 10.0), 2)),
    "turbidity_NTU": float(round(np.clip(turb_base, 0.0, 100.0), 2)),
    "temperature_C": float(round(np.clip(temp_base, 0.0, 40.0), 2)),
    "conductivity_uS_cm": float(round(cond_base, 2))
    }
    
    return payload

def main():
    load_dotenv()

    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", 1883))
    access_token = os.getenv("ACCESS_TOKEN")
    node_id = os.getenv("NODE_ID", "RPi1-Urban")
    interval = float(os.getenv("INTERVAL", 10))
    anomaly_prob = float(os.getenv("ANOMALY_PROB", 0.03))

    if not access_token:
        raise SystemExit("ACCESS_TOKEN must be set in .env")

    client = TBDeviceMqttClient(host, port=port, username=access_token)
    client.connect()
    print(f"Connected to ThingsBoard at {host}:{port} as {access_token}")

    state = {"last_anomaly": None}

    try:
        while True:
            payload = generate_sample(node_id, anomaly_prob, state)
            result = client.send_telemetry(payload)
            if result.get() == TBPublishInfo.TB_ERR_SUCCESS:
                if state["last_anomaly"]:
                    print(f"[ANOMALY:{state['last_anomaly']}] {json.dumps(payload)}")
                else:
                    print(payload)
            else:
                print("Failed to send:", payload)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()
