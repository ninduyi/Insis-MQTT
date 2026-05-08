# ============================================================
# Subscriber 1: Central Monitoring Dashboard
# ============================================================
# Dashboard web real-time menggunakan Flask + SocketIO.
# Fitur:
#   - Grafik konsumsi energi (Chart.js)
#   - Progress bar anggaran
#   - Tabel langganan + rekomendasi
#   - Status sistem (LWT)
#   - Panel alert
# Subscribe menggunakan Wildcard (+)
# ============================================================

import json
import time
import threading
from datetime import datetime

import paho.mqtt.client as mqtt
from flask import Flask, render_template
from flask_socketio import SocketIO

from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE,
    TOPIC_ENERGY_WILDCARD, TOPIC_SUBS_WILDCARD,
    TOPIC_BUDGET, TOPIC_HEALTH, TOPIC_ALERT,
    DASHBOARD_HOST, DASHBOARD_PORT
)

# ---- Flask App ----
app = Flask(__name__)
app.config["SECRET_KEY"] = "smart-finance-secret"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ---- State ----
dashboard_state = {
    "energy": {},
    "subscriptions": {},
    "budget": {"limit": 500000, "limit_formatted": "Rp500,000"},
    "health": {"status": "Waiting...", "timestamp": ""},
    "alerts": [],
}


# ---- MQTT Callbacks ----
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[Dashboard MQTT] ✅ Terhubung ke broker")

        # ---- WILDCARD SUBSCRIBE ----
        # Menggunakan + wildcard untuk menangkap SEMUA perangkat/layanan
        client.subscribe(TOPIC_ENERGY_WILDCARD, qos=0)
        print(f"  📡 Subscribe: {TOPIC_ENERGY_WILDCARD} (wildcard +)")

        client.subscribe(TOPIC_SUBS_WILDCARD, qos=1)
        print(f"  📡 Subscribe: {TOPIC_SUBS_WILDCARD} (wildcard +)")

        client.subscribe(TOPIC_BUDGET, qos=2)
        print(f"  📡 Subscribe: {TOPIC_BUDGET} (QoS 2 - exactly once)")

        client.subscribe(TOPIC_HEALTH, qos=1)
        print(f"  📡 Subscribe: {TOPIC_HEALTH}")

        client.subscribe(TOPIC_ALERT, qos=1)
        print(f"  📡 Subscribe: {TOPIC_ALERT}")
    else:
        print(f"[Dashboard MQTT] ❌ Gagal terhubung: {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    """Callback saat terputus dari broker."""
    print(f"[Dashboard MQTT] ⚠️  Terputus dari broker (kode: {reason_code})")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        topic = msg.topic

        if topic.startswith("finansial/energi/"):
            device = payload.get("device", "unknown")
            dashboard_state["energy"][device] = payload
            socketio.emit("energy_update", {
                "device": device,
                "data": payload,
                "all_energy": dashboard_state["energy"]
            })

        elif topic.startswith("finansial/subs/"):
            service = payload.get("service", "unknown")
            dashboard_state["subscriptions"][service] = payload
            socketio.emit("subs_update", {
                "service": service,
                "data": payload,
                "all_subs": dashboard_state["subscriptions"]
            })

        elif topic == TOPIC_BUDGET:
            dashboard_state["budget"] = payload
            socketio.emit("budget_update", payload)

        elif topic == TOPIC_HEALTH:
            dashboard_state["health"] = payload
            socketio.emit("health_update", payload)

        elif topic == TOPIC_ALERT:
            dashboard_state["alerts"].insert(0, payload)
            # Keep only last 20 alerts
            dashboard_state["alerts"] = dashboard_state["alerts"][:20]
            socketio.emit("alert_update", {
                "alert": payload,
                "all_alerts": dashboard_state["alerts"]
            })

    except Exception as e:
        print(f"[Dashboard MQTT] ❌ Error: {e}")


# ---- Flask Routes ----
@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/state")
def api_state():
    return json.dumps(dashboard_state)


# ---- SocketIO Events ----
@socketio.on("connect")
def handle_connect():
    print(f"[Dashboard] 🌐 Browser client terhubung")
    socketio.emit("initial_state", dashboard_state)


@socketio.on("disconnect")
def handle_disconnect():
    print(f"[Dashboard] 🌐 Browser client terputus")


# ---- MQTT Thread ----
def start_mqtt():
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="dashboard_sub",
        protocol=mqtt.MQTTv5
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[Dashboard MQTT] Mencoba terhubung ke broker (percobaan {attempt}/{max_retries})...")
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
            client.loop_forever()
            break
        except (ConnectionRefusedError, OSError, TimeoutError) as e:
            print(f"[Dashboard MQTT] ❌ Gagal terhubung: {e}")
            if attempt < max_retries:
                wait = attempt * 3
                print(f"[Dashboard MQTT] Mencoba lagi dalam {wait} detik...")
                time.sleep(wait)
            else:
                print("[Dashboard MQTT] ❌ Semua percobaan gagal!")
                print("[Dashboard MQTT] Pastikan Mosquitto berjalan di localhost:1883")
        except Exception as e:
            print(f"[Dashboard MQTT] ❌ Error tidak terduga: {e}")
            break


def main():
    print("=" * 60)
    print("  SUBSCRIBER 1: CENTRAL MONITORING DASHBOARD")
    print("=" * 60)
    print(f"  MQTT Broker : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  Dashboard   : http://localhost:{DASHBOARD_PORT}")
    print(f"  Wildcards   : finansial/energi/+/status")
    print(f"                finansial/subs/+/usage")
    print("=" * 60)

    # Start MQTT in separate thread
    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()

    # Start Flask-SocketIO server
    print(f"\n[Dashboard] 🌐 Buka browser: http://localhost:{DASHBOARD_PORT}\n")
    socketio.run(app, host=DASHBOARD_HOST, port=DASHBOARD_PORT,
                 debug=False, allow_unsafe_werkzeug=True)


if __name__ == "__main__":
    main()
