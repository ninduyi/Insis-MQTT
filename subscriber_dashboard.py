# ============================================================
# Subscriber 1: Central Monitoring Dashboard
# ============================================================
# Fitur MQTT v5 yang diimplementasikan:
#   [1] Subscribe QoS 0/1/2 → Berbeda per topik sesuai urgensi
#   [2] Wildcard (+)        → finansial/energi/+/status
#   [5] Retain              → Budget & subs langsung dapat data lama
#   [8] Request-Response    → Dashboard bisa request snapshot energy
#   [10] Flow Control       → receive_maximum=20 batasi in-flight msgs
#   Budget Manager         → Terintegrasi langsung di dashboard (form UI)
# ============================================================

import json
import time
import threading
import uuid
from datetime import datetime

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO

from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE,
    TOPIC_ENERGY_WILDCARD, TOPIC_SUBS_WILDCARD,
    TOPIC_BUDGET, TOPIC_HEALTH, TOPIC_ALERT,
    TOPIC_REQUEST, TOPIC_RESPONSE,
    DASHBOARD_HOST, DASHBOARD_PORT,
    FLOW_CONTROL_RECEIVE_MAX
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

# Global MQTT client (untuk digunakan oleh Flask route)
mqtt_client_ref = None


# ---- MQTT Callbacks ----
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[Dashboard MQTT] ✅ Terhubung ke broker")
        print(f"[Dashboard MQTT] 🔒 Flow Control: receive_maximum={FLOW_CONTROL_RECEIVE_MAX}")

        # ---- [FITUR 2: Wildcard Subscribe] ----
        client.subscribe(TOPIC_ENERGY_WILDCARD, qos=0)
        print(f"  📡 [QoS 0] Wildcard: {TOPIC_ENERGY_WILDCARD}")

        client.subscribe(TOPIC_SUBS_WILDCARD, qos=1)
        print(f"  📡 [QoS 1] Wildcard: {TOPIC_SUBS_WILDCARD}")

        # ---- [FITUR 5: Retain] Budget dengan QoS 2 ----
        client.subscribe(TOPIC_BUDGET, qos=2)
        print(f"  📡 [QoS 2] Retained: {TOPIC_BUDGET}")

        client.subscribe(TOPIC_HEALTH, qos=1)
        print(f"  📡 [QoS 1] {TOPIC_HEALTH}")

        client.subscribe(TOPIC_ALERT, qos=1)
        print(f"  📡 [QoS 1] {TOPIC_ALERT}")

        # ---- [FITUR 8: Request-Response] Subscribe ke response topik ----
        client.subscribe(TOPIC_RESPONSE, qos=1)
        print(f"  📡 [QoS 1] Response: {TOPIC_RESPONSE}")
    else:
        print(f"[Dashboard MQTT] ❌ Gagal terhubung: {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    print(f"[Dashboard MQTT] ⚠️  Terputus dari broker (kode: {reason_code})")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        topic = msg.topic

        # Tampilkan User Properties jika ada
        user_props = {}
        if msg.properties and hasattr(msg.properties, 'UserProperty'):
            for k, v in (msg.properties.UserProperty or []):
                user_props[k] = v

        if topic.startswith("finansial/energi/"):
            device = payload.get("device", "unknown")
            dashboard_state["energy"][device] = payload
            socketio.emit("energy_update", {
                "device": device,
                "data": payload,
                "all_energy": dashboard_state["energy"],
                "user_props": user_props
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
            dashboard_state["alerts"] = dashboard_state["alerts"][:20]
            socketio.emit("alert_update", {
                "alert": payload,
                "all_alerts": dashboard_state["alerts"]
            })

        elif topic == TOPIC_RESPONSE:
            # ---- [FITUR 8: Request-Response] Teruskan response ke browser ----
            socketio.emit("snapshot_response", payload)
            print(f"[Dashboard] 📊 Snapshot response diterima, diteruskan ke browser")

    except Exception as e:
        print(f"[Dashboard MQTT] ❌ Error: {e}")


# ---- Flask Routes ----
@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/state")
def api_state():
    return json.dumps(dashboard_state)


@app.route("/api/budget", methods=["POST"])
def api_set_budget():
    """
    [BUDGET MANAGER DARI DASHBOARD]
    Menerima input budget dari form di browser,
    lalu mempublish ke MQTT broker dengan QoS 2 + Retained.
    """
    global mqtt_client_ref
    try:
        data = request.get_json()
        amount = int(data.get("limit", 0))
        budget_type = data.get("type", "total_budget")
        category = data.get("category", "")

        if amount <= 0:
            return jsonify({"success": False, "message": "Nominal tidak valid"}), 400

        budget_data = {
            "type": budget_type,
            "limit": amount,
            "limit_formatted": f"Rp{amount:,}",
            "category": category,
            "bulan": datetime.now().strftime("%B %Y"),
            "timestamp": datetime.now().isoformat(),
            "source": "dashboard_web"
        }

        if mqtt_client_ref:
            # ---- [FITUR 1: QoS 2 + Fitur 5: Retain] ----
            # Data finansial: tepat 1 kali + disimpan broker
            pub_props = Properties(PacketTypes.PUBLISH)
            pub_props.UserProperty = [
                ("source", "web_dashboard"),
                ("publisher_version", "2.0"),
            ]

            mqtt_client_ref.publish(
                TOPIC_BUDGET,
                json.dumps(budget_data),
                qos=2,
                retain=True,
                properties=pub_props
            )
            print(f"[Dashboard] 💰 Budget di-set: Rp{amount:,} (QoS 2, Retained)")
            return jsonify({
                "success": True,
                "message": f"Budget Rp{amount:,} berhasil dikirim (QoS 2 Exactly Once + Retained)",
                "data": budget_data
            })
        else:
            return jsonify({"success": False, "message": "MQTT belum terhubung"}), 503

    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/snapshot", methods=["POST"])
def api_request_snapshot():
    """
    [FITUR 8: Request-Response]
    Dashboard meminta snapshot data energi real-time dari publisher.
    """
    global mqtt_client_ref
    try:
        correlation_id = str(uuid.uuid4())[:8]
        request_payload = json.dumps({
            "type": "snapshot",
            "correlation_id": correlation_id,
            "requester": "dashboard_web",
            "timestamp": datetime.now().isoformat()
        })

        if mqtt_client_ref:
            req_props = Properties(PacketTypes.PUBLISH)
            req_props.ResponseTopic = TOPIC_RESPONSE
            req_props.CorrelationData = correlation_id.encode()

            mqtt_client_ref.publish(TOPIC_REQUEST, request_payload, qos=1, properties=req_props)
            print(f"[Dashboard] 📨 Snapshot request dikirim (id: {correlation_id})")
            return jsonify({"success": True, "correlation_id": correlation_id})
        else:
            return jsonify({"success": False, "message": "MQTT belum terhubung"}), 503

    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


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
    global mqtt_client_ref

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="dashboard_sub",
        protocol=mqtt.MQTTv5
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    mqtt_client_ref = client

    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[Dashboard MQTT] Mencoba terhubung ke broker (percobaan {attempt}/{max_retries})...")

            # ---- [FITUR 10: Flow Control] ----
            # Batasi jumlah pesan QoS1/2 in-flight yang bisa diterima sekaligus
            connect_props = Properties(PacketTypes.CONNECT)
            connect_props.ReceiveMaximum = FLOW_CONTROL_RECEIVE_MAX

            client.connect(MQTT_BROKER, MQTT_PORT,
                           keepalive=MQTT_KEEPALIVE,
                           properties=connect_props)
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
        except Exception as e:
            print(f"[Dashboard MQTT] ❌ Error tidak terduga: {e}")
            break


def main():
    print("=" * 60)
    print("  SUBSCRIBER 1: CENTRAL MONITORING DASHBOARD")
    print("=" * 60)
    print(f"  MQTT Broker  : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  Dashboard    : http://localhost:{DASHBOARD_PORT}")
    print(f"  Flow Control : receive_maximum={FLOW_CONTROL_RECEIVE_MAX}")
    print(f"  Wildcards    : finansial/energi/+/status")
    print(f"                 finansial/subs/+/usage")
    print(f"  Budget API   : POST /api/budget (dari form di browser)")
    print(f"  Snapshot API : POST /api/snapshot (Request-Response)")
    print("=" * 60)

    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()

    print(f"\n[Dashboard] 🌐 Buka browser: http://localhost:{DASHBOARD_PORT}\n")
    socketio.run(app, host=DASHBOARD_HOST, port=DASHBOARD_PORT,
                 debug=False, allow_unsafe_werkzeug=True)


if __name__ == "__main__":
    main()
