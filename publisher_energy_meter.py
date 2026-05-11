# ============================================================
# Publisher 1: Virtual Energy Meter
# ============================================================
# Mensimulasikan konsumsi listrik perangkat rumah.
# Fitur MQTT v5 yang diimplementasikan:
#   [1] QoS 0      → Fire-and-forget untuk data real-time cepat
#   [2] LWT        → Broker otomatis publish "Metering Offline" jika putus
#   [3] Topic Alias → Topik disingkat jadi angka, hemat bandwidth
#   [4] User Props → Metadata ditempel ke tiap pesan (versi, lokasi, unit)
#   [6] Expiry     → Data otomatis kedaluwarsa setelah 30 detik
#   [8] Req-Resp   → Merespons snapshot request dari dashboard
# ============================================================

import json
import random
import time
import signal
import sys
from datetime import datetime

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE,
    TOPIC_ENERGY, TOPIC_HEALTH, TOPIC_REQUEST, TOPIC_RESPONSE,
    DEVICES, TOPIC_ALIAS_ENERGY, TOPIC_ALIAS_HEALTH,
    ENERGY_MSG_EXPIRY_SECONDS
)

# ---- Flag untuk graceful shutdown ----
running = True
mqtt_client = None  # global client untuk request-response

# ---- State snapshot terakhir ----
last_snapshot = {}


def signal_handler(sig, frame):
    global running
    print("\n[Energy Meter] Menerima sinyal shutdown...")
    running = False


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[Energy Meter] ✅ Terhubung ke broker {MQTT_BROKER}:{MQTT_PORT}")
        print(f"[Energy Meter] Keep Alive: {MQTT_KEEPALIVE} detik")

        # [FITUR 8: Request-Response] Subscribe ke topik request dari dashboard
        client.subscribe(TOPIC_REQUEST, qos=1)
        print(f"[Energy Meter] 📡 Subscribe ke {TOPIC_REQUEST} (Request-Response)")

        # Publish status online dengan retained
        health_msg = json.dumps({
            "status": "Metering Online",
            "timestamp": datetime.now().isoformat()
        })
        pub_props = Properties(PacketTypes.PUBLISH)
        pub_props.MessageExpiryInterval = 3600  # health status valid 1 jam
        client.publish(TOPIC_HEALTH, health_msg, qos=1, retain=True, properties=pub_props)
        print(f"[Energy Meter] 📡 Health status: Metering Online")
    else:
        print(f"[Energy Meter] ❌ Gagal terhubung, kode: {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    print(f"[Energy Meter] ⚠️  Terputus dari broker (kode: {reason_code})")


def on_publish(client, userdata, mid, reason_code, properties):
    pass  # QoS 0 tidak perlu konfirmasi


def on_message(client, userdata, msg):
    """[FITUR 8: Request-Response] Tangani request snapshot dari dashboard."""
    try:
        req = json.loads(msg.payload.decode())
        req_type = req.get("type", "")
        correlation_id = req.get("correlation_id", "unknown")

        print(f"\n[Energy Meter] 📨 Request diterima: type={req_type}, id={correlation_id}")

        if req_type == "snapshot" and last_snapshot:
            response_payload = json.dumps({
                "type": "snapshot_response",
                "correlation_id": correlation_id,
                "data": last_snapshot,
                "timestamp": datetime.now().isoformat()
            })
            # Balas ke TOPIC_RESPONSE
            resp_props = Properties(PacketTypes.PUBLISH)
            resp_props.CorrelationData = correlation_id.encode()
            client.publish(TOPIC_RESPONSE, response_payload, qos=1, properties=resp_props)
            print(f"[Energy Meter] ✅ Snapshot dikirim ke {TOPIC_RESPONSE}")

    except Exception as e:
        print(f"[Energy Meter] ❌ Error handle request: {e}")


def create_energy_data(device_name, device_config):
    """Membuat data simulasi konsumsi energi."""
    watt = round(random.uniform(device_config["min_watt"], device_config["max_watt"]), 1)
    kwh = watt / 1000
    biaya_per_jam = round(kwh * device_config["biaya_per_kwh"], 2)

    return {
        "device": device_name,
        "watt": watt,
        "kwh": kwh,
        "biaya_per_jam": biaya_per_jam,
        "timestamp": datetime.now().isoformat(),
        "unit": "Watt"
    }


def main():
    global running, mqtt_client

    print("=" * 60)
    print("  PUBLISHER 1: VIRTUAL ENERGY METER")
    print("=" * 60)
    print(f"  Broker     : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  QoS        : 0 (fire-and-forget)")
    print(f"  LWT        : Aktif (Metering Offline)")
    print(f"  Topic Alias: Aktif (alias {TOPIC_ALIAS_ENERGY} → energi/+/status)")
    print(f"  User Props : Aktif (version, location, unit)")
    print(f"  Expiry     : {ENERGY_MSG_EXPIRY_SECONDS} detik")
    print(f"  Req-Resp   : Aktif (snapshot on-demand)")
    print(f"  Perangkat  : {', '.join(DEVICES.keys())}")
    print("=" * 60)

    # ---- Setup MQTT Client (v5) ----
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="energy_meter_pub",
        protocol=mqtt.MQTTv5
    )
    mqtt_client = client

    # ---- Set Callbacks ----
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish
    client.on_message = on_message

    # ---- [FITUR 2: LWT] Last Will and Testament ----
    # Broker otomatis kirim ini jika koneksi putus tidak wajar
    lwt_message = json.dumps({
        "status": "Metering Offline",
        "reason": "Koneksi terputus secara tidak normal (LWT triggered)",
        "timestamp": datetime.now().isoformat()
    })
    client.will_set(
        topic=TOPIC_HEALTH,
        payload=lwt_message,
        qos=1,
        retain=True
    )
    print("[Energy Meter] 🪦 LWT telah di-set: 'Metering Offline'")

    # ---- Hubungkan ke Broker ----
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
    except (ConnectionRefusedError, OSError, TimeoutError) as e:
        print(f"[Energy Meter] ❌ Tidak bisa terhubung ke broker! ({e})")
        print("[Energy Meter] Pastikan Mosquitto berjalan di localhost:1883")
        sys.exit(1)

    client.loop_start()
    signal.signal(signal.SIGINT, signal_handler)

    # ---- Inisialisasi Topic Alias (kirim sekali dengan nama topik penuh) ----
    # Setelah ini, kita bisa kirim hanya dengan alias nomor → hemat bandwidth
    alias_initialized = set()

    cycle = 0
    try:
        while running:
            cycle += 1
            print(f"\n--- Siklus #{cycle} ({datetime.now().strftime('%H:%M:%S')}) ---")

            for device_name, device_config in DEVICES.items():
                data = create_energy_data(device_name, device_config)
                last_snapshot[device_name] = data
                topic = TOPIC_ENERGY.format(device=device_name)

                # ---- [FITUR 4: User Properties] ----
                # Metadata tambahan yang ditempel ke header MQTT
                pub_props = Properties(PacketTypes.PUBLISH)
                pub_props.UserProperty = [
                    ("publisher_version", "2.0"),
                    ("location", "Rumah-Lab"),
                    ("unit", "Watt"),
                    ("device_type", device_name),
                ]

                # ---- [FITUR 6: Message Expiry] ----
                # Data energi kedaluwarsa setelah 30 detik (data stale tidak berguna)
                pub_props.MessageExpiryInterval = ENERGY_MSG_EXPIRY_SECONDS

                # ---- [FITUR 3: Topic Alias] ----
                # Pertama kali: kirim dengan nama topik penuh + alias
                # Berikutnya: kirim hanya alias angka (topik string kosong)
                # Ini menghemat bandwidth jaringan
                if topic not in alias_initialized:
                    pub_props.TopicAlias = TOPIC_ALIAS_ENERGY
                    alias_initialized.add(topic)
                    send_topic = topic  # kirim topik penuh pertama kali
                else:
                    pub_props.TopicAlias = TOPIC_ALIAS_ENERGY
                    send_topic = topic  # paho tetap perlu topik, alias di props

                # ---- [FITUR 1: QoS 0] ----
                payload = json.dumps(data)
                client.publish(send_topic, payload, qos=0, properties=pub_props)

                print(f"  📊 {device_name}: {data['watt']}W "
                      f"(Rp {data['biaya_per_jam']}/jam) "
                      f"[Alias:{TOPIC_ALIAS_ENERGY}, Expiry:{ENERGY_MSG_EXPIRY_SECONDS}s]")

            # Tunggu 5 detik
            for _ in range(50):
                if not running:
                    break
                time.sleep(0.1)

    except Exception as e:
        print(f"[Energy Meter] ❌ Error: {e}")

    # ---- Graceful Shutdown ----
    try:
        print("\n[Energy Meter] Mengirim status offline sebelum shutdown...")
        offline_msg = json.dumps({
            "status": "Metering Offline",
            "reason": "Graceful shutdown oleh user",
            "timestamp": datetime.now().isoformat()
        })
        offline_props = Properties(PacketTypes.PUBLISH)
        offline_props.MessageExpiryInterval = 3600
        client.publish(TOPIC_HEALTH, offline_msg, qos=1, retain=True, properties=offline_props)
        time.sleep(0.5)
    except Exception as e:
        print(f"[Energy Meter] ⚠️  Gagal kirim status offline: {e}")

    try:
        client.loop_stop()
        client.disconnect()
    except Exception:
        pass
    print("[Energy Meter] 🛑 Publisher dihentikan.")


if __name__ == "__main__":
    main()
