# ============================================================
# Publisher 1: Virtual Energy Meter
# ============================================================
# Mensimulasikan konsumsi listrik perangkat rumah.
# Fitur MQTT:
#   - QoS 0 (data real-time, hilang satu paket tidak masalah)
#   - Last Will and Testament (LWT) → "Metering Offline"
#   - Keep Alive = 10 detik (untuk demo)
# ============================================================

import json
import random
import time
import signal
import sys
from datetime import datetime

import paho.mqtt.client as mqtt

from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE,
    TOPIC_ENERGY, TOPIC_HEALTH, DEVICES
)

# ---- Flag untuk graceful shutdown ----
running = True


def signal_handler(sig, frame):
    """Menangani Ctrl+C dengan graceful shutdown."""
    global running
    print("\n[Energy Meter] Menerima sinyal shutdown...")
    print("[Energy Meter] Mengirim status 'Metering Online → Shutting Down'...")
    running = False


def on_connect(client, userdata, flags, reason_code, properties):
    """Callback saat berhasil terhubung ke broker."""
    if reason_code == 0:
        print(f"[Energy Meter] ✅ Terhubung ke broker {MQTT_BROKER}:{MQTT_PORT}")
        print(f"[Energy Meter] Keep Alive: {MQTT_KEEPALIVE} detik")
        # Publish status online
        health_msg = json.dumps({
            "status": "Metering Online",
            "timestamp": datetime.now().isoformat()
        })
        client.publish(TOPIC_HEALTH, health_msg, qos=1, retain=True)
        print(f"[Energy Meter] 📡 Health status: Metering Online")
    else:
        print(f"[Energy Meter] ❌ Gagal terhubung, kode: {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    """Callback saat terputus dari broker."""
    print(f"[Energy Meter] ⚠️  Terputus dari broker (kode: {reason_code})")


def on_publish(client, userdata, mid, reason_code, properties):
    """Callback saat pesan berhasil dikirim."""
    pass  # QoS 0 tidak perlu konfirmasi


def create_energy_data(device_name, device_config):
    """Membuat data simulasi konsumsi energi."""
    watt = round(random.uniform(device_config["min_watt"], device_config["max_watt"]), 1)
    # Hitung estimasi biaya per jam (kWh)
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
    global running

    print("=" * 60)
    print("  PUBLISHER 1: VIRTUAL ENERGY METER")
    print("=" * 60)
    print(f"  Broker    : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  QoS       : 0 (fire-and-forget)")
    print(f"  LWT       : Aktif (Metering Offline)")
    print(f"  Keep Alive: {MQTT_KEEPALIVE} detik")
    print(f"  Perangkat : {', '.join(DEVICES.keys())}")
    print("=" * 60)

    # ---- Setup MQTT Client ----
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="energy_meter_pub",
        protocol=mqtt.MQTTv5
    )

    # ---- Set Callbacks ----
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish

    # ---- Set Last Will and Testament (LWT) ----
    # Pesan ini OTOMATIS dikirim oleh broker jika client terputus secara tidak normal
    lwt_message = json.dumps({
        "status": "Metering Offline",
        "reason": "Koneksi terputus secara tidak normal",
        "timestamp": datetime.now().isoformat()
    })
    client.will_set(
        topic=TOPIC_HEALTH,
        payload=lwt_message,
        qos=1,
        retain=True  # Agar subscriber baru langsung tahu status offline
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

    # ---- Register Signal Handler ----
    signal.signal(signal.SIGINT, signal_handler)

    # ---- Loop Utama: Publish Data Energi ----
    cycle = 0
    try:
        while running:
            cycle += 1
            print(f"\n--- Siklus #{cycle} ({datetime.now().strftime('%H:%M:%S')}) ---")

            for device_name, device_config in DEVICES.items():
                # Buat data simulasi
                data = create_energy_data(device_name, device_config)

                # Tentukan topik
                topic = TOPIC_ENERGY.format(device=device_name)

                # Publish dengan QoS 0
                payload = json.dumps(data)
                client.publish(topic, payload, qos=0)

                print(f"  📊 {device_name}: {data['watt']}W "
                      f"(Rp {data['biaya_per_jam']}/jam) → {topic}")

            # Tunggu 5 detik
            for _ in range(50):  # 50 x 0.1s = 5s
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
        client.publish(TOPIC_HEALTH, offline_msg, qos=1, retain=True)
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
