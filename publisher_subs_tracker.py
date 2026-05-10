# ============================================================
# Publisher 2: Subscription Tracker
# ============================================================
# Mensimulasikan durasi pemakaian layanan digital.
# Jika layanan jarang dibuka → status "Low Usage"
# Fitur MQTT:
#   - Retained Message = True (subscriber baru langsung dapat data terakhir)
#   - QoS 1 (memastikan data sampai)
# ============================================================

import json
import os
import random
import time
import signal
import sys
from datetime import datetime

import paho.mqtt.client as mqtt

from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE,
    TOPIC_SUBS, SUBSCRIPTIONS, LOW_USAGE_THRESHOLD_HOURS
)

# ---- Flag untuk graceful shutdown ----
running = True

# ---- File state jam pemakaian ----
STATE_FILE = os.path.join(os.path.dirname(__file__), 'subs_usage_state.json')


def load_usage_state():
    """Load jam pemakaian yang tersimpan dari file JSON."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_usage_state(state):
    """Simpan jam pemakaian ke file JSON."""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[Subs Tracker] ⚠️  Gagal simpan state: {e}")


def signal_handler(sig, frame):
    global running
    print("\n[Subs Tracker] Menerima sinyal shutdown...")
    running = False


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[Subs Tracker] ✅ Terhubung ke broker {MQTT_BROKER}:{MQTT_PORT}")
    else:
        print(f"[Subs Tracker] ❌ Gagal terhubung, kode: {reason_code}")


def on_publish(client, userdata, mid, reason_code, properties):
    """Callback saat pesan berhasil dikirim (QoS 1 acknowledged)."""
    pass


def on_disconnect(client, userdata, flags, reason_code, properties):
    """Callback saat terputus dari broker."""
    print(f"[Subs Tracker] ⚠️  Terputus dari broker (kode: {reason_code})")


def simulate_usage(persisted_hours):
    """Simulasi jam pemakaian layanan — akumulasi dari state sebelumnya."""
    usage_data = {}
    for service, info in SUBSCRIPTIONS.items():
        # Tambahkan pemakaian acak (0.1 - 0.5 jam per siklus 10 detik)
        delta = round(random.uniform(0.1, 0.5), 2)
        prev  = persisted_hours.get(service, 0.0)
        jam   = round(min(prev + delta, 744), 1)  # cap 744 jam/bln (31 hari × 24)
        persisted_hours[service] = jam
        usage_data[service] = {
            "jam_per_bulan": jam,
            "harga": info["harga"],
            "status": "Low Usage" if jam < LOW_USAGE_THRESHOLD_HOURS else "Active",
            "rekomendasi": "Pertimbangkan UNSUB! 💸" if jam < LOW_USAGE_THRESHOLD_HOURS else "Tetap langganan ✅",
            "biaya_per_jam": round(info["harga"] / max(jam, 0.1), 2),
        }
    return usage_data


def main():
    global running

    print("=" * 60)
    print("  PUBLISHER 2: SUBSCRIPTION TRACKER")
    print("=" * 60)
    print(f"  Broker   : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  QoS      : 1 (at least once)")
    print(f"  Retained : ✅ True (subscriber baru langsung dapat data)")
    print(f"  Layanan  : {', '.join(SUBSCRIPTIONS.keys())}")
    print(f"  Threshold: < {LOW_USAGE_THRESHOLD_HOURS} jam/bulan = Low Usage")
    print("=" * 60)

    # ---- Setup MQTT Client ----
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="subs_tracker_pub",
        protocol=mqtt.MQTTv5
    )
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
    except (ConnectionRefusedError, OSError, TimeoutError) as e:
        print(f"[Subs Tracker] ❌ Tidak bisa terhubung ke broker! ({e})")
        print("[Subs Tracker] Pastikan Mosquitto berjalan di localhost:1883")
        sys.exit(1)

    client.loop_start()

    signal.signal(signal.SIGINT, signal_handler)

    # Load state jam pemakaian yang tersimpan
    persisted_hours = load_usage_state()
    print(f"  💾 State tersimpan: {persisted_hours if persisted_hours else 'belum ada (mulai dari 0)'}")

    cycle = 0
    try:
        while running:
            cycle += 1
            print(f"\n--- Siklus #{cycle} ({datetime.now().strftime('%H:%M:%S')}) ---")

            usage_data = simulate_usage(persisted_hours)
            total_biaya = 0

            for service, data in usage_data.items():
                topic = TOPIC_SUBS.format(service=service)
                payload = json.dumps({
                    "service": service,
                    "jam_per_bulan": data["jam_per_bulan"],
                    "harga_langganan": data["harga"],
                    "biaya_per_jam": data["biaya_per_jam"],
                    "status": data["status"],
                    "rekomendasi": data["rekomendasi"],
                    "timestamp": datetime.now().isoformat()
                })

                # ---- RETAINED MESSAGE ----
                # retain=True → broker menyimpan pesan terakhir
                # Subscriber baru langsung dapat data tanpa menunggu update
                client.publish(topic, payload, qos=1, retain=True)

                status_icon = "🔴" if data["status"] == "Low Usage" else "🟢"
                print(f"  {status_icon} {service}: {data['jam_per_bulan']} jam/bln "
                      f"| Rp{data['harga']:,} | {data['status']} "
                      f"| {data['rekomendasi']}")

                total_biaya += data["harga"]

            print(f"\n  💰 Total biaya langganan: Rp{total_biaya:,}/bulan")

            # Simpan state jam pemakaian
            save_usage_state(persisted_hours)

            # Tunggu 10 detik
            for _ in range(100):
                if not running:
                    break
                time.sleep(0.1)

    except Exception as e:
        print(f"[Subs Tracker] ❌ Error: {e}")

    try:
        client.loop_stop()
        client.disconnect()
    except Exception:
        pass
    print("[Subs Tracker] 🛑 Publisher dihentikan.")


if __name__ == "__main__":
    main()
