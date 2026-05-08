# ============================================================
# Publisher 3: Budget & Policy Manager
# ============================================================
# Tempat user menginput batas maksimal pengeluaran bulanan.
# Fitur MQTT:
#   - QoS 2 (exactly once delivery - data budget HARUS sampai tepat 1 kali)
#   - Retained Message = True
# ============================================================

import json
import time
import signal
import sys
from datetime import datetime

import paho.mqtt.client as mqtt

from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE,
    TOPIC_BUDGET
)

running = True


def signal_handler(sig, frame):
    global running
    print("\n[Budget Manager] Menerima sinyal shutdown...")
    running = False


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[Budget Manager] ✅ Terhubung ke broker {MQTT_BROKER}:{MQTT_PORT}")
    else:
        print(f"[Budget Manager] ❌ Gagal terhubung, kode: {reason_code}")


def on_publish(client, userdata, mid, reason_code, properties):
    """Konfirmasi QoS 2 — pesan telah melalui 4-way handshake (PUBLISH→PUBREC→PUBREL→PUBCOMP)."""
    print(f"[Budget Manager] ✅ Pesan budget berhasil dikirim (mid: {mid}, QoS 2 COMPLETE)")


def on_disconnect(client, userdata, flags, reason_code, properties):
    """Callback saat terputus dari broker."""
    print(f"[Budget Manager] ⚠️  Terputus dari broker (kode: {reason_code})")


def main():
    global running

    print("=" * 60)
    print("  PUBLISHER 3: BUDGET & POLICY MANAGER")
    print("=" * 60)
    print(f"  Broker   : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  QoS      : 2 (exactly once delivery)")
    print(f"  Retained : ✅ True")
    print(f"  Topik    : {TOPIC_BUDGET}")
    print("=" * 60)

    # ---- Setup MQTT Client ----
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="budget_manager_pub",
        protocol=mqtt.MQTTv5
    )
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
    except (ConnectionRefusedError, OSError, TimeoutError) as e:
        print(f"[Budget Manager] ❌ Tidak bisa terhubung ke broker! ({e})")
        print("[Budget Manager] Pastikan Mosquitto berjalan di localhost:1883")
        sys.exit(1)

    client.loop_start()

    signal.signal(signal.SIGINT, signal_handler)
    time.sleep(1)  # Tunggu koneksi stabil

    print("\n📋 MENU BUDGET MANAGER")
    print("-" * 40)

    try:
        while running:
            print("\nPilihan:")
            print("  1. Set batas anggaran bulanan")
            print("  2. Set batas per-kategori")
            print("  3. Lihat anggaran saat ini")
            print("  4. Keluar")

            try:
                choice = input("\nPilihan (1-4): ").strip()
            except EOFError:
                break

            if choice == "1":
                try:
                    amount = input("Masukkan batas anggaran bulanan (Rp): ").strip()
                    amount = int(amount.replace(".", "").replace(",", ""))

                    budget_data = {
                        "type": "total_budget",
                        "limit": amount,
                        "limit_formatted": f"Rp{amount:,}",
                        "bulan": datetime.now().strftime("%B %Y"),
                        "timestamp": datetime.now().isoformat()
                    }

                    payload = json.dumps(budget_data)

                    # ---- QoS 2: Exactly Once Delivery ----
                    # 4-way handshake: PUBLISH → PUBREC → PUBREL → PUBCOMP
                    # Menjamin data budget diterima TEPAT SATU KALI (tidak duplikat)
                    result = client.publish(
                        TOPIC_BUDGET,
                        payload,
                        qos=2,
                        retain=True  # Subscriber baru langsung dapat budget
                    )

                    print(f"\n  💰 Budget di-set: Rp{amount:,}")
                    print(f"  📡 Topik: {TOPIC_BUDGET}")
                    print(f"  🔒 QoS: 2 (4-way handshake: PUBLISH→PUBREC→PUBREL→PUBCOMP)")

                except ValueError:
                    print("  ❌ Input tidak valid! Masukkan angka.")

            elif choice == "2":
                try:
                    print("\nKategori yang tersedia:")
                    print("  a. Energi/Listrik")
                    print("  b. Langganan Digital")
                    cat = input("Pilih kategori (a/b): ").strip().lower()

                    category = "energi" if cat == "a" else "langganan"
                    amount = input(f"Masukkan batas untuk {category} (Rp): ").strip()
                    amount = int(amount.replace(".", "").replace(",", ""))

                    budget_data = {
                        "type": "category_budget",
                        "category": category,
                        "limit": amount,
                        "limit_formatted": f"Rp{amount:,}",
                        "bulan": datetime.now().strftime("%B %Y"),
                        "timestamp": datetime.now().isoformat()
                    }

                    payload = json.dumps(budget_data)
                    client.publish(TOPIC_BUDGET, payload, qos=2, retain=True)

                    print(f"\n  💰 Budget {category}: Rp{amount:,}")

                except ValueError:
                    print("  ❌ Input tidak valid!")

            elif choice == "3":
                print("\n  ℹ️  Buka dashboard untuk melihat status anggaran lengkap.")
                print(f"  🌐 http://localhost:5000")

            elif choice == "4":
                running = False

            else:
                print("  ❌ Pilihan tidak valid!")

    except Exception as e:
        print(f"[Budget Manager] ❌ Error: {e}")

    try:
        client.loop_stop()
        client.disconnect()
    except Exception:
        pass
    print("[Budget Manager] 🛑 Publisher dihentikan.")


if __name__ == "__main__":
    main()
