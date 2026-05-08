# ============================================================
# Subscriber 2: Automation & Alert Engine
# ============================================================
# Logika otomasi:
#   - Jika total energi > budget → publish "MATIKAN PERANGKAT!"
#   - Deteksi anomali (listrik tinggi di jam tidak wajar)
#   - Kirim notifikasi Telegram (opsional)
# ============================================================

import json
import time
import signal
import sys
from datetime import datetime

import paho.mqtt.client as mqtt

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE,
    TOPIC_ENERGY_WILDCARD, TOPIC_SUBS_WILDCARD,
    TOPIC_BUDGET, TOPIC_HEALTH, TOPIC_ALERT,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    TARIF_LISTRIK_PER_KWH
)

running = True

# ---- State ----
energy_data = {}       # {device: {watt, biaya_per_jam, ...}}
subs_data = {}         # {service: {status, harga, ...}}
budget_limit = 500000  # Default Rp500.000
alert_cooldown = {}    # Mencegah spam alert


def signal_handler(sig, frame):
    global running
    print("\n[Alert Engine] Menerima sinyal shutdown...")
    running = False


def send_telegram(message):
    """Kirim notifikasi ke Telegram (jika dikonfigurasi)."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    if not HAS_REQUESTS:
        return False

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, data=data, timeout=5)
        return response.status_code == 200
    except Exception as e:
        print(f"[Alert Engine] ⚠️  Gagal kirim Telegram: {e}")
        return False


def can_send_alert(alert_type, cooldown_seconds=30):
    """Cegah spam alert — cooldown 30 detik per tipe."""
    now = time.time()
    if alert_type in alert_cooldown:
        if now - alert_cooldown[alert_type] < cooldown_seconds:
            return False
    alert_cooldown[alert_type] = now
    return True


def check_budget_exceeded(client):
    """Cek apakah estimasi biaya melebihi budget."""
    if not energy_data:
        return

    # Hitung total biaya energi per jam
    total_biaya_per_jam = sum(d.get("biaya_per_jam", 0) for d in energy_data.values())
    # Estimasi biaya per bulan (24 jam × 30 hari)
    estimasi_bulanan_energi = total_biaya_per_jam * 24 * 30

    # Total biaya langganan
    total_biaya_subs = sum(d.get("harga_langganan", 0) for d in subs_data.values())

    total_estimasi = estimasi_bulanan_energi + total_biaya_subs
    persentase = (total_estimasi / budget_limit * 100) if budget_limit > 0 else 0

    if total_estimasi > budget_limit and can_send_alert("budget_exceeded"):
        alert_msg = {
            "type": "BUDGET_EXCEEDED",
            "message": "⚠️ MATIKAN PERANGKAT! Estimasi pengeluaran MELEBIHI budget!",
            "estimasi_bulanan": round(total_estimasi),
            "budget_limit": budget_limit,
            "persentase": round(persentase, 1),
            "detail_energi": round(estimasi_bulanan_energi),
            "detail_subs": round(total_biaya_subs),
            "timestamp": datetime.now().isoformat()
        }

        client.publish(TOPIC_ALERT, json.dumps(alert_msg), qos=1)

        print(f"\n  🚨🚨🚨 ALERT: BUDGET EXCEEDED! 🚨🚨🚨")
        print(f"  Estimasi: Rp{total_estimasi:,.0f} > Budget: Rp{budget_limit:,}")
        print(f"  Persentase: {persentase:.1f}%")
        print(f"  → Pesan dikirim ke dashboard!")

        # Kirim ke Telegram
        tg_msg = (
            f"🚨 <b>BUDGET EXCEEDED!</b>\n\n"
            f"💰 Budget: Rp{budget_limit:,}\n"
            f"📊 Estimasi: Rp{total_estimasi:,.0f}\n"
            f"⚡ Energi: Rp{estimasi_bulanan_energi:,.0f}/bln\n"
            f"📱 Langganan: Rp{total_biaya_subs:,}/bln\n"
            f"📈 {persentase:.1f}% dari budget\n\n"
            f"<b>MATIKAN PERANGKAT YANG TIDAK PERLU!</b>"
        )
        if send_telegram(tg_msg):
            print(f"  📲 Notifikasi Telegram terkirim!")

    elif persentase > 80 and can_send_alert("budget_warning", cooldown_seconds=60):
        alert_msg = {
            "type": "BUDGET_WARNING",
            "message": f"⚠️ Peringatan! Budget sudah {persentase:.1f}%!",
            "estimasi_bulanan": round(total_estimasi),
            "budget_limit": budget_limit,
            "persentase": round(persentase, 1),
            "timestamp": datetime.now().isoformat()
        }
        client.publish(TOPIC_ALERT, json.dumps(alert_msg), qos=1)
        print(f"\n  ⚠️  WARNING: Budget sudah {persentase:.1f}%")


def check_anomaly(client, device, watt):
    """Deteksi anomali: listrik tinggi di jam tidak wajar (00:00 - 05:00)."""
    hour = datetime.now().hour

    # Anomali: AC atau PC masih nyala di jam 00:00 - 05:00 dengan watt tinggi
    if 0 <= hour < 5 and watt > 100 and device in ["AC", "PC"]:
        if can_send_alert(f"anomaly_{device}", cooldown_seconds=60):
            alert_msg = {
                "type": "ANOMALY",
                "message": f"🔍 Anomali! {device} masih menyala {watt}W di jam {hour}:00!",
                "device": device,
                "watt": watt,
                "jam": hour,
                "timestamp": datetime.now().isoformat()
            }
            client.publish(TOPIC_ALERT, json.dumps(alert_msg), qos=1)
            print(f"\n  🔍 ANOMALI: {device} nyala {watt}W di jam {hour}:00!")

            tg_msg = (
                f"🔍 <b>ANOMALI TERDETEKSI</b>\n\n"
                f"📟 {device}: {watt}W\n"
                f"🕐 Jam: {hour}:00\n"
                f"❓ Perangkat menyala di jam tidak wajar!\n"
                f"Cek apakah ada yang lupa matikan."
            )
            send_telegram(tg_msg)


def check_low_usage_subs(client):
    """Cek langganan yang jarang dipakai."""
    low_usage = [s for s, d in subs_data.items() if d.get("status") == "Low Usage"]

    if low_usage and can_send_alert("low_usage_subs", cooldown_seconds=120):
        total_waste = sum(subs_data[s].get("harga_langganan", 0) for s in low_usage)
        alert_msg = {
            "type": "LOW_USAGE_SUBS",
            "message": f"💸 {len(low_usage)} langganan jarang dipakai! Potensi hemat Rp{total_waste:,}/bln",
            "services": low_usage,
            "potensi_hemat": total_waste,
            "timestamp": datetime.now().isoformat()
        }
        client.publish(TOPIC_ALERT, json.dumps(alert_msg), qos=1)
        print(f"\n  💸 LOW USAGE: {', '.join(low_usage)} (hemat Rp{total_waste:,}/bln)")


# ---- MQTT Callbacks ----
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[Alert Engine] ✅ Terhubung ke broker {MQTT_BROKER}:{MQTT_PORT}")

        # Subscribe ke semua topik relevan
        client.subscribe(TOPIC_ENERGY_WILDCARD, qos=0)
        client.subscribe(TOPIC_SUBS_WILDCARD, qos=1)
        client.subscribe(TOPIC_BUDGET, qos=2)
        client.subscribe(TOPIC_HEALTH, qos=1)

        print(f"[Alert Engine] 📡 Subscribe ke:")
        print(f"  • {TOPIC_ENERGY_WILDCARD} (QoS 0)")
        print(f"  • {TOPIC_SUBS_WILDCARD} (QoS 1)")
        print(f"  • {TOPIC_BUDGET} (QoS 2 - exactly once)")
        print(f"  • {TOPIC_HEALTH} (QoS 1)")
    else:
        print(f"[Alert Engine] ❌ Gagal terhubung: {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    """Callback saat terputus dari broker."""
    print(f"[Alert Engine] ⚠️  Terputus dari broker (kode: {reason_code})")


def on_message(client, userdata, msg):
    global budget_limit

    try:
        payload = json.loads(msg.payload.decode())
        topic = msg.topic

        # ---- Energy Data ----
        if topic.startswith("finansial/energi/"):
            device = payload.get("device", "unknown")
            watt = payload.get("watt", 0)
            energy_data[device] = payload
            print(f"  ⚡ {device}: {watt}W", end="")

            # Cek anomali
            check_anomaly(client, device, watt)

            # Cek budget
            check_budget_exceeded(client)
            print()

        # ---- Subscription Data ----
        elif topic.startswith("finansial/subs/"):
            service = payload.get("service", "unknown")
            subs_data[service] = payload
            status = payload.get("status", "")
            if status == "Low Usage":
                print(f"  📱 {service}: LOW USAGE ⚠️")

            # Cek low usage subscriptions
            check_low_usage_subs(client)

        # ---- Budget Data ----
        elif topic == TOPIC_BUDGET:
            new_limit = payload.get("limit", budget_limit)
            budget_limit = new_limit
            print(f"\n  💰 Budget updated: Rp{budget_limit:,}")

            # Re-check budget
            check_budget_exceeded(client)

        # ---- Health/LWT ----
        elif topic == TOPIC_HEALTH:
            status = payload.get("status", "")
            if "Offline" in status:
                print(f"\n  🔴 SYSTEM HEALTH: {status}")
                if can_send_alert("system_offline"):
                    alert_msg = {
                        "type": "SYSTEM_OFFLINE",
                        "message": "🔴 Energy Meter OFFLINE! Monitoring terputus.",
                        "timestamp": datetime.now().isoformat()
                    }
                    client.publish(TOPIC_ALERT, json.dumps(alert_msg), qos=1)

                    tg_msg = (
                        f"🔴 <b>SYSTEM OFFLINE</b>\n\n"
                        f"Energy Meter terputus!\n"
                        f"Monitoring energi tidak aktif.\n"
                        f"Cek koneksi perangkat."
                    )
                    send_telegram(tg_msg)
            else:
                print(f"\n  🟢 SYSTEM HEALTH: {status}")

    except json.JSONDecodeError:
        print(f"[Alert Engine] ⚠️  Payload bukan JSON: {msg.payload}")
    except Exception as e:
        print(f"[Alert Engine] ❌ Error processing message: {e}")


def main():
    global running

    print("=" * 60)
    print("  SUBSCRIBER 2: AUTOMATION & ALERT ENGINE")
    print("=" * 60)
    print(f"  Broker  : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  Telegram: {'✅ Aktif' if TELEGRAM_BOT_TOKEN else '❌ Tidak dikonfigurasi'}")
    print(f"  Budget  : Rp{budget_limit:,} (default)")
    print("=" * 60)
    print("\n[Alert Engine] Menunggu data...")

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="alert_engine_sub",
        protocol=mqtt.MQTTv5
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
    except (ConnectionRefusedError, OSError, TimeoutError) as e:
        print(f"[Alert Engine] ❌ Tidak bisa terhubung ke broker! ({e})")
        print("[Alert Engine] Pastikan Mosquitto berjalan di localhost:1883")
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    client.loop_start()

    try:
        while running:
            time.sleep(0.1)
    except Exception as e:
        print(f"[Alert Engine] ❌ Error: {e}")

    try:
        client.loop_stop()
        client.disconnect()
    except Exception:
        pass
    print("[Alert Engine] 🛑 Alert Engine dihentikan.")


if __name__ == "__main__":
    main()
