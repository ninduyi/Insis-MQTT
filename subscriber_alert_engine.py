# ============================================================
# Subscriber 2: Automation & Alert Engine
# ============================================================
# Logika otomasi dan deteksi anomali.
# Fitur MQTT v5 yang diimplementasikan:
#   [9] Shared Subscription → $share/alert_group/... untuk load balancing
#       (Jika 2 Alert Engine berjalan, pesan dibagi rata antar instance)
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
    TOPIC_SHARED_ENERGY, TOPIC_SHARED_SUBS,
    TOPIC_ENERGY_WILDCARD, TOPIC_SUBS_WILDCARD,
    TOPIC_BUDGET, TOPIC_HEALTH, TOPIC_ALERT,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    TARIF_LISTRIK_PER_KWH
)

running = True

# ---- State ----
energy_data = {}
subs_data = {}
budget_limit = 500000
alert_cooldown = {}


def signal_handler(sig, frame):
    global running
    print("\n[Alert Engine] Menerima sinyal shutdown...")
    running = False


def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    if not HAS_REQUESTS:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        response = requests.post(url, data=data, timeout=5)
        return response.status_code == 200
    except Exception as e:
        print(f"[Alert Engine] ⚠️  Gagal kirim Telegram: {e}")
        return False


def can_send_alert(alert_type, cooldown_seconds=30):
    now = time.time()
    if alert_type in alert_cooldown:
        if now - alert_cooldown[alert_type] < cooldown_seconds:
            return False
    alert_cooldown[alert_type] = now
    return True


def check_budget_exceeded(client):
    if not energy_data:
        return
    total_biaya_per_jam = sum(d.get("biaya_per_jam", 0) for d in energy_data.values())
    estimasi_bulanan_energi = total_biaya_per_jam * 24 * 30
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

        tg_msg = (
            f"🚨 <b>BUDGET EXCEEDED!</b>\n\n"
            f"💰 Budget: Rp{budget_limit:,}\n"
            f"📊 Estimasi: Rp{total_estimasi:,.0f}\n"
            f"⚡ Energi: Rp{estimasi_bulanan_energi:,.0f}/bln\n"
            f"📱 Langganan: Rp{total_biaya_subs:,}/bln\n"
            f"📈 {persentase:.1f}% dari budget"
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
    hour = datetime.now().hour
    if 0 <= hour < 5 and watt > 100 and device in ["AC", "PC"]:
        if can_send_alert(f"anomaly_{device}", cooldown_seconds=60):
            alert_msg = {
                "type": "ANOMALY",
                "message": f"🔍 Anomali! {device} masih menyala {watt}W di jam {hour}:00!",
                "device": device, "watt": watt, "jam": hour,
                "timestamp": datetime.now().isoformat()
            }
            client.publish(TOPIC_ALERT, json.dumps(alert_msg), qos=1)
            print(f"\n  🔍 ANOMALI: {device} nyala {watt}W di jam {hour}:00!")


def check_low_usage_subs(client):
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

        # ---- [FITUR 9: Shared Subscription] ----
        # Jika ada 2 instance Alert Engine, pesan dibagi (load-balanced)
        # Format: $share/{group}/{topic_filter}
        client.subscribe(TOPIC_SHARED_ENERGY, qos=0)
        print(f"  📡 [SHARED] {TOPIC_SHARED_ENERGY}")

        client.subscribe(TOPIC_SHARED_SUBS, qos=1)
        print(f"  📡 [SHARED] {TOPIC_SHARED_SUBS}")

        # Budget, Health → subscribe normal (TIDAK shared, harus diterima semua instance)
        client.subscribe(TOPIC_BUDGET, qos=2)
        print(f"  📡 [NORMAL] {TOPIC_BUDGET} (QoS 2)")

        client.subscribe(TOPIC_HEALTH, qos=1)
        print(f"  📡 [NORMAL] {TOPIC_HEALTH}")
    else:
        print(f"[Alert Engine] ❌ Gagal terhubung: {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    print(f"[Alert Engine] ⚠️  Terputus dari broker (kode: {reason_code})")


def on_message(client, userdata, msg):
    global budget_limit

    # ---- Tampilkan User Properties jika ada ----
    user_props = {}
    if msg.properties and hasattr(msg.properties, 'UserProperty'):
        for k, v in (msg.properties.UserProperty or []):
            user_props[k] = v

    try:
        payload = json.loads(msg.payload.decode())
        topic = msg.topic

        # Normalisasi topik dari shared subscription (hapus prefix $share/group/)
        normalized_topic = topic
        if topic.startswith("$share/"):
            parts = topic.split("/", 2)
            if len(parts) == 3:
                normalized_topic = parts[2]

        if normalized_topic.startswith("finansial/energi/"):
            device = payload.get("device", "unknown")
            watt = payload.get("watt", 0)
            energy_data[device] = payload
            ver = user_props.get("publisher_version", "?")
            print(f"  ⚡ {device}: {watt}W [v{ver}]", end="")
            check_anomaly(client, device, watt)
            check_budget_exceeded(client)
            print()

        elif normalized_topic.startswith("finansial/subs/"):
            service = payload.get("service", "unknown")
            subs_data[service] = payload
            status = payload.get("status", "")
            if status == "Low Usage":
                print(f"  📱 {service}: LOW USAGE ⚠️")
            check_low_usage_subs(client)

        elif msg.topic == TOPIC_BUDGET:
            new_limit = payload.get("limit", budget_limit)
            budget_limit = new_limit
            print(f"\n  💰 Budget updated: Rp{budget_limit:,}")
            check_budget_exceeded(client)

        elif msg.topic == TOPIC_HEALTH:
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
                    tg_msg = "🔴 <b>SYSTEM OFFLINE</b>\n\nEnergy Meter terputus!\nMonitoring energi tidak aktif."
                    send_telegram(tg_msg)
            else:
                print(f"\n  🟢 SYSTEM HEALTH: {status}")

    except json.JSONDecodeError:
        print(f"[Alert Engine] ⚠️  Payload bukan JSON: {msg.payload}")
    except Exception as e:
        print(f"[Alert Engine] ❌ Error: {e}")


def main():
    global running

    print("=" * 60)
    print("  SUBSCRIBER 2: AUTOMATION & ALERT ENGINE")
    print("=" * 60)
    print(f"  Broker     : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  Shared Sub : $share/alert_group/... (load balanced)")
    print(f"  Telegram   : {'✅ Aktif' if TELEGRAM_BOT_TOKEN else '❌ Tidak dikonfigurasi'}")
    print(f"  Budget     : Rp{budget_limit:,} (default)")
    print("=" * 60)
    print("\n[Alert Engine] 💡 Tip: Jalankan 2 instance ini untuk demo Shared Subscription!")
    print("[Alert Engine] Menunggu data...\n")

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
