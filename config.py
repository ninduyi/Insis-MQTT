# ============================================================
# Smart Finance & Energy Orchestrator - Konfigurasi Sentral
# ============================================================

# --- MQTT Broker ---
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 10  # detik (sengaja pendek untuk demo Keep Alive)

# --- Topik MQTT ---
TOPIC_ENERGY = "finansial/energi/{device}/status"
TOPIC_SUBS = "finansial/subs/{service}/usage"
TOPIC_BUDGET = "finansial/budget/limit"
TOPIC_HEALTH = "finansial/system/health"
TOPIC_ALERT = "finansial/alerts/warning"

# --- Wildcard Subscribe ---
TOPIC_ENERGY_WILDCARD = "finansial/energi/+/status"
TOPIC_SUBS_WILDCARD = "finansial/subs/+/usage"

# --- Simulasi Perangkat ---
DEVICES = {
    "PC": {"min_watt": 120, "max_watt": 200, "biaya_per_kwh": 1444.70},
    "Lampu": {"min_watt": 15, "max_watt": 25, "biaya_per_kwh": 1444.70},
    "AC": {"min_watt": 400, "max_watt": 800, "biaya_per_kwh": 1444.70},
    "Kulkas": {"min_watt": 80, "max_watt": 150, "biaya_per_kwh": 1444.70},
}

# --- Simulasi Langganan ---
SUBSCRIPTIONS = {
    "Netflix": {"harga": 54000, "jam_per_bulan": 0},
    "Spotify": {"harga": 54990, "jam_per_bulan": 0},
    "ChatGPT_Plus": {"harga": 320000, "jam_per_bulan": 0},
    "YouTube_Premium": {"harga": 68900, "jam_per_bulan": 0},
    "Adobe_CC": {"harga": 150000, "jam_per_bulan": 0},
}

# Threshold: jika pemakaian < X jam/bulan, dianggap "Low Usage"
LOW_USAGE_THRESHOLD_HOURS = 5

# --- Telegram (Opsional) ---
TELEGRAM_BOT_TOKEN = ""  # Isi dengan token dari @BotFather
TELEGRAM_CHAT_ID = ""    # Isi dengan chat_id kamu

# --- Dashboard ---
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 5000

# --- Biaya Listrik ---
TARIF_LISTRIK_PER_KWH = 1444.70  # Rp/kWh (tarif PLN golongan R1 900VA)
