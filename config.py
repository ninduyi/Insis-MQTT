# ============================================================
# Smart Finance & Energy Orchestrator - Konfigurasi Sentral
# ============================================================

# --- MQTT Broker ---
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60  # detik

# --- Topik MQTT ---
TOPIC_ENERGY = "finansial/energi/{device}/status"
TOPIC_SUBS = "finansial/subs/{service}/usage"
TOPIC_BUDGET = "finansial/budget/limit"
TOPIC_HEALTH = "finansial/system/health"
TOPIC_ALERT = "finansial/alerts/warning"

# --- Wildcard Subscribe ---
TOPIC_ENERGY_WILDCARD = "finansial/energi/+/status"
TOPIC_SUBS_WILDCARD = "finansial/subs/+/usage"

# --- [FITUR 8: Request-Response] ---
# Dashboard mengirim request ke topik ini, publisher membalas ke response topic
TOPIC_REQUEST = "finansial/system/request"
TOPIC_RESPONSE = "finansial/system/response"

# --- [FITUR 9: Shared Subscription] ---
# Alert Engine subscribe via shared subscription untuk load balancing
# Format: $share/{group_name}/{topic_filter}
TOPIC_SHARED_ENERGY = "$share/alert_group/finansial/energi/+/status"
TOPIC_SHARED_SUBS = "$share/alert_group/finansial/subs/+/usage"

# --- Simulasi Perangkat ---
DEVICES = {
    "PC":      {"min_watt": 120, "max_watt": 200, "biaya_per_kwh": 1444.70},
    "Lampu":   {"min_watt": 15,  "max_watt": 25,  "biaya_per_kwh": 1444.70},
    "AC":      {"min_watt": 400, "max_watt": 800,  "biaya_per_kwh": 1444.70},
    "Kulkas":  {"min_watt": 80,  "max_watt": 150,  "biaya_per_kwh": 1444.70},
}

# --- Simulasi Langganan ---
SUBSCRIPTIONS = {
    "Netflix":          {"harga": 54000,  "jam_per_bulan": 0},
    "Spotify":          {"harga": 54990,  "jam_per_bulan": 0},
    "ChatGPT_Plus":     {"harga": 320000, "jam_per_bulan": 0},
    "YouTube_Premium":  {"harga": 68900,  "jam_per_bulan": 0},
    "Adobe_CC":         {"harga": 150000, "jam_per_bulan": 0},
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

# --- [FITUR 10: Flow Control] ---
# Batasi jumlah pesan in-flight yang bisa diterima subscriber sekaligus
# Mencegah subscriber kelebihan beban saat banyak data masuk bersamaan
FLOW_CONTROL_RECEIVE_MAX = 20  # maksimal 20 pesan MQTT QoS1/2 in-flight sekaligus

# --- [FITUR 3: Topic Alias] ---
# Alias untuk topik yang sering digunakan (hemat bandwidth jaringan)
TOPIC_ALIAS_ENERGY = 1   # alias 1 = finansial/energi/{device}/status
TOPIC_ALIAS_HEALTH = 2   # alias 2 = finansial/system/health

# --- [FITUR 6: Message Expiry] ---
# Pesan energi kedaluwarsa setelah 30 detik (data stale tidak berguna)
ENERGY_MSG_EXPIRY_SECONDS = 30
# Pesan subs kedaluwarsa setelah 120 detik
SUBS_MSG_EXPIRY_SECONDS = 120
