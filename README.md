# 🚀 Smart Finance & Energy Orchestrator

Sistem monitoring keuangan dan energi real-time berbasis **MQTT** dengan dashboard web interaktif.

## 📋 Arsitektur Sistem

```
┌─────────────────────────────────────────────────────────────────┐
│                        MQTT Broker (Mosquitto)                  │
│                         localhost:1883                           │
└──────┬──────────────┬──────────────┬──────────────┬─────────────┘
       │              │              │              │
  ┌────▼────┐   ┌─────▼─────┐  ┌────▼────┐   ┌────▼────┐
  │Publisher1│   │Publisher 2 │  │Publisher3│   │  LWT    │
  │ Energy   │   │   Subs     │  │ Budget  │   │(auto)   │
  │ Meter    │   │  Tracker   │  │ Manager │   │         │
  │ QoS 0   │   │QoS1+Retain │  │ QoS 2   │   │         │
  └─────────┘   └────────────┘  └─────────┘   └─────────┘
       │              │              │              │
  ┌────▼──────────────▼──────────────▼──────────────▼─────────────┐
  │              Subscriber 1: Web Dashboard                      │
  │         (Flask + SocketIO + Chart.js) port 5000               │
  │              Wildcard: finansial/energi/+/status               │
  └───────────────────────────────────────────────────────────────┘
  ┌───────────────────────────────────────────────────────────────┐
  │           Subscriber 2: Automation & Alert Engine             │
  │         Budget check + Anomaly detection + Telegram           │
  └───────────────────────────────────────────────────────────────┘
```

## 📂 Struktur File

```
Tugas MQTT/
├── config.py                    # Konfigurasi sentral
├── requirements.txt             # Dependencies
├── publisher_energy_meter.py    # Publisher 1: Virtual Energy Meter
├── publisher_subs_tracker.py    # Publisher 2: Subscription Tracker
├── publisher_budget_manager.py  # Publisher 3: Budget Manager
├── subscriber_dashboard.py      # Subscriber 1: Web Dashboard
├── subscriber_alert_engine.py   # Subscriber 2: Alert Engine
├── templates/
│   └── dashboard.html           # Frontend dashboard
└── README.md
```

## 🔧 Instalasi

### 1. Install Mosquitto MQTT Broker

Download dari: https://mosquitto.org/download/

Setelah install, jalankan broker:
```bash
mosquitto -v
```
> Flag `-v` untuk verbose mode agar terlihat log Keep Alive, koneksi, dll.

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

## 🚀 Cara Menjalankan

Buka **5 terminal** terpisah dan jalankan secara berurutan:

### Terminal 1: Mosquitto Broker
```bash
mosquitto -v
```

### Terminal 2: Web Dashboard (Subscriber 1)
```bash
python subscriber_dashboard.py
```
Buka browser: **http://localhost:5000**

### Terminal 3: Alert Engine (Subscriber 2)
```bash
python subscriber_alert_engine.py
```

### Terminal 4: Energy Meter (Publisher 1)
```bash
python publisher_energy_meter.py
```

### Terminal 5: Subscription Tracker (Publisher 2)
```bash
python publisher_subs_tracker.py
```

### Terminal 6 (opsional): Budget Manager (Publisher 3)
```bash
python publisher_budget_manager.py
```
Ikuti menu interaktif untuk set budget.

## 📡 Struktur Topik MQTT

| Topik | QoS | Retained | Publisher |
|-------|-----|----------|----------|
| `finansial/energi/{device}/status` | 0 | No | Energy Meter |
| `finansial/subs/{service}/usage` | 1 | **Yes** | Subs Tracker |
| `finansial/budget/limit` | **2** | **Yes** | Budget Manager |
| `finansial/system/health` | 1 | **Yes** | LWT (Energy Meter) |
| `finansial/alerts/warning` | 1 | No | Alert Engine |

## ✅ Fitur MQTT yang Diimplementasikan

### 1. Last Will and Testament (LWT)
- Energy Meter mendaftarkan LWT saat connect
- Jika Energy Meter mati (Ctrl+C / kill), broker otomatis publish `"Metering Offline"`
- Dashboard berubah status menjadi merah "Disconnected"

**Demo:** Matikan paksa `publisher_energy_meter.py` → lihat dashboard berubah status

### 2. Wildcard (+)
- Dashboard subscribe ke `finansial/energi/+/status`
- Satu subscription menangkap semua device (PC, Lampu, AC, Kulkas)
- Juga `finansial/subs/+/usage` untuk semua layanan

### 3. QoS Level (Semua Level Digunakan)
- **QoS 0** (Fire & Forget): Data energi real-time → `publisher_energy_meter.py`
- **QoS 1** (At Least Once): Data langganan → `publisher_subs_tracker.py`
- **QoS 2** (Exactly Once): Data budget → `publisher_budget_manager.py` (4-way handshake: PUBLISH→PUBREC→PUBREL→PUBCOMP)

### 4. Retained Message
- Status langganan dan budget disimpan di broker
- Subscriber baru langsung dapat data terakhir tanpa menunggu

### 5. Keep Alive
- Diset 10 detik (pendek untuk demo)
- Terlihat di log Mosquitto sebagai PINGREQ/PINGRESP

