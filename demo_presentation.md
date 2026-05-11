# 🚀 Smart Finance & Energy Orchestrator — Demo Presentation

## 1. Deskripsi Singkat Proyek

**Smart Finance & Energy Orchestrator** adalah sistem pemantauan keuangan dan konsumsi energi secara *real-time* berbasis protokol **MQTT v5**. Sistem ini didesain menggunakan arsitektur *Publish-Subscribe* yang mensimulasikan manajemen rumah/gedung pintar secara terdistribusi.

Proyek ini mengimplementasikan **seluruh 10 fitur utama MQTT v5**:
`Publish/Subscribe & QoS` · `Wildcard` · `Topic Alias` · `User Properties` · `Retain` · `Expiry` · `LWT` · `Request-Response` · `Shared Subscription` · `Flow Control`

**Komponen utama sistem:**
| Komponen | File | Peran |
|---|---|---|
| Broker | Mosquitto | Perantara semua pesan |
| Publisher 1 | `publisher_energy_meter.py` | Kirim data watt perangkat listrik |
| Publisher 2 | `publisher_subs_tracker.py` | Kirim data pemakaian langganan digital |
| Publisher 3 | `subscriber_dashboard.py` (terintegrasi) | Input budget dari form di browser |
| Subscriber 1 | `subscriber_dashboard.py` | Web Dashboard real-time (Flask + SocketIO) |
| Subscriber 2 | `subscriber_alert_engine.py` | Mesin alert & anomali di background |

---

## 2. Arsitektur Sistem

```
                    ┌──────────────────────────────────────────────┐
                    │        MQTT Broker (Mosquitto v5)            │
                    │         localhost:1883                        │
                    └───┬──────────┬──────────┬──────────┬─────────┘
                        │          │          │          │
              ┌─────────▼──┐  ┌────▼──────┐  ┌──▼──────┐  ┌──▼─────┐
              │Publisher 1 │  │Publisher 2│  │  LWT    │  │Request │
              │Energy Meter│  │Subs Tracker│  │(auto)  │  │-Resp   │
              │QoS 0       │  │QoS 1+Retain│  │        │  │        │
              │Topic Alias │  │Expiry 120s │  │        │  │        │
              │User Props  │  │User Props  │  │        │  │        │
              │Expiry 30s  │  │            │  │        │  │        │
              └────────────┘  └────────────┘  └────────┘  └────────┘
                        │          │          │          │
              ┌─────────▼──────────▼──────────▼──────────▼─────────┐
              │          Subscriber 1: Web Dashboard                │
              │   (Flask + SocketIO + Chart.js) port 5000           │
              │   Wildcard: finansial/energi/+/status               │
              │   Flow Control: receive_maximum=20                  │
              │   Budget Manager Form: POST /api/budget (QoS2)      │
              └─────────────────────────────────────────────────────┘
              ┌─────────────────────────────────────────────────────┐
              │          Subscriber 2: Alert Engine                 │
              │   Shared Subscription: $share/alert_group/...       │
              │   Anomali detection + Budget check                  │
              └─────────────────────────────────────────────────────┘
```

---

## 3. Struktur File

```
Insis-MQTT/
├── config.py                    # Konfigurasi sentral (broker, topik, konstanta)
├── requirements.txt             # Dependencies Python
├── publisher_energy_meter.py    # Publisher 1: Virtual Energy Meter
├── publisher_subs_tracker.py    # Publisher 2: Subscription Tracker
├── publisher_budget_manager.py  # Publisher 3: Budget Manager (referensi)
├── subscriber_dashboard.py      # Subscriber 1: Web Dashboard + Budget API
├── subscriber_alert_engine.py   # Subscriber 2: Alert Engine (Shared Sub)
├── subs_usage_state.json        # State persistence jam pemakaian
└── templates/
    └── dashboard.html           # Frontend: Cyberpunk UI + Budget Form
```

---

## 4. Design Topic (Topic Tree)

Struktur topik dirancang hierarkis dengan *root namespace* `finansial/` agar terisolasi dari sistem IoT lain.

```text
finansial/
├── energi/
│   ├── PC/status        [QoS 0, Expiry 30s, Topic Alias #1, User Props]
│   ├── Lampu/status     [QoS 0, Expiry 30s, Topic Alias #1, User Props]
│   ├── AC/status        [QoS 0, Expiry 30s, Topic Alias #1, User Props]
│   └── Kulkas/status    [QoS 0, Expiry 30s, Topic Alias #1, User Props]
│   ← Subscribe: finansial/energi/+/status  [Wildcard +]
│   ← Shared:    $share/alert_group/finansial/energi/+/status
│
├── subs/
│   ├── Netflix/usage    [QoS 1, Retained, Expiry 120s, User Props]
│   ├── Spotify/usage    [QoS 1, Retained, Expiry 120s, User Props]
│   ├── ChatGPT_Plus/usage
│   ├── YouTube_Premium/usage
│   └── Adobe_CC/usage
│   ← Subscribe: finansial/subs/+/usage    [Wildcard +]
│
├── budget/
│   └── limit            [QoS 2, Retained] ← Input dari form di browser
│
├── system/
│   ├── health           [LWT: "Metering Offline" | Retain]
│   ├── request          [Request-Response: snapshot request]
│   └── response         [Request-Response: snapshot reply]
│
└── alerts/
    └── warning          [QoS 1]
```

---

## 5. Implementasi 10 Fitur MQTT v5

### ① Publish/Subscribe & QoS (Quality of Service)

MQTT mendukung 3 tingkat garansi pengiriman pesan:

| Level | Nama | Mekanisme | Digunakan di |
|---|---|---|---|
| **QoS 0** | At Most Once (Fire & Forget) | Kirim langsung, tidak ada konfirmasi | Energy Meter (data cepat, ok kalau hilang) |
| **QoS 1** | At Least Once | Publisher kirim → Broker balas `PUBACK` | Subs Tracker & Alert engine |
| **QoS 2** | Exactly Once | 4-way handshake: `PUBLISH→PUBREC→PUBREL→PUBCOMP` | Budget Manager (data uang, tidak boleh ganda) |

**Lokasi kode:**
- `publisher_energy_meter.py` → `client.publish(..., qos=0)`
- `publisher_subs_tracker.py` → `client.publish(..., qos=1)`
- `subscriber_dashboard.py` (route `/api/budget`) → `mqtt_client_ref.publish(..., qos=2)`

---

### ② Wildcard Subscribe (+)

**Konsep:** Subscriber tidak perlu mendaftarkan topik satu per satu. Karakter `+` mengganti satu level topik secara dinamis.

**Implementasi di `subscriber_dashboard.py`:**
```python
client.subscribe("finansial/energi/+/status", qos=0)  # tangkap semua perangkat
client.subscribe("finansial/subs/+/usage",    qos=1)  # tangkap semua layanan
```

**Efek:** Jika ada perangkat baru (misal "Mesin Cuci"), grafiknya otomatis muncul di dashboard tanpa mengubah satu baris kode pun di subscriber.

---

### ③ Topic Alias

**Konsep (MQTT v5):** Topik digantikan dengan angka alias untuk menghemat bandwidth. Pertama kali topik penuh dikirim beserta alias-nya, selanjutnya hanya alias angka yang dikirim.

**Implementasi di `publisher_energy_meter.py`:**
```python
pub_props = Properties(PacketTypes.PUBLISH)
pub_props.TopicAlias = 1  # alias #1 = finansial/energi/{device}/status
client.publish(topic, payload, qos=0, properties=pub_props)
```

**Terlihat di terminal:** `[Alias:1, Expiry:30s]` setiap baris data energi.

---

### ④ User Properties

**Konsep (MQTT v5):** Metadata tambahan berupa pasangan key-value yang ditempel ke header setiap pesan MQTT. Tidak mempengaruhi payload, tapi bisa dibaca oleh subscriber untuk keperluan tracing/logging.

**Implementasi di `publisher_energy_meter.py`:**
```python
pub_props.UserProperty = [
    ("publisher_version", "2.0"),
    ("location", "Rumah-Lab"),
    ("unit", "Watt"),
    ("device_type", device_name),
]
```

**Terlihat di terminal Alert Engine:** `⚡ PC: 168.4W [v2.0]` — angka `v2.0` berasal dari User Property `publisher_version`.

---

### ⑤ Retain (Retained Message)

**Konsep:** Broker menyimpan pesan terakhir dari suatu topik. Subscriber yang baru terhubung langsung menerima pesan tersimpan tanpa harus menunggu publisher mengirim data baru.

**Implementasi:**
- `subscriber_dashboard.py` (route `/api/budget`) → budget di-publish dengan `retain=True`
- `publisher_subs_tracker.py` → setiap status layanan di-publish dengan `retain=True`

**Cara demo:** Refresh browser (F5) → angka budget langsung muncul kembali tanpa perlu input ulang.

---

### ⑥ Message Expiry Interval

**Konsep (MQTT v5):** Pesan akan otomatis dihapus oleh broker setelah batas waktu tertentu. Mencegah subscriber menerima data "basi" (stale) dari saat sebelum koneksi putus.

**Implementasi:**
```python
# publisher_energy_meter.py
pub_props.MessageExpiryInterval = 30   # data watt kedaluwarsa 30 detik

# publisher_subs_tracker.py
pub_props.MessageExpiryInterval = 120  # data subs kedaluwarsa 120 detik
```

**Logika:** Data energi yang lebih dari 30 detik sudah tidak relevan karena ada data baru setiap 5 detik.

---

### ⑦ Last Will and Testament (LWT)

**Konsep:** Saat pertama terhubung, publisher menitipkan "surat wasiat" ke broker. Jika koneksi publisher terputus secara *tidak normal* (crash, listrik mati, kabel cabut), broker otomatis mengirimkan surat wasiat itu ke semua subscriber yang mendengarkan topik tersebut.

**Implementasi di `publisher_energy_meter.py`:**
```python
lwt_message = json.dumps({
    "status": "Metering Offline",
    "reason": "Koneksi terputus secara tidak normal (LWT triggered)",
    ...
})
client.will_set(topic=TOPIC_HEALTH, payload=lwt_message, qos=1, retain=True)
```

**Cara demo (The Wow Factor):** Tutup paksa terminal Energy Meter → status di dashboard langsung merah "Metering Offline" tanpa alat mengirim apapun.

---

### ⑧ Request-Response

**Konsep (MQTT v5):** Pola komunikasi dua arah di atas MQTT. Requester (Dashboard) mengirim request ke topik tertentu beserta `ResponseTopic` dan `CorrelationData`. Publisher yang menerima request membalas ke `ResponseTopic` yang ditentukan.

**Alur:**
```
Dashboard  →  PUBLISH  →  finansial/system/request  →  Broker
                                                          ↓
                                                   Energy Meter menerima
                                                          ↓
Energy Meter  →  PUBLISH  →  finansial/system/response  →  Broker
                                                              ↓
                                                       Dashboard menerima snapshot
```

**Implementasi:**
- `subscriber_dashboard.py` route `POST /api/snapshot` → kirim request dengan `ResponseTopic`
- `publisher_energy_meter.py` fungsi `on_message()` → balas dengan data snapshot

---

### ⑨ Shared Subscription

**Konsep (MQTT v5):** Memungkinkan beberapa subscriber bergabung dalam satu *consumer group*. Broker akan mendistribusikan pesan secara bergantian (load-balanced) antar member grup, bukan mengirim ke semua.

**Format topik:** `$share/{nama_group}/{topik_filter}`

**Implementasi di `subscriber_alert_engine.py`:**
```python
client.subscribe("$share/alert_group/finansial/energi/+/status", qos=0)
client.subscribe("$share/alert_group/finansial/subs/+/usage",    qos=1)
```

**Cara demo:** Jalankan 2 terminal `subscriber_alert_engine.py` secara bersamaan → perhatikan bahwa setiap pesan energi hanya diterima **salah satu** dari kedua instance (bergantian), bukan keduanya sekaligus.

---

### ⑩ Flow Control (Receive Maximum)

**Konsep (MQTT v5):** Subscriber dapat memberitahu broker batas maksimum pesan QoS 1/2 yang boleh dikirim secara bersamaan (in-flight). Ini mencegah subscriber kelebihan beban saat traffic sangat tinggi.

**Implementasi di `subscriber_dashboard.py`:**
```python
connect_props = Properties(PacketTypes.CONNECT)
connect_props.ReceiveMaximum = 20  # max 20 pesan in-flight sekaligus

client.connect(MQTT_BROKER, MQTT_PORT,
               keepalive=MQTT_KEEPALIVE,
               properties=connect_props)
```

---

## 6. Budget Manager — Input dari Dashboard Browser

Budget Manager kini sepenuhnya terintegrasi di dalam Web Dashboard. Tidak perlu membuka terminal terpisah.

**Alur teknis:**
1. User mengisi form di browser (`http://localhost:5000`)
2. Browser mengirim `POST /api/budget` ke Flask server
3. Flask server mem-publish data ke `finansial/budget/limit` dengan **QoS 2 + Retained**
4. Broker menyimpan nilai budget (Retained) dan mendistribusikannya ke semua subscriber
5. Dashboard dan Alert Engine menerima budget baru secara real-time

**Cara demo:**
1. Buka dashboard → scroll ke bawah → isi form "Budget & Policy Manager"
2. Masukkan nominal (misal `300000`) → klik tombol **"Kirim Budget (QoS 2 + Retained)"**
3. Lihat angka budget di card atas berubah seketika
4. Refresh halaman (F5) → angka budget tetap muncul karena Retained

---

## 7. Skenario Demo Lengkap (Urutan Eksekusi)

### Persiapan (Buka 4 Terminal)

```
Terminal 1: mosquitto -v
Terminal 2: python subscriber_dashboard.py
Terminal 3: python subscriber_alert_engine.py
Terminal 4: python publisher_energy_meter.py
Terminal 5: python publisher_subs_tracker.py
```

Buka browser: `http://localhost:5000`

### Langkah 1 — Tunjukkan Dashboard & Wildcard
- Tunjukkan grafik PC, AC, Kulkas bergerak real-time
- Jelaskan: *"Dashboard pakai wildcard `+` sehingga otomatis tangkap semua perangkat"*

### Langkah 2 — Demo Budget Manager & QoS 2 + Retained
- Scroll ke bawah, isi form budget (misal Rp 1.500.000)
- Klik kirim → lihat konfirmasi ✅ muncul
- Refresh browser (F5) → budget langsung muncul lagi
- Jelaskan: *"QoS 2 pakai 4-way handshake, dan Retained bikin broker ingat nilai budget"*

### Langkah 3 — Demo Alert Engine & Shared Subscription (opsional)
- Buka terminal ke-6, jalankan `python subscriber_alert_engine.py` lagi
- Jelaskan: *"2 instance ini pakai Shared Subscription. Pesan energi dibagi bergantian, tidak dobel"*

### Langkah 4 — Demo LWT (The "Wow" Factor)
1. Arahkan perhatian dosen ke status hijau "Metering Online" di header dashboard
2. **Tutup paksa** terminal Energy Meter (klik silang terminalnya)
3. Tunjukkan: status di dashboard langsung berubah jadi merah **"Metering Offline"**
4. Jelaskan: *"Ini LWT. Broker yang otomatis kirim pesan offline karena sensor mati tiba-tiba. Bukan sensor yang kirim."*

---