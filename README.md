# ApsGo Railway Worker

**Background automation service untuk ApsGo IoT System**

Menjalankan scheduled watering (Waktu Mode) dan sensor-based watering (Sensor Mode) 24/7 di Railway.

## 📋 Isi Folder

```
railway-worker/
├── worker.js                    # Main worker service (ACTIVE)
├── worker_remote.js            # Alternative worker version (backup)
├── package.json                # Dependencies
├── .env                        # Configuration (sensitive data)
├── .env.example                # Example configuration
├── debug.js                    # Debug & troubleshooting script
├── test-firebase-*.js          # Firebase connection tests
├── check-queue.js              # Redis queue status checker
├── .git/                       # Separate git repo untuk railway deployment
└── node_modules/               # Dependencies (npm install)
```

## 🚀 Quickstart

### 1. Setup Environment
```bash
cd railway-worker
npm install
cp .env.example .env
# Edit .env dengan credentials Firebase Anda
```

### 2. Run Worker
```bash
# Production
npm start

# Development (dengan auto-reload)
npm run dev

# Debug mode
npm run debug
```

### 3. Test Connection
```bash
npm run test:firebase
node check-queue.js            # Check Redis queue status
```

## ⚙️ Configuration

File `.env` harus berisi:

```env
FIREBASE_PROJECT_ID=your-project
FIREBASE_CLIENT_EMAIL=your-email@firebase...
FIREBASE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
FIREBASE_DATABASE_URL=https://your-db.firebaseio.com
REDIS_URL=redis://localhost:6379
NODE_ENV=production
```

## 📝 Features

### Waktu Mode (Schedule-based)
- Jalankan penyiraman sesuai jadwal (Jadwal 1, Jadwal 2)
- Check setiap 30 detik, eksekusi jika waktu cocok
- Support multiple durations per schedule

### Sensor Mode (Threshold-based)  
- Monitor kelembapan tanah real-time
- Trigger penyiraman otomatis saat mencapai threshold
- Cooldown 2 menit antar penyiraman per pot (prevent flooding)
- Skip jika Flutter app sudah handle sensor mode

### Background Queue
- Menggunakan **BullMQ** + Redis untuk queue management
- Prevent race conditions concurrent tasks
- Retry mechanism untuk failed jobs

## 🔧 Deployment to Railway

### Option 1: Automated (Recommended)
Railway.json sudah configured, cukup:
```bash
cd railway-worker
git push railway main
```

### Option 2: Manual
1. Create Railway project
2. Connect git repository ini (atau fork)
3. Set environment variables di Railway dashboard
4. Redeploy saat ada update

## 📊 Monitoring

### View Logs
```bash
railway logs
```

### Check Queue Status
```bash
node check-queue.js
```

### Debug Connection
```bash
node debug.js
```

## 🐛 Troubleshooting

| Problem | Solution |
|---------|----------|
| **Worker tidak jalan** | Check `.env` credentials, test dengan `npm run test:firebase` |
| **Queue stuck** | Restart worker, check Redis connection |
| **Duplicate watering** | Check cooldown settings di `automation_constants.dart` |
| **Missing tasks** | Verify `kontrol_1` path di Firebase structure |

## 📌 Important Notes

- **Hanya gunakan `kontrol_1`** - semua jadwal & threshold di sini
- **Cooldown 2 menit** - hardcoded untuk semua pot (prevent over-watering)
- **Tidak perlu `pot_cooldowns` / `sensor_cooldowns` di database** - gunakan default
- **Worker hanya handle Waktu Mode** jika Flutter app tidak active

## 🔄 Git Workflow

```bash
# Ini adalah SEPARATE git repo dari main project
cd railway-worker
git add .
git commit -m "fix: update worker configuration"
git push origin main     # Push ke myrailway repository
```

## 📞 Support

Jika ada masalah:
1. Check `.env` configuration
2. Run `npm run test:firebase` untuk verify Firebase connection
3. Check logs dengan `npm run debug`
4. Review worker.js log output

---

**Last Updated**: April 12, 2026
**Status**: ✅ Production Ready
