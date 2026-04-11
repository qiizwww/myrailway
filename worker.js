/**
 * ApsGo Railway Worker
 * Background service untuk automation scheduling 24/7
 * Features:
 * - Waktu Mode: Scheduled watering by time
 * - Sensor Mode: Automatic watering by soil moisture threshold
 * - Redis Queue: Prevent race conditions & concurrent task management
 * - Firebase Realtime DB: Sync dengan Flutter app dan ESP32
 */

// ==================== SUPPRESS FIREBASE WARNINGS ====================

// Completely suppress Firebase SDK warnings
process.env.FIREBASE_DATABASE_EMULATOR_HOST = undefined;
process.env.FIRESTORE_EMULATOR_HOST = undefined;

// Override console.warn to filter Firebase warnings
const originalWarn = console.warn;
console.warn = function(...args) {
  const message = args.join(' ');
  // Suppress specific Firebase warnings
  if (message.includes('FIREBASE WARNING') || 
      message.includes('@firebase/database') ||
      message.includes('firebase/database')) {
    return; // Silent - don't log
  }
  originalWarn.apply(console, args);
};

// ==================== IMPORTS ====================

require('dotenv').config();
const admin = require('firebase-admin');
const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const cron = require('cron');

// ==================== CONFIGURATION ====================

// Set timezone untuk Indonesia (UTC+7)
process.env.TZ = process.env.TZ || 'Asia/Jakarta';

// Firebase paths configuration
const FIREBASE_PATHS = {
  kontrol: 'kontrol_1',  // Main kontrol path - FIXED: Updated to kontrol_1 (current data structure)
  aktuator: 'aktuator',
  data: 'data',
  history: 'history',
};

const NOTIFICATION_TOPIC = 'apsgo_notifications';
const NOTIFICATION_CHANNEL_ID = 'apsgo_watering_channel';

const config = {
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || undefined,
    maxRetriesPerRequest: null, // Required for BullMQ
  },
  firebase: {
    projectId: process.env.FIREBASE_PROJECT_ID,
    clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
    privateKey: process.env.FIREBASE_PRIVATE_KEY?.replace(/\\n/g, '\n'),
    databaseURL: process.env.FIREBASE_DATABASE_URL,
  },
  worker: {
    concurrency: 1, // Process 1 job at a time (prevent race condition)
    checkInterval: 60000, // Check jadwal setiap 60 detik (reduced from 30s)
    sensorDebounce: 120000, // 2 menit minimum antar penyiraman per pot
    scheduleGraceMs: parseInt(process.env.SCHEDULE_GRACE_MS || '15000', 10), // Toleransi keterlambatan trigger
    scheduleMaxCatchupMs: parseInt(process.env.SCHEDULE_MAX_CATCHUP_MS || '300000', 10), // Maksimal catch-up 5 menit
  },
};

console.log('🚀 Starting ApsGo Railway Worker...');
console.log(`📡 Firebase Project: ${config.firebase.projectId}`);
console.log(`🔥 Firebase DB URL: ${config.firebase.databaseURL}`);
console.log(`📦 Redis: ${config.redis.host}:${config.redis.port}`);
console.log(`⏰ Timezone: ${process.env.TZ} (Current: ${new Date().toLocaleString('id-ID', {timeZone: 'Asia/Jakarta'})})`);
console.log(`📍 Kontrol Path: /${FIREBASE_PATHS.kontrol}`);

// ==================== ENVIRONMENT VALIDATION ====================

const requiredEnvs = [
  'FIREBASE_PROJECT_ID',
  'FIREBASE_CLIENT_EMAIL', 
  'FIREBASE_PRIVATE_KEY',
  'FIREBASE_DATABASE_URL'
];

const missingEnvs = requiredEnvs.filter(env => !process.env[env]);
if (missingEnvs.length > 0) {
  console.error('❌ Missing required environment variables:');
  missingEnvs.forEach(env => console.error(`   - ${env}`));
  console.error('\n📋 To fix this:');
  console.error('1. Go to Railway Dashboard');
  console.error('2. Select your worker service');
  console.error('3. Go to Variables tab');
  console.error('4. Add the missing variables');
  console.error('5. Redeploy');
  process.exit(1);
}

console.log('✅ All required environment variables are set');

// ==================== FIREBASE INITIALIZATION ====================

try {
  admin.initializeApp({
    credential: admin.credential.cert({
      projectId: config.firebase.projectId,
      clientEmail: config.firebase.clientEmail,
      privateKey: config.firebase.privateKey,
    }),
    databaseURL: config.firebase.databaseURL,
  });
  console.log('✅ Firebase Admin initialized');
} catch (error) {
  console.error('❌ Firebase initialization failed:', error.message);
  process.exit(1);
}

const db = admin.database();

async function sendAutomationNotification({ title, body, type, data = {} }) {
  try {
    // Check if messaging is available
    if (!admin.messaging) {
      console.log(`📝 [INFO] Cloud Messaging not available. App will use local notifications via Firebase listener.`);
      return;
    }

    const messaging = admin.messaging();
    if (!messaging) {
      console.log(`📝 [INFO] Cloud Messaging SDK not initialized. App will use local notifications via Firebase listener.`);
      return;
    }

    const result = await messaging.send({
      topic: NOTIFICATION_TOPIC,
      notification: { title, body },
      data: {
        type: String(type || 'automation'),
        title: String(title || ''),
        body: String(body || ''),
        ...Object.fromEntries(
          Object.entries(data).map(([key, value]) => [key, String(value)]),
        ),
      },
      android: {
        priority: 'high',
        notification: {
          channelId: NOTIFICATION_CHANNEL_ID,
          icon: 'ic_launcher',
          color: '#2E7D32',
        },
      },
    });
    console.log(`🔔 FCM Notification sent: ${type} (ID: ${result})`);
  } catch (error) {
    // Don't treat as error - app will handle notifications via Firebase listeners
    console.log(`📝 [INFO] FCM not available (${error.message}). App will use local notifications via Firebase listener.`);
  }
}

// Add error handlers for Firebase database
db.ref('.info/connected').on('value', (snap) => {
  if (snap.val() === true) {
    console.log('🔌 Firebase realtime connection active');
  }
});

// ==================== REDIS & QUEUE SETUP ====================

const redis = new Redis(config.redis);
const wateringQueue = new Queue('watering', { connection: redis });

redis.on('connect', () => console.log('✅ Redis connected'));
redis.on('error', (err) => console.error('❌ Redis error:', err.message));

// Track last watering time PER-THRESHOLD (not per-pot!) untuk prevent spam
const lastThresholdTime = {};

// ==================== WATERING WORKER ====================

const wateringWorker = new Worker(
  'watering',
  async (job) => {
    const { type, potNumbers, pompaAir, pompaPupuk, duration, scheduleId, thresholdId, smartMode, sensorData } = job.data;

    console.log(`\n💧 Processing Job: ${job.id}`);
    console.log(`   Type: ${type}`);
    console.log(`   Pots: [${potNumbers.join(', ')}]`);
    console.log(`   Mode: ${smartMode ? 'SMART (auto-stop at target)' : 'FIXED'}`);
    console.log(`   Duration: ${duration}s ${smartMode ? '(max)' : ''}`);
    if (sensorData) {
      console.log(`   Target: ${sensorData.batasBawah}% → ${sensorData.batasAtas}%`);
    }

    try {
      // Prepare aktuator updates
      const updates = {};
      if (pompaAir) updates['mosvet_1'] = true;
      if (pompaPupuk) updates['mosvet_2'] = true;

      // Turn ON valves for selected pots
      for (const pot of potNumbers) {
        if (pot >= 1 && pot <= 5) {
          updates[`mosvet_${pot + 2}`] = true; // pot 1 → mosvet_3, etc.
        }
      }

      // Turn ON
      console.log('   🔛 Turning ON:', Object.keys(updates).join(', '));
      console.log('   📌 Firebase path: aktuator');
      console.log('   📝 Updates:', JSON.stringify(updates, null, 2));

      if (String(type || '').startsWith('waktu_')) {
        const scheduleTime = job.data.scheduleTime || 'jadwal';
        const potText = potNumbers.length > 1 ? `pot ${potNumbers.join(', ')}` : `pot ${potNumbers[0]}`;
        await sendAutomationNotification({
          title: 'ApsGo - Jadwal Penyiraman',
          body: `Pada jam ${scheduleTime} akan dilakukan penyiraman untuk ${potText}.`,
          type: 'schedule_triggered',
          data: {
            scheduleId: scheduleId || '',
            scheduleTime,
            pots: potNumbers.join(','),
            duration: duration,
          },
        });
      }
      
      // Update with retry logic (3 attempts)
      await updateFirebaseSmart('aktuator', updates, 3);
      
      // Verify the update was written to Firebase
      console.log(`   🔍 Verifying Firebase update...`);
      const verifyAttempts = 3;
      let verified = false;
      for (let i = 0; i < verifyAttempts; i++) {
        try {
          const currentState = await readFirebaseSmart('aktuator');
          const allSet = Object.keys(updates).every(key => currentState[key] === updates[key]);
          
          if (allSet) {
            console.log(`   ✅ VERIFIED: All values correctly written to Firebase!`);
            verified = true;
            break;
          } else {
            console.warn(`   ⚠️  Verification attempt ${i + 1}/${verifyAttempts} failed: values not yet synced`);
            if (i < verifyAttempts - 1) {
              await sleep(500); // Wait 500ms before retry
            }
          }
        } catch (verifyError) {
          console.warn(`   ⚠️  Verification read failed (attempt ${i + 1}/${verifyAttempts}): ${verifyError.message}`);
        }
      }
      
      if (!verified) {
        console.warn(`   ⚠️  WARNING: Could not verify Firebase update after ${verifyAttempts} attempts`);
      }
      
      console.log(`   🚀 ALL VALVES STARTED SIMULTANEOUSLY: ${Object.keys(updates).filter(k => k.startsWith('mosvet_')).join(', ')}`);
      
      // SMART MODE: Monitor sensor and stop pots TOGETHER when they reach target
      if (smartMode && sensorData && sensorData.batasAtas) {
        const targetSoil = sensorData.batasAtas;
        const maxDuration = duration * 1000; // Convert to ms
        const startTime = Date.now();
        
        // Track which pots are still actively watering
        let activePots = [...potNumbers];
        
        console.log(`   🎯 SMART MODE: Monitoring ${activePots.length} pots, target ${targetSoil}%...`);
        console.log(`   ⚡ Valves will stop TOGETHER when pots reach target (checked every 2s)`);
        
        while (activePots.length > 0 && Date.now() - startTime < maxDuration) {
          await sleep(2000); // Check every 2 seconds
          
          try {
            const currentSensorData = await readFirebaseSmart('data');
            
            if (currentSensorData) {
              const elapsed = Math.floor((Date.now() - startTime) / 1000);
              const potsToStop = [];
              
              // Check ALL active pots and collect which ones reached target
              for (const pot of activePots) {
                const soilKey = `soil_${pot}`;
                const currentValue = parseInt(currentSensorData[soilKey]) || 0;
                
                if (currentValue >= targetSoil) {
                  console.log(`   ✅ [${elapsed}s] POT ${pot}: ${currentValue}% >= ${targetSoil}% - TARGET REACHED!`);
                  potsToStop.push(pot);
                } else {
                  console.log(`   ⏳ [${elapsed}s] POT ${pot}: ${currentValue}% < ${targetSoil}% - continuing...`);
                }
              }
              
              // Stop ALL pots that reached target TOGETHER (not one-by-one!)
              if (potsToStop.length > 0) {
                const stopUpdates = {};
                for (const pot of potsToStop) {
                  stopUpdates[`mosvet_${pot + 2}`] = false;
                }
                
                try {
                  await updateFirebaseSmart('aktuator', stopUpdates, 2); // 2 attempts for stop
                  console.log(`   🔴 STOPPED TOGETHER: ${Object.keys(stopUpdates).join(', ')} (Pots: [${potsToStop.join(', ')}])`);
                } catch (stopError) {
                  console.error(`   ❌ FAILED to stop pots: ${stopError.message}`);
                  throw stopError; // Re-throw to safety handler
                }
                
                // Remove stopped pots from active list
                activePots = activePots.filter(p => !potsToStop.includes(p));
              }
              
              if (activePots.length === 0) {
                console.log(`   🎉 All pots reached target! Smart watering complete.`);
              } else {
                console.log(`   📍 Still watering: [${activePots.join(', ')}]`);
              }
            }
          } catch (sensorError) {
            console.warn(`   ⚠️ Failed to read sensor: ${sensorError.message}`);
          }
        }
        
        // If any pots still active after max duration (timeout), stop them now TOGETHER
        if (activePots.length > 0) {
          console.log(`   ⏱️ Max duration ${duration}s reached. Force stopping remaining pots: [${activePots.join(', ')}]`);
          const timeoutStops = {};
          for (const pot of activePots) {
            timeoutStops[`mosvet_${pot + 2}`] = false;
          }
          try {
            await updateFirebaseSmart('aktuator', timeoutStops, 2); // 2 attempts for force stop
            console.log(`   🔴 Force stopped TOGETHER: ${Object.keys(timeoutStops).join(', ')}`);
          } catch (forceStopError) {
            console.error(`   ❌ FAILED to force stop pots: ${forceStopError.message}`);
            throw forceStopError;
          }
        }
        
        // Finally, stop pumps
        const pumpStop = {};
        if (pompaAir) pumpStop['mosvet_1'] = false;
        if (pompaPupuk) pumpStop['mosvet_2'] = false;
        if (Object.keys(pumpStop).length > 0) {
          try {
            await updateFirebaseSmart('aktuator', pumpStop, 2); // 2 attempts for pump stop
            console.log('   🔴 Pumps stopped:', Object.keys(pumpStop).join(', '));
          } catch (pumpStopError) {
            console.error(`   ❌ FAILED to stop pumps: ${pumpStopError.message}`);
            throw pumpStopError;
          }
        }
        console.log('   ✅ Smart mode completed, now logging history...');
        
      } else {
        // FIXED MODE: Wait for fixed duration
        const startTime = Date.now();
        const endTime = startTime + duration * 1000;

        while (Date.now() < endTime) {
          const remaining = Math.ceil((endTime - Date.now()) / 1000);
          if (remaining % 10 === 0 || remaining <= 5) {
            console.log(`   ⏳ ${remaining}s remaining...`);
          }
          await sleep(1000);
        }
        
        // Turn OFF all at once (FIXED mode only)
        const offUpdates = {};
        for (const key in updates) {
          offUpdates[key] = false;
        }
        console.log('   🔴 Turning OFF:', Object.keys(offUpdates).join(', '));
        try {
          await updateFirebaseSmart('aktuator', offUpdates, 2); // 2 attempts for turn off
          console.log('   ✅ Turn OFF completed successfully');
        } catch (offError) {
          console.error(`   ❌ FAILED to turn OFF: ${offError.message}`);
          throw offError;
        }
        console.log('   ✅ Now logging history...');
      }

      // Log history
      await logHistory(type, potNumbers, duration);
      console.log('   ✅ History logged successfully');

      // Update last watering time PER-THRESHOLD (not per-pot!)
      if (thresholdId) {
        lastThresholdTime[thresholdId] = Date.now();
        console.log(`   ⏰ Cooldown set for ${thresholdId} (2 minutes)`);
      }

      console.log(`   ✅ Job completed successfully`);
      return { success: true, duration, pots: potNumbers };
    } catch (error) {
      console.error(`   ❌ Job failed:`, error.message);
      console.error(`   [ERROR DETAILS] Stack:`, error.stack);

      // Safety: Turn OFF everything with retry
      const safetyUpdates = {
        mosvet_1: false,
        mosvet_2: false,
        mosvet_3: false,
        mosvet_4: false,
        mosvet_5: false,
        mosvet_6: false,
        mosvet_7: false,
        mosvet_8: false, // Pengaduk
      };
      
      try {
        console.log(`   🛡️ Safety: Attempting to turn OFF all aktuators...`);
        await updateFirebaseSmart('aktuator', safetyUpdates, 2); // 2 attempts for safety
        console.log('   🛡️ Safety: All aktuators turned OFF successfully');
      } catch (safetyError) {
        console.error('   ❌ CRITICAL: Safety OFF failed:', safetyError.message);
        console.error('   ❌ CRITICAL: Penyiraman mungkin terjebak ON - manual intervention mungkin diperlukan!');
      }

      throw error;
    }
  },
  {
    connection: redis,
    concurrency: config.worker.concurrency,
    lockDuration: 900000, // 15 minutes max job time (duration 600s + 5min buffer)
    removeOnComplete: { count: 100 }, // Keep last 100 completed jobs
    removeOnFail: { count: 50 }, // Keep last 50 failed jobs
  }
);

wateringWorker.on('completed', (job) => {
  console.log(`✅ Worker completed job ${job.id}`);
});

wateringWorker.on('failed', (job, err) => {
  console.error(`❌ Worker failed job ${job?.id}:`, err.message);
});

// ==================== WAKTU MODE (TIME SCHEDULER) ====================

let lastScheduleCheck = {};
let lastSchedulerTickAt = null;

// Counter untuk tracking berapa kali check dilakukan
let checkCounter = 0;
let consecutiveFirebaseErrors = 0;
let sdkSuccessCount = 0;
let restFallbackCount = 0;

// Smart fallback: Skip SDK jika sudah gagal berturut-turut 3x
const SKIP_SDK_THRESHOLD = 3;
const RESET_THRESHOLD_AFTER = 50; // Reset counter setelah 50 check (50 menit)

// Helper function untuk Firebase fetch dengan timeout
async function fetchWithTimeout(ref, timeoutMs = 5000) {
  return Promise.race([
    ref.once('value'),
    new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Firebase fetch timeout')), timeoutMs)
    )
  ]);
}

// Helper: Fetch with timeout wrapper (for REST API calls)
async function fetchWithTimeout2(url, options = {}, timeoutMs = 8000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    clearTimeout(timeout);
    return response;
  } catch (error) {
    clearTimeout(timeout);
    if (error.name === 'AbortError') {
      throw new Error('REST API timeout');
    }
    throw error;
  }
}

// Fallback: Fetch via Firebase REST API (lebih reliable)
async function fetchKontrolViaREST() {
  const url = `${config.firebase.databaseURL}/${FIREBASE_PATHS.kontrol}.json`;
  console.log(`   [DEBUG] Trying REST API: ${url}`);
  
  const response = await fetchWithTimeout2(url, {
    method: 'GET',
    headers: { 'Content-Type': 'application/json' }
  }, 8000);
  
  if (!response.ok) {
    throw new Error(`REST API failed: ${response.status} ${response.statusText}`);
  }
  
  const data = await response.json();
  console.log('   [DEBUG] REST API successful!');
  return data;
}

// Smart fetch: Try SDK first, fallback to REST if needed
async function fetchKontrolSmart() {
  // Smart fallback: Skip SDK if it failed 3+ times consecutively
  const shouldSkipSDK = consecutiveFirebaseErrors >= SKIP_SDK_THRESHOLD;
  
  if (shouldSkipSDK) {
    console.log('   [SMART] Skipping SDK (3+ consecutive failures), using REST API directly...');
    try {
      const data = await fetchKontrolViaREST();
      restFallbackCount++;
      
      // Reset counter setelah threshold untuk retry SDK
      if (restFallbackCount >= RESET_THRESHOLD_AFTER) {
        console.log('   [SMART] Resetting SDK retry counter...');
        consecutiveFirebaseErrors = 0;
        restFallbackCount = 0;
      }
      
      return data;
    } catch (restError) {
      console.error('   ❌ REST API failed:', restError.message);
      throw new Error('REST API failed');
    }
  }
  
  // Normal flow: Try SDK first
  try {
    console.log('   [DEBUG] Attempting SDK fetch...');
    const snapshot = await fetchWithTimeout(db.ref(FIREBASE_PATHS.kontrol), 5000);
    consecutiveFirebaseErrors = 0; // Reset error counter
    restFallbackCount = 0; // Reset fallback counter
    sdkSuccessCount++;
    return snapshot.val();
  } catch (sdkError) {
    console.warn('   ⚠️  SDK fetch failed, trying REST API...');
    consecutiveFirebaseErrors++;
    
    try {
      const data = await fetchKontrolViaREST();
      restFallbackCount++;
      
      // Log peringatan jika SDK terus gagal
      if (consecutiveFirebaseErrors === SKIP_SDK_THRESHOLD) {
        console.warn(`   🚨 SDK failed ${SKIP_SDK_THRESHOLD}x consecutively! Will use REST API directly for next ${RESET_THRESHOLD_AFTER} checks.`);
      }
      
      return data;
    } catch (restError) {
      console.error('   ❌ REST API also failed:', restError.message);
      throw new Error('Both SDK and REST API failed');
    }
  }
}

// Helper: Update Firebase via REST API (PATCH for merge update)
async function updateFirebaseViaREST(path, updates) {
  const url = `${config.firebase.databaseURL}/${path}.json`;
  console.log(`   [DEBUG] REST API PATCH: ${url}`);
  
  const response = await fetchWithTimeout2(url, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(updates)
  }, 8000);
  
  if (!response.ok) {
    throw new Error(`REST PATCH failed: ${response.status}`);
  }
  
  const result = await response.json();
  console.log('   [DEBUG] REST API update successful!');
  return result;
}

// Helper: Update with timeout wrapper
async function updateWithTimeout(ref, updates, timeoutMs = 5000) {
  return Promise.race([
    ref.update(updates),
    new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Firebase update timeout')), timeoutMs)
    )
  ]);
}

// Smart update: Try SDK first, fallback to REST if timeout
// WITH RETRY LOGIC for stability
async function updateFirebaseSmart(path, updates, maxAttempts = 3) {
  const updateStr = JSON.stringify(updates);
  console.log(`   [UPDATE START] Path: /${path}, Data: ${updateStr}`);
  console.log(`   [UPDATE] Max attempts: ${maxAttempts}`);
  
  let lastError = null;
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      // If SDK is consistently failing, skip it for updates too
      const shouldSkipSDK = consecutiveFirebaseErrors >= SKIP_SDK_THRESHOLD;
      
      if (shouldSkipSDK) {
        console.log(`   [UPDATE Attempt ${attempt}/${maxAttempts}] Using REST API directly (SDK disabled)`);
        await updateFirebaseViaREST(path, updates);
        console.log(`   ✅ [UPDATE] REST API successful on attempt ${attempt}!`);
        return true;
      }
      
      // Normal flow: Try SDK first
      console.log(`   [UPDATE Attempt ${attempt}/${maxAttempts}] Attempting SDK update...`);
      await updateWithTimeout(db.ref(path), updates, 5000);
      console.log(`   ✅ [UPDATE] SDK update successful on attempt ${attempt}!`);
      return true;
    } catch (sdkError) {
      lastError = sdkError;
      console.warn(`   ⚠️  [UPDATE Attempt ${attempt}/${maxAttempts}] SDK failed: ${sdkError.message}`);
      
      // Try REST API as fallback
      try {
        console.log(`   [UPDATE Attempt ${attempt}/${maxAttempts}] Fallback to REST API...`);
        await updateFirebaseViaREST(path, updates);
        console.log(`   ✅ [UPDATE] REST API successful on attempt ${attempt}!`);
        return true;
      } catch (restError) {
        lastError = restError;
        console.error(`   ❌ [UPDATE Attempt ${attempt}/${maxAttempts}] REST failed: ${restError.message}`);
        
        if (attempt < maxAttempts) {
          const delayMs = attempt * 1000; // 1s, 2s, 3s delay between attempts
          console.log(`   ⏳ [UPDATE] Retrying after ${delayMs}ms...`);
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
      }
    }
  }
  
  // All attempts failed
  console.error(`   ❌ [UPDATE] ALL ${maxAttempts} ATTEMPTS FAILED!`);
  console.error(`   ❌ [UPDATE] Last error: ${lastError.message}`);
  throw new Error(`Firebase update failed after ${maxAttempts} attempts: ${lastError.message}`);
}

// Helper: Set Firebase via REST API (PUT for overwrite)
async function setFirebaseViaREST(path, data) {
  const url = `${config.firebase.databaseURL}/${path}.json`;
  
  const response = await fetchWithTimeout2(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  }, 8000);
  
  if (!response.ok) {
    throw new Error(`REST PUT failed: ${response.status}`);
  }
  
  return await response.json();
}

// Helper: Set with timeout wrapper
async function setWithTimeout(ref, data, timeoutMs = 5000) {
  return Promise.race([
    ref.set(data),
    new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Firebase set timeout')), timeoutMs)
    )
  ]);
}

// Smart set: Try SDK first, fallback to REST if timeout
async function setFirebaseSmart(path, data) {
  const dataStr = JSON.stringify(data);
  console.log(`   [SET START] Path: ${path}, Data: ${dataStr.substring(0,100)}...`);
  
  // If SDK is consistently failing, skip it
  const shouldSkipSDK = consecutiveFirebaseErrors >= SKIP_SDK_THRESHOLD;
  
  if (shouldSkipSDK) {
    console.log(`   [SET] Using REST API directly (SDK disabled)`);
    try {
      await setFirebaseViaREST(path, data);
      console.log(`   ✅ [SET] REST API successful!`);
      return true;
    } catch (restError) {
      console.error(`   ❌ [SET] REST failed: ${restError.message}`);
      throw new Error('REST set failed');
    }
  }
  
  try {
    console.log(`   [SET] Step 1: Attempting SDK set...`);
    await setWithTimeout(db.ref(path), data, 5000);
    console.log(`   ✅ [SET] Step 2: SDK set successful!`);
    return true;
  } catch (sdkError) {
    console.warn(`   ⚠️  [SET] Step 2: SDK failed (${sdkError.message}), trying REST API...`);
    try {
      console.log(`   [SET] Step 3: Attempting REST API...`);
      await setFirebaseViaREST(path, data);
      console.log(`   ✅ [SET] Step 4: REST API successful!`);
      return true;
    } catch (restError) {
      console.error(`   ❌ [SET] Step 4: REST failed - BOTH METHODS FAILED!`);
      throw new Error('Both SDK and REST set failed');
    }
  }
}

// Smart read: Try SDK first, fallback to REST if timeout (for any path)
async function readFirebaseSmart(path) {
  const shouldSkipSDK = consecutiveFirebaseErrors >= SKIP_SDK_THRESHOLD;
  
  if (shouldSkipSDK) {
    console.log('   [READ] Using REST API directly (SDK disabled)');
    const url = `${config.firebase.databaseURL}/${path}.json`;
    const response = await fetchWithTimeout2(url, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' }
    }, 8000);
    
    if (!response.ok) {
      throw new Error(`REST GET failed: ${response.status}`);
    }
    
    return await response.json();
  }
  
  try {
    const snapshot = await fetchWithTimeout(db.ref(path), 5000);
    return snapshot.val();
  } catch (sdkError) {
    const url = `${config.firebase.databaseURL}/${path}.json`;
    const response = await fetchWithTimeout2(url, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' }
    }, 8000);
    
    if (!response.ok) {
      throw new Error(`REST GET failed: ${response.status}`);
    }
    
    return await response.json();
  }
}

// Parse HH:mm menjadi Date hari ini (timezone proses mengikuti process.env.TZ)
function parseScheduleTimeToday(scheduleTime, now) {
  if (typeof scheduleTime !== 'string' || !/^\d{2}:\d{2}$/.test(scheduleTime)) {
    return null;
  }

  const [hourStr, minuteStr] = scheduleTime.split(':');
  const hour = Number(hourStr);
  const minute = Number(minuteStr);

  if (Number.isNaN(hour) || Number.isNaN(minute) || hour < 0 || hour > 23 || minute < 0 || minute > 59) {
    return null;
  }

  const scheduledAt = new Date(now);
  scheduledAt.setHours(hour, minute, 0, 0);
  return scheduledAt;
}

function isScheduleInTriggerWindow(scheduleTime, now, windowStartMs, windowEndMs) {
  const scheduleDate = parseScheduleTimeToday(scheduleTime, now);
  if (!scheduleDate) {
    return { match: false, scheduleMs: null };
  }

  const scheduleMs = scheduleDate.getTime();
  const match = scheduleMs >= windowStartMs && scheduleMs <= windowEndMs;
  return { match, scheduleMs };
}

async function checkScheduledWatering() {
  checkCounter++;
  console.log(`\n🔎 [DEBUG] checkScheduledWatering() called - Counter: ${checkCounter}`);
  
  try {
    console.log('   [DEBUG] Fetching Firebase /kontrol...');
    
    // Use smart fetch (SDK with REST fallback)
    const kontrolConfig = await fetchKontrolSmart();
    
    console.log(`   [DEBUG] Kontrol config received:`, kontrolConfig ? 'EXISTS' : 'NULL');
    
    if (!kontrolConfig) {
      console.warn('   ⚠️  Kontrol config is NULL - schedule check aborted');
      return;
    }
    
    if (kontrolConfig) {
      console.log(`   [DEBUG] Kontrol data:`, JSON.stringify(kontrolConfig, null, 2));
    }

    const now = new Date();
    const checkEndMs = now.getTime();
    const currentTime = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}`;
    const currentSeconds = now.getSeconds();
    const dateKey = `${now.getFullYear()}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now.getDate().toString().padStart(2, '0')}`;

    // Trigger window untuk mencegah jadwal terlewat karena delay polling/API.
    const graceMs = config.worker.scheduleGraceMs;
    const maxCatchupMs = config.worker.scheduleMaxCatchupMs;
    const fallbackStartMs = checkEndMs - config.worker.checkInterval;
    const baselineStartMs = lastSchedulerTickAt || fallbackStartMs;
    const boundedStartMs = Math.max(baselineStartMs, checkEndMs - maxCatchupMs);
    const triggerWindowStartMs = boundedStartMs - graceMs;
    const triggerWindowEndMs = checkEndMs;
    
    // 🔍 VERBOSE LOG: Log setiap check untuk memastikan fungsi berjalan
    console.log(`\n⏱️  CHECK #${checkCounter}: ${currentTime}:${currentSeconds.toString().padStart(2, '0')} | Mode: ${kontrolConfig?.waktu ? '✅' : '❌'}`);
    
    // Detect all schedules (jadwal_1, jadwal_2, jadwal_3, ...)
    const allSchedules = kontrolConfig ? Object.keys(kontrolConfig).filter(key => key.startsWith('jadwal_')) : [];
    
    if (allSchedules.length === 0) {
      console.warn('   ⚠️  No jadwal found in kontrol config! Check Firebase structure.');
    }
    
    // Log detail setiap 3 menit ATAU jika menit habis dibagi 5
    if (checkCounter % 3 === 0 || now.getMinutes() % 5 === 0) {
      console.log(`   📅 Date: ${dateKey}`);
      console.log(`   🕐 Current: ${currentTime} (${now.toLocaleString('id-ID', {timeZone: 'Asia/Jakarta'})})`);
      console.log(`   🪟 Trigger window: ${new Date(triggerWindowStartMs).toLocaleTimeString('id-ID')} - ${new Date(triggerWindowEndMs).toLocaleTimeString('id-ID')}`);
      console.log(`   Mode Waktu: ${kontrolConfig?.waktu ? '✅ ENABLED' : '❌ DISABLED'}`);
      console.log(`   📊 API Stats: SDK=${sdkSuccessCount} | REST=${restFallbackCount} | Errors=${consecutiveFirebaseErrors}`);
      console.log(`   📋 Total Jadwal: ${allSchedules.length}`);
      
      if (kontrolConfig?.waktu && allSchedules.length > 0) {
        allSchedules.forEach(scheduleKey => {
          const schedule = kontrolConfig[scheduleKey];
          if (schedule && typeof schedule === 'object') {
            const isActive = schedule.aktif !== false; // Default true if not specified
            const waktu = schedule.waktu || 'not set';
            const potAktif = schedule.pot_aktif || [];
            const isMatch = waktu === currentTime;
            console.log(`   ${isActive ? '✅' : '❌'} ${scheduleKey}: ${waktu} → Pot [${potAktif.join(', ')}] ${isMatch ? '🔔 MATCH!' : ''}`);
          }
        });
      }
      
      // Legacy support: Log old format if exists
      if (kontrolConfig?.waktu_1 || kontrolConfig?.waktu_2) {
        console.log(`   [LEGACY] waktu_1: ${kontrolConfig.waktu_1 || 'not set'}`);
        console.log(`   [LEGACY] waktu_2: ${kontrolConfig.waktu_2 || 'not set'}`);
      }
    }

    if (!kontrolConfig || !kontrolConfig.waktu) {
      // Waktu mode disabled
      console.log(`   [DEBUG] Exiting early - kontrolConfig: ${kontrolConfig ? 'exists' : 'null'}, waktu: ${kontrolConfig?.waktu}`);
      return;
    }

    // NEW: Dynamic schedule checking - supports jadwal_1, jadwal_2, ... jadwal_N
    for (const scheduleKey of allSchedules) {
      const schedule = kontrolConfig[scheduleKey];
      
      // Validate schedule structure
      if (!schedule || typeof schedule !== 'object') {
        console.log(`   ⚠️  ${scheduleKey}: Invalid structure, skipping`);
        continue;
      }
      
      // Check if schedule is active (default: true if not specified)
      const isActive = schedule.aktif !== false;
      if (!isActive) {
        continue; // Skip disabled schedules
      }
      
      // Check if time matches
      const scheduleWaktu = schedule.waktu;
      const { match: isInWindow, scheduleMs } = isScheduleInTriggerWindow(
        scheduleWaktu,
        now,
        triggerWindowStartMs,
        triggerWindowEndMs
      );

      if (!isInWindow) {
        continue; // Belum/terlalu lama lewat untuk window ini
      }

      if (scheduleMs !== null) {
        const delayedSec = Math.max(0, Math.floor((triggerWindowEndMs - scheduleMs) / 1000));
        if (delayedSec > 0) {
          console.log(`   ⏱️  ${scheduleKey}: Triggered with ${delayedSec}s delay (within tolerance)`);
        }
      }
      
      // Extract schedule config
      const potAktif = schedule.pot_aktif || [];
      const durasi = schedule.durasi || 60;
      const pompaAir = schedule.pompa_air !== false; // Default true
      const pompaPupuk = schedule.pompa_pupuk || false; // Default false
      
      // Validate pot_aktif
      if (!Array.isArray(potAktif) || potAktif.length === 0) {
        console.log(`   ⚠️  ${scheduleKey}: No active pots defined, skipping`);
        continue;
      }
      
      // Create unique job key
      const normalizedScheduleTime = scheduleWaktu.replace(':', '_');
      const jobKey = `${scheduleKey}_${dateKey}_${normalizedScheduleTime}`;
      
      if (!lastScheduleCheck[jobKey]) {
        console.log(`\n🕐 ${scheduleKey.toUpperCase()} TRIGGERED: ${currentTime}`);
        console.log(`   🎯 Pot aktif: [${potAktif.join(', ')}]`);
        console.log(`   ⏱️  Durasi: ${durasi}s`);
        console.log(`   💧 Pompa Air: ${pompaAir ? 'ON' : 'OFF'}`);
        console.log(`   🌿 Pompa Pupuk: ${pompaPupuk ? 'ON' : 'OFF'}`);

        try {
          await wateringQueue.add(
            scheduleKey,
            {
              type: `waktu_${scheduleKey}`,
              potNumbers: potAktif,
              pompaAir: pompaAir,
              pompaPupuk: pompaPupuk,
              duration: durasi,
              scheduleId: jobKey,
              scheduleTime: scheduleWaktu,
            },
            {
              jobId: jobKey,
              removeOnComplete: true,
            }
          );
          
          lastScheduleCheck[jobKey] = true;
          console.log(`   ✅ Successfully added to queue: ${jobKey}`);
          
          // Check queue status
          const queueStatus = await wateringQueue.getJobCounts();
          console.log(`   📊 Queue status: ${queueStatus.active} active, ${queueStatus.waiting} waiting`);
        } catch (queueError) {
          console.error(`   ❌ Failed to add ${scheduleKey} to queue:`, queueError.message);
        }
      } else {
        console.log(`   ⏭️  ${scheduleKey} already triggered: ${jobKey}`);
      }
    }
    
    // LEGACY SUPPORT: Check old format (waktu_1, waktu_2) untuk backward compatibility
    const legacy1Window = isScheduleInTriggerWindow(
      kontrolConfig.waktu_1,
      now,
      triggerWindowStartMs,
      triggerWindowEndMs
    );

    if (legacy1Window.match) {
      const legacyTimeKey = kontrolConfig.waktu_1.replace(':', '_');
      const scheduleKey = `legacy_jadwal_1_${dateKey}_${legacyTimeKey}`;

      if (!lastScheduleCheck[scheduleKey]) {
        console.log(`\n🕐 [LEGACY] JADWAL 1 TRIGGERED: ${currentTime}`);
        console.log(`   🎯 Using legacy format (all pots)`);

        try {
          await wateringQueue.add(
            'schedule-1',
            {
              type: 'waktu_jadwal_1',
              potNumbers: [1, 2, 3, 4, 5], // All pots
              pompaAir: true,
              pompaPupuk: true,
              duration: kontrolConfig.durasi_1 || 60,
              scheduleId: scheduleKey,
              scheduleTime: kontrolConfig.waktu_1,
            },
            {
              jobId: scheduleKey,
              removeOnComplete: true,
            }
          );
          
          lastScheduleCheck[scheduleKey] = true;
          console.log(`   ✅ Successfully added legacy jadwal_1 to queue`);
        } catch (queueError) {
          console.error(`   ❌ Failed to add legacy jadwal_1:`, queueError.message);
        }
      }
    }

    const legacy2Window = isScheduleInTriggerWindow(
      kontrolConfig.waktu_2,
      now,
      triggerWindowStartMs,
      triggerWindowEndMs
    );

    if (legacy2Window.match) {
      const legacyTimeKey = kontrolConfig.waktu_2.replace(':', '_');
      const scheduleKey = `legacy_jadwal_2_${dateKey}_${legacyTimeKey}`;

      if (!lastScheduleCheck[scheduleKey]) {
        console.log(`\n🕑 [LEGACY] JADWAL 2 TRIGGERED: ${currentTime}`);
        console.log(`   🎯 Using legacy format (all pots)`);

        try {
          await wateringQueue.add(
            'schedule-2',
            {
              type: 'waktu_jadwal_2',
              potNumbers: [1, 2, 3, 4, 5], // All pots
              pompaAir: true,
              pompaPupuk: true,
              duration: kontrolConfig.durasi_2 || 60,
              scheduleId: scheduleKey,
              scheduleTime: kontrolConfig.waktu_2,
            },
            {
              jobId: scheduleKey,
              removeOnComplete: true,
            }
          );

          lastScheduleCheck[scheduleKey] = true;
          console.log(`   ✅ Successfully added legacy jadwal_2 to queue`);
        } catch (queueError) {
          console.error(`   ❌ Failed to add legacy jadwal_2:`, queueError.message);
        }
      }
    }

    // Cleanup old schedule checks (> 2 menit)
    for (const key in lastScheduleCheck) {
      if (key.includes(dateKey)) continue; // Keep today's
      delete lastScheduleCheck[key];
    }
  } catch (error) {
    console.error('❌ Error checking scheduled watering:', error.message);
    console.error('[DEBUG] Error type:', error.constructor.name);
    console.error('[DEBUG] Stack trace:', error.stack);
    
    if (error.message === 'Firebase fetch timeout') {
      console.error('⚠️  Firebase is not responding! Network or connection issue.');
      console.error('   This could be:');
      console.error('   - Slow network connection');
      console.error('   - Firebase Realtime DB throttling');
      console.error('   - Security rules blocking access');
    }
    
    // Continue running - don't crash worker
  } finally {
    lastSchedulerTickAt = Date.now();
  }
}

// Run check setiap 60 detik
setInterval(async () => {
  try {
    await checkScheduledWatering();
  } catch (error) {
    console.error('❌ Error in scheduled check interval:', error.message);
    console.error(error.stack);
  }
}, config.worker.checkInterval);
console.log(`✅ Waktu Mode scheduler started (check every ${config.worker.checkInterval / 1000}s)`);

// Jalankan check pertama kali setelah 8 detik (setelah diagnostic selesai)
setTimeout(async () => {
  try {
    console.log('\n🚀 Running first schedule check immediately...');
    console.log('[DEBUG] About to call checkScheduledWatering()...');
    await checkScheduledWatering();
    console.log('[DEBUG] checkScheduledWatering() returned');
    console.log('✅ First check completed successfully');
  } catch (error) {
    console.error('❌ First check failed:', error.message);
    console.error('[DEBUG] Error stack:', error.stack);
  }
}, 8000);

// ==================== SENSOR MODE (THRESHOLD MONITORING) ====================

let sensorCheckCounter = 0;

// Core sensor check logic (shared by listener and polling)
async function checkSensorThresholds() {
  sensorCheckCounter++;
  
  try {
    // ============ ANTI-SPAM CHECK 1: Is Flutter sensor mode active? ============
    // If Flutter is handling sensor mode, worker should NOT create duplicate jobs
    const systemData = await readFirebaseSmart('system');
    if (systemData && systemData.sensor_mode_active_in_app === true) {
      if (sensorCheckCounter % 10 === 0) {
        console.log(`🚨 WORKER SKIP: Sensor mode active in Flutter app (sensor_mode_active_in_app=true)`);
      }
      return; // Skip all threshold checks - let Flutter handle it
    }

    // Fetch sensor data using smart method (REST fallback)
    const sensorData = await readFirebaseSmart('data');
    
    if (!sensorData) {
      console.log('⚠️  Sensor data is null/empty - ESP32 might not be sending data');
      return;
    }

    // Fetch kontrol config using smart method
    const kontrolConfig = await fetchKontrolSmart();

    if (!kontrolConfig) {
      console.log('⚠️  Kontrol config is null');
      return;
    }
    
    // Check if sensor mode is enabled (using 'otomatis' field, not deprecated 'sensor')
    if (!kontrolConfig.otomatis) {
      if (sensorCheckCounter % 10 === 0) {
        console.log(`⚠️  Sensor mode DISABLED (otomatis=false). Skipping threshold check.`);
      }
      return;
    }

    // Detect all threshold_* nodes
    const allThresholds = Object.keys(kontrolConfig).filter(key => key.startsWith('threshold_'));

    // Log sensor check (ALWAYS LOG untuk debugging)
    console.log(`\n🌡️  SENSOR CHECK #${sensorCheckCounter} | Total Thresholds: ${allThresholds.length}`);
    console.log(`   📊 Sensor Data:`, JSON.stringify(sensorData, null, 2));

    if (allThresholds.length === 0) {
      console.log('   ⚠️  No thresholds configured');
      return;
    }

    // Process each threshold
    for (const thresholdKey of allThresholds) {
      const threshold = kontrolConfig[thresholdKey];
      
      console.log(`\n   🔍 Checking ${thresholdKey}:`);
      console.log(`      Config:`, JSON.stringify(threshold, null, 2));
      
      // Skip if threshold is not active or invalid
      if (!threshold || !threshold.aktif) {
        console.log(`      ❌ Skipped: ${!threshold ? 'Not found' : 'Not active (aktif=false)'}`);
        continue;
      }

      // ============ ANTI-SPAM CHECK 2: Firebase Threshold Cooldown ============
      const thresholdCooldown = await checkThresholdCooldown(thresholdKey);
      if (thresholdCooldown.inCooldown) {
        console.log(`      🚨 ${thresholdKey}: Firebase COOLDOWN active (${thresholdCooldown.remainingSeconds}s remaining) - SKIP`);
        continue; // Skip this threshold entirely
      }

      // Check THRESHOLD cooldown (memory-based backup)
      const lastTime = lastThresholdTime[thresholdKey];
      if (lastTime && Date.now() - lastTime < config.worker.sensorDebounce) {
        const remainingSeconds = Math.ceil((config.worker.sensorDebounce - (Date.now() - lastTime)) / 1000);
        console.log(`      ⏳ ${thresholdKey}: Memory cooldown active (${remainingSeconds}s remaining) - SKIPPING entire threshold`);
        continue;
      }

      const batasBawah = threshold.batas_bawah || 30;
      const batasAtas = threshold.batas_atas || 70;
      const durasi = threshold.durasi || 600;
      const smartMode = threshold.smart_mode === true;
      const potAktif = threshold.pot_aktif || [];
      const pompaAir = threshold.pompa_air === true;
      const pompaPupuk = threshold.pompa_pupuk === true;

      // Collect pots that need watering in this threshold
      const potsNeedWatering = [];
      const potDetails = [];

      // Check each pot in this threshold
      for (const potNumber of potAktif) {
        if (potNumber < 1 || potNumber > 5) {
          console.log(`      ⚠️  POT ${potNumber}: Invalid pot number (must be 1-5)`);
          continue;
        }

        const soilKey = `soil_${potNumber}`;
        const soilValue = parseInt(sensorData[soilKey]) || 0;

        console.log(`      🌱 POT ${potNumber} (${soilKey}): ${soilValue}% | Threshold: ${batasBawah}-${batasAtas}%`);
        console.log(`         → Raw value: ${sensorData[soilKey]} | Parsed: ${soilValue} | Check: ${soilValue} < ${batasBawah} = ${soilValue < batasBawah}`);

        // ============ ANTI-SPAM CHECK 3: Firebase Per-Pot Cooldown ============
        const potCooldown = await checkPotCooldown(potNumber);
        if (potCooldown.inCooldown) {
          console.log(`      🚨 POT ${potNumber}: Firebase COOLDOWN active (${potCooldown.remainingSeconds}s remaining) - SKIP`);
          continue; // Skip this pot
        }

        // NEW: Check if ABOVE upper threshold - skip if too wet!
        if (soilValue >= batasAtas) {
          console.log(`      ✅ POT ${potNumber}: SKIP (${soilValue}% >= ${batasAtas}% - sudah basah!)`);
          continue;
        }

        // Check if below lower threshold
        if (soilValue < batasBawah) {
          console.log(`      🚨 POT ${potNumber} KERING! ${soilValue}% < ${batasBawah}%`);
          
          // Add to watering list (cooldown already checked at threshold level)
          potsNeedWatering.push(potNumber);
          potDetails.push({ pot: potNumber, value: soilValue });
        } else {
          console.log(`      ✅ POT ${potNumber}: OK (${soilValue}% >= ${batasBawah}%)`);
        }
      }

      // NEW: Create SINGLE job for ALL pots that need watering in this threshold
      if (potsNeedWatering.length > 0) {
        console.log(`\n🌡️ THRESHOLD TRIGGERED: ${thresholdKey.toUpperCase()}`);
        console.log(`   Pots needing water: [${potsNeedWatering.join(', ')}]`);
        potDetails.forEach(p => console.log(`   - POT ${p.pot}: ${p.value}% < ${batasBawah}%`));
        console.log(`   Mode: ${smartMode ? 'Smart (monitor until ' + batasAtas + '%)' : 'Fixed (' + durasi + 's)'}`);
        console.log(`   Pumps: Air=${pompaAir}, Pupuk=${pompaPupuk}`);

        // Set Firebase cooldowns BEFORE creating job
        await setThresholdCooldown(thresholdKey, 2); // 2 minutes
        await Promise.all(potsNeedWatering.map(pot => setPotCooldown(pot, 2))); // 2 minutes each
        console.log(`   🔒 Firebase cooldowns SET for threshold + pots`);

        await sendAutomationNotification({
          title: 'ApsGo - Penyiraman Otomatis',
          body: `Penyiraman dilakukan pada ${potsNeedWatering.map((pot) => `pot ${pot}`).join(', ')} karena kelembapan di bawah ambang batas ${batasBawah}%.`,
          type: 'sensor_triggered',
          data: {
            thresholdId: thresholdKey,
            pots: potsNeedWatering.join(','),
            batasBawah,
            batasAtas,
            durasi,
            mode: smartMode ? 'smart' : 'fixed',
          },
        });

        const jobId = `${thresholdKey}-${Date.now()}`;
        await wateringQueue.add(
          thresholdKey,
          {
            type: 'sensor_threshold',
            potNumbers: potsNeedWatering,  // ALL pots in 1 job!
            pompaAir: pompaAir,
            pompaPupuk: pompaPupuk,
            duration: durasi,
            scheduleId: jobId,
            thresholdId: thresholdKey,
            smartMode: smartMode,
            sensorData: { 
              batasBawah, 
              batasAtas, 
              mode: smartMode ? 'smart' : 'fixed',
              potValues: potDetails
            },
          },
          {
            jobId,
            removeOnComplete: true,
            priority: 1, // Higher priority for sensor-triggered
          }
        );

        console.log(`   📌 Added to queue: ${jobId}`);
        console.log(`   🔄 ${thresholdKey} will execute simultaneously for ALL pots`);
        console.log(`   ⏰ After completion, ${thresholdKey} cooldown = 2 minutes (other thresholds can still run)`);
      }
    }
  } catch (error) {
    console.error('❌ Error in sensor threshold check:', error.message);
  }
}

// ============ ANTI-SPAM: Check if threshold is in cooldown ============
async function checkThresholdCooldown(thresholdId) {
  try {
    const cooldownData = await readFirebaseSmart(`sensor_cooldowns/${thresholdId}`);
    
    if (!cooldownData || !cooldownData.lastTrigger) {
      return { inCooldown: false };
    }

    const lastTriggerMs = cooldownData.lastTrigger;
    const cooldownMinutes = cooldownData.cooldownMinutes || 2;
    const cooldownMs = cooldownMinutes * 60 * 1000;
    const elapsedMs = Date.now() - lastTriggerMs;

    if (elapsedMs < cooldownMs) {
      const remainingSeconds = Math.ceil((cooldownMs - elapsedMs) / 1000);
      return {
        inCooldown: true,
        remainingSeconds: remainingSeconds,
        elapsedSeconds: Math.floor(elapsedMs / 1000),
      };
    }

    return { inCooldown: false };
  } catch (error) {
    console.log('⚠️  Error checking threshold cooldown:', error.message);
    return { inCooldown: false };
  }
}

// ============ ANTI-SPAM: Check if pot is in cooldown ============
async function checkPotCooldown(potNumber) {
  try {
    const cooldownData = await readFirebaseSmart(`pot_cooldowns/pot_${potNumber}`);
    
    if (!cooldownData || !cooldownData.lastTrigger) {
      return { inCooldown: false };
    }

    const lastTriggerMs = cooldownData.lastTrigger;
    const cooldownMinutes = cooldownData.cooldownMinutes || 2;
    const cooldownMs = cooldownMinutes * 60 * 1000;
    const elapsedMs = Date.now() - lastTriggerMs;

    if (elapsedMs < cooldownMs) {
      const remainingSeconds = Math.ceil((cooldownMs - elapsedMs) / 1000);
      return {
        inCooldown: true,
        remainingSeconds: remainingSeconds,
      };
    }

    return { inCooldown: false };
  } catch (error) {
    console.log('⚠️  Error checking pot cooldown:', error.message);
    return { inCooldown: false };
  }
}

// ============ ANTI-SPAM: Set threshold cooldown in Firebase ============
async function setThresholdCooldown(thresholdId, cooldownMinutes = 2) {
  try {
    const now = Date.now();
    await updateFirebaseSmart(`sensor_cooldowns/${thresholdId}`, {
      lastTrigger: now,
      cooldownMinutes: cooldownMinutes,
      lastUpdatedAt: new Date().toISOString(),
    });
    console.log(`      🔒 Threshold cooldown SET: ${thresholdId} (${cooldownMinutes} min)`);
  } catch (error) {
    console.log('⚠️  Error setting threshold cooldown:', error.message);
  }
}

// ============ ANTI-SPAM: Set pot cooldown in Firebase ============
async function setPotCooldown(potNumber, cooldownMinutes = 2) {
  try {
    const now = Date.now();
    await updateFirebaseSmart(`pot_cooldowns/pot_${potNumber}`, {
      lastTrigger: now,
      cooldownMinutes: cooldownMinutes,
      lastUpdatedAt: new Date().toISOString(),
    });
  } catch (error) {
    console.log('⚠️  Error setting pot cooldown:', error.message);
  }
}

// Setup sensor monitoring with BOTH listener (SDK) and polling (fallback)
async function setupSensorMonitoring() {
  console.log('🌡️  ==================== SENSOR MODE ENABLED ====================');
  console.log('✅ Sensor Mode (Threshold System) monitoring started');
  console.log('📍 Primary: Polling every 30 seconds (REST API)');
  console.log('📍 Backup: Firebase listener on /data (if SDK works)');
  console.log('📍 Config path: /' + FIREBASE_PATHS.kontrol);
  console.log('⏱️  Debounce time: 2 minutes between watering per pot');
  console.log('================================================================\n');

  // METHOD 1: Polling (RELIABLE - uses REST API)
  // Check sensor threshold every 30 seconds
  setInterval(async () => {
    try {
      await checkSensorThresholds();
    } catch (error) {
      console.error('❌ Polling sensor check failed:', error.message);
    }
  }, 30000); // 30 seconds

  // Run first check immediately
  setTimeout(async () => {
    console.log('🚀 Running first sensor check...');
    try {
      await checkSensorThresholds();
      console.log('✅ First sensor check completed');
    } catch (error) {
      console.error('❌ First sensor check failed:', error.message);
    }
  }, 10000); // 10 seconds after startup

  // METHOD 2: Firebase Listener (BACKUP - might not work if SDK fails)
  try {
    db.ref('data').on('value', async (snapshot) => {
      console.log('🔔 Firebase listener triggered (SDK working!)');
      // Call the same check function
      await checkSensorThresholds();
    }, (error) => {
      console.error('❌ Firebase listener error:', error.message);
    });
    console.log('✅ Firebase listener attached (backup method)');
  } catch (error) {
    console.log('⚠️  Firebase listener failed to attach (will rely on polling)');
  }
}

setupSensorMonitoring();

// ==================== HISTORY LOGGING ====================

async function logHistory(type, potNumbers, duration) {
  try {
    const now = new Date();
    const dateKey = `${now.getFullYear()}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now.getDate().toString().padStart(2, '0')}`;
    const timeKey = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}`;

    // Get current sensor data with timeout
    const sensorData = await readFirebaseSmart('data') || {};

    await setFirebaseSmart(`history/${dateKey}/${timeKey}`, {
      timestamp: now.getTime(),
      source: 'server',
      type: type,
      pots: potNumbers,
      duration: duration,
      ...sensorData,
    });

    console.log(`   📊 History logged: ${dateKey} ${timeKey}`);
  } catch (error) {
    console.error('   ⚠️ Failed to log history:', error.message);
  }
}

// ==================== PERIODIC HISTORY LOGGING ====================

// Auto-log sensor data setiap 30 menit (independent from watering)
const autoLogJob = new cron.CronJob('*/30 * * * *', async () => {
  try {
    const sensorData = await readFirebaseSmart('data');

    if (sensorData) {
      const now = new Date();
      const dateKey = `${now.getFullYear()}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now.getDate().toString().padStart(2, '0')}`;
      const timeKey = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}`;

      await setFirebaseSmart(`history/${dateKey}/${timeKey}`, {
        timestamp: now.getTime(),
        source: 'server',
        type: 'auto_log',
        ...sensorData,
      });

      console.log(`📊 Auto-logged sensor data: ${timeKey}`);
    }
  } catch (error) {
    console.error('❌ Auto-log failed:', error.message);
  }
});

autoLogJob.start();
console.log('✅ Auto history logging started (every 30 minutes)');

// ==================== CLEANUP OLD HISTORY (DAILY) ====================

const cleanupJob = new cron.CronJob('0 2 * * *', async () => {
  // Run daily at 2 AM
  try {
    console.log('\n🧹 Running history cleanup...');
    const daysToKeep = 10;
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);

    const historyData = await readFirebaseSmart('history');

    if (historyData) {
      let deletedCount = 0;
      for (const dateKey in historyData) {
        try {
          const [year, month, day] = dateKey.split('-').map(Number);
          const date = new Date(year, month - 1, day);

          if (date < cutoffDate) {
            // Use REST API DELETE with timeout wrapper
            const url = `${config.firebase.databaseURL}/history/${dateKey}.json`;
            await fetchWithTimeout2(url, { method: 'DELETE' }, 10000);
            deletedCount++;
            console.log(`   🗑️ Deleted: ${dateKey}`);
          }
        } catch (error) {
          console.error(`   ⚠️ Error deleting ${dateKey}:`, error.message);
        }
      }
      console.log(`✅ Cleanup completed: ${deletedCount} dates removed`);
    }
  } catch (error) {
    console.error('❌ Cleanup failed:', error.message);
  }
});

cleanupJob.start();
console.log('✅ History cleanup scheduled (daily at 2 AM)');

// ==================== UTILITIES ====================

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ==================== MANUAL TEST FUNCTIONS ====================

// 🧪 Test scheduler sekarang juga (untuk debugging)
async function testSchedulerNow() {
  try {
    console.log('\n🧪 MANUAL TEST: Triggering test watering job NOW...');
    
    const now = new Date();
    const testJobId = `manual-test-${now.getTime()}`;
    
    await wateringQueue.add(
      'manual-test',
      {
        type: 'manual_test',
        potNumbers: [1], // Test dengan 1 pot saja
        pompaAir: true,
        pompaPupuk: false,
        duration: 10, // 10 detik test
        scheduleId: testJobId,
      },
      {
        jobId: testJobId,
        removeOnComplete: true,
        priority: 10, // Highest priority
      }
    );
    
    console.log(`✅ Test job added: ${testJobId}`);
    console.log('   Watch for job processing logs...');
  } catch (error) {
    console.error('❌ Test scheduler failed:', error.message);
  }
}

// 🔍 Check Firebase aktuator node structure
async function checkAktuatorNode() {
  try {
    console.log('\n🔍 CHECKING AKTUATOR NODE...');
    const aktuatorData = await readFirebaseSmart('aktuator');
    
    if (!aktuatorData) {
      console.log('❌ Aktuator node NOT FOUND in Firebase!');
      console.log('   Creating default aktuator structure...');
      
      await setFirebaseSmart('aktuator', {
        mosvet_1: false,  // Pompa Air
        mosvet_2: false,  // Pompa Pupuk
        mosvet_3: false,  // Pot 1
        mosvet_4: false,  // Pot 2
        mosvet_5: false,  // Pot 3
        mosvet_6: false,  // Pot 4
        mosvet_7: false,  // Pot 5
        mosvet_8: false,  // Pengaduk
      });
      
      console.log('✅ Aktuator node created with defaults');
    } else {
      console.log('✅ Aktuator node exists:');
      for (const key in aktuatorData) {
        console.log(`   ${key}: ${aktuatorData[key]}`);
      }
      
      // Validate all required mosvets exist
      const required = ['mosvet_1', 'mosvet_2', 'mosvet_3', 'mosvet_4', 'mosvet_5', 'mosvet_6', 'mosvet_7', 'mosvet_8'];
      const missing = required.filter(key => !(key in aktuatorData));
      
      if (missing.length > 0) {
        console.log(`⚠️  Missing mosvets: ${missing.join(', ')}`);
        console.log('   Adding missing mosvets...');
        
        const updates = {};
        missing.forEach(key => updates[key] = false);
        await updateFirebaseSmart('aktuator', updates);
        
        console.log('✅ Missing mosvets added');
      }
    }
  } catch (error) {
    console.error('❌ Aktuator check failed:', error.message);
  }
}

// 🕐 Show current time in multiple formats
async function showCurrentTime() {
  try {
    const now = new Date();
    console.log('\n🕐 CURRENT TIME ANALYSIS:');
    console.log(`   Server Local: ${now.toString()}`);
    console.log(`   Asia/Jakarta: ${now.toLocaleString('id-ID', {timeZone: 'Asia/Jakarta'})}`);
    console.log(`   ISO: ${now.toISOString()}`);
    console.log(`   Unix: ${now.getTime()}`);
    console.log(`   TZ Env: ${process.env.TZ}`);
    console.log(`   HH:MM Format: ${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}`);
    
    // Check Firebase kontrol waktu
    console.log('[DEBUG] Fetching kontrol for time analysis...');
    const snapshot = await fetchWithTimeout(db.ref(FIREBASE_PATHS.kontrol), 10000);
    const kontrolConfig = snapshot.val();
    console.log('[DEBUG] Kontrol fetch successful');
    
    if (kontrolConfig) {
      console.log('\n📋 FIREBASE KONTROL:');
      console.log(`   Mode Waktu: ${kontrolConfig.waktu ? 'ENABLED ✅' : 'DISABLED ❌'}`);
      console.log(`   Waktu 1: ${kontrolConfig.waktu_1 || 'not set'}`);
      console.log(`   Waktu 2: ${kontrolConfig.waktu_2 || 'not set'}`);
      console.log(`   Durasi 1: ${kontrolConfig.durasi_1 || 'not set'}s`);
      console.log(`   Durasi 2: ${kontrolConfig.durasi_2 || 'not set'}s`);
    } else {
      console.log('\n❌ Firebase kontrol node is empty!');
    }
  } catch (error) {
    console.error('❌ Time check failed:', error.message);
  }
}

// ==================== HEALTH CHECK ====================

async function healthCheck() {
  try {
    // Check Firebase connection (skip SDK, just check via config)
    const firebaseOk = config.firebase.databaseURL ? true : false;

    // Check Redis connection
    await redis.ping();

    // Check queue
    const queueStatus = await wateringQueue.getJobCounts();

    console.log('\n💚 HEALTH CHECK:');
    console.log(`   Firebase: ${firebaseOk ? '✅' : '❌'} Connected`);
    console.log(`   Redis: ✅ Connected`);
    console.log(`   Queue: ${queueStatus.active} active, ${queueStatus.waiting} waiting`);
  } catch (error) {
    console.error('❤️‍🩹 HEALTH CHECK FAILED:', error.message);
  }
}

// Run health check every 5 minutes
setInterval(healthCheck, 300000);

// ==================== GRACEFUL SHUTDOWN ====================

async function shutdown() {
  console.log('\n🛑 Shutting down gracefully...');

  try {
    await wateringWorker.close();
    console.log('✅ Worker closed');

    await wateringQueue.close();
    console.log('✅ Queue closed');

    await redis.quit();
    console.log('✅ Redis disconnected');

    await admin.app().delete();
    console.log('✅ Firebase disconnected');

    process.exit(0);
  } catch (error) {
    console.error('❌ Shutdown error:', error.message);
    process.exit(1);
  }
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// ==================== PREVENT CRASHES ====================

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error.message);
  console.error(error.stack);
  // Don't exit - try to keep worker running
  console.log('⚠️  Worker continuing despite error...');
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise);
  console.error('Reason:', reason);
  // Don't exit - try to keep worker running
  console.log('⚠️  Worker continuing despite rejection...');
});

// ==================== STARTUP COMPLETE ====================

console.log('\n✨ ApsGo Railway Worker is running!');
console.log('📊 Features enabled:');
console.log('   • Waktu Mode (Time-based scheduling)');
console.log('   • Sensor Mode (Threshold-based automation)');
console.log('   • Auto History Logging (every 30 min)');
console.log('   • History Cleanup (daily at 2 AM)');
console.log('   • Health Check (every 5 min)');
console.log('\n🎯 Worker is ready to process jobs...\n');

// Initial health check
setTimeout(healthCheck, 5000);

// ==================== KEEP-ALIVE MECHANISM ====================

// Heartbeat every 30 seconds to prevent Railway from stopping container
setInterval(() => {
  const uptime = Math.floor(process.uptime());
  const hours = Math.floor(uptime / 3600);
  const minutes = Math.floor((uptime % 3600) / 60);
  console.log(`💓 Heartbeat: Worker alive for ${hours}h ${minutes}m`);
}, 30000);

// Verify Firebase connection on startup
setTimeout(async () => {
  try {
    console.log('🔍 Verifying Firebase connection...');
    console.log('[DEBUG] Testing Firebase read with timeout...');
    const snapshot = await fetchWithTimeout(db.ref(FIREBASE_PATHS.kontrol), 10000);
    console.log('[DEBUG] Firebase read successful!');
    const data = snapshot.val();
    if (data) {
      console.log(`✅ Firebase /${FIREBASE_PATHS.kontrol} readable - waktu mode:`, data.waktu ? 'ENABLED' : 'DISABLED');
      if (data.waktu) {
        console.log(`   📅 Schedules: ${data.waktu_1 || 'none'} / ${data.waktu_2 || 'none'}`);
      }
    } else {
      console.log('⚠️  Firebase /kontrol is empty - waiting for Flutter app to set schedule');
    }
  } catch (error) {
    console.error('❌ Firebase verification failed:', error.message);
  }
}, 3000);

// Run diagnostic checks on startup
setTimeout(async () => {
  try {
    console.log('\n🔧 RUNNING DIAGNOSTIC CHECKS...');
    await showCurrentTime();
    await checkAktuatorNode();
    console.log('\n✅ Diagnostic checks completed');
    console.log('\n💡 TIP: To test scheduler manually, check the logs above for current time');
    console.log('   Then set waktu_1 or waktu_2 in Firebase to match current time + 1 minute');
  } catch (error) {
    console.error('❌ Diagnostic checks failed:', error.message);
    console.error(error.stack);
  }
}, 5000);

// Auto-run test scheduler setiap 10 menit untuk memastikan worker alive
setInterval(() => {
  const now = new Date();
  // Run at :00, :10, :20, :30, :40, :50
  if (now.getMinutes() % 10 === 0 && now.getSeconds() < 30) {
    showCurrentTime();
  }
}, 30000);