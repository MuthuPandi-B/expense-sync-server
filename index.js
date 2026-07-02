const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const admin = require('firebase-admin');

const PORT = process.env.PORT || 8080;

// ── Version configuration ──────────────────────────────────────
const pkg = require('./package.json');
const SERVER_VERSION = pkg.version;
const LATEST_APP_VERSION = process.env.LATEST_APP_VERSION || SERVER_VERSION;
const APK_DOWNLOAD_URL = process.env.APK_DOWNLOAD_URL || '';
const FORCE_UPDATE = process.env.FORCE_UPDATE === 'true';

// ── Firebase Admin SDK initialization ──────────────────────────
let fcmEnabled = false;
try {
  const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || '{}');
  if (serviceAccount.project_id) {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
    fcmEnabled = true;
    console.log('[firebase] Admin SDK initialized successfully');
  } else {
    console.warn('[firebase] FIREBASE_SERVICE_ACCOUNT not set — FCM disabled');
  }
} catch (err) {
  console.error('[firebase] Failed to initialize:', err.message);
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// In-memory connection map: deviceUUID -> { ws, email }
const connections = new Map();

// In-memory FCM token map: email -> Set of { deviceUUID, fcmToken }
// Persists across WebSocket disconnects (tokens remain valid when app is closed)
const fcmTokens = new Map();

app.get('/api/version', (_req, res) => {
  res.json({
    latestAppVersion: LATEST_APP_VERSION,
    serverVersion: SERVER_VERSION,
    downloadUrl: APK_DOWNLOAD_URL,
    forceUpdate: FORCE_UPDATE,
  });
});

app.get('/health', (_req, res) => {
  let fcmTokenCount = 0;
  for (const tokenSet of fcmTokens.values()) {
    fcmTokenCount += tokenSet.size;
  }
  res.json({
    status: 'ok',
    connections: connections.size,
    fcmTokenCount,
    fcmEnabled,
    uptime: process.uptime(),
  });
});

wss.on('connection', (ws) => {
  let registeredUUID = null;

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === 'register') {
      const { deviceUUID, email, fcmToken } = msg;
      if (!deviceUUID || !email) return;

      registeredUUID = deviceUUID;
      connections.set(deviceUUID, { ws, email });
      console.log(`[register] ${deviceUUID} (${email}) — total: ${connections.size}`);

      // Store FCM token if provided
      if (fcmToken) {
        if (!fcmTokens.has(email)) {
          fcmTokens.set(email, new Set());
        }
        // Remove any existing entry for this device (token may have changed)
        const tokenSet = fcmTokens.get(email);
        for (const entry of tokenSet) {
          if (entry.deviceUUID === deviceUUID) {
            tokenSet.delete(entry);
            break;
          }
        }
        tokenSet.add({ deviceUUID, fcmToken });
        console.log(`[fcm-token] stored for ${deviceUUID} (${email}) — ${tokenSet.size} token(s)`);
      }
      return;
    }

    if (msg.type === 'sync_completed') {
      const { deviceUUID, email, timestamp } = msg;
      if (!deviceUUID || !email) return;

      const ts = timestamp || new Date().toISOString();
      console.log(`[sync_completed] from ${deviceUUID} (${email})`);

      // 1. Broadcast via WebSocket to all same-email connections except sender
      for (const [uuid, conn] of connections) {
        if (uuid !== deviceUUID && conn.email === email && conn.ws.readyState === 1) {
          conn.ws.send(JSON.stringify({
            type: 'sync_completed',
            deviceUUID,
            timestamp: ts,
          }));
        }
      }

      // 2. Send FCM data messages to same-email devices except sender
      if (fcmEnabled) {
        sendFcmToOtherDevices(email, deviceUUID, ts);
      }
      return;
    }

    if (msg.type === 'expense_changed') {
      const { deviceUUID, email, expense } = msg;
      if (!deviceUUID || !email || !expense) return;

      console.log(`[expense_changed] from ${deviceUUID} (${email}) — expense ${expense.id}`);

      // Relay expense data to all same-email WebSocket connections except sender
      for (const [uuid, conn] of connections) {
        if (uuid !== deviceUUID && conn.email === email && conn.ws.readyState === 1) {
          conn.ws.send(JSON.stringify({
            type: 'expense_changed',
            deviceUUID,
            expense,
          }));
        }
      }

      // Send FCM data message to offline devices
      if (fcmEnabled) {
        sendFcmExpenseChanged(email, deviceUUID, expense);
      }
      return;
    }
  });

  ws.on('close', () => {
    if (registeredUUID) {
      connections.delete(registeredUUID);
      console.log(`[disconnect] ${registeredUUID} — total: ${connections.size}`);
      // NOTE: FCM tokens are NOT removed on disconnect.
      // They remain valid even when the app is closed.
    }
  });

  ws.on('error', (err) => {
    console.error(`[ws error] ${registeredUUID || 'unknown'}:`, err.message);
  });
});

/**
 * Send FCM data-only message to all devices registered under `email`,
 * excluding the device that triggered the sync (`senderUUID`).
 */
async function sendFcmToOtherDevices(email, senderUUID, timestamp) {
  const tokenSet = fcmTokens.get(email);
  if (!tokenSet || tokenSet.size === 0) return;

  const messaging = admin.messaging();

  for (const entry of tokenSet) {
    if (entry.deviceUUID === senderUUID) continue;

    try {
      await messaging.send({
        token: entry.fcmToken,
        data: {
          type: 'sync_completed',
          deviceUUID: senderUUID,
          timestamp: timestamp,
        },
        android: {
          priority: 'high',
        },
      });
      console.log(`[fcm-send] sent to ${entry.deviceUUID}`);
    } catch (err) {
      console.error(`[fcm-send] failed for ${entry.deviceUUID}:`, err.code || err.message);

      // Remove invalid/expired tokens
      if (
        err.code === 'messaging/invalid-registration-token' ||
        err.code === 'messaging/registration-token-not-registered'
      ) {
        tokenSet.delete(entry);
        console.log(`[fcm-cleanup] removed stale token for ${entry.deviceUUID}`);
      }
    }
  }
}

/**
 * Send FCM data-only message with expense data to offline devices.
 * Falls back to sync_completed if expense payload exceeds FCM limits.
 */
async function sendFcmExpenseChanged(email, senderUUID, expense) {
  const tokenSet = fcmTokens.get(email);
  if (!tokenSet || tokenSet.size === 0) return;

  const messaging = admin.messaging();
  const expenseJson = JSON.stringify(expense);

  // FCM data values must be strings; total payload limit is 4KB
  const useFullPayload = Buffer.byteLength(expenseJson, 'utf8') < 3500;

  for (const entry of tokenSet) {
    if (entry.deviceUUID === senderUUID) continue;

    try {
      const data = useFullPayload
        ? { type: 'expense_changed', deviceUUID: senderUUID, expense: expenseJson }
        : { type: 'sync_completed', deviceUUID: senderUUID, timestamp: new Date().toISOString() };

      await messaging.send({
        token: entry.fcmToken,
        data,
        android: { priority: 'high' },
      });
      console.log(`[fcm-send] expense_changed to ${entry.deviceUUID}`);
    } catch (err) {
      console.error(`[fcm-send] failed for ${entry.deviceUUID}:`, err.code || err.message);
      if (
        err.code === 'messaging/invalid-registration-token' ||
        err.code === 'messaging/registration-token-not-registered'
      ) {
        tokenSet.delete(entry);
        console.log(`[fcm-cleanup] removed stale token for ${entry.deviceUUID}`);
      }
    }
  }
}

server.listen(PORT, () => {
  console.log(`Expense Tracker sync server listening on port ${PORT}`);
});
