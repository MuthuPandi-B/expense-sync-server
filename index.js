const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const admin = require('firebase-admin');

const PORT = process.env.PORT || 8080;

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

server.listen(PORT, () => {
  console.log(`Expense Tracker sync server listening on port ${PORT}`);
});
