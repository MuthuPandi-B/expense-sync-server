const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// In-memory connection map: deviceUUID -> { ws, email }
const connections = new Map();

app.get('/health', (_req, res) => {
  res.json({
    status: 'ok',
    connections: connections.size,
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
      const { deviceUUID, email } = msg;
      if (!deviceUUID || !email) return;

      registeredUUID = deviceUUID;
      connections.set(deviceUUID, { ws, email });
      console.log(`[register] ${deviceUUID} (${email}) — total: ${connections.size}`);
      return;
    }

    if (msg.type === 'sync_completed') {
      const { deviceUUID, email, timestamp } = msg;
      if (!deviceUUID || !email) return;

      console.log(`[sync_completed] from ${deviceUUID} (${email})`);

      // Broadcast to all connections with the same email, except the sender
      for (const [uuid, conn] of connections) {
        if (uuid !== deviceUUID && conn.email === email && conn.ws.readyState === 1) {
          conn.ws.send(JSON.stringify({
            type: 'sync_completed',
            deviceUUID,
            timestamp: timestamp || new Date().toISOString(),
          }));
        }
      }
      return;
    }
  });

  ws.on('close', () => {
    if (registeredUUID) {
      connections.delete(registeredUUID);
      console.log(`[disconnect] ${registeredUUID} — total: ${connections.size}`);
    }
  });

  ws.on('error', (err) => {
    console.error(`[ws error] ${registeredUUID || 'unknown'}:`, err.message);
  });
});

server.listen(PORT, () => {
  console.log(`Expense Tracker sync server listening on port ${PORT}`);
});
