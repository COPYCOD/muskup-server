const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const ALLOWED = new Set(['connect-request', 'connect-response', 'offer', 'answer', 'ice', 'hangup']);
const clients = new Map();

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

const server = http.createServer((_req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('MUSKUP signaling OK. Online: ' + clients.size);
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });

wss.on('connection', (ws) => {
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch (_) { return; }

    if (m.type === 'register') {
      if (!/^\d{9}$/.test(String(m.id))) return send(ws, { type: 'error', code: 'bad-id' });
      const cur = clients.get(m.id);
      if (cur && cur !== ws && cur.readyState === 1) return send(ws, { type: 'id-taken' });
      ws.id = m.id;
      clients.set(m.id, ws);
      return send(ws, { type: 'registered', id: m.id });
    }

    if (!ws.id || !ALLOWED.has(m.type) || !m.to) return;
    const target = clients.get(String(m.to));
    if (!target) return send(ws, { type: 'peer-offline', to: m.to });
    m.from = ws.id;
    send(target, m);
  });

  ws.on('close', () => {
    if (ws.id && clients.get(ws.id) === ws) clients.delete(ws.id);
  });
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.alive) return ws.terminate();
    ws.alive = false;
    ws.ping();
  });
}, 15000);

server.listen(PORT, () => console.log('MUSKUP signaling on port ' + PORT));
