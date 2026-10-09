const express = require('express'), http = require('http');
const { WebSocketServer } = require('ws');
const { Chess } = require('chess.js');

const app = express();
app.use(express.static(__dirname + '/public'));
app.get('/healthz', (q, r) => r.send('ok'));
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const TC = { '0': [0, 0], '1': [60, 0], '3': [180, 2], '5': [300, 0], '10': [600, 0] };
const queues = {}, rooms = new Map(), games = new Set();
const send = (ws, o) => ws && ws.readyState === 1 && ws.send(JSON.stringify(o));
const clean = n => String(n || '').replace(/[<>&"]/g, '').trim().slice(0, 16) || 'Guest';
const online = () => wss.clients.forEach(c => send(c, { t: 'online', n: wss.clients.size }));

function push(g) {
  const turn = g.chess.turn(), clock = { w: g.clock.w, b: g.clock.b };
  if (g.tc && !g.over) clock[turn] -= Date.now() - g.last;
  const h = g.chess.history({ verbose: true }), l = h[h.length - 1];
  const m = { t: 'state', fen: g.chess.fen(), clock, tc: g.tc, hist: h.map(x => x.san),
    last: l ? { from: l.from, to: l.to } : null, over: g.over, reason: g.why };
  send(g.p.w, m); send(g.p.b, m);
}
function end(g, why) {
  if (g.over) return;
  g.over = true; g.why = why; push(g);
  setTimeout(() => games.delete(g), 5000);
}
function start(a, b, tcKey) {
  const [s, inc] = TC[tcKey] || TC['0'];
  const [w, bl] = Math.random() < 0.5 ? [a, b] : [b, a];
  const g = { chess: new Chess(), p: { w, b: bl }, names: { w: w.name, b: bl.name },
    tc: s > 0, inc: inc * 1000, clock: { w: s * 1000, b: s * 1000 }, last: Date.now(), over: false };
  w.game = bl.game = g; w.color = 'w'; bl.color = 'b'; games.add(g);
  send(w, { t: 'start', color: 'w', names: g.names });
  send(bl, { t: 'start', color: 'b', names: g.names });
  push(g);
}
function leaveLobby(ws) {
  for (const k in queues) queues[k] = queues[k].filter(x => x !== ws);
  if (ws.room) { rooms.delete(ws.room); ws.room = null; }
}
function leave(ws) {
  leaveLobby(ws);
  const g = ws.game;
  if (g && !g.over) {
    const other = ws.color === 'w' ? 'b' : 'w';
    end(g, `${g.names[other]} wins — ${g.names[ws.color]} left`);
  }
  ws.game = null;
}

wss.on('connection', ws => {
  ws.name = 'Guest'; ws.isAlive = true;
  ws.on('pong', () => ws.isAlive = true);
  online();
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.name) ws.name = clean(m.name);
    const g = ws.game;
    switch (m.t) {
      case 'queue': {
        leave(ws);
        const k = TC[m.tc] ? m.tc : '0';
        const q = queues[k] = queues[k] || [];
        const opp = q.shift();
        if (opp) start(opp, ws, k); else { q.push(ws); send(ws, { t: 'wait' }); }
        break;
      }
      case 'create': {
        leave(ws);
        let code; do { code = Math.random().toString(36).slice(2, 6).toUpperCase(); } while (rooms.has(code));
        rooms.set(code, { host: ws, tc: TC[m.tc] ? m.tc : '0' });
        ws.room = code; send(ws, { t: 'room', code });
        break;
      }
      case 'join': {
        const code = String(m.code || '').toUpperCase().trim(), r = rooms.get(code);
        if (!r || r.host === ws) return send(ws, { t: 'err', m: 'Room not found.' });
        rooms.delete(code); r.host.room = null;
        start(r.host, ws, r.tc);
        break;
      }
      case 'move': {
        if (!g || g.over || g.chess.turn() !== ws.color) break;
        if (!g.chess.move({ from: m.from, to: m.to, promotion: m.promotion || 'q' })) { push(g); break; }
        const now = Date.now();
        if (g.tc) {
          g.clock[ws.color] -= now - g.last;
          if (g.clock[ws.color] <= 0) {
            g.clock[ws.color] = 0;
            const o = ws.color === 'w' ? 'b' : 'w';
            return end(g, `${g.names[o]} wins on time`);
          }
          g.clock[ws.color] += g.inc;
        }
        g.last = now; g.offer = null;
        const c = g.chess;
        if (c.in_checkmate()) end(g, `${g.names[ws.color]} wins by checkmate`);
        else if (c.in_stalemate()) end(g, 'Draw by stalemate');
        else if (c.insufficient_material()) end(g, 'Draw — not enough pieces');
        else if (c.in_threefold_repetition()) end(g, 'Draw by repetition');
        else if (c.in_draw()) end(g, 'Draw by 50-move rule');
        else push(g);
        break;
      }
      case 'resign':
        if (g && !g.over) { const o = ws.color === 'w' ? 'b' : 'w'; end(g, `${g.names[o]} wins — ${g.names[ws.color]} resigned`); }
        break;
      case 'offer':
        if (g && !g.over) { g.offer = ws.color; send(g.p[ws.color === 'w' ? 'b' : 'w'], { t: 'draw' }); }
        break;
      case 'accept':
        if (g && !g.over && g.offer && g.offer !== ws.color) end(g, 'Draw by agreement');
        break;
      case 'leave': leave(ws); break;
    }
  });
  ws.on('close', () => { leave(ws); online(); });
});

setInterval(() => {
  for (const g of games) {
    if (g.over || !g.tc) continue;
    const t = g.chess.turn();
    if (g.clock[t] - (Date.now() - g.last) <= 0) {
      g.clock[t] = 0;
      end(g, `${g.names[t === 'w' ? 'b' : 'w']} wins on time`);
    }
  }
}, 500);
setInterval(() => wss.clients.forEach(c => { if (!c.isAlive) return c.terminate(); c.isAlive = false; c.ping(); }), 25000);

server.listen(process.env.PORT || 3000, () => console.log('Chess running'));
