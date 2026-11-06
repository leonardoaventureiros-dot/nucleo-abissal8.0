// Servidor multiplayer do Núcleo Abissal (relay de salas via WebSocket).
// A simulação roda no host (jogador que criou a sala); aqui só gerenciamos salas e repassamos mensagens.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const AL = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
// Procura o index.html na pasta do servidor (e em ./public), aceitando variações de nome como "Index.html" ou "index (1).html"
function findIndex() {
  for (const dir of [__dirname, path.join(__dirname, 'public')]) {
    let files = [];
    try { files = fs.readdirSync(dir); } catch (e) { continue; }
    const f = files.find(n => n === 'index.html') || files.find(n => /^index.*\.html?$/i.test(n)) || files.find(n => /\.html?$/i.test(n));
    if (f) return path.join(dir, f);
  }
  return null;
}
console.log('Pasta do servidor:', __dirname);
try { console.log('Arquivos aqui:', fs.readdirSync(__dirname).join(', ')); } catch (e) {}
console.log('Jogo encontrado:', findIndex() || 'NENHUM (suba o index.html na mesma pasta do server.js)');

// ---------- HTTP: serve o jogo e um health check ----------
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (url === '/' || url === '/index.html') {
    const file = findIndex();
    if (!file) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('index.html não encontrado. Ele precisa estar na mesma pasta do server.js (raiz do repositório). Pasta atual: ' + __dirname); }
    fs.readFile(file, (err, data) => {
      if (err) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Erro ao ler o jogo'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
    return;
  }
  res.writeHead(404); res.end('Not found');
});

// ---------- Estado ----------
const rooms = new Map();           // code -> { code, name, pass, max, pub, state, players: Map(id -> player) }
const fails = new Map();           // ip -> { n, t } tentativas de senha erradas

const clean = (s, n) => String(s == null ? '' : s).replace(/[<>]/g, '').slice(0, n);
const nick = s => String(s || '').replace(/[^\w\- \u00C0-\u00FF]/g, '').trim().slice(0, 16) || 'Jogador';
const hex = h => (typeof h === 'string' && /^#[0-9a-fA-F]{6}$/.test(h));
const look = l => ({ shirt: hex(l && l.shirt) ? l.shirt : '#3a8a3a', pants: hex(l && l.pants) ? l.pants : '#6e6e74', helm: hex(l && l.helm) ? l.helm : '#6e6e74' });
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const newCode = () => { let c; do { c = Array.from({ length: 5 }, () => AL[crypto.randomInt(AL.length)]).join(''); } while (rooms.has(c)); return c; };
const info = r => ({ name: r.name, max: r.max, mode: 'coop', pub: r.pub ? 1 : 0, state: r.state, lock: r.pass ? 1 : 0 });
const roster = r => [...r.players.values()].map(p => ({ id: p.id, nick: p.nick, look: p.look, host: p.host ? 1 : 0 }));
const bcast = (r, o, except) => { for (const p of r.players.values()) if (p.ws !== except) send(p.ws, o); };

function blocked(ip) { const f = fails.get(ip); return f && Date.now() - f.t < 60000 && f.n >= 6; }
function fail(ip) { const f = fails.get(ip); if (!f || Date.now() - f.t >= 60000) fails.set(ip, { n: 1, t: Date.now() }); else f.n++; }

function leave(ws) {
  const r = ws.room; if (!r) return;
  const me = r.players.get(ws.pid);
  ws.room = null; ws.pid = null;
  if (!me) return;
  if (me.host) {
    for (const p of r.players.values()) if (p.ws !== ws) { send(p.ws, { t: 'closed', msg: 'O host encerrou a sala' }); p.ws.room = null; p.ws.pid = null; }
    rooms.delete(r.code);
  } else {
    r.players.delete(me.id);
    bcast(r, { t: 'roster', roster: roster(r), info: info(r) });
  }
}

// ---------- WebSocket ----------
const wss = new WebSocketServer({ server, maxPayload: 32 * 1024 });

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  ws.isAlive = true; ws.room = null; ws.pid = null; ws.cnt = 0; ws.cntT = Date.now();
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    // limite de taxa: ~300 msgs/s por conexão
    const now = Date.now();
    if (now - ws.cntT > 1000) { ws.cnt = 0; ws.cntT = now; }
    if (++ws.cnt > 300) return;

    let o; try { o = JSON.parse(raw); } catch (e) { return; }
    if (!o || typeof o.t !== 'string') return;

    // repasse de mensagens do jogo
    if (o.t === 'g') {
      const r = ws.room; if (!r || !o.m || typeof o.m !== 'object') return;
      o.m.f = ws.pid;                       // impede falsificar remetente
      const out = { t: 'g', m: o.m };
      if (o.m.to) { const p = r.players.get(String(o.m.to)); if (p) send(p.ws, out); }
      else bcast(r, out, ws);
      return;
    }

    switch (o.t) {
      case 'create': {
        if (ws.room) return;
        const code = newCode(), id = crypto.randomBytes(4).toString('hex');
        const r = { code, name: clean(o.name, 24) || 'Sala', pass: clean(o.pass, 32), max: Math.min(4, Math.max(2, o.max | 0 || 4)), pub: !!o.pub, state: 'wait', players: new Map() };
        r.players.set(id, { id, ws, nick: nick(o.nick), look: look(o.look), host: true });
        rooms.set(code, r); ws.room = r; ws.pid = id;
        send(ws, { t: 'created', id, code, info: info(r), roster: roster(r) });
        break;
      }
      case 'join': {
        if (ws.room) return;
        if (blocked(ip)) return send(ws, { t: 'err', c: 'muitas_tentativas' });
        const r = rooms.get(String(o.code || '').toUpperCase());
        if (!r) return send(ws, { t: 'err', c: 'nao_existe' });
        if (r.state !== 'wait') return send(ws, { t: 'err', c: 'em_partida' });
        if (r.players.size >= r.max) return send(ws, { t: 'err', c: 'cheia' });
        if (r.pass && clean(o.pass, 32) !== r.pass) { fail(ip); return send(ws, { t: 'err', c: 'senha' }); }
        const id = crypto.randomBytes(4).toString('hex');
        r.players.set(id, { id, ws, nick: nick(o.nick), look: look(o.look), host: false });
        ws.room = r; ws.pid = id;
        send(ws, { t: 'joined', id, code: r.code, info: info(r), roster: roster(r) });
        bcast(r, { t: 'roster', roster: roster(r), info: info(r) }, ws);
        break;
      }
      case 'list': {
        const list = [];
        for (const r of rooms.values()) if (r.pub && r.state === 'wait') list.push({ code: r.code, name: r.name, n: r.players.size, max: r.max, lock: r.pass ? 1 : 0 });
        send(ws, { t: 'list', rooms: list.slice(0, 20) });
        break;
      }
      case 'start': {
        const r = ws.room; if (!r) return;
        const me = r.players.get(ws.pid); if (!me || !me.host || r.state !== 'wait') return;
        r.state = 'play';
        bcast(r, { t: 'start', sd: crypto.randomInt(1e9), sw: Math.min(29, Math.max(0, o.sw | 0)), df: Math.min(2, Math.max(0, o.df | 0)) });
        break;
      }
      case 'leave': leave(ws); break;
    }
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => {});
});

// mantém conexões vivas (importante no Render) e derruba as mortas
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; try { ws.ping(); } catch (e) {}
  }
}, 25000);

server.listen(PORT, () => console.log('Núcleo Abissal rodando na porta ' + PORT));
