// Integration tests for server.js: each suite starts the real server as a child
// process and talks to it over WebSocket/HTTP. Run with `npm test`.
const { describe, it, before, after } = require('node:test');
const assert = require('node:assert');
const { spawn } = require('node:child_process');
const path = require('node:path');
const http = require('node:http');
const WebSocket = require('ws');

const SERVER = path.join(__dirname, '..', 'server.js');
let nextPort = 4100 + Math.floor(Math.random() * 500);

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function startServer(env = {}) {
    const port = nextPort++;
    const proc = spawn(process.execPath, [SERVER], {
        env: { ...process.env, PORT: String(port), PUBLIC_DEPLOY: '', AUTO_ROOM: '', ...env },
        stdio: ['ignore', 'pipe', 'pipe']
    });
    let output = '';
    proc.stdout.on('data', d => { output += d; });
    proc.stderr.on('data', d => { output += d; });

    const ready = new Promise((resolve, reject) => {
        const check = setInterval(() => {
            if (output.includes('Server Started')) { clearInterval(check); resolve(); }
        }, 20);
        proc.on('exit', code => { clearInterval(check); reject(new Error(`server exited (${code}):\n${output}`)); });
    });

    return {
        port,
        proc,
        ready,
        alive: () => proc.exitCode === null,
        stop: () => proc.kill()
    };
}

function connect(port, query = '') {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(`ws://127.0.0.1:${port}/${query}`);
        ws.messages = [];
        ws.on('message', m => ws.messages.push(JSON.parse(m)));
        ws.on('open', () => resolve(ws));
        ws.on('error', reject);
    });
}

function getInfo(port) {
    return new Promise((resolve, reject) => {
        http.get(`http://127.0.0.1:${port}/api/info`, res => {
            let body = '';
            res.on('data', c => { body += c; });
            res.on('end', () => resolve(JSON.parse(body)));
        }).on('error', reject);
    });
}

const ofType = (ws, type) => ws.messages.filter(m => m.type === type);
const errorCodes = ws => ofType(ws, 'error').map(m => m.code);
const base64 = n => Buffer.alloc(n, 7).toString('base64').slice(0, n);

function shareText(ws, fields) {
    ws.send(JSON.stringify({
        type: 'text-share', rev: 'r1', seq: 0, count: 1,
        enc: 'gzip', language: 'auto', data: 'QUJD', ...fields
    }));
}

describe('local mode', () => {
    let server;
    const sockets = [];
    const open = async (query) => { const ws = await connect(server.port, query); sockets.push(ws); return ws; };

    before(async () => { server = startServer(); await server.ready; });
    after(() => { sockets.forEach(ws => ws.terminate()); server.stop(); });

    it('survives a malformed device name', async () => {
        await open('?deviceName=%25');
        await sleep(100);
        assert.ok(server.alive());
        assert.ok((await getInfo(server.port)).totalClients >= 1);
    });

    it('strips control characters and caps device names at 30 chars', async () => {
        const raw = 'A\u0000B' + 'x'.repeat(100);
        const named = await open('?deviceName=' + encodeURIComponent(encodeURIComponent(raw)));
        const watcher = await open();
        await sleep(100);
        const peer = ofType(watcher, 'init')[0].peers.find(p => p.id === ofType(named, 'init')[0].clientId);
        assert.strictEqual(peer.name, ('AB' + 'x'.repeat(100)).slice(0, 30));
    });

    it('ignores malformed messages without crashing', async () => {
        const ws = await open();
        ws.send(JSON.stringify({ type: 'set-name', name: 123 }));
        ws.send(JSON.stringify({ type: 'offer', targetId: { x: 1 } }));
        ws.send('null');
        ws.send('{not json');
        await sleep(150);
        assert.ok(server.alive());
        assert.strictEqual(ws.readyState, WebSocket.OPEN);
    });

    it('relays valid shared text and rejects invalid chunks', async () => {
        const a = await open();
        const b = await open();
        await sleep(100);
        shareText(a, { rev: 'ok' });
        shareText(a, { rev: 'bad-enc', enc: 'evil' });
        shareText(a, { rev: 'bad-count', count: 100000, seq: 5 });
        shareText(a, { rev: 'bad-b64', data: '<script>' });
        await sleep(150);
        const relayed = ofType(b, 'text-update');
        assert.deepStrictEqual(relayed.map(m => m.rev), ['ok']);
    });

    it('stops relaying a revision once it exceeds 10MB', async () => {
        const a = await open();
        const b = await open();
        await sleep(100);
        for (let i = 0; i < 30; i++) {
            shareText(a, { rev: 'big', seq: i, count: 30, data: base64(400 * 1024) });
        }
        await sleep(600);
        assert.ok(ofType(b, 'text-update').filter(m => m.rev === 'big').length < 30);
        assert.ok(errorCodes(a).includes('TEXT_TOO_LARGE'));
    });

    it('rejects WebSocket messages over 1MB', async () => {
        const ws = await open();
        const closed = new Promise(resolve => ws.on('close', code => resolve(code)));
        ws.send('x'.repeat(2 * 1024 * 1024));
        assert.strictEqual(await closed, 1009); // message too big
        assert.ok(server.alive());
    });
});

describe('shared text rate limit and snapshots', () => {
    let server;
    before(async () => { server = startServer(); await server.ready; });
    after(() => server.stop());

    it('limits a client to ~30MB per minute and streams snapshots to late joiners', async () => {
        const a = await connect(server.port);
        const b = await connect(server.port);
        await sleep(100);
        for (let r = 0; r < 4; r++) {
            for (let i = 0; i < 20; i++) {
                shareText(a, { rev: 'r' + r, seq: i, count: 20, data: base64(480 * 1024) });
            }
            await sleep(250);
        }
        await sleep(300);
        assert.ok(ofType(b, 'text-update').length < 80);
        assert.ok(errorCodes(a).includes('TEXT_RATE_LIMIT'));
        assert.strictEqual(a.readyState, WebSocket.OPEN);

        const late = await connect(server.port);
        await sleep(1200);
        const snapshot = ofType(late, 'text-update');
        assert.ok(snapshot.length > 1);
        assert.strictEqual(new Set(snapshot.map(m => m.rev)).size, 1);
        assert.strictEqual(snapshot.length, snapshot[0].count);
        [a, b, late].forEach(ws => ws.terminate());
    });
});

describe('public mode rooms', () => {
    let server;
    before(async () => { server = startServer({ PUBLIC_DEPLOY: 'true' }); await server.ready; });
    after(() => server.stop());

    it('falls back to room selection for an invalid room code in the URL', async () => {
        const ws = await connect(server.port, '?room=' + encodeURIComponent('<script>'));
        await sleep(100);
        assert.strictEqual(ofType(ws, 'init')[0].needsRoom, true);
        ws.terminate();
    });

    it('rejects malformed join-room codes', async () => {
        const ws = await connect(server.port);
        ws.send(JSON.stringify({ type: 'join-room', roomCode: 'x'.repeat(5000) }));
        ws.send(JSON.stringify({ type: 'join-room', roomCode: 5 }));
        await sleep(100);
        assert.deepStrictEqual(errorCodes(ws), ['INVALID_ROOM_CODE', 'INVALID_ROOM_CODE']);
        ws.terminate();
    });

    it('joins by code and by link, case-insensitively, and survives re-joining', async () => {
        const a = await connect(server.port);
        a.send(JSON.stringify({ type: 'join-room', roomCode: 'abc123' }));
        await sleep(100);
        const b = await connect(server.port, '?room=ABC123');
        await sleep(100);
        assert.strictEqual(ofType(a, 'room-joined')[0].roomCode, 'ABC123');
        assert.strictEqual(ofType(b, 'room-joined')[0].peers.length, 1);
        assert.ok(ofType(b, 'room-joined')[0].clientId);

        a.send(JSON.stringify({ type: 'join-room', roomCode: 'ABC123' }));
        await sleep(100);
        assert.strictEqual(ofType(a, 'room-joined').at(-1).peers.length, 1);
        [a, b].forEach(ws => ws.terminate());
    });

    it('only forwards signaling between members of the same room', async () => {
        const a = await connect(server.port, '?room=ROOMA1');
        const b = await connect(server.port, '?room=ROOMB1');
        await sleep(100);
        const bId = ofType(b, 'room-joined')[0].clientId;
        a.send(JSON.stringify({ type: 'offer', targetId: bId, offer: {} }));
        await sleep(100);
        assert.strictEqual(ofType(b, 'offer').length, 0);
        assert.ok(errorCodes(a).includes('PEER_NOT_ACCESSIBLE'));
        [a, b].forEach(ws => ws.terminate());
    });

    // Last: uses up this IP's room quota for the rest of the suite
    it('limits room creation per IP', async () => {
        const ws = await connect(server.port);
        for (let i = 0; i < 25; i++) ws.send(JSON.stringify({ type: 'create-room' }));
        await sleep(300);
        assert.ok(ofType(ws, 'room-created').length <= 20);
        assert.ok(errorCodes(ws).includes('ROOM_LIMIT'));
        ws.terminate();
    });
});

describe('auto rooms', () => {
    let server;
    before(async () => { server = startServer({ PUBLIC_DEPLOY: 'true', AUTO_ROOM: 'true' }); await server.ready; });
    after(() => server.stop());

    it('groups by the IP the server sees, ignoring a claimed local IP', async () => {
        const a = await connect(server.port);
        const b = await connect(server.port, '?localIP=10.9.9.9');
        await sleep(100);
        const roomA = ofType(a, 'room-joined')[0];
        const roomB = ofType(b, 'room-joined')[0];
        assert.match(roomA.roomCode, /^[A-Z0-9]{6}$/);
        assert.strictEqual(roomA.roomCode, roomB.roomCode);
        assert.strictEqual(roomB.peers.length, 1);
        [a, b].forEach(ws => ws.terminate());
    });
});

describe('static files', () => {
    let server;
    before(async () => { server = startServer(); await server.ready; });
    after(() => server.stop());

    it('serves the changelog and version info', async () => {
        const status = await new Promise((resolve, reject) => {
            http.get(`http://127.0.0.1:${server.port}/changelog.html`, res => { res.resume(); resolve(res.statusCode); })
                .on('error', reject);
        });
        assert.strictEqual(status, 200);
    });
});
