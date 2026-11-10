// Integration tests for the Backseat short-code endpoints (/api/backseat).
// Each suite starts the real server as a child process. Run with `npm test`.
const { describe, it, before, after } = require('node:test');
const assert = require('node:assert');
const { spawn } = require('node:child_process');
const path = require('node:path');
const http = require('node:http');

const SERVER = path.join(__dirname, '..', 'server.js');
let nextPort = 4700 + Math.floor(Math.random() * 500);
let nextIp = 1;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
// A distinct fake client IP per test, so rate-limit counters don't interfere.
const freshIp = () => `203.0.113.${nextIp++}`;
const TEXT = 'BACKSEAT1.' + 'Ab0_-'.repeat(140); // 710 chars, like the real app

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
    return { port, ready, output: () => output, stop: () => proc.kill() };
}

function request(port, method, urlPath, { ip, body, contentType = 'application/json' } = {}) {
    return new Promise((resolve, reject) => {
        const headers = { 'X-Forwarded-For': ip || freshIp() };
        const payload = body === undefined ? null : (typeof body === 'string' ? body : JSON.stringify(body));
        if (payload !== null) {
            headers['Content-Type'] = contentType;
            headers['Content-Length'] = Buffer.byteLength(payload);
        }
        const req = http.request({ host: '127.0.0.1', port, method, path: urlPath, headers }, res => {
            let raw = '';
            res.on('data', c => { raw += c; });
            res.on('end', () => {
                let json = null;
                try { json = JSON.parse(raw); } catch (_) { /* not JSON */ }
                resolve({ status: res.statusCode, headers: res.headers, json, raw });
            });
        });
        req.on('error', reject);
        if (payload !== null) req.write(payload);
        req.end();
    });
}

const create = (port, text = TEXT, opts = {}) =>
    request(port, 'POST', '/api/backseat/code', { body: { text }, ...opts });
const read = (port, code, opts = {}) =>
    request(port, 'GET', '/api/backseat/code/' + encodeURIComponent(code), opts);

describe('backseat short codes', () => {
    let server;
    before(async () => { server = startServer(); await server.ready; });
    after(() => server.stop());

    it('creates a code and reads it back, also lowercase and with a dash', async () => {
        const created = await create(server.port);
        assert.strictEqual(created.status, 201);
        assert.match(created.json.code, /^[0-9A-HJKMNP-TV-Z]{8}$/);
        assert.strictEqual(created.json.expires_in, 300);
        assert.strictEqual(created.headers['cache-control'], 'no-store');

        const code = created.json.code;
        const variants = [code, code.toLowerCase(), code.slice(0, 4) + '-' + code.slice(4),
            code.slice(0, 4).toLowerCase() + ' ' + code.slice(4)];
        for (const variant of variants) {
            const got = await read(server.port, variant);
            assert.strictEqual(got.status, 200, variant);
            assert.strictEqual(got.json.text, TEXT);
            assert.ok(got.json.expires_in > 290 && got.json.expires_in <= 300);
            assert.strictEqual(got.headers['cache-control'], 'no-store');
        }
    });

    it('returns 404 for unknown and malformed codes', async () => {
        for (const code of ['00000000', 'ZZZZ-ZZZZ', 'short', 'x'.repeat(50)]) {
            const got = await read(server.port, code);
            assert.strictEqual(got.status, 404, code);
            assert.deepStrictEqual(got.json, { error: 'not_found' });
        }
    });

    it('rejects anything that is not a BACKSEAT1 text with 400 bad_text', async () => {
        const bad = [
            'hello', 'BACKSEAT1.', 'BACKSEAT1.short', 'BACKSEAT2.' + 'a'.repeat(100),
            'BACKSEAT1.' + 'a'.repeat(50) + '+/=', 'BACKSEAT1.' + 'a'.repeat(50) + '\n',
            'x' + TEXT, 'BACKSEAT1.' + 'a'.repeat(4001)
        ];
        for (const text of bad) {
            const res = await create(server.port, text);
            assert.strictEqual(res.status, 400, JSON.stringify(text).slice(0, 40));
            assert.deepStrictEqual(res.json, { error: 'bad_text' });
        }
        for (const body of [{}, { text: 123 }, [TEXT], { text: [TEXT] }]) {
            const res = await request(server.port, 'POST', '/api/backseat/code', { body });
            assert.strictEqual(res.status, 400, JSON.stringify(body));
            assert.deepStrictEqual(res.json, { error: 'bad_text' });
        }
    });

    it('rejects malformed JSON, wrong content type and oversized bodies', async () => {
        const broken = await request(server.port, 'POST', '/api/backseat/code', { body: '{"text":' });
        assert.strictEqual(broken.status, 400);
        assert.deepStrictEqual(broken.json, { error: 'bad_json' });

        const plain = await request(server.port, 'POST', '/api/backseat/code',
            { body: JSON.stringify({ text: TEXT }), contentType: 'text/plain' });
        assert.strictEqual(plain.status, 415);
        assert.deepStrictEqual(plain.json, { error: 'unsupported_media_type' });

        const big = await create(server.port, 'BACKSEAT1.' + 'a'.repeat(5000));
        assert.strictEqual(big.status, 413);
        assert.deepStrictEqual(big.json, { error: 'too_large' });
    });

    it('limits POSTs to 20 per minute per IP with 429 and Retry-After', async () => {
        const ip = freshIp();
        for (let i = 0; i < 20; i++) {
            assert.strictEqual((await create(server.port, TEXT, { ip })).status, 201, `post ${i}`);
        }
        const over = await create(server.port, TEXT, { ip });
        assert.strictEqual(over.status, 429);
        assert.deepStrictEqual(over.json, { error: 'rate_limited' });
        const retryAfter = Number(over.headers['retry-after']);
        assert.ok(retryAfter >= 1 && retryAfter <= 60, String(retryAfter));
        // Another IP is unaffected
        assert.strictEqual((await create(server.port)).status, 201);
    });

    it('limits GETs to 60 per minute per IP', async () => {
        const { json } = await create(server.port);
        const ip = freshIp();
        for (let i = 0; i < 60; i++) {
            assert.strictEqual((await read(server.port, json.code, { ip })).status, 200, `get ${i}`);
        }
        const over = await read(server.port, json.code, { ip });
        assert.strictEqual(over.status, 429);
        assert.deepStrictEqual(over.json, { error: 'rate_limited' });
        assert.ok(Number(over.headers['retry-after']) >= 1);
    });

    it('logs only counts, never the text, the code or the IP', async () => {
        const ip = freshIp();
        const { json } = await create(server.port, TEXT, { ip });
        await read(server.port, json.code, { ip });
        await sleep(50);
        const out = server.output();
        assert.ok(out.includes('[Backseat] code created'));
        assert.ok(out.includes('[Backseat] code read'));
        assert.ok(!out.includes(TEXT.slice(10, 40)));
        assert.ok(!out.includes(json.code));
        assert.ok(!out.includes(ip));
    });

    it('leaves the existing endpoints working', async () => {
        assert.strictEqual((await request(server.port, 'GET', '/live')).status, 200);
        assert.strictEqual((await request(server.port, 'GET', '/api/version')).status, 200);
    });
});

describe('backseat expiry and capacity', () => {
    let server;
    before(async () => {
        server = startServer({ BACKSEAT_TTL_SEC: '1', BACKSEAT_MAX_ENTRIES: '3' });
        await server.ready;
    });
    after(() => server.stop());

    it('expires a code after the TTL', async () => {
        const { status, json } = await create(server.port);
        assert.strictEqual(status, 201);
        assert.strictEqual(json.expires_in, 1);
        assert.strictEqual((await read(server.port, json.code)).status, 200);
        await sleep(1200);
        const gone = await read(server.port, json.code);
        assert.strictEqual(gone.status, 404);
        assert.deepStrictEqual(gone.json, { error: 'not_found' });
    });

    it('answers 503 busy when the store is full, and recovers after expiry', async () => {
        await sleep(1100); // let earlier entries expire
        for (let i = 0; i < 3; i++) assert.strictEqual((await create(server.port)).status, 201);
        const full = await create(server.port);
        assert.strictEqual(full.status, 503);
        assert.deepStrictEqual(full.json, { error: 'busy' });
        await sleep(1100);
        assert.strictEqual((await create(server.port)).status, 201);
    });
});
