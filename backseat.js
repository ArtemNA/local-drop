// Short-code relay for the Backseat app: the app stores its ~700-char
// connection text here and gets an 8-char code the user can send to a friend;
// the friend's app fetches the text by that code. Texts contain IP addresses,
// so storage is minimal: in-memory only, short TTL, nothing logged but counts.
const express = require('express');
const crypto = require('crypto');

const CODE_ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'; // Crockford base32
const CODE_LENGTH = 8;
const CODE_RE = /^[0-9A-HJKMNP-TV-Z]{8}$/;
const TEXT_RE = /^BACKSEAT1\.[A-Za-z0-9_-]{16,4000}$/;
const RATE_WINDOW_MS = 60 * 1000;

function envInt(name, fallback) {
    const value = parseInt(process.env[name] || '', 10);
    return Number.isFinite(value) && value > 0 ? value : fallback;
}

function generateCode() {
    let code = '';
    for (let i = 0; i < CODE_LENGTH; i++) {
        code += CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
    }
    return code;
}

// "k7q2-9xmb" -> "K7Q29XMB". Also maps the Crockford look-alikes
// (I, L -> 1, O -> 0) so a hand-typed code still resolves.
function normalizeCode(raw) {
    return String(raw)
        .toUpperCase()
        .replace(/[\s-]/g, '')
        .replace(/[IL]/g, '1')
        .replace(/O/g, '0');
}

function createBackseatRouter({ getClientIp }) {
    const ttlMs = envInt('BACKSEAT_TTL_SEC', 300) * 1000;
    const maxEntries = envInt('BACKSEAT_MAX_ENTRIES', 10000);
    const postLimit = envInt('BACKSEAT_POST_PER_MIN', 20);
    const getLimit = envInt('BACKSEAT_GET_PER_MIN', 60);

    const entries = new Map();    // code -> { text, expiresAt }
    const rateCounters = new Map(); // `${kind}:${ip}` -> { count, windowStart }

    function sweep() {
        const now = Date.now();
        for (const [code, entry] of entries) {
            if (entry.expiresAt <= now) entries.delete(code);
        }
        for (const [key, counter] of rateCounters) {
            if (now - counter.windowStart >= RATE_WINDOW_MS) rateCounters.delete(key);
        }
    }
    setInterval(sweep, 30 * 1000).unref();

    // Fixed one-minute window per IP and method. Returns seconds to wait, or 0.
    function rateLimit(kind, ip, limit) {
        const now = Date.now();
        const key = kind + ':' + ip;
        let counter = rateCounters.get(key);
        if (!counter || now - counter.windowStart >= RATE_WINDOW_MS) {
            counter = { count: 0, windowStart: now };
            rateCounters.set(key, counter);
        }
        counter.count++;
        if (counter.count <= limit) return 0;
        return Math.max(1, Math.ceil((counter.windowStart + RATE_WINDOW_MS - now) / 1000));
    }

    function limited(kind, limit) {
        return (req, res, next) => {
            const retryAfter = rateLimit(kind, getClientIp(req), limit);
            if (!retryAfter) return next();
            console.log('[Backseat] rate limited');
            res.set('Retry-After', String(retryAfter));
            res.status(429).json({ error: 'rate_limited' });
        };
    }

    const router = express.Router();

    router.use((req, res, next) => {
        res.set('Cache-Control', 'no-store');
        next();
    });

    router.post('/code',
        limited('post', postLimit),
        (req, res, next) => {
            if (!req.is('application/json')) {
                return res.status(415).json({ error: 'unsupported_media_type' });
            }
            next();
        },
        express.json({ limit: '4kb' }),
        (req, res) => {
            const text = req.body && req.body.text;
            if (typeof text !== 'string' || !TEXT_RE.test(text)) {
                return res.status(400).json({ error: 'bad_text' });
            }

            if (entries.size >= maxEntries) sweep();
            if (entries.size >= maxEntries) {
                console.log('[Backseat] busy');
                return res.status(503).json({ error: 'busy' });
            }

            let code;
            do {
                code = generateCode();
            } while (entries.has(code));
            entries.set(code, { text, expiresAt: Date.now() + ttlMs });

            console.log(`[Backseat] code created (entries: ${entries.size})`);
            res.status(201).json({ code, expires_in: Math.round(ttlMs / 1000) });
        });

    router.get('/code/:code', limited('get', getLimit), (req, res) => {
        const code = normalizeCode(req.params.code);
        const entry = CODE_RE.test(code) ? entries.get(code) : undefined;
        const now = Date.now();
        if (!entry || entry.expiresAt <= now) {
            if (entry) entries.delete(code);
            console.log('[Backseat] not found');
            return res.status(404).json({ error: 'not_found' });
        }
        // Not deleted on read: the app may retry after a network error.
        console.log('[Backseat] code read');
        res.json({ text: entry.text, expires_in: Math.ceil((entry.expiresAt - now) / 1000) });
    });

    // Body-parser errors (oversized or malformed JSON) as JSON, without echoing input
    router.use((err, req, res, next) => {
        if (err && err.type === 'entity.too.large') {
            return res.status(413).json({ error: 'too_large' });
        }
        if (err && err.type === 'entity.parse.failed') {
            return res.status(400).json({ error: 'bad_json' });
        }
        if (err && err.status >= 400 && err.status < 500) {
            return res.status(err.status).json({ error: 'bad_request' });
        }
        next(err);
    });

    return router;
}

module.exports = { createBackseatRouter, normalizeCode, TEXT_RE };
