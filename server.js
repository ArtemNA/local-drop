const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const https = require('https');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const os = require('os');
const crypto = require('crypto');
const cors = require('cors');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ── Maintenance mode ─────────────────────────────
// Set to true to show maintenance page for all visitors.
// WebSocket connections and /api/version still work so you can verify the deploy.
const MAINTENANCE_MODE = false;

app.use((req, res, next) => {
    if (MAINTENANCE_MODE
        && req.path !== '/maintenance.html'
        && !req.path.startsWith('/api/')
        && req.path !== '/live') {
        return res.sendFile(path.join(__dirname, 'public', 'maintenance.html'));
    }
    next();
});
// ─────────────────────────────────────────────────

// Serve static files
app.use(express.static('public'));

app.use(express.json());
app.use(cors());

// Store rooms and clients
const rooms = new Map(); // roomCode -> Set of clientIds
const clients = new Map(); // clientId -> clientInfo
const ipRooms = new Map(); // IP address -> roomCode (for AUTO_ROOM mode)

// Get local IP address
function getLocalIP() {
    const interfaces = os.networkInterfaces();
    for (const name of Object.keys(interfaces)) {
        for (const iface of interfaces[name]) {
            if (iface.family === 'IPv4' && !iface.internal) {
                return iface.address;
            }
        }
    }
    return 'localhost';
}

// Generate room code
function generateRoomCode() {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let code = '';
    for (let i = 0; i < 6; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
}

// Generate room code from IP (for auto-room mode)
function generateRoomFromIP(ip) {
    // Clean IP address (remove IPv6 prefix if present)
    const cleanIP = ip.replace(/^::ffff:/, '');

    // Create a hash of the IP for privacy
    const hash = crypto.createHash('sha256').update(cleanIP).digest('hex');

    // Take first 6 characters and convert to uppercase alphanumeric
    let roomCode = '';
    for (let i = 0; i < 6; i++) {
        const charCode = hash.charCodeAt(i);
        if (charCode < 58) { // 0-9
            roomCode += String.fromCharCode(charCode);
        } else {
            // Convert to A-Z
            roomCode += String.fromCharCode(65 + (charCode % 26));
        }
    }

    return roomCode;
}

// Get clients in room
function getClientsInRoom(roomCode) {
    const roomClientIds = rooms.get(roomCode);
    if (!roomClientIds) return [];

    return Array.from(roomClientIds)
        .map(id => clients.get(id))
        .filter(client => client !== undefined);
}

// Check if auto-room mode is enabled
const AUTO_ROOM_MODE = process.env.AUTO_ROOM === 'true';
const PUBLIC_DEPLOY = process.env.PUBLIC_DEPLOY === 'true';

// Message size limit for signaling (100KB should be enough for SDP/ICE)
const MAX_SIGNAL_SIZE = 100 * 1024; // 100KB

// Shared-text ("shared clipboard") sync. Text is gzip-compressed and base64'd on
// the client, then split into chunks so arbitrarily large payloads can travel over
// the signaling channel. This is exempt from the file-transfer heuristics below —
// it's legitimate collaborative text, not a covert binary tunnel — but still capped.
const MAX_TEXT_CHUNK = 512 * 1024;        // max size of a single chunk WS frame
const MAX_TEXT_TOTAL = 10 * 1024 * 1024;  // max assembled (compressed+base64) snapshot ≈ tens of MB of raw text
const SNAP_CHUNK = 128 * 1024;            // chunk size when streaming a snapshot to a late joiner
// Clients send 128KB chunks, so a full-size payload never needs more than this
// many — bounds the reassembly array before anything is allocated.
const MAX_TEXT_COUNT = Math.ceil(MAX_TEXT_TOTAL / (64 * 1024));
const TEXT_ENCODINGS = new Set(['gzip', 'raw']);
const BASE64_RE = /^[A-Za-z0-9+/]*={0,2}$/;
const TEXT_RATE_BYTES = 30 * 1024 * 1024;         // per client per minute (compressed+base64)
const TEXT_MEMORY_BUDGET = 200 * 1024 * 1024;     // all snapshots + reassembly buffers together
const SNAPSHOT_TTL_MS = 12 * 60 * 60 * 1000;      // drop snapshots nobody has updated for this long
const SNAP_BUFFER_HIGH = 1024 * 1024;             // pause snapshot streaming above this ws.bufferedAmount

const sharedTexts = new Map();  // channelKey -> { enc, language, data (base64), updatedAt }
const pendingTexts = new Map(); // channelKey -> in-progress reassembly { rev, count, parts, received, bytes, enc, language }
let snapRevCounter = 0;

// Track message sizes per client to detect file transfer attempts
const clientMessageStats = new Map();

// The "channel" a client's shared text belongs to: their room in public mode,
// or a single global channel in local mode (everyone on the LAN shares one).
function getChannelKey(clientInfo) {
    if (clientInfo.roomCode) return 'ROOM:' + clientInfo.roomCode;
    if (!PUBLIC_DEPLOY) return 'LOCAL';
    return null; // public mode but not in a room yet — nothing to sync
}

// Send a payload to every other client on the same channel as clientInfo.
function broadcastChannel(clientInfo, payload) {
    if (clientInfo.roomCode) {
        getClientsInRoom(clientInfo.roomCode).forEach(c => {
            if (c.id !== clientInfo.id && c.ws.readyState === 1) c.ws.send(payload);
        });
    } else if (!PUBLIC_DEPLOY) {
        clients.forEach((c, id) => {
            if (id !== clientInfo.id && c.ws.readyState === 1) c.ws.send(payload);
        });
    }
}

// Handle one incoming shared-text chunk: relay it live to peers, and assemble the
// full snapshot server-side so devices that join later can catch up.
function handleTextShare(clientInfo, message) {
    const key = getChannelKey(clientInfo);
    if (!key) return;

    const { rev, seq, count, enc, language, data } = message;
    if (typeof rev !== 'string' || rev.length > 100 ||
        typeof data !== 'string' || !TEXT_ENCODINGS.has(enc)) return;
    if (!Number.isInteger(count) || count < 1 || count > MAX_TEXT_COUNT) return;
    if (!Number.isInteger(seq) || seq < 0 || seq >= count) return;
    if (!BASE64_RE.test(data)) return;

    const lang = typeof language === 'string' ? language.slice(0, 30) : 'auto';

    // Per-client rate limit. Over the limit, chunks are dropped (not relayed)
    // until the window resets; the sender is told once per window.
    const now = Date.now();
    if (now - clientInfo.textWindowStart > 60000) {
        clientInfo.textWindowStart = now;
        clientInfo.textBytes = 0;
        clientInfo.textLimitNotified = false;
    }
    clientInfo.textBytes += data.length;
    if (clientInfo.textBytes > TEXT_RATE_BYTES) {
        if (!clientInfo.textLimitNotified) {
            clientInfo.textLimitNotified = true;
            sendTextError(clientInfo, 'TEXT_RATE_LIMIT',
                'Shared text is changing too fast to sync through the server. Wait a minute or turn on "LAN only".');
        }
        return;
    }

    // 1) Account for the chunk first, so an oversized payload is neither stored
    //    nor relayed.
    let pend = pendingTexts.get(key);
    if (!pend || pend.rev !== rev) {
        pend = {
            rev,
            count,
            parts: new Array(count),
            received: 0,
            bytes: 0,
            enc,
            language: lang,
            rejected: false,
            updatedAt: Date.now()
        };
        pendingTexts.set(key, pend);
    }
    if (pend.rejected || pend.count !== count) return;
    pend.updatedAt = Date.now();
    if (pend.parts[seq] !== undefined) return; // duplicate chunk

    if (pend.bytes + data.length > MAX_TEXT_TOTAL) {
        // Too big — drop what we have and ignore the rest of this revision.
        rejectTextRevision(pend);
        sendTextError(clientInfo, 'TEXT_TOO_LARGE',
            'Shared text is too large to sync through the server. Turn on "LAN only" to share it directly.');
        return;
    }
    if (!makeRoomForText(data.length, key)) {
        rejectTextRevision(pend);
        sendTextError(clientInfo, 'TEXT_SERVER_BUSY',
            'The server is low on space for shared text right now. Try again later or turn on "LAN only".');
        return;
    }
    pend.parts[seq] = data;
    pend.received++;
    pend.bytes += data.length;

    // 2) Relay this chunk live to everyone else on the channel (pass-through).
    broadcastChannel(clientInfo, JSON.stringify({
        type: 'text-update',
        rev, seq, count, enc, language: lang, data,
        fromId: clientInfo.id,
        fromName: clientInfo.name
    }));

    // 3) Once complete, keep the snapshot for late joiners.
    if (pend.received >= pend.count) {
        sharedTexts.set(key, {
            enc: pend.enc,
            language: pend.language,
            data: pend.parts.join(''),
            updatedAt: Date.now()
        });
        pendingTexts.delete(key);
    }
}

function rejectTextRevision(pend) {
    pend.rejected = true;
    pend.parts = [];
    pend.bytes = 0;
}

function sendTextError(clientInfo, code, message) {
    if (clientInfo.ws.readyState === 1) {
        clientInfo.ws.send(JSON.stringify({ type: 'error', message, code }));
    }
}

// Bytes currently held for shared text (base64 strings are one byte per char).
function textMemoryUsed() {
    let used = 0;
    for (const snap of sharedTexts.values()) used += snap.data.length;
    for (const pend of pendingTexts.values()) used += pend.bytes;
    return used;
}

// Make sure `bytes` more fit in the global budget, evicting the least recently
// updated snapshots of other channels if needed. Returns false if it can't.
function makeRoomForText(bytes, keepKey) {
    let used = textMemoryUsed();
    if (used + bytes <= TEXT_MEMORY_BUDGET) return true;

    const victims = [...sharedTexts.entries()]
        .filter(([key]) => key !== keepKey)
        .sort((a, b) => a[1].updatedAt - b[1].updatedAt);
    for (const [key, snap] of victims) {
        sharedTexts.delete(key);
        used -= snap.data.length;
        console.warn(`[Text] Evicted snapshot of ${key} to stay within memory budget`);
        if (used + bytes <= TEXT_MEMORY_BUDGET) return true;
    }
    return false;
}

// Stream the current channel snapshot to a single (usually just-joined) client,
// chunked the same way live edits are, so the client reuses one reassembly path.
// Waits for the socket to drain between chunks instead of queueing it all at once.
async function streamSnapshotTo(clientInfo, key) {
    const snap = sharedTexts.get(key);
    if (!snap || !snap.data) return;

    const data = snap.data;
    const count = Math.max(1, Math.ceil(data.length / SNAP_CHUNK));
    const rev = 'snap-' + (snapRevCounter++);
    const ws = clientInfo.ws;

    for (let seq = 0; seq < count; seq++) {
        while (ws.readyState === 1 && ws.bufferedAmount > SNAP_BUFFER_HIGH) {
            await new Promise(resolve => setTimeout(resolve, 50));
        }
        // Stop if the client left or a newer snapshot replaced this one — the
        // client already gets the newer text as a live update.
        if (ws.readyState !== 1 || sharedTexts.get(key) !== snap) return;
        ws.send(JSON.stringify({
            type: 'text-update',
            rev, seq, count,
            enc: snap.enc,
            language: snap.language,
            data: data.slice(seq * SNAP_CHUNK, (seq + 1) * SNAP_CHUNK)
            // no fromName — a snapshot shouldn't show as "Updated by <peer>"
        }));
    }
}

wss.on('connection', (ws, req) => {
    const clientId = uuidv4();

    // Parse URL parameters
    const url = new URL(req.url, `http://${req.headers.host}`);
    const urlRoomCode = url.searchParams.get('room');
    const clientLocalIP = url.searchParams.get('localIP');
    const deviceName = url.searchParams.get('deviceName');

    // Get real client IP (considering proxies)
    let clientIp = req.socket.remoteAddress;
    const forwardedFor = req.headers['x-forwarded-for'];
    if (forwardedFor) {
        clientIp = forwardedFor.split(',')[0].trim();
    }
    clientIp = clientIp.replace(/^::ffff:/, ''); // Clean IPv6 prefix

    // Initialize client info
    const clientInfo = {
        id: clientId,
        ws: ws,
        name: deviceName ? decodeURIComponent(deviceName) : `Device-${clientId.slice(0, 6)}`,
        ip: clientIp,
        localIP: clientLocalIP, // Store client's local IP for auto-room
        roomCode: null,
        bytesReceived: 0,
        messageCount: 0,
        lastResetTime: Date.now(),
        textBytes: 0,
        textWindowStart: Date.now(),
        textLimitNotified: false
    };

    clients.set(clientId, clientInfo);
    clientMessageStats.set(clientId, {
        totalBytes: 0,
        largeMessages: 0,
        suspiciousActivity: false
    });

    console.log(`[Connect] Client ${clientId} connected from ${clientIp} (local: ${clientLocalIP})`);

    // Handle different modes
    if (!PUBLIC_DEPLOY) {
        // LOCAL MODE - no rooms, all devices see each other
        ws.send(JSON.stringify({
            type: 'init',
            clientId: clientId,
            deviceName: clientInfo.name,
            localMode: true,
            peers: Array.from(clients.entries())
                .filter(([id]) => id !== clientId)
                .map(([id, info]) => ({
                    id: id,
                    name: info.name
                }))
        }));

        // Catch the new device up on the current shared text (if any)
        streamSnapshotTo(clientInfo, 'LOCAL');

        // Notify other clients about new peer
        broadcast(clientId, {
            type: 'peer-joined',
            peer: {
                id: clientId,
                name: clientInfo.name
            }
        });
    } else if (urlRoomCode) {
        // PUBLIC MODE - Join specific room from URL
        const normalizedRoomCode = urlRoomCode.toUpperCase();
        console.log(`[Room] Client attempting to join room ${normalizedRoomCode} from URL`);

        // Try to join the room from URL
        if (normalizedRoomCode.length === 6) {
            // Create room if it doesn't exist
            if (!rooms.has(normalizedRoomCode)) {
                rooms.set(normalizedRoomCode, new Set());
                console.log(`[Room] Created room ${normalizedRoomCode} from URL`);
            }

            // Join the room
            rooms.get(normalizedRoomCode).add(clientId);
            clientInfo.roomCode = normalizedRoomCode;

            // Get other peers in room
            const peersInRoom = getClientsInRoom(normalizedRoomCode)
                .filter(client => client.id !== clientId)
                .map(client => ({
                    id: client.id,
                    name: client.name
                }));

            // Send room joined message
            ws.send(JSON.stringify({
                type: 'room-joined',
                clientId: clientId,
                roomCode: normalizedRoomCode,
                deviceName: clientInfo.name,
                peers: peersInRoom
            }));

            streamSnapshotTo(clientInfo, 'ROOM:' + normalizedRoomCode);

            // Notify other room members
            broadcastToRoom(normalizedRoomCode, clientId, {
                type: 'peer-joined',
                peer: {
                    id: clientId,
                    name: clientInfo.name
                }
            });
        } else {
            // Invalid room code, show room selection
            ws.send(JSON.stringify({
                type: 'init',
                clientId: clientId,
                deviceName: clientInfo.name,
                needsRoom: true
            }));
        }
    } else if (AUTO_ROOM_MODE && clientLocalIP) {
        // AUTO ROOM MODE - Use client's local IP to group devices
        console.log(`[Auto-Room] Processing client with local IP: ${clientLocalIP}`);

        // Generate room code from local IP
        const roomCode = generateRoomFromIP(clientLocalIP);

        // Create room if it doesn't exist
        if (!rooms.has(roomCode)) {
            rooms.set(roomCode, new Set());
            console.log(`[Auto-Room] Created room ${roomCode} for local IP ${clientLocalIP}`);
        }

        // Join the room
        rooms.get(roomCode).add(clientId);
        clientInfo.roomCode = roomCode;

        // Get other peers in room
        const peersInRoom = getClientsInRoom(roomCode)
            .filter(client => client.id !== clientId)
            .map(client => ({
                id: client.id,
                name: client.name
            }));

        console.log(`[Auto-Room] Client ${clientId} auto-joined room ${roomCode}`);

        // Send init with room info and peers
        ws.send(JSON.stringify({
            type: 'room-joined',
            clientId: clientId,
            roomCode: roomCode,
            deviceName: clientInfo.name,
            autoRoom: true,
            peers: peersInRoom
        }));

        streamSnapshotTo(clientInfo, 'ROOM:' + roomCode);

        // Notify other room members
        broadcastToRoom(roomCode, clientId, {
            type: 'peer-joined',
            peer: {
                id: clientId,
                name: clientInfo.name
            }
        });
    } else {
        // PUBLIC MODE - Manual room selection
        ws.send(JSON.stringify({
            type: 'init',
            clientId: clientId,
            deviceName: clientInfo.name,
            needsRoom: true
        }));
    }

    ws.on('message', (data) => {
        try {
            const messageSize = data.length;

            // Shared-text sync is handled first and bypasses the file-transfer
            // heuristics below (it is legitimate small text, not a chunked file).
            let earlyParsed = null;
            try {
                earlyParsed = JSON.parse(data);
            } catch (_) { /* handled below */
            }
            if (earlyParsed && earlyParsed.type === 'text-share') {
                if (messageSize > MAX_TEXT_CHUNK + 4096) {
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: 'Shared-text chunk is too large.',
                        code: 'TEXT_CHUNK_TOO_LARGE'
                    }));
                    return;
                }
                handleTextShare(clientInfo, earlyParsed);
                return;
            }

            const stats = clientMessageStats.get(clientId);

            // Update statistics
            stats.totalBytes += messageSize;
            clientInfo.bytesReceived += messageSize;
            clientInfo.messageCount++;

            // Reset stats every minute
            const now = Date.now();
            if (now - clientInfo.lastResetTime > 60000) {
                clientInfo.bytesReceived = 0;
                clientInfo.messageCount = 0;
                clientInfo.lastResetTime = now;
                stats.totalBytes = 0;
                stats.largeMessages = 0;
            }

            // Check for suspicious activity
            if (messageSize > MAX_SIGNAL_SIZE) {
                stats.largeMessages++;
                console.warn(`[Warning] Large message from ${clientId}: ${messageSize} bytes`);

                // If multiple large messages, it's likely file transfer attempt
                if (stats.largeMessages > 2) {
                    stats.suspiciousActivity = true;
                    console.error(`[Security] File transfer attempt detected from ${clientId}`);

                    // Send error to client and disconnect
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: 'File transfer through server is not allowed. Use P2P connection only.',
                        code: 'SERVER_TRANSFER_BLOCKED'
                    }));

                    // Close connection after sending error
                    setTimeout(() => {
                        ws.close(1008, 'Server transfer not allowed');
                        // Cleanup
                        if (clientInfo.roomCode) {
                            leaveRoom(clientId, clientInfo.roomCode);
                        }
                        clients.delete(clientId);
                        clientMessageStats.delete(clientId);
                    }, 100);

                    return;
                }
            }

            // Check for rapid small messages (potential chunked file transfer)
            if (clientInfo.messageCount > 100 && clientInfo.bytesReceived > 500 * 1024) {
                console.error(`[Security] Potential chunked file transfer from ${clientId}`);

                ws.send(JSON.stringify({
                    type: 'error',
                    message: 'Excessive data transfer detected. Use P2P for file transfers.',
                    code: 'EXCESSIVE_DATA_TRANSFER'
                }));

                setTimeout(() => {
                    ws.close(1008, 'Excessive data transfer');
                    if (clientInfo.roomCode) {
                        leaveRoom(clientId, clientInfo.roomCode);
                    }
                    clients.delete(clientId);
                    clientMessageStats.delete(clientId);
                }, 100);

                return;
            }

            const message = JSON.parse(data);

            // Validate message types (only signaling and room management allowed)
            const allowedTypes = ['offer', 'answer', 'ice-candidate', 'set-name', 'ping', 'join-room', 'create-room'];
            if (!allowedTypes.includes(message.type)) {
                console.warn(`[Warning] Unknown message type from ${clientId}: ${message.type}`);
                return;
            }

            switch (message.type) {
                case 'create-room':
                    if (PUBLIC_DEPLOY) {
                        handleCreateRoom(clientInfo);
                    }
                    break;

                case 'join-room':
                    if (PUBLIC_DEPLOY) {
                        // Normalize room code to uppercase
                        const normalizedCode = message.roomCode.toUpperCase();
                        handleJoinRoom(clientInfo, normalizedCode);
                    }
                    break;

                case 'set-name':
                    clientInfo.name = message.name.slice(0, 30); // Limit name length

                    if (PUBLIC_DEPLOY && clientInfo.roomCode) {
                        // Broadcast to room members only (public mode)
                        broadcastToRoom(clientInfo.roomCode, clientId, {
                            type: 'peer-updated',
                            peer: {
                                id: clientId,
                                name: clientInfo.name
                            }
                        });
                    } else if (!PUBLIC_DEPLOY) {
                        // Broadcast to all (local mode)
                        broadcast(clientId, {
                            type: 'peer-updated',
                            peer: {
                                id: clientId,
                                name: clientInfo.name
                            }
                        });
                    }
                    break;

                case 'offer':
                case 'answer':
                case 'ice-candidate':
                    // Forward signaling messages to target peer
                    if (message.targetId && clients.has(message.targetId)) {
                        const targetClient = clients.get(message.targetId);

                        // Check room compatibility based on mode
                        let canCommunicate = false;

                        if (!PUBLIC_DEPLOY) {
                            // Local mode - all clients can communicate
                            canCommunicate = true;
                        } else if (targetClient.roomCode === clientInfo.roomCode && clientInfo.roomCode !== null) {
                            // Public mode - must be in same room
                            canCommunicate = true;
                        }

                        if (canCommunicate) {
                            targetClient.ws.send(JSON.stringify({
                                ...message,
                                fromId: clientId,
                                fromName: clientInfo.name
                            }));
                        } else {
                            ws.send(JSON.stringify({
                                type: 'error',
                                message: 'Target peer not accessible',
                                code: 'PEER_NOT_ACCESSIBLE'
                            }));
                        }
                    }
                    break;

                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong' }));
                    break;
            }
        } catch (error) {
            console.error(`[Error] Message processing error from ${clientId}:`, error.message);
        }
    });

    ws.on('close', () => {
        console.log(`[Disconnect] Client ${clientId} disconnected`);

        // Leave room if in one
        if (clientInfo.roomCode) {
            leaveRoom(clientId, clientInfo.roomCode);
        } else if (!PUBLIC_DEPLOY) {
            // Notify other clients in local mode
            broadcast(clientId, {
                type: 'peer-left',
                peerId: clientId
            });
        }

        clients.delete(clientId);
        clientMessageStats.delete(clientId);

        // In local mode everyone shares the 'LOCAL' channel — once the last device
        // leaves, drop its shared text so nothing lingers in memory.
        if (!PUBLIC_DEPLOY && clients.size === 0) {
            sharedTexts.delete('LOCAL');
            pendingTexts.delete('LOCAL');
        }
    });

    ws.on('error', (error) => {
        console.error(`[Error] WebSocket error for ${clientId}:`, error.message);
    });
});

// Handle room creation
function handleCreateRoom(clientInfo) {
    // Leave current room if in one
    if (clientInfo.roomCode) {
        leaveRoom(clientInfo.id, clientInfo.roomCode);
    }

    // Generate unique room code
    let roomCode;
    do {
        roomCode = generateRoomCode();
    } while (rooms.has(roomCode));

    // Create room and add client
    rooms.set(roomCode, new Set([clientInfo.id]));
    clientInfo.roomCode = roomCode;

    console.log(`[Room] Client ${clientInfo.id} created room ${roomCode}`);

    // Send room info to client
    clientInfo.ws.send(JSON.stringify({
        type: 'room-created',
        clientId: clientInfo.id,
        roomCode: roomCode,
        peers: [] // Empty room initially
    }));
}

// Handle joining room
function handleJoinRoom(clientInfo, roomCode) {
    // Normalize room code
    const normalizedCode = roomCode.toUpperCase();

    // Check if room exists
    if (!rooms.has(normalizedCode)) {
        // Create the room if it doesn't exist
        rooms.set(normalizedCode, new Set());
        console.log(`[Room] Created room ${normalizedCode} for joining`);
    }

    // Leave current room if in one
    if (clientInfo.roomCode) {
        leaveRoom(clientInfo.id, clientInfo.roomCode);
    }

    // Join new room
    rooms.get(normalizedCode).add(clientInfo.id);
    clientInfo.roomCode = normalizedCode;

    console.log(`[Room] Client ${clientInfo.id} joined room ${normalizedCode}`);

    // Get other peers in room
    const peersInRoom = getClientsInRoom(normalizedCode)
        .filter(client => client.id !== clientInfo.id)
        .map(client => ({
            id: client.id,
            name: client.name
        }));

    // Send room info to joining client
    clientInfo.ws.send(JSON.stringify({
        type: 'room-joined',
        clientId: clientInfo.id,
        roomCode: normalizedCode,
        peers: peersInRoom
    }));

    streamSnapshotTo(clientInfo, 'ROOM:' + normalizedCode);

    // Notify other room members
    broadcastToRoom(normalizedCode, clientInfo.id, {
        type: 'peer-joined',
        peer: {
            id: clientInfo.id,
            name: clientInfo.name
        }
    });
}

// Leave room
function leaveRoom(clientId, roomCode) {
    const room = rooms.get(roomCode);
    if (room) {
        room.delete(clientId);

        // Delete room if empty
        if (room.size === 0) {
            rooms.delete(roomCode);
            sharedTexts.delete('ROOM:' + roomCode);
            pendingTexts.delete('ROOM:' + roomCode);
            console.log(`[Room] Room ${roomCode} deleted (empty)`);
        } else {
            // Notify remaining room members
            broadcastToRoom(roomCode, clientId, {
                type: 'peer-left',
                peerId: clientId
            });
        }
    }
}

// Broadcast message to all clients except sender (for local mode)
function broadcast(senderId, message) {
    clients.forEach((client, id) => {
        if (id !== senderId && client.ws.readyState === 1) {
            client.ws.send(JSON.stringify(message));
        }
    });
}

// Broadcast message to all clients in room except sender
function broadcastToRoom(roomCode, senderId, message) {
    const roomClients = getClientsInRoom(roomCode);
    roomClients.forEach(client => {
        if (client.id !== senderId && client.ws.readyState === 1) {
            client.ws.send(JSON.stringify(message));
        }
    });
}

const SERVER_START_TIME = new Date().toISOString();
const APP_VERSION = '2.2.0';

// API endpoint to get server info
app.get('/api/info', (req, res) => {
    res.json({
        totalClients: clients.size,
        totalRooms: rooms.size,
        sharedTextChannels: sharedTexts.size,
        pendingTextChannels: pendingTexts.size,
        isPublicDeployment: PUBLIC_DEPLOY,
        autoRoomEnabled: AUTO_ROOM_MODE
    });
});

// Version endpoint — returns version + exact server start time
app.get('/api/version', (req, res) => {
    res.json({
        version: APP_VERSION,
        startedAt: SERVER_START_TIME
    });
});

app.get('/live', (req, res) => {
    const text = new Date().toISOString();
    res.json({ text });
});

function keepAlive() {
    https.get('https://local-drop.onrender.com/live', (res) => {
        //console.log(res)
        res.on('data', () => {
        });
        res.on('end', () => {
        });
    }).on('error', (err) => {
        console.log('Error: ', err.message);
    });
}

// Run the keep-alive function every 14 minutes
setInterval(keepAlive, 4 * 60 * 1000);

// ── Shared-text garbage collection ───────────────
// Belt-and-suspenders so shared-text buffers can never accumulate: even though
// channels are cleared on room-empty / last-local-device-leaves, this sweep
// also drops interrupted reassembly buffers and any orphaned snapshot.
const PENDING_STALE_MS = 2 * 60 * 1000; // interrupted transfer buffer lifetime

function channelHasClients(key) {
    if (key === 'LOCAL') return clients.size > 0;
    if (key.startsWith('ROOM:')) {
        const room = rooms.get(key.slice(5));
        return !!room && room.size > 0;
    }
    return false;
}

function sweepSharedTexts() {
    const now = Date.now();
    for (const [key, pend] of pendingTexts) {
        if (now - (pend.updatedAt || 0) > PENDING_STALE_MS || !channelHasClients(key)) {
            pendingTexts.delete(key);
        }
    }
    for (const [key, snap] of sharedTexts) {
        if (!channelHasClients(key) || now - snap.updatedAt > SNAPSHOT_TTL_MS) {
            sharedTexts.delete(key);
        }
    }
}

setInterval(sweepSharedTexts, 60 * 1000);

// Start server
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    const localIP = getLocalIP();

    console.log('');
    console.log('🚀 Local File Transfer Server Started');
    console.log('=====================================');
    console.log(`📡 Local:    http://localhost:${PORT}`);
    console.log(`🌐 Network:  http://${localIP}:${PORT}`);
    console.log('=====================================');

    if (PUBLIC_DEPLOY) {
        if (AUTO_ROOM_MODE) {
            console.log('🌍 Running in PUBLIC mode with AUTO-ROOM');
            console.log('🔐 Devices from same local network auto-join same room');
            console.log('📝 Room codes are generated from client local IP');
        } else {
            console.log('🌍 Running in PUBLIC mode with MANUAL ROOMS');
            console.log('📝 Users must create/join rooms to connect');
        }
    } else {
        console.log('🏠 Running in LOCAL mode');
        console.log('📱 Open this URL on all devices in the same network');
    }

    console.log('🔒 All file transfers are P2P (not through server)');
    console.log('');
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\n[Shutdown] Closing server...');

    // Notify all clients
    clients.forEach(client => {
        client.ws.send(JSON.stringify({
            type: 'server-shutdown',
            message: 'Server is shutting down'
        }));
        client.ws.close();
    });

    server.close(() => {
        console.log('[Shutdown] Server closed');
        process.exit(0);
    });
});
