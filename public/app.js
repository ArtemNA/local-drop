// Configuration
const CONFIG = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' }
    ],
    chunkSize: 64 * 1024, // 64KB default — safe fallback, overridden per-connection via SCTP maxMessageSize
    bufferThreshold: 2 * 1024 * 1024, // 2MB buffer threshold (under browser ~16MB limit)
    reconnectDelay: 3000,
    reconnectMaxAttempts: 5,
    pingInterval: 30000
};

// Application state
const state = {
    ws: null,
    clientId: null,
    deviceName: 'Device',
    roomCode: null,
    inRoom: false,
    localMode: false,
    sharedText: {
        timer: null, revCounter: 0, lastSentSig: null, sending: false,
        incoming: new Map(),      // rev -> in-progress reassembly of server-relayed text
        lastChangeAt: 0,          // local clock time of the last local or remote change
        updatedAt: 0,             // when the current text was last changed (by anyone)
        p2pIncoming: new Map(),   // peerId -> in-progress P2P reassembly
        p2pChain: Promise.resolve(), // serializes P2P sends so chunks of different revs never interleave
        p2pLiveSeq: 0,
        lanOnlyContent: false     // current text came from a LAN-only peer — never send it via the server
    },
    peers: new Map(),
    connections: new Map(),
    dataChannels: new Map(),
    transferQueue: [],
    activeTransfers: new Map(),
    pendingFiles: new Map(),
    selectedPeer: null,
    reconnectAttempts: 0,
    sendWorkers: new Set(),     // peers we are currently sending files to (one file at a time each)
    currentReceive: new Map(),  // peerId -> transferId that incoming binary chunks belong to
    peerReconnects: new Map(),  // peerId -> WebRTC reconnect attempts since last success
    certificate: null,          // this device's DTLS certificate (stable across sessions)
    peerIdentity: new Map(),    // peerId -> verification state for the current connection
    verifyInbox: new Map(),     // peerId -> verify messages that arrived before we were ready
    untrustedPeers: new Set(),  // peers whose codes didn't match — no automatic reconnect
    localIP: null,
    peerLocality: new Map(), // peerId -> 'verified' | 'unverified' | 'confirmed' | 'blocked'
    localityChecks: new Map(), // peerId -> promise of the running locality check
    // LAN only (default on): files and shared text go only to peers whose
    // connection is verified local, and shared text never via the server
    lanOnly: true
};

// Get local IP address using WebRTC
async function getLocalIP() {
    return new Promise((resolve) => {
        const pc = new RTCPeerConnection({
            iceServers: [{ urls: 'stun:stun.l.google.com:19302' }]
        });
        
        pc.createDataChannel('');
        pc.createOffer().then(offer => pc.setLocalDescription(offer));
        
        pc.onicecandidate = (event) => {
            if (!event || !event.candidate || !event.candidate.candidate) {
                pc.close();
                resolve(null);
                return;
            }
            
            const candidate = event.candidate.candidate;
            const ipRegex = /([0-9]{1,3}\.){3}[0-9]{1,3}/;
            const match = candidate.match(ipRegex);
            
            if (match) {
                const ip = match[0];
                // Filter out common non-routable IPs
                if (!ip.startsWith('0.') && !ip.startsWith('127.')) {
                    pc.close();
                    resolve(ip);
                }
            }
        };
        
        // Timeout after 2 seconds
        setTimeout(() => {
            pc.close();
            resolve(null);
        }, 2000);
    });
}

// Initialize application
document.addEventListener('DOMContentLoaded', async () => {
    // Initialize theme
    initializeTheme();
    
    // Local IP is only a hint for the locality check — detect it in the
    // background instead of delaying startup
    getLocalIP().then(ip => {
        state.localIP = ip;
        console.log('Detected local IP:', ip);
    });

    loadDeviceName();
    initLanOnly();
    initSharedText();
    setupVerifyDialog();
    // Every connection must use this device's long-lived certificate (see
    // "Connection verification"), so have it ready before connecting
    state.certificate = await loadDeviceCertificate();
    initializeWebSocket();
    setupEventListeners();
    setupDragAndDrop();
    loadVersionInfo();
});

// Theme management
function initializeTheme() {
    const savedTheme = localStorage.getItem('theme') || 'dark';
    document.documentElement.setAttribute('data-theme', savedTheme);
    
    const themeSwitcher = document.getElementById('themeSwitcher');
    if (themeSwitcher) {
        themeSwitcher.setAttribute('data-theme', savedTheme);
        
        themeSwitcher.addEventListener('click', () => {
            const currentTheme = document.documentElement.getAttribute('data-theme');
            const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
            
            document.documentElement.setAttribute('data-theme', newTheme);
            themeSwitcher.setAttribute('data-theme', newTheme);
            localStorage.setItem('theme', newTheme);
        });
    }
}

// WebSocket connection
function initializeWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    
    // Check if room code in URL params
    const urlParams = new URLSearchParams(window.location.search);
    const roomFromUrl = urlParams.get('room');
    
    // Build WebSocket URL with parameters
    let wsUrl = `${protocol}//${window.location.host}`;
    const params = new URLSearchParams();
    
    if (roomFromUrl) {
        params.append('room', roomFromUrl);
    }
    
    // Include saved device name
    if (state.deviceName && state.deviceName !== 'Device') {
        params.append('deviceName', encodeURIComponent(state.deviceName));
    }
    
    if (params.toString()) {
        wsUrl += '?' + params.toString();
    }
    
    state.ws = new WebSocket(wsUrl);
    
    state.ws.onopen = () => {
        console.log('WebSocket connected');
        updateConnectionStatus(true);
        state.reconnectAttempts = 0;
        startPingInterval();
    };
    
    state.ws.onmessage = handleWebSocketMessage;
    
    state.ws.onclose = () => {
        console.log('WebSocket disconnected');
        updateConnectionStatus(false);
        stopPingInterval();
        
        // Clear all connections
        state.connections.forEach(conn => conn.close());
        state.connections.clear();
        state.dataChannels.clear();
        state.peers.clear();
        updateDevicesList();
        
        // Attempt reconnection. Each failed attempt closes its socket and lands
        // here again, so attempts chain until one succeeds or we give up.
        if (state.reconnectAttempts < CONFIG.reconnectMaxAttempts) {
            state.reconnectAttempts++;

            showToast(`Reconnecting... (${state.reconnectAttempts}/${CONFIG.reconnectMaxAttempts})`, 'warning');

            // Back off a little more on every attempt
            setTimeout(() => {
                initializeWebSocket();
            }, CONFIG.reconnectDelay * state.reconnectAttempts);
        } else {
            showToast('Failed to connect to server. Please refresh the page.', 'error');
        }
    };
    
    state.ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        showToast('Connection error occurred', 'error');
    };
}

// Handle WebSocket messages
function handleWebSocketMessage(event) {
    const message = JSON.parse(event.data);
    
    switch (message.type) {
        case 'init':
            state.clientId = message.clientId;
            
            // Update device name if server sends one
            if (message.deviceName) {
                state.deviceName = message.deviceName;
                document.getElementById('deviceName').textContent = message.deviceName;
            }
            
            if (message.localMode) {
                // Local mode - no rooms needed
                state.inRoom = false;
                state.localMode = true;
                message.peers.forEach(peer => {
                    state.peers.set(peer.id, peer);
                });

                // Hide room section, show devices
                document.getElementById('roomSection').style.display = 'none';
                document.getElementById('devicesList').parentElement.style.display = 'block';
                updateDevicesList();
                connectPeersForLanText();
            } else if (message.needsRoom) {
                // Need to select/create room
                showRoomSection();
                
                // Pre-fill room input if in URL
                const urlParams = new URLSearchParams(window.location.search);
                const roomFromUrl = urlParams.get('room');
                if (roomFromUrl) {
                    document.getElementById('roomInput').value = roomFromUrl;
                    // Auto-click join if room code is pre-filled
                    setTimeout(() => {
                        document.getElementById('joinRoomBtn').click();
                    }, 500);
                }
            }
            break;
            
        case 'room-created':
            handleRoomCreated(message);
            break;
            
        case 'room-joined':
            // Update device name if server sends one
            if (message.deviceName) {
                state.deviceName = message.deviceName;
                document.getElementById('deviceName').textContent = message.deviceName;
            }
            
            if (message.clientId) {
                state.clientId = message.clientId;
            }
            state.roomCode = message.roomCode;
            state.inRoom = true;

            // Add existing peers in room
            message.peers.forEach(peer => {
                state.peers.set(peer.id, peer);
            });

            hideRoomSection();
            showRoomInfo(message.roomCode);
            updateDevicesList();
            connectPeersForLanText();

            // Update URL with room code
            const url = new URL(window.location);
            url.searchParams.set('room', message.roomCode);
            window.history.pushState({}, '', url);
            
            if (message.autoRoom) {
                showToast(`Auto-joined room ${message.roomCode} (same network)`, 'success');
            } else {
                showToast(`Joined room: ${message.roomCode}`, 'success');
            }
            break;
            
        case 'peer-joined':
            state.peers.set(message.peer.id, message.peer);
            updateDevicesList();
            showToast(`${message.peer.name} joined`, 'info');
            connectPeersForLanText();
            break;
            
        case 'peer-left':
            handlePeerDisconnect(message.peerId);
            break;
            
        case 'peer-updated':
            if (state.peers.has(message.peer.id)) {
                // Merge: the update only carries the name, keep e.g. sameNetwork
                state.peers.set(message.peer.id, { ...state.peers.get(message.peer.id), ...message.peer });
                updateDevicesList();
            }
            break;
            
        case 'offer':
            handleOffer(message);
            break;
            
        case 'answer':
            handleAnswer(message);
            break;
            
        case 'ice-candidate':
            handleIceCandidate(message);
            break;
            
        case 'error':
            console.error('Server error:', message.message);
            showToast(message.message, 'error');
            
            if (message.code === 'ROOM_NOT_FOUND') {
                showRoomSection();
            } else if (message.code === 'SERVER_TRANSFER_BLOCKED' || message.code === 'EXCESSIVE_DATA_TRANSFER') {
                // Server detected file transfer attempt
                state.ws.close();
            }
            break;
            
        case 'text-update':
            handleTextUpdateChunk(message);
            break;

        case 'server-shutdown':
            showToast('Server is shutting down', 'warning');
            break;
            
        case 'pong':
            // Keep-alive response
            break;
    }
}

// Handle room created
function handleRoomCreated(message) {
    if (message.clientId) {
        state.clientId = message.clientId;
    }
    state.roomCode = message.roomCode;
    state.inRoom = true;
    
    hideRoomSection();
    showRoomInfo(message.roomCode);
    updateDevicesList();
    
    // Update URL with room code for easy sharing
    const url = new URL(window.location);
    url.searchParams.set('room', message.roomCode);
    window.history.pushState({}, '', url);
    
    showToast(`Room created: ${message.roomCode}`, 'success');
}

// Show room section
function showRoomSection() {
    document.getElementById('roomSection').style.display = 'block';
    document.getElementById('devicesList').parentElement.style.display = 'none';
    document.getElementById('queueSection').style.display = 'none';
}

// Hide room section
function hideRoomSection() {
    document.getElementById('roomSection').style.display = 'none';
    document.getElementById('devicesList').parentElement.style.display = 'block';
}

// Show room info in header
function showRoomInfo(roomCode) {
    const roomInfo = document.getElementById('roomInfo');
    const roomCodeElement = document.getElementById('roomCode');
    
    roomCodeElement.textContent = roomCode;
    roomInfo.style.display = 'flex';
}

// WebRTC connection management
async function createConnection(peerId, isInitiator = true) {
    console.log(`Creating connection to ${peerId}, initiator: ${isInitiator}`);
    
    const rtcConfig = {
        iceServers: CONFIG.iceServers,
        // Optional: Add more aggressive settings for better performance
        bundlePolicy: 'max-bundle',
        rtcpMuxPolicy: 'require'
    };
    if (state.certificate) {
        rtcConfig.certificates = [state.certificate];
    }
    const connection = new RTCPeerConnection(rtcConfig);
    
    // See handleOffer (glare) and handleIceCandidate
    connection.makingOffer = isInitiator;
    connection.pendingCandidates = [];
    state.connections.set(peerId, connection);

    // Handle ICE candidates
    connection.onicecandidate = (event) => {
        if (event.candidate && state.ws.readyState === WebSocket.OPEN) {
            state.ws.send(JSON.stringify({
                type: 'ice-candidate',
                targetId: peerId,
                candidate: event.candidate
            }));
        }
    };
    
    // Monitor connection state
    connection.onconnectionstatechange = () => {
        console.log(`Connection state with ${peerId}: ${connection.connectionState}`);

        if (connection.connectionState === 'failed' || connection.connectionState === 'disconnected') {
            handleConnectionFailure(peerId, connection);
        } else if (connection.connectionState === 'connected') {
            state.peerReconnects.delete(peerId);
            showToast(`Connected to ${state.peers.get(peerId)?.name || 'device'}`, 'success');
            // Verify locality of the connection
            const check = verifyLocality(connection, peerId);
            state.localityChecks.set(peerId, check);
            check.then(() => onPeerRouteChecked(peerId));
        }
    };
    
    // Create data channel only if initiator
    if (isInitiator) {
        const dataChannel = connection.createDataChannel('fileTransfer', {
            ordered: true
        });
        
        setupDataChannel(dataChannel, peerId);
        state.dataChannels.set(peerId, dataChannel);
    } else {
        // Wait for data channel from initiator
        connection.ondatachannel = (event) => {
            console.log(`Received data channel from ${peerId}`);
            const dataChannel = event.channel;
            setupDataChannel(dataChannel, peerId);
            state.dataChannels.set(peerId, dataChannel);
        };
    }
    
    return connection;
}

// Setup data channel handlers
function setupDataChannel(dataChannel, peerId) {
    dataChannel.binaryType = 'arraybuffer';
    
    dataChannel.onopen = () => {
        console.log(`Data channel opened with ${peerId}`);
        state.peerIdentity.delete(peerId); // new connection — new code
        updateDevicesList();
        onPeerRouteChecked(peerId);
        startVerification(peerId, dataChannel);

        // Pick up transfers that were cut off by a dropped connection. The
        // receiver knows how much arrived, so it asks the sender to continue.
        state.activeTransfers.forEach(transfer => {
            if (transfer.peerId === peerId && transfer.direction === 'receive' &&
                transfer.status === 'paused' && transfer.interrupted) {
                requestResume(transfer);
            }
        });
    };

    dataChannel.onclose = () => {
        console.log(`Data channel closed with ${peerId}`);
        // A replaced connection's old channel may close after the new one is
        // registered — only forget the channel if it is still the current one.
        if (state.dataChannels.get(peerId) === dataChannel) {
            state.dataChannels.delete(peerId);
            interruptTransfers(peerId);
        }
        updateDevicesList();
    };

    dataChannel.onerror = (event) => {
        // SCTP cause 12 = "User-Initiated Abort": we closed it ourselves
        if (event.error && event.error.sctpCauseCode === 12) return;
        console.error(`Data channel error with ${peerId}:`, event);
        showToast(`Connection error with ${state.peers.get(peerId)?.name || 'device'}`, 'error');
    };
    
    dataChannel.onmessage = (event) => {
        handleDataChannelMessage(event, peerId);
    };
}

// Handle offer
async function handleOffer(message) {
    const { fromId, offer } = message;

    // Codes didn't match for this peer — only reconnect when the user asks
    if (state.untrustedPeers.has(fromId)) {
        return;
    }

    // Both sides sent an offer at the same time (e.g. both have LAN-only shared
    // text on). The side with the smaller id keeps its own offer; the other drops
    // its own connection and answers instead. "Making an offer" counts from the
    // moment the connection is created — setLocalDescription is async, and the
    // peer's offer can arrive before it finishes.
    const existing = state.connections.get(fromId);
    if (existing) {
        const collision = existing.makingOffer || existing.signalingState === 'have-local-offer';
        if (collision && state.clientId && state.clientId < fromId) {
            return;
        }
        // Yielding, or the peer started over (e.g. after a dropped connection)
        existing.close();
    }

    try {
        const connection = await createConnection(fromId, false);
        await connection.setRemoteDescription(new RTCSessionDescription(offer));
        await flushPendingCandidates(connection);

        const answer = await connection.createAnswer();
        await connection.setLocalDescription(answer);
        
        if (state.ws.readyState === WebSocket.OPEN) {
            state.ws.send(JSON.stringify({
                type: 'answer',
                targetId: fromId,
                answer: answer
            }));
        }
    } catch (error) {
        console.error('Error handling offer:', error);
        showToast('Failed to establish connection', 'error');
    }
}

// Handle answer
async function handleAnswer(message) {
    const { fromId, answer } = message;
    const connection = state.connections.get(fromId);

    // Only a connection waiting for an answer can take one (an answer to an
    // offer we dropped while yielding is stale)
    if (connection && connection.signalingState === 'have-local-offer') {
        try {
            await connection.setRemoteDescription(new RTCSessionDescription(answer));
            await flushPendingCandidates(connection);
        } catch (error) {
            console.error('Error handling answer:', error);
        }
    }
}

// Handle ICE candidate
async function handleIceCandidate(message) {
    const { fromId, candidate } = message;
    const connection = state.connections.get(fromId);

    if (connection) {
        // Candidates can overtake the offer/answer — hold them until the
        // remote description is set
        if (!connection.remoteDescription) {
            connection.pendingCandidates.push(candidate);
            return;
        }
        try {
            await connection.addIceCandidate(new RTCIceCandidate(candidate));
        } catch (error) {
            console.error('Error adding ICE candidate:', error);
        }
    }
}

async function flushPendingCandidates(connection) {
    const pending = connection.pendingCandidates.splice(0);
    for (const candidate of pending) {
        try {
            await connection.addIceCandidate(new RTCIceCandidate(candidate));
        } catch (error) {
            // Typically from a connection the peer already replaced
            console.warn('Dropped stale ICE candidate:', error.message);
        }
    }
}

// Initiate connection with peer
async function connectToPeer(peerId) {
    const existingConn = state.connections.get(peerId);
    if (existingConn) {
        if (existingConn.connectionState === 'connected') {
            console.log(`Already connected to ${peerId}`);
            return;
        }
        if (existingConn.connectionState === 'new' || existingConn.connectionState === 'connecting') {
            return; // already on its way
        }
        existingConn.close();
    }

    try {
        const connection = await createConnection(peerId, true);
        try {
            const offer = await connection.createOffer();
            await connection.setLocalDescription(offer);

            if (state.ws.readyState === WebSocket.OPEN) {
                state.ws.send(JSON.stringify({
                    type: 'offer',
                    targetId: peerId,
                    offer: offer
                }));
            }
        } finally {
            connection.makingOffer = false;
        }
    } catch (error) {
        console.error('Error connecting to peer:', error);
        showToast('Failed to connect to device', 'error');
    }
}

// Locality verification
//
// Decides whether a peer connection really stays inside the local network:
//   'verified'   — direct and local: private / link-local addresses, the same
//                  IPv6 /64, or — when the browser hides the addresses — a
//                  LAN-like round-trip time or the server seeing both devices
//                  behind the same network
//   'unverified' — anything else, e.g. NAT traversal across the internet or a
//                  direct IPv6 connection between two different networks
//   'blocked'    — goes through a TURN relay
//   'confirmed'  — not verified, but the user chose to send anyway
//                  (only possible with LAN only turned off)
const LAN_RTT_MS = 10;          // typical LAN round trip is 1–5 ms
const SAME_NETWORK_RTT_MS = 30; // with the server's hint, allow for Wi-Fi jitter

function isPrivateIP(ip) {
    if (!ip) return false;
    // IPv4 private ranges
    if (/^10\./.test(ip)) return true;
    if (/^172\.(1[6-9]|2\d|3[01])\./.test(ip)) return true;
    if (/^192\.168\./.test(ip)) return true;
    if (/^169\.254\./.test(ip)) return true; // link-local
    if (/^127\./.test(ip) || ip === '::1') return true; // same machine
    // IPv6 unique-local (fc00::/7) and link-local
    if (/^f[cd][0-9a-f]{2}:/i.test(ip)) return true;
    if (/^fe80:/i.test(ip)) return true;
    return false;
}

function isIPv6(ip) {
    return typeof ip === 'string' && ip.includes(':');
}

// "2001:db8::1" → first four groups as a string ("2001:db8:0:0"), i.e. the /64
// network a home router hands out. Returns null for anything unparsable.
function ipv6Prefix64(ip) {
    const halves = ip.split('%')[0].toLowerCase().split('::');
    if (halves.length > 2) return null;
    const head = halves[0] ? halves[0].split(':') : [];
    const tail = halves.length === 2 && halves[1] ? halves[1].split(':') : [];
    const missing = 8 - head.length - tail.length;
    if (halves.length === 1 ? head.length !== 8 : missing < 0) return null;
    const groups = [...head, ...new Array(halves.length === 2 ? missing : 0).fill('0'), ...tail];
    if (!groups.every(g => /^[0-9a-f]{1,4}$/.test(g))) return null;
    return groups.slice(0, 4).map(g => parseInt(g, 16).toString(16)).join(':');
}

// The candidate pair the connection actually uses, plus its round-trip time
async function readSelectedRoute(connection) {
    const stats = await connection.getStats();
    let pair = null;
    stats.forEach(report => {
        if (report.type === 'transport' && report.selectedCandidatePairId) {
            pair = stats.get(report.selectedCandidatePairId) || pair;
        }
    });
    if (!pair) {
        // Older browsers: fall back to the nominated succeeded pair
        stats.forEach(report => {
            if (report.type === 'candidate-pair' && report.state === 'succeeded' && (report.nominated || !pair)) {
                pair = report;
            }
        });
    }
    if (!pair) return null;

    const local = stats.get(pair.localCandidateId) || {};
    const remote = stats.get(pair.remoteCandidateId) || {};
    // Browsers hide local addresses (empty, or an mDNS "<uuid>.local" name)
    const visible = ip => (ip && !ip.endsWith('.local')) ? ip : null;
    return {
        localType: local.candidateType,
        remoteType: remote.candidateType,
        localIP: visible(local.address || local.ip),
        remoteIP: visible(remote.address || remote.ip),
        rttMs: typeof pair.currentRoundTripTime === 'number' ? pair.currentRoundTripTime * 1000 : null
    };
}

function classifyRoute(route, rttMs, sameNetwork) {
    if (!route) return 'unverified';
    const { localType, remoteType, localIP, remoteIP } = route;

    if (localType === 'relay' || remoteType === 'relay') return 'blocked';

    // srflx = the path was punched through a NAT from outside → another network.
    // prflx shows up on LANs too (it's how mDNS-hidden peers get learned).
    const direct = ['host', 'prflx'].includes(localType) && ['host', 'prflx'].includes(remoteType);
    if (!direct) return 'unverified';

    if (localIP && remoteIP) {
        if (isPrivateIP(localIP) && isPrivateIP(remoteIP)) return 'verified';
        if (isIPv6(localIP) && isIPv6(remoteIP)) {
            const prefix = ipv6Prefix64(localIP);
            if (prefix && prefix === ipv6Prefix64(remoteIP)) return 'verified';
        }
        return 'unverified'; // public addresses of different networks
    }

    // Addresses hidden by the browser: a direct path alone could still be IPv6
    // across the internet, so also require LAN-like latency or the server's hint
    if (rttMs !== null && rttMs <= LAN_RTT_MS) return 'verified';
    if (sameNetwork && (rttMs === null || rttMs <= SAME_NETWORK_RTT_MS)) return 'verified';
    return 'unverified';
}

async function verifyLocality(connection, peerId) {
    let locality = 'unverified';
    let route = null;
    let bestRtt = null;
    try {
        // A few samples: the first RTT measurement is often missing or noisy
        for (let i = 0; i < 6; i++) {
            const sample = await readSelectedRoute(connection);
            if (sample) {
                route = sample;
                if (sample.rttMs !== null) {
                    bestRtt = bestRtt === null ? sample.rttMs : Math.min(bestRtt, sample.rttMs);
                }
            }
            if (route && bestRtt !== null && i >= 2) break;
            await new Promise(resolve => setTimeout(resolve, 250));
        }
        const sameNetwork = state.peers.get(peerId)?.sameNetwork === true;
        locality = classifyRoute(route, bestRtt, sameNetwork);
        console.log(`Locality check for ${peerId}: ${locality} — local=${route?.localType}(${route?.localIP || 'hidden'}), ` +
            `remote=${route?.remoteType}(${route?.remoteIP || 'hidden'}), rtt=${bestRtt === null ? '?' : bestRtt.toFixed(1) + 'ms'}, sameNetwork=${sameNetwork}`);
    } catch (err) {
        console.error('Error verifying locality:', err);
    }

    if (state.connections.get(peerId) !== connection) return; // replaced meanwhile
    state.peerLocality.set(peerId, locality);
    if (locality === 'blocked') {
        showToast('Connection uses a relay server — transfers blocked (not local)', 'error');
    }
    updateDevicesList();
}

// Wait for the running locality check of a peer, if any
async function localityOf(peerId) {
    try {
        await state.localityChecks.get(peerId);
    } catch (_) { /* recorded as unverified */ }
    return state.peerLocality.get(peerId);
}

async function checkLocalityBeforeTransfer(peerId) {
    const locality = await localityOf(peerId);
    const peerName = state.peers.get(peerId)?.name || 'device';

    if (locality === 'blocked') {
        showToast('Transfer blocked: connection route is not local (relay detected)', 'error');
        return false;
    }

    if (state.lanOnly) {
        if (locality === 'verified') return true;
        showToast(`${peerName} is not verified to be on your local network. Turn off "LAN only" to send over the internet.`, 'warning');
        return false;
    }

    if (locality === 'verified' || locality === 'confirmed') {
        return true;
    }

    // Unverified or unknown - ask user
    return new Promise(resolve => {
        const dialog = document.createElement('div');
        dialog.className = 'locality-dialog';
        dialog.innerHTML = `
            <div class="locality-dialog-overlay"></div>
            <div class="locality-dialog-content">
                <h3>Not a local connection</h3>
                <p>The connection to <strong>${escapeHtml(peerName)}</strong> could not be verified as local, so the file may travel over the internet (still encrypted, directly between the devices). Send anyway?</p>
                <div class="locality-dialog-actions">
                    <button class="locality-btn-cancel">Cancel</button>
                    <button class="locality-btn-confirm">Transfer Anyway</button>
                </div>
            </div>
        `;

        document.body.appendChild(dialog);

        dialog.querySelector('.locality-btn-cancel').onclick = () => {
            dialog.remove();
            resolve(false);
        };
        dialog.querySelector('.locality-btn-confirm').onclick = () => {
            state.peerLocality.set(peerId, 'confirmed');
            updateDevicesList();
            dialog.remove();
            resolve(true);
        };
        dialog.querySelector('.locality-dialog-overlay').onclick = () => {
            dialog.remove();
            resolve(false);
        };
    });
}

// Receiver side of LAN only: refuse a file from a peer whose route isn't local
async function enforceLanForIncoming(transfer) {
    const locality = await localityOf(transfer.peerId);
    if (!state.lanOnly || locality === 'verified') return;
    if (transfer.status !== 'receiving' && transfer.status !== 'paused') return;

    transfer.status = 'cancelled';
    notifyPeer(transfer, 'transfer-cancel', { reason: 'lan-only' });
    stopTransferLocally(transfer);
    updateTransferUI(transfer);
    showToast(`Blocked ${transfer.fileName}: ${state.peers.get(transfer.peerId)?.name || 'the sender'} is not verified to be on your local network (LAN only)`, 'warning');
}

// ─────────────────────────────────────────────
// Connection verification (protection against a man-in-the-middle)
//
// The data channel is encrypted with DTLS, but the certificate fingerprints that
// authenticate it are exchanged through the signaling server — a malicious
// server could put itself in the middle with its own certificates. To detect
// that, both devices show a short code derived from the fingerprints they
// actually see; if the users confirm the codes match, nobody is in between.
//
// A 6-digit code alone could be brute-forced by an attacker who picks
// certificates until the codes collide, so the code also mixes in a random
// nonce from each side, exchanged with a commitment (SAS / ZRTP style):
//   A → B: commit = SHA-256(nonceA)        A commits before seeing nonceB
//   B → A: nonceB
//   A → B: nonceA                          B checks it against the commitment
//   code  = SHA-256(fpA | fpB | nonceA | nonceB) → 6 digits
// A man in the middle must fix its nonces before learning the other side's,
// so it gets a single 1-in-a-million guess at matching codes.
//
// Each device keeps one certificate (IndexedDB), so a confirmed peer is
// remembered by its fingerprint and shows as verified on later connections.
// ─────────────────────────────────────────────

const VERIFIED_PEERS_KEY = 'verifiedPeers';
const CERT_LIFETIME_MS = 365 * 24 * 60 * 60 * 1000; // browsers may cap this

// SHA-256 → hex. crypto.subtle only exists on secure origins (https or
// localhost); local mode is usually opened as http://<LAN IP>, so fall back to
// a small JS implementation there.
async function sha256Hex(text) {
    const bytes = new TextEncoder().encode(text);
    const digest = (window.crypto && crypto.subtle)
        ? new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))
        : sha256Js(bytes);
    return [...digest].map(b => b.toString(16).padStart(2, '0')).join('');
}

// Plain SHA-256 (FIPS 180-4) for insecure contexts
function sha256Js(data) {
    const primes = [];
    for (let n = 2; primes.length < 64; n++) {
        if (primes.every(p => n % p !== 0)) primes.push(n);
    }
    // Round constants / initial hash: fractional parts of cube / square roots of primes
    const frac = x => ((x - Math.floor(x)) * 4294967296) >>> 0;
    const K = primes.map(p => frac(Math.cbrt(p)));
    const H = primes.slice(0, 8).map(p => frac(Math.sqrt(p)));

    const bitLength = data.length * 8;
    const total = ((data.length + 9 + 63) >> 6) << 6;
    const buffer = new Uint8Array(total);
    buffer.set(data);
    buffer[data.length] = 0x80;
    const view = new DataView(buffer.buffer);
    view.setUint32(total - 8, Math.floor(bitLength / 4294967296));
    view.setUint32(total - 4, bitLength >>> 0);

    const rotr = (x, n) => (x >>> n) | (x << (32 - n));
    const W = new Uint32Array(64);
    for (let offset = 0; offset < total; offset += 64) {
        for (let i = 0; i < 16; i++) W[i] = view.getUint32(offset + i * 4);
        for (let i = 16; i < 64; i++) {
            const s0 = rotr(W[i - 15], 7) ^ rotr(W[i - 15], 18) ^ (W[i - 15] >>> 3);
            const s1 = rotr(W[i - 2], 17) ^ rotr(W[i - 2], 19) ^ (W[i - 2] >>> 10);
            W[i] = (W[i - 16] + s0 + W[i - 7] + s1) >>> 0;
        }
        let [a, b, c, d, e, f, g, h] = H;
        for (let i = 0; i < 64; i++) {
            const t1 = (h + (rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)) + ((e & f) ^ (~e & g)) + K[i] + W[i]) >>> 0;
            const t2 = ((rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)) + ((a & b) ^ (a & c) ^ (b & c))) >>> 0;
            h = g; g = f; f = e; e = (d + t1) >>> 0;
            d = c; c = b; b = a; a = (t1 + t2) >>> 0;
        }
        [a, b, c, d, e, f, g, h].forEach((v, i) => { H[i] = (H[i] + v) >>> 0; });
    }

    const out = new Uint8Array(32);
    const outView = new DataView(out.buffer);
    H.forEach((v, i) => outView.setUint32(i * 4, v));
    return out;
}

function randomHex(byteCount) {
    const bytes = crypto.getRandomValues(new Uint8Array(byteCount));
    return [...bytes].map(b => b.toString(16).padStart(2, '0')).join('');
}

// Load this device's DTLS certificate from IndexedDB, or create one. Without a
// stable certificate a confirmed peer could not be recognized next time.
function idbRequest(request) {
    return new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
    });
}

async function loadDeviceCertificate() {
    if (typeof RTCPeerConnection === 'undefined' || !RTCPeerConnection.generateCertificate) return null;
    const generate = () => RTCPeerConnection.generateCertificate({
        name: 'ECDSA',
        namedCurve: 'P-256',
        expires: CERT_LIFETIME_MS
    });

    try {
        const openRequest = indexedDB.open('local-drop', 1);
        openRequest.onupgradeneeded = () => openRequest.result.createObjectStore('keys');
        const db = await idbRequest(openRequest);

        let cert = await idbRequest(db.transaction('keys').objectStore('keys').get('dtls-certificate'));
        // Renew a day before it expires (peers will need to verify again)
        if (!cert || cert.expires < Date.now() + 24 * 60 * 60 * 1000) {
            cert = await generate();
            await idbRequest(db.transaction('keys', 'readwrite').objectStore('keys').put(cert, 'dtls-certificate'));
        }
        db.close();
        return cert;
    } catch (error) {
        // Storage unavailable (private mode, Safari can't store certificates…):
        // verification still works, it just isn't remembered across reloads
        console.warn('Could not persist the device certificate:', error);
        try {
            return await generate();
        } catch (_) {
            return null;
        }
    }
}

function loadVerifiedPeers() {
    try {
        return JSON.parse(localStorage.getItem(VERIFIED_PEERS_KEY)) || {};
    } catch (_) {
        return {};
    }
}

function rememberVerifiedPeer(fingerprint, name) {
    const peers = loadVerifiedPeers();
    peers[fingerprint] = { name, verifiedAt: Date.now() };
    // Keep the list bounded — drop the oldest entries
    const entries = Object.entries(peers).sort((a, b) => b[1].verifiedAt - a[1].verifiedAt).slice(0, 100);
    try {
        localStorage.setItem(VERIFIED_PEERS_KEY, JSON.stringify(Object.fromEntries(entries)));
    } catch (_) { /* not persisted — still verified for this session */ }
}

// Fingerprints of the certificates actually used by the DTLS session
async function getDtlsFingerprints(connection) {
    let local = null;
    let remote = null;
    try {
        const stats = await connection.getStats();
        stats.forEach(report => {
            if (report.type === 'transport' && report.localCertificateId && report.remoteCertificateId) {
                local = stats.get(report.localCertificateId)?.fingerprint || local;
                remote = stats.get(report.remoteCertificateId)?.fingerprint || remote;
            }
        });
    } catch (_) { /* fall back to the SDP below */ }

    // Fallback: the fingerprints negotiated in the SDP (DTLS enforces them)
    const fromSdp = sdp => ((sdp || '').match(/a=fingerprint:\S+ ([0-9A-Fa-f:]+)/) || [])[1];
    local = local || fromSdp(connection.localDescription?.sdp);
    remote = remote || fromSdp(connection.remoteDescription?.sdp);

    return local && remote ? { local: local.toUpperCase(), remote: remote.toUpperCase() } : null;
}

// Run the code exchange on a freshly opened data channel (both sides do this)
async function startVerification(peerId, dataChannel) {
    const connection = state.connections.get(peerId);
    if (!connection) return;

    const fingerprints = await getDtlsFingerprints(connection);
    if (!fingerprints || state.dataChannels.get(peerId) !== dataChannel) return;

    // Roles must come out opposite on both ends. Two tabs of one browser share
    // a certificate, so break ties with the client ids.
    const isA = fingerprints.local !== fingerprints.remote
        ? fingerprints.local < fingerprints.remote
        : (state.clientId || '') < peerId;

    const v = {
        dataChannel,
        fingerprints,
        role: isA ? 'A' : 'B',
        nonce: randomHex(32),
        theirCommit: null,
        theirNonce: null,
        code: null,
        status: 'pending' // pending → ready | verified ; or mismatch / failed
    };
    state.peerIdentity.set(peerId, v);

    if (v.role === 'A') {
        sendVerifyMessage(v, { type: 'verify-commit', commit: await sha256Hex(v.nonce) });
    }

    // Messages that arrived while we were reading the fingerprints
    const early = state.verifyInbox.get(peerId) || [];
    state.verifyInbox.delete(peerId);
    for (const message of early) {
        await handleVerifyMessage(message, peerId);
    }
}

function sendVerifyMessage(v, message) {
    if (v.dataChannel.readyState === 'open') {
        v.dataChannel.send(JSON.stringify(message));
    }
}

async function handleVerifyMessage(message, peerId) {
    const v = state.peerIdentity.get(peerId);
    if (!v || v.dataChannel !== state.dataChannels.get(peerId)) {
        // Not set up yet for this channel — keep it for startVerification
        const inbox = state.verifyInbox.get(peerId) || [];
        if (inbox.length < 5) inbox.push(message);
        state.verifyInbox.set(peerId, inbox);
        return;
    }
    if (v.status !== 'pending' || typeof message !== 'object') return;
    const isHex = value => typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);

    if (v.role === 'B' && message.type === 'verify-commit' && isHex(message.commit) && !v.theirCommit) {
        v.theirCommit = message.commit;
        sendVerifyMessage(v, { type: 'verify-nonce', nonce: v.nonce });
    } else if (v.role === 'A' && message.type === 'verify-nonce' && isHex(message.nonce) && !v.theirNonce) {
        v.theirNonce = message.nonce;
        sendVerifyMessage(v, { type: 'verify-reveal', nonce: v.nonce });
        await finishVerification(peerId, v);
    } else if (v.role === 'B' && message.type === 'verify-reveal' && isHex(message.nonce) && v.theirCommit) {
        if (await sha256Hex(message.nonce) !== v.theirCommit) {
            // The other side changed its nonce after seeing ours
            v.status = 'failed';
            updateDevicesList();
            refreshVerifyDialog(peerId);
            return;
        }
        v.theirNonce = message.nonce;
        await finishVerification(peerId, v);
    }
}

async function finishVerification(peerId, v) {
    const { local, remote } = v.fingerprints;
    const [fpA, fpB] = v.role === 'A' ? [local, remote] : [remote, local];
    const [nonceA, nonceB] = v.role === 'A' ? [v.nonce, v.theirNonce] : [v.theirNonce, v.nonce];

    const digest = await sha256Hex(`local-drop-sas-v1|${fpA}|${fpB}|${nonceA}|${nonceB}`);
    const number = parseInt(digest.slice(0, 8), 16) % 1000000;
    v.code = String(number).padStart(6, '0');

    // Seen and confirmed before → trust it again without asking
    v.status = loadVerifiedPeers()[remote] ? 'verified' : 'ready';
    updateDevicesList();
    refreshVerifyDialog(peerId);
}

// ── Verify dialog ──
let verifyDialogPeer = null;

function openVerifyDialog(peerId) {
    verifyDialogPeer = peerId;
    document.getElementById('verifyModal').classList.remove('hidden');
    refreshVerifyDialog(peerId);
}

function closeVerifyDialog() {
    verifyDialogPeer = null;
    document.getElementById('verifyModal').classList.add('hidden');
}

function refreshVerifyDialog(peerId) {
    if (verifyDialogPeer !== peerId) return;
    const v = state.peerIdentity.get(peerId);
    const peerName = state.peers.get(peerId)?.name || 'the other device';

    const codeEl = document.getElementById('verifyCode');
    const textEl = document.getElementById('verifyText');
    const matchBtn = document.getElementById('verifyMatchBtn');
    const mismatchBtn = document.getElementById('verifyMismatchBtn');

    document.getElementById('verifyPeerName').textContent = peerName;
    const ready = v && (v.status === 'ready' || v.status === 'verified');
    codeEl.textContent = ready ? `${v.code.slice(0, 3)} ${v.code.slice(3)}` : '··· ···';
    matchBtn.style.display = v && v.status === 'ready' ? '' : 'none';
    mismatchBtn.style.display = v && v.status === 'ready' ? '' : 'none';

    if (!v || v.status === 'pending') {
        textEl.textContent = 'Calculating the code…';
    } else if (v.status === 'ready') {
        textEl.textContent = `Open this dialog on ${peerName} and compare. If both screens show the same code, nobody is intercepting the connection.`;
    } else if (v.status === 'verified') {
        textEl.textContent = `Verified — you confirmed this code for ${peerName}. It is remembered for future connections.`;
    } else {
        textEl.textContent = 'Verification failed: the other side did not follow the protocol. Do not trust this connection.';
    }
}

function confirmCodesMatch() {
    const peerId = verifyDialogPeer;
    const v = state.peerIdentity.get(peerId);
    if (!v || v.status !== 'ready') return;
    v.status = 'verified';
    rememberVerifiedPeer(v.fingerprints.remote, state.peers.get(peerId)?.name || '');
    updateDevicesList();
    refreshVerifyDialog(peerId);
    showToast('Device verified', 'success');
}

// Different codes mean the connection runs through someone else: drop it and
// don't reconnect automatically.
function reportCodesMismatch() {
    const peerId = verifyDialogPeer;
    const v = state.peerIdentity.get(peerId);
    if (!v) return;
    v.status = 'mismatch';
    state.untrustedPeers.add(peerId);
    closeVerifyDialog();

    const connection = state.connections.get(peerId);
    if (connection) connection.close();
    state.connections.delete(peerId);
    state.dataChannels.delete(peerId);
    interruptTransfers(peerId);
    updateDevicesList();
    showToast('Codes did not match — connection closed. Someone may be intercepting it.', 'error');
}

function setupVerifyDialog() {
    document.getElementById('verifyMatchBtn').addEventListener('click', confirmCodesMatch);
    document.getElementById('verifyMismatchBtn').addEventListener('click', reportCodesMismatch);
    document.getElementById('verifyCloseBtn').addEventListener('click', closeVerifyDialog);
    document.getElementById('verifyModal').addEventListener('click', (e) => {
        if (e.target.id === 'verifyModal') closeVerifyDialog();
    });
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && verifyDialogPeer) closeVerifyDialog();
    });
}

// File transfer functions
//
// Protocol over the data channel (JSON messages + raw binary chunks):
//   file-start {transferId, name, size, ...}  → binary chunks →  file-end {totalBytes}
//   transfer-pause / transfer-cancel            either side
//   transfer-resume {offset}                    receiver → sender: continue from offset
//   transfer-resume-request                     sender → receiver: please send transfer-resume
//   file-resume-start {offset}                  sender → receiver: chunks from offset follow
// Binary chunks carry no header: they belong to the file most recently started
// (or resumed) by that peer. A sender therefore sends one file at a time, and a
// receiver drops chunks between asking to resume and file-resume-start (they
// are leftovers from before the pause and will be sent again).
async function sendFiles(peerId, files) {
    const dataChannel = state.dataChannels.get(peerId);

    if (!dataChannel || dataChannel.readyState !== 'open') {
        showToast('Connection not ready. Establishing connection...', 'warning');
        await connectToPeer(peerId);

        // Wait for connection
        setTimeout(() => {
            const dc = state.dataChannels.get(peerId);
            if (dc && dc.readyState === 'open') {
                sendFiles(peerId, files);
            } else {
                showToast('Failed to establish connection', 'error');
            }
        }, 3000);
        return;
    }

    // Locality check after connection is established (verifyLocality runs on 'connected')
    // Explains itself (toast) or asks the user — nothing more to say on refusal
    const localityOk = await checkLocalityBeforeTransfer(peerId);
    if (!localityOk) {
        return;
    }

    // Add files to queue
    for (const file of files) {
        const transferId = generateTransferId();
        const transfer = {
            id: transferId,
            file: file,
            fileName: file.name,
            fileSize: file.size,
            peerId: peerId,
            direction: 'send',
            progress: 0,
            bytesTransferred: 0,
            status: 'queued',
            startTime: null,
            startBytes: 0
        };

        state.transferQueue.push(transfer);
        state.activeTransfers.set(transferId, transfer);
        addTransferToUI(transfer);
    }

    // Process queue
    processTransferQueue();
}

// Start a send worker for every peer that has queued files
function processTransferQueue() {
    const peers = new Set(state.transferQueue
        .filter(t => t.status === 'queued')
        .map(t => t.peerId));
    peers.forEach(peerId => runSendWorker(peerId));
}

// Send a peer's queued files strictly one after another (see protocol above)
async function runSendWorker(peerId) {
    if (state.sendWorkers.has(peerId)) return; // the running worker picks new files up
    state.sendWorkers.add(peerId);
    try {
        let transfer;
        while ((transfer = state.transferQueue.find(t => t.peerId === peerId && t.status === 'queued'))) {
            await startFileSend(transfer);
            const dc = state.dataChannels.get(peerId);
            if (!dc || dc.readyState !== 'open') break; // resumes once reconnected
        }
    } finally {
        state.sendWorkers.delete(peerId);
    }
}

function removeFromQueue(transfer) {
    const index = state.transferQueue.indexOf(transfer);
    if (index > -1) {
        state.transferQueue.splice(index, 1);
    }
}

// A send stopped on an error: if the channel is gone it can be resumed later,
// otherwise it failed.
function markSendStopped(transfer, dataChannel) {
    if (dataChannel.readyState !== 'open') {
        transfer.status = 'paused';
        transfer.interrupted = true;
    } else {
        transfer.status = 'failed';
        showToast(`Failed to send ${transfer.fileName}`, 'error');
    }
    updateTransferUI(transfer);
}

// Send one file (or the rest of it, when resuming)
async function startFileSend(transfer) {
    const dataChannel = state.dataChannels.get(transfer.peerId);

    if (!dataChannel || dataChannel.readyState !== 'open') {
        transfer.status = 'paused';
        transfer.interrupted = true;
        updateTransferUI(transfer);
        return;
    }

    const resuming = transfer.resumeOffset !== undefined;
    const startOffset = resuming ? transfer.resumeOffset : 0;
    delete transfer.resumeOffset;

    transfer.status = 'sending';
    transfer.interrupted = false;
    transfer.bytesTransferred = startOffset;
    transfer.startTime = Date.now();
    transfer.startBytes = startOffset;
    updateTransferUI(transfer);

    // Compute effective chunk size for this connection
    const effectiveChunkSize = getEffectiveChunkSize(transfer.peerId);
    console.log(`Using chunk size ${effectiveChunkSize} for transfer ${transfer.id}`);

    try {
        if (resuming) {
            await sendWithFlowControl(dataChannel, JSON.stringify({
                type: 'file-resume-start',
                transferId: transfer.id,
                offset: startOffset
            }));
        } else {
            // File metadata as JSON (chunkSize tells the receiver the boundaries)
            await sendWithFlowControl(dataChannel, JSON.stringify({
                type: 'file-start',
                transferId: transfer.id,
                name: transfer.file.name,
                size: transfer.file.size,
                mimeType: transfer.file.type,
                lastModified: transfer.file.lastModified,
                chunkSize: effectiveChunkSize
            }));
        }
    } catch (error) {
        markSendStopped(transfer, dataChannel);
        return;
    }

    let offset = startOffset;
    while (offset < transfer.file.size) {
        // Paused or cancelled by either side — stop after the chunk in flight
        if (transfer.status !== 'sending') return;

        let buffer;
        try {
            buffer = await transfer.file.slice(offset, offset + effectiveChunkSize).arrayBuffer();
            if (transfer.status !== 'sending') return;
            // Send only binary data (no per-chunk JSON metadata)
            await sendWithFlowControl(dataChannel, buffer);
        } catch (error) {
            console.error('Error sending chunk:', error);
            markSendStopped(transfer, dataChannel);
            return;
        }

        offset += buffer.byteLength;
        transfer.bytesTransferred = offset;
        transfer.progress = (offset / transfer.file.size) * 100;
        updateTransferUI(transfer);
    }

    try {
        await sendWithFlowControl(dataChannel, JSON.stringify({
            type: 'file-end',
            transferId: transfer.id,
            totalBytes: offset
        }));
    } catch (error) {
        markSendStopped(transfer, dataChannel);
        return;
    }

    transfer.status = 'completed';
    transfer.progress = 100;
    updateTransferUI(transfer);
    showToast(`Sent ${transfer.fileName}`, 'success');
    // Re-added if the receiver turns out to be missing data and asks to resume
    removeFromQueue(transfer);
}

// Get effective chunk size for a connection, respecting SCTP maxMessageSize
function getEffectiveChunkSize(peerId) {
    const connection = state.connections.get(peerId);
    if (connection && connection.sctp && connection.sctp.maxMessageSize) {
        const maxMsg = connection.sctp.maxMessageSize;
        const safeMax = maxMsg - 16384; // safety margin for SCTP overhead
        if (safeMax <= 0) return 16 * 1024; // very small limit, use conservative 16KB
        return Math.min(safeMax, 256 * 1024); // cap at 256KB, never exceed safeMax
    }
    return CONFIG.chunkSize; // fallback to default 64KB
}

// Send data with flow control to prevent buffer overflow
async function sendWithFlowControl(dataChannel, data) {
    if (dataChannel.readyState !== 'open') {
        throw new Error('DataChannel is not open');
    }

    while (dataChannel.bufferedAmount > CONFIG.bufferThreshold) {
        await new Promise(resolve => {
            dataChannel.bufferedAmountLowThreshold = CONFIG.bufferThreshold;
            // addEventListener rather than onbufferedamountlow: a file transfer and a
            // shared-text send may wait on the same channel at once, and a single
            // handler property would let one overwrite (and stall) the other.
            dataChannel.addEventListener('bufferedamountlow', resolve, { once: true });
            // ...and don't wait forever if the channel closes meanwhile
            dataChannel.addEventListener('close', resolve, { once: true });
        });

        if (dataChannel.readyState !== 'open') {
            throw new Error('DataChannel is not open');
        }
    }

    dataChannel.send(data);
}

// Handle incoming data channel messages
function handleDataChannelMessage(event, peerId) {
    // Handle binary data (file chunks)
    if (event.data instanceof ArrayBuffer) {
        handleBinaryData(event.data, peerId);
        return;
    }

    // Handle JSON messages
    try {
        const message = JSON.parse(event.data);

        switch (message.type) {
            case 'file-start':
                handleFileStart(message, peerId);
                break;

            case 'file-resume-start':
                handleFileResumeStart(message, peerId);
                break;

            case 'file-end':
                handleFileEnd(message, peerId);
                break;

            case 'transfer-cancel':
                handleTransferCancel(message, peerId);
                break;

            case 'transfer-pause':
                handleTransferPause(message, peerId);
                break;

            case 'transfer-resume':
                handleTransferResume(message, peerId);
                break;

            case 'transfer-resume-request':
                handleTransferResumeRequest(message, peerId);
                break;

            case 'text-sync':
                handleP2PTextChunk(message, peerId);
                break;

            case 'verify-commit':
            case 'verify-nonce':
            case 'verify-reveal':
                handleVerifyMessage(message, peerId);
                break;
        }
    } catch (error) {
        console.error('Error handling data channel message:', error);
    }
}

// Find a transfer that belongs to this peer (ids come from the other side)
function getPeerTransfer(transferId, peerId, direction) {
    const transfer = state.activeTransfers.get(transferId);
    if (!transfer || transfer.peerId !== peerId || transfer.direction !== direction) return null;
    return transfer;
}

// Handle binary data (actual file chunks - no per-chunk metadata needed)
function handleBinaryData(data, peerId) {
    const transfer = state.activeTransfers.get(state.currentReceive.get(peerId));

    // Chunks still in flight after a pause are kept ('paused'); after cancel,
    // failure or a resume request they are dropped.
    if (!transfer || transfer.awaitingResume ||
        (transfer.status !== 'receiving' && transfer.status !== 'paused')) {
        return;
    }

    const chunkData = new Uint8Array(data);
    transfer.bytesReceived += chunkData.byteLength;
    transfer.progress = transfer.fileSize ? (transfer.bytesReceived / transfer.fileSize) * 100 : 100;

    if (transfer.writableStream) {
        // Streaming to disk: serialize writes via a promise chain
        const stream = transfer.writableStream;
        transfer.writeChain = (transfer.writeChain || Promise.resolve())
            .then(() => stream.write(chunkData))
            .catch(err => {
                console.error('Error writing to stream:', err);
                transfer.status = 'failed';
                updateTransferUI(transfer);
            });
    } else {
        // Accumulate in memory
        transfer.chunks.push(chunkData);
    }
    updateTransferUI(transfer);
}

// Handle incoming file start
function handleFileStart(message, peerId) {
    // The id ends up in element ids, selectors and onclick handlers — only
    // accept the shape our own generateTransferId() produces.
    if (typeof message.transferId !== 'string' || !/^[A-Za-z0-9_-]{1,100}$/.test(message.transferId) ||
        typeof message.name !== 'string' || !Number.isFinite(message.size) || message.size < 0) {
        console.warn('Ignoring malformed file-start from', peerId);
        return;
    }

    const canSaveToDisk = message.size > 100 * 1024 * 1024 && typeof window.showSaveFilePicker === 'function';
    const transfer = {
        id: message.transferId,
        peerId: peerId,
        direction: 'receive',
        fileName: message.name,
        fileSize: message.size,
        mimeType: typeof message.mimeType === 'string' ? message.mimeType : '',
        progress: 0,
        bytesReceived: 0,
        status: 'receiving',
        startTime: Date.now(),
        startBytes: 0,
        chunks: [],
        writableStream: null,
        writeChain: null,
        canSaveToDisk: canSaveToDisk,
        awaitingResume: false
    };

    // Register transfer FIRST so incoming binary chunks are not lost
    state.activeTransfers.set(message.transferId, transfer);
    state.currentReceive.set(peerId, message.transferId);
    addTransferToUI(transfer);

    showToast(`Receiving ${message.name} from ${state.peers.get(peerId)?.name || 'device'}`, 'info');

    // LAN only: refuse it if the sender's route turns out not to be local
    if (state.lanOnly) {
        enforceLanForIncoming(transfer);
    }

    // Saving straight to disk needs a click (browsers only open the save
    // dialog on a user gesture), so offer a button; meanwhile chunks collect in memory.
    if (canSaveToDisk) {
        showToast(`${message.name} is large — click "Save to disk" in the queue to stream it to a file`, 'info');
    } else if (message.size > 500 * 1024 * 1024) {
        showToast(`Warning: ${message.name} is very large (${formatFileSize(message.size)}). May use significant memory.`, 'warning');
    }
}

// "Save to disk" button: switch a running receive to streaming into a file.
// Chunks received so far are flushed first.
async function saveTransferToDisk(transferId) {
    const transfer = state.activeTransfers.get(transferId);
    if (!transfer || !transfer.canSaveToDisk || transfer.writableStream) return;

    let writable;
    try {
        const fileHandle = await window.showSaveFilePicker({ suggestedName: transfer.fileName });
        writable = await fileHandle.createWritable();
    } catch (err) {
        // User cancelled the dialog — stay in memory mode, button stays available
        console.log('Save dialog cancelled, keeping the file in memory');
        return;
    }

    // The transfer may have finished or stopped while the dialog was open
    if (transfer.status !== 'receiving' && transfer.status !== 'paused') {
        try { await writable.abort(); } catch (_) { /* nothing to clean up */ }
        return;
    }

    // Switch to streaming mode FIRST, so new chunks go through writeChain
    // instead of appending to transfer.chunks
    transfer.writableStream = writable;
    transfer.canSaveToDisk = false;

    // Snapshot the accumulated chunks and clear the array atomically
    const pendingChunks = transfer.chunks;
    transfer.chunks = [];

    // Flush pending chunks through the write chain so they serialize
    // with any new chunks arriving via handleBinaryData
    let flushChain = transfer.writeChain || Promise.resolve();
    for (const chunk of pendingChunks) {
        flushChain = flushChain.then(() => writable.write(chunk));
    }
    transfer.writeChain = flushChain.catch(err => {
        console.error('Error writing to stream:', err);
        transfer.status = 'failed';
        updateTransferUI(transfer);
    });

    updateTransferUI(transfer);
    console.log(`Streaming ${transfer.fileName} to disk (flushed ${pendingChunks.length} buffered chunks)`);
}

// Throw away whatever a receive has collected (cancel / failure)
async function discardReceivedData(transfer) {
    transfer.chunks = [];
    if (transfer.writableStream) {
        const stream = transfer.writableStream;
        transfer.writableStream = null;
        try {
            await (transfer.writeChain || Promise.resolve());
            await stream.abort(); // removes the partial file
        } catch (_) { /* already closed */ }
    }
}

// Handle file end
async function handleFileEnd(message, peerId) {
    const transfer = getPeerTransfer(message.transferId, peerId, 'receive');
    if (!transfer) {
        console.error('Transfer not found:', message.transferId);
        return;
    }
    // End of a stream we already asked to restart — the resumed stream sends its own
    if (transfer.awaitingResume || (transfer.status !== 'receiving' && transfer.status !== 'paused')) {
        return;
    }
    if (state.currentReceive.get(peerId) === transfer.id) {
        state.currentReceive.delete(peerId);
    }

    // Wait for all pending disk writes before checking anything
    if (transfer.writeChain) {
        await transfer.writeChain;
    }

    // Integrity check BEFORE handing the file over
    const expected = Number.isFinite(message.totalBytes) ? message.totalBytes : transfer.fileSize;
    if (transfer.status === 'failed' || transfer.bytesReceived !== expected || expected !== transfer.fileSize) {
        console.error(`Size mismatch: received ${transfer.bytesReceived}, expected ${transfer.fileSize}`);
        transfer.status = 'failed';
        await discardReceivedData(transfer);
        updateTransferUI(transfer);
        showToast(`Transfer corrupted: ${transfer.fileName} (incomplete)`, 'error');
        return;
    }

    if (transfer.writableStream) {
        try {
            await transfer.writableStream.close();
        } catch (err) {
            console.error('Error closing writable stream:', err);
            transfer.status = 'failed';
            updateTransferUI(transfer);
            showToast(`Failed to save ${transfer.fileName}`, 'error');
            return;
        }
        transfer.writableStream = null;
        transfer.writeChain = null;
    } else {
        // In-memory mode: combine chunks and trigger download
        const blob = new Blob(transfer.chunks, { type: transfer.mimeType });

        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = transfer.fileName;
        a.click();

        // Cleanup
        setTimeout(() => URL.revokeObjectURL(url), 1000);
        transfer.chunks = []; // Free memory
    }

    transfer.status = 'completed';
    transfer.progress = 100;
    transfer.canSaveToDisk = false;
    updateTransferUI(transfer);

    showToast(`Received ${transfer.fileName}`, 'success');
}

// Receiver side: ask the sender to continue from what actually arrived
function requestResume(transfer) {
    const dataChannel = state.dataChannels.get(transfer.peerId);
    if (!dataChannel || dataChannel.readyState !== 'open') {
        showToast('Device is not connected — reconnect it to resume', 'warning');
        return;
    }
    // Until the sender's file-resume-start arrives, leftover chunks are dropped
    transfer.awaitingResume = true;
    transfer.interrupted = false;
    dataChannel.send(JSON.stringify({
        type: 'transfer-resume',
        transferId: transfer.id,
        offset: transfer.bytesReceived
    }));
    updateTransferUI(transfer);
}

// Receiver: the sender restarts the stream at `offset`
function handleFileResumeStart(message, peerId) {
    const transfer = getPeerTransfer(message.transferId, peerId, 'receive');
    if (!transfer || !transfer.awaitingResume) return;

    if (message.offset !== transfer.bytesReceived) {
        // Should not happen — the offset is ours. Don't risk a corrupt file.
        transfer.status = 'failed';
        discardReceivedData(transfer);
        updateTransferUI(transfer);
        notifyPeer(transfer, 'transfer-cancel');
        showToast(`Could not resume ${transfer.fileName}`, 'error');
        return;
    }

    transfer.awaitingResume = false;
    transfer.status = 'receiving';
    transfer.startTime = Date.now();
    transfer.startBytes = transfer.bytesReceived;
    state.currentReceive.set(peerId, transfer.id);
    updateTransferUI(transfer);
}

// Sender: the receiver wants the rest of the file from `offset`
function handleTransferResume(message, peerId) {
    const transfer = getPeerTransfer(message.transferId, peerId, 'send');
    if (!transfer || transfer.status === 'cancelled') return;

    const offset = message.offset;
    if (!Number.isInteger(offset) || offset < 0 || offset > transfer.file.size) return;

    transfer.resumeOffset = offset;
    transfer.status = 'queued';
    if (!state.transferQueue.includes(transfer)) {
        state.transferQueue.push(transfer);
    }
    updateTransferUI(transfer);
    processTransferQueue();
}

// Receiver: the sender pressed Resume
function handleTransferResumeRequest(message, peerId) {
    const transfer = getPeerTransfer(message.transferId, peerId, 'receive');
    if (transfer && transfer.status === 'paused' && !transfer.awaitingResume) {
        requestResume(transfer);
    }
}

function notifyPeer(transfer, type, extra = {}) {
    const dataChannel = state.dataChannels.get(transfer.peerId);
    if (dataChannel && dataChannel.readyState === 'open') {
        dataChannel.send(JSON.stringify({ ...extra, type, transferId: transfer.id }));
    }
}

// Pause (button) — both sides stop; Resume continues where it left off
function pauseTransfer(transferId) {
    const transfer = state.activeTransfers.get(transferId);
    if (!transfer || (transfer.status !== 'sending' && transfer.status !== 'receiving')) return;

    transfer.status = 'paused';
    transfer.interrupted = false;
    updateTransferUI(transfer);
    notifyPeer(transfer, 'transfer-pause');
}

function handleTransferPause(message, peerId) {
    const transfer = state.activeTransfers.get(message.transferId);
    if (!transfer || transfer.peerId !== peerId) return;
    if (transfer.status === 'sending' || transfer.status === 'receiving') {
        transfer.status = 'paused';
        transfer.interrupted = false;
        updateTransferUI(transfer);
        showToast(`Transfer paused by ${state.peers.get(peerId)?.name || 'device'}`, 'info');
    }
}

// Resume (button)
function resumeTransfer(transferId) {
    const transfer = state.activeTransfers.get(transferId);

    if (!transfer || transfer.status !== 'paused') {
        return;
    }

    if (transfer.direction === 'receive') {
        if (!transfer.awaitingResume) requestResume(transfer);
    } else {
        // The receiver decides the offset, so ask it to request the resume
        const dataChannel = state.dataChannels.get(transfer.peerId);
        if (!dataChannel || dataChannel.readyState !== 'open') {
            showToast('Device is not connected — reconnect it to resume', 'warning');
            return;
        }
        notifyPeer(transfer, 'transfer-resume-request');
    }
}

// Cancel transfer
function cancelTransfer(transferId) {
    const transfer = state.activeTransfers.get(transferId);

    if (!transfer) {
        return;
    }

    transfer.status = 'cancelled';
    updateTransferUI(transfer);
    notifyPeer(transfer, 'transfer-cancel');
    stopTransferLocally(transfer);

    // Remove from UI after delay
    setTimeout(() => {
        removeTransferFromUI(transfer.id);
        state.activeTransfers.delete(transfer.id);
    }, 3000);
}

// Free what a stopped transfer holds
function stopTransferLocally(transfer) {
    removeFromQueue(transfer);
    if (transfer.direction === 'receive') {
        discardReceivedData(transfer);
        if (state.currentReceive.get(transfer.peerId) === transfer.id) {
            state.currentReceive.delete(transfer.peerId);
        }
    }
}

// Handle transfer cancel from peer
function handleTransferCancel(message, peerId) {
    const transfer = state.activeTransfers.get(message.transferId);

    if (transfer && transfer.peerId === peerId &&
        transfer.status !== 'completed' && transfer.status !== 'cancelled') {
        transfer.status = 'cancelled';
        stopTransferLocally(transfer);
        updateTransferUI(transfer);
        const peerName = state.peers.get(peerId)?.name || 'device';
        showToast(message.reason === 'lan-only'
            ? `${peerName} only accepts files over the local network (LAN only)`
            : `Transfer cancelled by ${peerName}`, 'warning');
    }
}

// Mark running transfers with a peer as interrupted (connection dropped);
// they resume automatically when a new data channel opens.
function interruptTransfers(peerId) {
    state.activeTransfers.forEach(transfer => {
        if (transfer.peerId === peerId && (transfer.status === 'sending' || transfer.status === 'receiving')) {
            transfer.status = 'paused';
            transfer.interrupted = true;
            updateTransferUI(transfer);
        }
    });
}

// Handle peer disconnect
function handlePeerDisconnect(peerId) {
    const peerName = state.peers.get(peerId)?.name || 'Device';

    state.peers.delete(peerId);
    state.peerLocality.delete(peerId);
    state.peerReconnects.delete(peerId);
    state.peerIdentity.delete(peerId);
    state.verifyInbox.delete(peerId);
    state.untrustedPeers.delete(peerId);
    state.localityChecks.delete(peerId);
    state.sharedText.p2pIncoming.delete(peerId);

    // Close connection
    const connection = state.connections.get(peerId);
    if (connection) {
        connection.close();
        state.connections.delete(peerId);
    }

    // Remove data channel
    state.dataChannels.delete(peerId);

    // The peer's session is gone for good (a returning device gets a new id),
    // so its unfinished transfers can't be resumed
    state.activeTransfers.forEach(transfer => {
        if (transfer.peerId === peerId &&
            !['completed', 'cancelled', 'failed'].includes(transfer.status)) {
            transfer.status = 'failed';
            stopTransferLocally(transfer);
            updateTransferUI(transfer);
        }
    });

    updateDevicesList();
    showToast(`${peerName} disconnected`, 'info');
}

// Handle a WebRTC connection that went 'disconnected' or 'failed'
function handleConnectionFailure(peerId, connection) {
    // 'disconnected' often recovers by itself (e.g. a Wi-Fi blip) — give it time
    const grace = connection.connectionState === 'disconnected' ? 5000 : 0;

    setTimeout(() => {
        if (state.connections.get(peerId) !== connection) return; // already replaced
        if (connection.connectionState === 'connected') return;  // recovered

        console.log(`Connection with ${peerId} lost, reconnecting...`);
        connection.close();
        state.connections.delete(peerId);
        state.dataChannels.delete(peerId);
        interruptTransfers(peerId);
        updateDevicesList();

        const attempts = (state.peerReconnects.get(peerId) || 0) + 1;
        if (!state.peers.has(peerId)) return;
        if (attempts > 3) {
            showToast(`Lost connection to ${state.peers.get(peerId)?.name || 'device'}. Click it to reconnect.`, 'error');
            state.peerReconnects.delete(peerId);
            return;
        }
        state.peerReconnects.set(peerId, attempts);

        // Attempt reconnection (unless the other side already did)
        setTimeout(() => {
            if (state.peers.has(peerId) && !state.connections.has(peerId)) {
                connectToPeer(peerId);
            }
        }, CONFIG.reconnectDelay);
    }, grace);
}

// UI Functions
function updateDevicesList() {
    const devicesList = document.getElementById('devicesList');
    
    if (state.peers.size === 0) {
        devicesList.innerHTML = `
            <div class="no-devices">
                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                    <rect x="5" y="2" width="14" height="20" rx="2" ry="2"></rect>
                    <line x1="12" y1="18" x2="12" y2="18"></line>
                </svg>
                <p>No other devices found</p>
                <small>Make sure devices are on the same network</small>
                <small class="mobile-hint">On mobile? Ask others to share the room link</small>
            </div>
        `;
        return;
    }
    
    devicesList.innerHTML = '';
    
    state.peers.forEach((peer, peerId) => {
        const isConnected = state.dataChannels.has(peerId) && 
                          state.dataChannels.get(peerId).readyState === 'open';
        
        const deviceCard = document.createElement('div');
        deviceCard.className = 'device-card';
        deviceCard.dataset.peerId = peerId;
        
        if (isConnected) {
            deviceCard.classList.add('active');
        }
        
        const locality = state.peerLocality.get(peerId);
        let localityIndicator = '';
        if (isConnected && locality === 'verified') {
            localityIndicator = '<span class="locality-badge verified" title="Verified local connection">&#x1f512;</span>';
        } else if (isConnected && locality === 'confirmed') {
            localityIndicator = '<span class="locality-badge confirmed" title="User confirmed local">&#x26a0;</span>';
        } else if (isConnected && locality === 'blocked') {
            localityIndicator = '<span class="locality-badge blocked" title="Relay detected - blocked">&#x1f6ab;</span>';
        } else if (isConnected && locality === 'unverified') {
            localityIndicator = '<span class="locality-badge remote" title="Not verified to be on your local network">&#x1f310;</span>';
        }

        // With LAN only on, only verified-local peers can be sent to
        const lanBlocked = isConnected && state.lanOnly && locality !== undefined &&
            locality !== 'verified';

        let statusText = 'Click to connect';
        if (isConnected && locality === 'blocked') {
            statusText = 'Blocked (not local)';
        } else if (lanBlocked) {
            statusText = 'Not on your local network';
        } else if (isConnected) {
            statusText = 'Click to send files';
        } else if (state.untrustedPeers.has(peerId)) {
            statusText = 'Codes didn\'t match — not connected';
        }

        // Verification (see "Connection verification")
        const identity = state.peerIdentity.get(peerId);
        let identityControl = '';
        if (isConnected && identity && identity.status === 'verified') {
            identityControl = '<button class="verify-btn verified" title="You confirmed the codes match">&#x2714; Verified</button>';
        } else if (isConnected && identity && identity.status === 'failed') {
            identityControl = '<button class="verify-btn failed" title="Verification failed">&#x26a0; Not trusted</button>';
        } else if (isConnected) {
            identityControl = '<button class="verify-btn" title="Compare a code on both devices">Verify</button>';
        }

        deviceCard.innerHTML = `
            <div class="device-icon">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <rect x="5" y="2" width="14" height="20" rx="2" ry="2"></rect>
                    <line x1="12" y1="18" x2="12" y2="18"></line>
                </svg>
            </div>
            <div class="device-name">${escapeHtml(peer.name)} ${localityIndicator}</div>
            <div class="device-status">${statusText}</div>
            ${identityControl}
        `;

        deviceCard.addEventListener('click', (e) => {
            if (e.target.closest('.verify-btn')) {
                openVerifyDialog(peerId);
                return;
            }
            if (!isConnected) {
                if (state.untrustedPeers.has(peerId)) {
                    if (!confirm('The verification codes for this device did not match last time, so someone may be intercepting the connection. Connect anyway?')) return;
                    state.untrustedPeers.delete(peerId);
                }
                connectToPeer(peerId);
            } else if (lanBlocked) {
                showToast(`${peer.name} is not verified to be on your local network. Turn off "LAN only" to send over the internet.`, 'warning');
            } else {
                // Show file selection dialog for connected device
                openFilePicker(peerId);
            }
        });
        
        devicesList.appendChild(deviceCard);
    });
}

// Open file picker for sending files
function openFilePicker(peerId) {
    state.selectedPeer = peerId;
    const peer = state.peers.get(peerId);
    
    // Show drop zone
    const dropZone = document.getElementById('dropZone');
    const dropTargetName = document.getElementById('dropTargetName');
    dropTargetName.textContent = peer?.name || 'device';
    
    // Clear preview area
    clearPreviews();
    document.getElementById('sendFilesBtn').style.display = 'none';

    dropZone.classList.remove('hidden');
}

// Image previews hold object URLs — release them along with the previews
const previewUrls = [];

function clearPreviews() {
    previewUrls.splice(0).forEach(url => URL.revokeObjectURL(url));
    document.getElementById('previewArea').innerHTML = '';
}

// Handle file selection
function handleFileSelection(files, peerId) {
    if (files.length === 0) return;

    const previewArea = document.getElementById('previewArea');
    const sendFilesBtn = document.getElementById('sendFilesBtn');

    // Clear previous previews
    clearPreviews();
    
    // Create previews
    Array.from(files).forEach(file => {
        const preview = document.createElement('div');
        preview.className = 'file-preview';
        
        if (file.type.startsWith('image/')) {
            const img = document.createElement('img');
            img.src = URL.createObjectURL(file);
            previewUrls.push(img.src);
            preview.appendChild(img);
        } else {
            const icon = document.createElement('div');
            icon.className = 'file-preview-icon';
            icon.innerHTML = `
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path>
                    <polyline points="13 2 13 9 20 9"></polyline>
                </svg>
            `;
            preview.appendChild(icon);
        }
        
        const info = document.createElement('div');
        info.className = 'file-info';
        info.innerHTML = `
            <div class="file-name">${escapeHtml(file.name)}</div>
            <div class="file-size">${formatFileSize(file.size)}</div>
        `;
        preview.appendChild(info);
        
        previewArea.appendChild(preview);
    });
    // Store files for sending
    state.pendingFiles.set(peerId, Array.from(files));
    
    // Show send button
    sendFilesBtn.style.display = 'inline-block';
}

function updateConnectionStatus(connected) {
    const statusDot = document.querySelector('.status-dot');
    const statusText = document.getElementById('statusText');
    
    if (connected) {
        statusDot.classList.add('connected');
        statusText.textContent = 'Connected';
    } else {
        statusDot.classList.remove('connected');
        statusText.textContent = 'Disconnected';
    }
}

function addTransferToUI(transfer) {
    const queueSection = document.getElementById('queueSection');
    const transferQueue = document.getElementById('transferQueue');
    
    queueSection.style.display = 'block';
    
    const transferItem = document.createElement('div');
    transferItem.className = 'transfer-item';
    transferItem.dataset.transferId = transfer.id;
    
    const fileName = transfer.fileName || transfer.file?.name || 'Unknown';
    const fileSize = transfer.fileSize || transfer.file?.size || 0;
    const peerName = state.peers.get(transfer.peerId)?.name || 'Device';
    
    transferItem.innerHTML = `
        <div class="transfer-header">
            <div class="transfer-info">
                <div class="transfer-filename">${escapeHtml(fileName)}</div>
                <div class="transfer-details">
                    ${transfer.direction === 'send' ? 'To' : 'From'} ${escapeHtml(peerName)} • ${formatFileSize(fileSize)}
                </div>
                <div class="transfer-speed" id="speed-${transfer.id}"></div>
            </div>
            <div class="transfer-status ${transfer.status}">${transfer.status}</div>
        </div>
        <div class="progress-bar">
            <div class="progress-fill" style="width: ${transfer.progress}%"></div>
        </div>
        <div class="progress-text" id="progress-${transfer.id}">0% • 0 B / ${formatFileSize(fileSize)}</div>
        <div class="transfer-actions"></div>
    `;

    // Newest on top, so the transfer you just started is always visible
    transferQueue.prepend(transferItem);
    updateTransferUI(transfer);
}

function updateTransferUI(transfer) {
    const transferItem = document.querySelector(`[data-transfer-id="${transfer.id}"]`);
    
    if (!transferItem) {
        return;
    }
    
    const statusElement = transferItem.querySelector('.transfer-status');
    const progressFill = transferItem.querySelector('.progress-fill');
    const progressText = document.getElementById(`progress-${transfer.id}`);
    const speedElement = document.getElementById(`speed-${transfer.id}`);
    const actionsDiv = transferItem.querySelector('.transfer-actions');
    
    const statusLabel = transfer.awaitingResume ? 'resuming'
        : (transfer.status === 'paused' && transfer.interrupted) ? 'interrupted'
        : transfer.status;
    statusElement.className = `transfer-status ${transfer.status}`;
    statusElement.textContent = statusLabel;
    progressFill.style.width = `${transfer.progress}%`;

    // Update progress text
    const transferred = transfer.bytesTransferred || transfer.bytesReceived || 0;
    const total = transfer.file?.size || transfer.fileSize || 0;
    progressText.textContent = `${Math.round(transfer.progress)}% • ${formatFileSize(transferred)} / ${formatFileSize(total)}`;

    // Calculate and show speed (since the transfer last started or resumed)
    if (transfer.startTime && (transfer.status === 'sending' || transfer.status === 'receiving')) {
        const elapsed = (Date.now() - transfer.startTime) / 1000; // seconds
        const speed = (transferred - (transfer.startBytes || 0)) / elapsed; // bytes per second
        const remaining = (total - transferred) / speed; // seconds

        speedElement.textContent = `${formatFileSize(speed)}/s • ${formatTime(remaining)} remaining`;
    } else {
        speedElement.textContent = '';
    }

    // Update actions
    let actionsHTML = '';
    if (transfer.canSaveToDisk && !transfer.writableStream &&
        (transfer.status === 'receiving' || transfer.status === 'paused')) {
        actionsHTML += `<button onclick="saveTransferToDisk('${transfer.id}')">Save to disk</button>`;
    }
    if (transfer.status === 'paused' && !transfer.awaitingResume) {
        actionsHTML += `<button onclick="resumeTransfer('${transfer.id}')">Resume</button>`;
    } else if (transfer.status === 'sending' || transfer.status === 'receiving') {
        actionsHTML += `<button onclick="pauseTransfer('${transfer.id}')">Pause</button>`;
    }

    if (transfer.status !== 'completed' && transfer.status !== 'cancelled') {
        actionsHTML += `<button onclick="cancelTransfer('${transfer.id}')">${transfer.status === 'failed' ? 'Dismiss' : 'Cancel'}</button>`;
    }

    // Only touch the buttons when they change — this runs for every chunk, and
    // re-creating a button under the cursor would swallow the click
    if (actionsDiv.dataset.html !== actionsHTML) {
        actionsDiv.dataset.html = actionsHTML;
        actionsDiv.innerHTML = actionsHTML;
    }
}

// Format time in seconds to readable format
function formatTime(seconds) {
    if (!isFinite(seconds) || seconds < 0) return '...';
    
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    if (hours > 0) {
        return `${hours}h ${minutes}m`;
    } else if (minutes > 0) {
        return `${minutes}m ${secs}s`;
    } else {
        return `${secs}s`;
    }
}

function removeTransferFromUI(transferId) {
    const transferItem = document.querySelector(`[data-transfer-id="${transferId}"]`);
    
    if (transferItem) {
        transferItem.remove();
    }
    
    // Hide queue section if empty
    const transferQueue = document.getElementById('transferQueue');
    if (transferQueue.children.length === 0) {
        document.getElementById('queueSection').style.display = 'none';
    }
}

function showToast(message, type = 'info') {
    const toastContainer = document.getElementById('toastContainer');
    
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    
    toastContainer.appendChild(toast);
    
    setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

// Drag and Drop + File Picker
function setupDragAndDrop() {
    const dropZone = document.getElementById('dropZone');
    const previewArea = document.getElementById('previewArea');
    const sendFilesBtn = document.getElementById('sendFilesBtn');
    const selectFilesBtn = document.getElementById('selectFilesBtn');
    const cancelDropBtn = document.getElementById('cancelDropBtn');
    const fileInput = document.getElementById('fileInput');

    // File input change handler
    fileInput.addEventListener('change', (e) => {
        const files = e.target.files;
        if (files.length > 0 && state.selectedPeer) {
            handleFileSelection(files, state.selectedPeer);
        }
        // Reset input for next selection
        fileInput.value = '';
    });
    
    // Select files button
    selectFilesBtn.addEventListener('click', () => {
        fileInput.click();
    });
    
    // Prevent default drag behaviors
    document.addEventListener('dragover', (e) => {
        e.preventDefault();
    });
    
    document.addEventListener('drop', (e) => {
        e.preventDefault();
    });
    
    // Device card drag events
    document.addEventListener('dragenter', (e) => {
        const deviceCard = e.target.closest('.device-card');
        if (deviceCard) {
            const peerId = deviceCard.dataset.peerId;
            const isConnected = state.dataChannels.has(peerId) && 
                              state.dataChannels.get(peerId).readyState === 'open';
            
            if (isConnected) {
                document.body.classList.add('dragging');
                deviceCard.classList.add('drag-over');
            }
        }
    });
    
    document.addEventListener('dragleave', (e) => {
        const deviceCard = e.target.closest('.device-card');
        if (deviceCard) {
            deviceCard.classList.remove('drag-over');
        }
    });
    
    document.addEventListener('drop', async (e) => {
        e.preventDefault();
        document.body.classList.remove('dragging');
        
        const deviceCard = e.target.closest('.device-card');
        if (!deviceCard) {
            return;
        }
        
        deviceCard.classList.remove('drag-over');
        
        const peerId = deviceCard.dataset.peerId;
        const isConnected = state.dataChannels.has(peerId) && 
                          state.dataChannels.get(peerId).readyState === 'open';
        
        if (!isConnected) {
            showToast('Please connect to the device first', 'warning');
            return;
        }
        
        const files = Array.from(e.dataTransfer.files);
        
        if (files.length === 0) {
            return;
        }
        
        state.selectedPeer = peerId;

        // Show drop zone with preview
        openFilePicker(peerId);
        handleFileSelection(files, peerId);
    });
    
    // Drop zone drag events for additional files
    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        e.stopPropagation();
        dropZone.classList.add('drag-active');
    });
    
    dropZone.addEventListener('dragleave', (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.target === dropZone) {
            dropZone.classList.remove('drag-active');
        }
    });
    
    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        e.stopPropagation();
        dropZone.classList.remove('drag-active');
        
        const files = Array.from(e.dataTransfer.files);
        if (files.length > 0 && state.selectedPeer) {
            handleFileSelection(files, state.selectedPeer);
        }
    });
    
    // Send files button
    sendFilesBtn.addEventListener('click', async () => {
        const peerId = state.selectedPeer;
        const files = state.pendingFiles.get(peerId);

        if (files && files.length > 0 && peerId) {
            await sendFiles(peerId, files);
            hideDropZone();
        }
    });
    
    // Cancel button
    cancelDropBtn.addEventListener('click', () => {
        hideDropZone();
    });
    
    function hideDropZone() {
        dropZone.classList.add('hidden');
        clearPreviews();
        state.selectedPeer = null;
        // Queued transfers keep their own File references
        state.pendingFiles.clear();
    }
}

// Event listeners
function setupEventListeners() {
    // Room management
    const createRoomBtn = document.getElementById('createRoomBtn');
    const joinRoomBtn = document.getElementById('joinRoomBtn');
    const roomInput = document.getElementById('roomInput');
    const copyRoomBtn = document.getElementById('copyRoomBtn');
    
    createRoomBtn.addEventListener('click', () => {
        if (state.ws && state.ws.readyState === WebSocket.OPEN) {
            state.ws.send(JSON.stringify({ type: 'create-room' }));
        }
    });
    
    joinRoomBtn.addEventListener('click', () => {
        const roomCode = roomInput.value.trim().toUpperCase();
        if (roomCode && roomCode.length === 6) {
            if (state.ws && state.ws.readyState === WebSocket.OPEN) {
                state.ws.send(JSON.stringify({
                    type: 'join-room',
                    roomCode: roomCode
                }));
            }
        } else {
            showToast('Please enter a valid 6-character room code', 'warning');
        }
    });
    
    roomInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            joinRoomBtn.click();
        }
    });
    
    roomInput.addEventListener('input', (e) => {
        e.target.value = e.target.value.toUpperCase().replace(/[^A-Z0-9]/g, '');
    });
    
    copyRoomBtn.addEventListener('click', () => {
        const roomCode = document.getElementById('roomCode').textContent;
        
        // Create shareable link
        const url = new URL(window.location);
        url.searchParams.set('room', roomCode);
        const shareableLink = url.toString();
        
        // Copy to clipboard
        navigator.clipboard.writeText(shareableLink).then(() => {
            showToast('Room link copied to clipboard!', 'success');
        }).catch(() => {
            // Fallback to just room code
            navigator.clipboard.writeText(roomCode).then(() => {
                showToast('Room code copied!', 'success');
            });
        });
    });
    
    // Device name edit
    const deviceNameBtn = document.getElementById('deviceNameBtn');
    const nameModal = document.getElementById('nameModal');
    const nameInput = document.getElementById('nameInput');
    const saveNameBtn = document.getElementById('saveNameBtn');
    const cancelNameBtn = document.getElementById('cancelNameBtn');
    
    deviceNameBtn.addEventListener('click', () => {
        nameInput.value = state.deviceName;
        nameModal.classList.remove('hidden');
        nameInput.focus();
    });
    
    saveNameBtn.addEventListener('click', () => {
        const newName = nameInput.value.trim();
        if (newName && newName !== state.deviceName) {
            state.deviceName = newName;
            document.getElementById('deviceName').textContent = newName;
            localStorage.setItem('deviceName', newName);
            
            // Notify server about name change
            if (state.ws && state.ws.readyState === WebSocket.OPEN) {
                state.ws.send(JSON.stringify({
                    type: 'set-name',
                    name: newName
                }));
            }
        }
        nameModal.classList.add('hidden');
    });
    
    cancelNameBtn.addEventListener('click', () => {
        nameModal.classList.add('hidden');
    });
    
    // Close modal on escape
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            nameModal.classList.add('hidden');
            document.getElementById('dropZone').classList.add('hidden');
        }
    });
}

// ─────────────────────────────────────────────
// Shared Text (live shared clipboard, synced via the server relay so it works
// over the internet, not just the LAN). Formatting is preserved byte-for-byte;
// syntax highlighting is cosmetic and degrades gracefully if hljs isn't loaded.
// ─────────────────────────────────────────────
const ts = {}; // cached DOM refs, populated in initSharedText()

function initSharedText() {
    ts.fab = document.getElementById('sharedTextFab');
    ts.modal = document.getElementById('textShareModal');
    ts.editor = document.getElementById('tsEditor');
    ts.textarea = document.getElementById('tsTextarea');
    ts.pre = document.querySelector('.ts-highlight');
    ts.code = document.getElementById('tsHighlight');
    ts.language = document.getElementById('tsLanguage');
    ts.status = document.getElementById('tsStatus');
    ts.copyBtn = document.getElementById('tsCopyBtn');
    ts.clearBtn = document.getElementById('tsClearBtn');
    ts.closeBtn = document.getElementById('tsCloseBtn');
    ts.lanToggle = document.getElementById('tsLanOnly');
    ts.hint = document.getElementById('tsHint');

    if (!ts.fab || !ts.modal || !ts.textarea) return;

    // Same switch as in the header (see initLanOnly)
    if (ts.lanToggle) {
        ts.lanToggle.checked = state.lanOnly;
        ts.lanToggle.addEventListener('change', () => setLanOnly(ts.lanToggle.checked));
    }
    updateSharedHint();

    ts.fab.addEventListener('click', openSharedText);
    ts.closeBtn.addEventListener('click', closeSharedText);
    ts.modal.addEventListener('click', (e) => {
        if (e.target === ts.modal) closeSharedText();
    });

    // Typing → re-highlight + debounced broadcast
    ts.textarea.addEventListener('input', () => {
        renderSharedHighlight();
        syncSharedScroll();
        sendSharedText(false);
    });
    ts.textarea.addEventListener('scroll', syncSharedScroll);

    // Insert a real tab instead of moving focus (handy for code)
    ts.textarea.addEventListener('keydown', (e) => {
        if (e.key === 'Tab') {
            e.preventDefault();
            const s = ts.textarea.selectionStart, en = ts.textarea.selectionEnd;
            const v = ts.textarea.value;
            ts.textarea.value = v.slice(0, s) + '    ' + v.slice(en);
            ts.textarea.selectionStart = ts.textarea.selectionEnd = s + 4;
            renderSharedHighlight();
            sendSharedText(false);
        }
    });

    // Language choice is part of the shared state — propagate it to peers
    ts.language.addEventListener('change', () => {
        renderSharedHighlight();
        sendSharedText(true);
    });

    ts.copyBtn.addEventListener('click', copySharedText);
    ts.clearBtn.addEventListener('click', clearSharedText);

    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && !ts.modal.classList.contains('hidden')) {
            closeSharedText();
        }
    });
}

function openSharedText() {
    ts.modal.classList.remove('hidden');
    ts.fab.classList.remove('has-activity');
    renderSharedHighlight();
    updateSharedStatus();
    setTimeout(() => ts.textarea.focus(), 50);
}

function closeSharedText() {
    ts.modal.classList.add('hidden');
}

function isSharedChannelReady() {
    return state.localMode || state.inRoom;
}

// Text goes peer-to-peer only: LAN only is on, or the current text came from a
// device that keeps it on the local network
function textStaysLocal() {
    return state.lanOnly || state.sharedText.lanOnlyContent;
}

function updateSharedStatus(force) {
    if (textStaysLocal()) {
        const { ready } = getLanTextTargets();
        if (ready.length > 0) {
            setSharedStatus(`LAN only · ${ready.length} local device${ready.length > 1 ? 's' : ''}`, 'synced');
        } else {
            setSharedStatus('LAN only · waiting for a verified local device', '');
        }
    } else if (!isSharedChannelReady()) {
        setSharedStatus('Join a room to sync live', '');
    } else if (force || !ts.status.textContent || ts.status.textContent === 'Idle') {
        setSharedStatus('Live', 'synced');
    }
}

function updateSharedHint() {
    if (!ts.hint) return;
    ts.hint.textContent = state.lanOnly
        ? 'LAN only: text goes directly device-to-device over the encrypted P2P connection and never through the server. Only devices verified to be on your local network receive it (files too).'
        : state.sharedText.lanOnlyContent
        ? 'This text was shared on the local network only, so it stays there: it goes only to verified local devices, not through the server. Clear it to sync through the server again.'
        : 'Types live to every device in this room. Formatting is preserved — paste code, copy it back byte-for-byte anywhere.';
}

// ── LAN-only mode ──
// One switch for the whole app (header and shared-text dialog):
// - files are only sent to / accepted from peers whose route is verified local
//   (no "Transfer Anyway");
// - shared text travels over the WebRTC data channel instead of the server
//   relay, and only to verified-local peers. Receiving works from both paths.
// A per-device preference, on by default (keeping data inside the local
// network is the point of the app); an explicit choice is kept. The storage
// key predates files being covered, hence the name.
const LAN_ONLY_KEY = 'sharedTextLanOnly';

function initLanOnly() {
    try {
        state.lanOnly = localStorage.getItem(LAN_ONLY_KEY) !== '0';
    } catch (_) { /* storage unavailable — keep the default */ }

    const headerToggle = document.getElementById('lanOnlyToggle');
    if (headerToggle) {
        headerToggle.checked = state.lanOnly;
        headerToggle.addEventListener('change', () => setLanOnly(headerToggle.checked));
    }
}

function setLanOnly(on) {
    state.lanOnly = !!on;
    try {
        localStorage.setItem(LAN_ONLY_KEY, state.lanOnly ? '1' : '0');
    } catch (_) { /* not persisted — fine */ }
    const headerToggle = document.getElementById('lanOnlyToggle');
    if (headerToggle) headerToggle.checked = state.lanOnly;
    if (ts.lanToggle) ts.lanToggle.checked = state.lanOnly;
    updateSharedHint();
    updateSharedStatus(true);
    updateDevicesList();

    if (state.lanOnly) {
        connectPeersForLanText();
        // Push the current text to peers that are already reachable locally
        if (ts.textarea && ts.textarea.value) {
            sendSharedTextP2P('hello');
        }
    }
}

// Peers that may receive LAN-only text right now, and how many are left out.
function getLanTextTargets() {
    const ready = [];
    let skipped = 0;
    state.peers.forEach((_, peerId) => {
        const dc = state.dataChannels.get(peerId);
        const locality = state.peerLocality.get(peerId);
        // 'confirmed' (send anyway) only exists with LAN only off — it doesn't count here
        if (dc && dc.readyState === 'open' && locality === 'verified') {
            ready.push(peerId);
        } else {
            skipped++;
        }
    });
    return { ready, skipped };
}

// In LAN-only mode every peer needs a data channel, so connect to them all
// instead of waiting for the user to click each device.
function connectPeersForLanText() {
    if (!state.lanOnly) return;
    state.peers.forEach((_, peerId) => {
        if (state.untrustedPeers.has(peerId)) return;
        const conn = state.connections.get(peerId);
        if (conn && !['failed', 'closed', 'disconnected'].includes(conn.connectionState)) return;
        connectToPeer(peerId);
    });
}

// Called when a peer's route gets classified and when its data channel opens —
// these happen in either order, so catch-up is sent once both are done.
function onPeerRouteChecked(peerId) {
    if (!state.lanOnly || !ts.textarea) return;
    updateSharedStatus(true);

    const dc = state.dataChannels.get(peerId);
    if (!dc || !getLanTextTargets().ready.includes(peerId)) return;
    if (lanHelloSent.has(dc)) return;
    lanHelloSent.add(dc);
    if (ts.textarea.value) {
        sendSharedTextP2P('hello', [peerId]);
    }
}
const lanHelloSent = new WeakSet(); // data channels that already got the catch-up text

// Compress + chunk the current text and send it over the data channels.
// kind 'live' = a local edit (a newer edit supersedes a queued older one);
// kind 'hello' = catch-up sent when a peer becomes reachable.
// Resolves to { sent, skipped }, or null if superseded by a newer edit.
function sendSharedTextP2P(kind, onlyPeers) {
    const st = state.sharedText;
    const liveSeq = kind === 'live' ? ++st.p2pLiveSeq : st.p2pLiveSeq;

    const job = st.p2pChain.then(async () => {
        if (kind === 'live' && liveSeq !== st.p2pLiveSeq) return null;

        let { ready, skipped } = getLanTextTargets();
        if (onlyPeers) ready = ready.filter(id => onlyPeers.includes(id));
        if (ready.length === 0) return { sent: 0, skipped };

        const text = ts.textarea.value;
        const language = ts.language.value;
        const { enc, b64 } = await encodeSharedText(text);
        const rev = (state.clientId || 'me') + '-p2p-' + (++st.revCounter);
        const count = Math.max(1, Math.ceil(b64.length / TS_P2P_CHUNK_SIZE));

        let sent = 0;
        for (const peerId of ready) {
            const dc = state.dataChannels.get(peerId);
            try {
                for (let seq = 0; seq < count; seq++) {
                    await sendWithFlowControl(dc, JSON.stringify({
                        type: 'text-sync',
                        kind, rev, seq, count, enc, language,
                        updatedAt: st.updatedAt,
                        data: b64.slice(seq * TS_P2P_CHUNK_SIZE, (seq + 1) * TS_P2P_CHUNK_SIZE)
                    }));
                }
                sent++;
            } catch (e) {
                console.error(`Shared text P2P send to ${peerId} failed:`, e);
                skipped++;
            }
        }
        return { sent, skipped };
    });

    // Keep the chain alive even if a step throws
    st.p2pChain = job.catch(() => null);
    return job;
}

// Reassemble a shared-text payload received over a data channel.
function handleP2PTextChunk(message, peerId) {
    if (!ts.textarea) return;
    const { rev, seq, count, data } = message;
    if (typeof rev !== 'string' || typeof data !== 'string' ||
        !Number.isInteger(seq) || !Number.isInteger(count) ||
        count < 1 || count > TS_P2P_MAX_CHUNKS || seq < 0 || seq >= count) return;

    const st = state.sharedText;
    let inc = st.p2pIncoming.get(peerId);
    if (!inc || inc.rev !== rev) {
        inc = { rev, count, parts: new Array(count), received: 0 };
        st.p2pIncoming.set(peerId, inc);
    }
    if (inc.parts[seq] === undefined) {
        inc.parts[seq] = data;
        inc.received++;
    }
    if (inc.received < inc.count) {
        setSharedStatus(`Receiving… ${inc.received}/${inc.count}`, 'remote');
        return;
    }
    st.p2pIncoming.delete(peerId);

    const peerName = state.peers.get(peerId)?.name || 'device';
    const theirTime = typeof message.updatedAt === 'number' ? message.updatedAt : 0;

    decodeSharedText(inc.parts.join(''), message.enc).then(async text => {
        // With LAN only on, accept text only over a verified local route
        if (state.lanOnly && await localityOf(peerId) !== 'verified') return;

        if (message.kind === 'hello' && ts.textarea.value && ts.textarea.value !== text) {
            // Catch-up from a peer: keep whichever text is newer. If ours is
            // newer, answer with it so both sides converge.
            if (theirTime < st.updatedAt) {
                if (state.lanOnly) sendSharedTextP2P('hello', [peerId]);
                return;
            }
            if (theirTime === st.updatedAt) return;
        }

        // The sender keeps this text inside the LAN — keep it there too, otherwise
        // our next edit would carry it to the server (until it's cleared or
        // replaced by text that came through the server)
        applyRemoteText(text, message.language, peerName, theirTime || Date.now(), 'local network');
        if (!state.lanOnly && !st.lanOnlyContent) {
            showToast(`${peerName} shares this text only on the local network — it won't be synced through the server`, 'info');
        }
        st.lanOnlyContent = true;
        updateSharedHint();
    }).catch(err => {
        console.error('Shared text P2P decode failed:', err);
    });
}

function setSharedStatus(text, cls) {
    if (!ts.status) return;
    ts.status.textContent = text;
    ts.status.className = 'ts-status' + (cls ? ' ' + cls : '');
}

// Escape for HTML text and attribute values. Use for anything that comes from
// another device (names, file names) before putting it into innerHTML.
function escapeHtml(str) {
    return String(str).replace(/[&<>"']/g, (c) => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));
}

// Above this many characters we stop syntax-highlighting (highlighting megabytes
// would freeze the tab) and drop to "plain mode": the textarea text is made
// visible directly and the highlight overlay is hidden. Sync still works fully.
const TS_HIGHLIGHT_MAX = 100 * 1024;
const TS_CHUNK_SIZE = 128 * 1024;         // base64 chars per outgoing chunk
const TS_MAX_CHARS = 80 * 1024 * 1024;    // hard client cap on raw text length
const TS_SERVER_MAX_B64 = 10 * 1024 * 1024; // mirrors MAX_TEXT_TOTAL on the server
const TS_P2P_CHUNK_SIZE = 64 * 1024;     // base64 chars per data-channel message (well under SCTP limits)
const TS_P2P_MAX_CHUNKS = 2000;           // reject absurd incoming chunk counts

function renderSharedHighlight() {
    if (!ts.textarea || !ts.code) return;
    const text = ts.textarea.value;

    // Plain mode for very large text — keep the editor responsive.
    if (text.length > TS_HIGHLIGHT_MAX) {
        ts.editor.classList.add('plain');
        ts.code.innerHTML = '';
        return;
    }
    ts.editor.classList.remove('plain');

    const lang = ts.language.value;
    let html;
    if (typeof hljs !== 'undefined' && lang !== 'plaintext') {
        try {
            html = (lang === 'auto')
                ? hljs.highlightAuto(text).value
                : hljs.highlight(text, { language: lang, ignoreIllegals: true }).value;
        } catch (_) {
            html = escapeHtml(text);
        }
    } else {
        html = escapeHtml(text);
    }
    // Trailing newline keeps the last line visible and scroll heights aligned
    ts.code.innerHTML = html + '\n';
}

function syncSharedScroll() {
    if (ts.pre && ts.textarea) {
        ts.pre.scrollTop = ts.textarea.scrollTop;
        ts.pre.scrollLeft = ts.textarea.scrollLeft;
    }
}

// ── Compression + base64 helpers (support arbitrarily large payloads) ──
function bytesToBase64(bytes) {
    let bin = '';
    const CH = 0x8000; // avoid arg-count limits on fromCharCode
    for (let i = 0; i < bytes.length; i += CH) {
        bin += String.fromCharCode.apply(null, bytes.subarray(i, i + CH));
    }
    return btoa(bin);
}

function base64ToBytes(b64) {
    const bin = atob(b64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return bytes;
}

async function encodeSharedText(text) {
    const raw = new TextEncoder().encode(text);
    if (typeof CompressionStream !== 'undefined') {
        try {
            const stream = new Blob([raw]).stream().pipeThrough(new CompressionStream('gzip'));
            const buf = await new Response(stream).arrayBuffer();
            return { enc: 'gzip', b64: bytesToBase64(new Uint8Array(buf)) };
        } catch (_) { /* fall through to raw */ }
    }
    return { enc: 'raw', b64: bytesToBase64(raw) };
}

async function decodeSharedText(b64, enc) {
    const bytes = base64ToBytes(b64);
    if (enc === 'gzip' && typeof DecompressionStream !== 'undefined') {
        const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
        return await new Response(stream).text();
    }
    return new TextDecoder().decode(bytes);
}

// Compress the current text, split into chunks, and stream them to the channel.
function sendSharedText(immediate) {
    if (state.sharedText.timer) {
        clearTimeout(state.sharedText.timer);
        state.sharedText.timer = null;
    }
    // Only called for local changes — remember when the text last changed
    state.sharedText.updatedAt = Date.now();
    state.sharedText.lastChangeAt = Date.now();

    const doSend = async () => {
        state.sharedText.timer = null;
        const lanOnly = textStaysLocal();
        if (!lanOnly && (!state.ws || state.ws.readyState !== WebSocket.OPEN)) return;

        const text = ts.textarea.value;
        const language = ts.language.value;
        const sig = language + ' ' + text;
        if (sig === state.sharedText.lastSentSig) return; // nothing changed

        if (text.length > TS_MAX_CHARS) {
            setSharedStatus('Too large to sync (over 80M chars)', '');
            showToast('Text is too large to share', 'error');
            return;
        }

        if (lanOnly) {
            try {
                const res = await sendSharedTextP2P('live');
                if (!res) return; // superseded by a newer edit
                state.sharedText.lastSentSig = sig;
                if (res.sent > 0) {
                    setSharedStatus(
                        `LAN only · sent to ${res.sent} device${res.sent > 1 ? 's' : ''}` +
                        (res.skipped ? ` · ${res.skipped} not verified local` : ''),
                        'synced'
                    );
                } else {
                    setSharedStatus('LAN only · no verified local device — kept on this device', '');
                }
            } catch (e) {
                console.error('Shared text P2P send failed:', e);
                setSharedStatus('Sync failed', '');
            }
            return;
        }

        try {
            state.sharedText.sending = true;
            const { enc, b64 } = await encodeSharedText(text);
            if (b64.length > TS_SERVER_MAX_B64) {
                setSharedStatus('Too large to sync via the server — turn on LAN only', '');
                showToast('Text is too large to sync through the server. Use "LAN only" to share it directly.', 'error');
                return;
            }
            const rev = (state.clientId || 'me') + '-' + (++state.sharedText.revCounter);
            const count = Math.max(1, Math.ceil(b64.length / TS_CHUNK_SIZE));

            for (let seq = 0; seq < count; seq++) {
                if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
                state.ws.send(JSON.stringify({
                    type: 'text-share',
                    rev, seq, count, enc, language,
                    data: b64.slice(seq * TS_CHUNK_SIZE, (seq + 1) * TS_CHUNK_SIZE)
                }));
            }

            state.sharedText.lastSentSig = sig;
            const size = formatFileSize(new Blob([text]).size);
            setSharedStatus(
                isSharedChannelReady()
                    ? `Synced · ${size}${count > 1 ? ' · ' + count + ' chunks' : ''}`
                    : `Saved locally · ${size}`,
                'synced'
            );
        } catch (e) {
            console.error('Shared text send failed:', e);
            setSharedStatus('Sync failed', '');
        } finally {
            state.sharedText.sending = false;
        }
    };

    if (immediate) {
        doSend();
    } else {
        setSharedStatus('Saving…', 'saving');
        // Longer debounce for large text so we don't re-compress on every keystroke
        const delay = ts.textarea.value.length > 500 * 1024 ? 900 : 400;
        state.sharedText.timer = setTimeout(doSend, delay);
    }
}

// Reassemble incoming shared-text chunks (from a live edit or a joined-snapshot
// stream), then decompress and apply once the full payload has arrived.
function handleTextUpdateChunk(message) {
    if (!ts.textarea) return;
    const { rev, seq, count, enc, language, data, fromName } = message;
    if (typeof rev !== 'string' || typeof data !== 'string' ||
        !Number.isInteger(seq) || !Number.isInteger(count) ||
        count < 1 || count > TS_P2P_MAX_CHUNKS || seq < 0 || seq >= count) return;

    // Several revisions can be in flight at once (e.g. a join snapshot streaming
    // while a peer types), so reassemble each separately.
    const st = state.sharedText;
    let inc = st.incoming.get(rev);
    if (!inc) {
        if (st.incoming.size >= 4) {
            st.incoming.delete(st.incoming.keys().next().value); // drop the oldest
        }
        inc = { rev, count, enc, language, fromName, parts: new Array(count), received: 0, startedAt: Date.now() };
        st.incoming.set(rev, inc);
    }
    if (inc.count !== count) return;
    if (inc.parts[seq] === undefined) {
        inc.parts[seq] = data;
        inc.received++;
    }

    if (inc.count > 1 && inc.received < inc.count) {
        setSharedStatus(`Receiving… ${inc.received}/${inc.count}`, 'remote');
        if (ts.modal.classList.contains('hidden')) ts.fab.classList.add('has-activity');
        return;
    }
    if (inc.received < inc.count) return;

    const b64 = inc.parts.join('');
    st.incoming.delete(rev);
    decodeSharedText(b64, inc.enc).then(text => {
        // A join snapshot is older than any edit made while it was streaming
        if (rev.startsWith('snap-') && st.lastChangeAt > inc.startedAt) return;
        applyRemoteText(text, inc.language, inc.fromName, undefined, 'server');
        // Text that came through the server is not LAN-only content
        if (st.lanOnlyContent) {
            st.lanOnlyContent = false;
            updateSharedHint();
        }
    }).catch(err => {
        console.error('Shared text decode failed:', err);
    });
}

// Apply fully-assembled remote text without echoing it back. Programmatic
// .value assignment does not fire 'input', so no re-send loop occurs.
// `via` says which path the text took ('server' or 'local network') — shown in
// the status so it's clear whether LAN only actually applied
function applyRemoteText(text, language, fromName, updatedAt, via) {
    if (!ts.textarea || typeof text !== 'string') return;
    state.sharedText.updatedAt = updatedAt || Date.now();
    state.sharedText.lastChangeAt = Date.now();

    if (typeof language === 'string' && language &&
        [...ts.language.options].some(o => o.value === language)) {
        ts.language.value = language;
    }

    if (ts.textarea.value !== text) {
        const start = ts.textarea.selectionStart;
        const end = ts.textarea.selectionEnd;
        ts.textarea.value = text;
        const len = ts.textarea.value.length;
        try { ts.textarea.setSelectionRange(Math.min(start, len), Math.min(end, len)); } catch (_) {}
    }

    // Mark as already-synced so a later local edit diffs against this correctly
    // and we don't rebroadcast identical content.
    state.sharedText.lastSentSig = ts.language.value + ' ' + text;

    renderSharedHighlight();
    syncSharedScroll();

    if (fromName) {
        setSharedStatus('Updated by ' + fromName + (via ? ' · via ' + via : ''), 'remote');
        if (ts.modal.classList.contains('hidden')) {
            ts.fab.classList.add('has-activity');
        }
    }
}

function copySharedText() {
    const text = ts.textarea.value;
    if (!text) { showToast('Nothing to copy', 'warning'); return; }
    const done = () => showToast('Copied to clipboard', 'success');
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(done).catch(() => {
            ts.textarea.select();
            document.execCommand('copy');
            done();
        });
    } else {
        ts.textarea.select();
        document.execCommand('copy');
        done();
    }
}

function clearSharedText() {
    if (!ts.textarea.value) return;
    ts.textarea.value = '';
    renderSharedHighlight();
    syncSharedScroll();
    sendSharedText(true); // clears for everyone on the channel
    // Nothing LAN-only left to protect
    state.sharedText.lanOnlyContent = false;
    updateSharedHint();
    showToast(isSharedChannelReady() ? 'Cleared for everyone' : 'Cleared', 'info');
    ts.textarea.focus();
}

// Utility functions
function generateTransferId() {
    return `transfer-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function loadDeviceName() {
    const savedName = localStorage.getItem('deviceName');
    if (savedName) {
        state.deviceName = savedName;
        document.getElementById('deviceName').textContent = savedName;
    } else {
        // Generate default name
        const defaultName = `Device-${Math.random().toString(36).substr(2, 6)}`;
        state.deviceName = defaultName;
        document.getElementById('deviceName').textContent = defaultName;
        localStorage.setItem('deviceName', defaultName);
    }
}

// Ping interval for keep-alive
let pingInterval;

function startPingInterval() {
    pingInterval = setInterval(() => {
        if (state.ws && state.ws.readyState === WebSocket.OPEN) {
            state.ws.send(JSON.stringify({ type: 'ping' }));
        }
    }, CONFIG.pingInterval);
}

function stopPingInterval() {
    if (pingInterval) {
        clearInterval(pingInterval);
        pingInterval = null;
    }
}

// Fetch and display version info
async function loadVersionInfo() {
    try {
        const res = await fetch('/api/version');
        const data = await res.json();
        const link = document.getElementById('appVersionLink');
        const deploy = document.getElementById('appDeploy');
        if (link && data.version) {
            const d = new Date(data.startedAt);
            const pad = n => String(n).padStart(2, '0');
            const stamp = `${d.getFullYear()}.${pad(d.getMonth()+1)}.${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
            link.textContent = `v${data.version}`;
            if (deploy) deploy.textContent = ` · deploy ${stamp}`;
        }
    } catch (e) {
        // silent — footer keeps the static fallback
    }
}

// Make functions globally accessible for inline onclick handlers
window.resumeTransfer = resumeTransfer;
window.cancelTransfer = cancelTransfer;
window.pauseTransfer = pauseTransfer;
window.saveTransferToDisk = saveTransferToDisk;
