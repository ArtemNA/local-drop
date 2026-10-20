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
    pingInterval: 30000,
    inactivityTimeout: 60000 // 60 seconds of no chunk activity = stalled
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
        lanOnly: false,           // LAN-only mode: sync over P2P data channels, never via the server
        updatedAt: 0,             // when the current text was last changed (by anyone)
        p2pIncoming: new Map(),   // peerId -> in-progress P2P reassembly
        p2pChain: Promise.resolve(), // serializes P2P sends so chunks of different revs never interleave
        p2pLiveSeq: 0
    },
    peers: new Map(),
    connections: new Map(),
    dataChannels: new Map(),
    transferQueue: [],
    activeTransfers: new Map(),
    pendingFiles: new Map(),
    selectedPeer: null,
    reconnectAttempts: 0,
    isReconnecting: false,
    localIP: null,
    peerLocality: new Map() // peerId -> 'verified' | 'confirmed' | 'blocked'
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
    
    // Try to detect local IP for auto-room feature
    state.localIP = await getLocalIP();
    console.log('Detected local IP:', state.localIP);
    
    loadDeviceName();
    initSharedText();
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
    
    // Include local IP if detected (for auto-room feature)
    if (state.localIP) {
        params.append('localIP', state.localIP);
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
        state.isReconnecting = false;
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
        
        // Attempt reconnection
        if (!state.isReconnecting && state.reconnectAttempts < CONFIG.reconnectMaxAttempts) {
            state.isReconnecting = true;
            state.reconnectAttempts++;
            
            showToast(`Reconnecting... (${state.reconnectAttempts}/${CONFIG.reconnectMaxAttempts})`, 'warning');
            
            setTimeout(() => {
                initializeWebSocket();
            }, CONFIG.reconnectDelay);
        } else if (state.reconnectAttempts >= CONFIG.reconnectMaxAttempts) {
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
                state.peers.set(message.peer.id, message.peer);
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
    
    const connection = new RTCPeerConnection({ 
        iceServers: CONFIG.iceServers,
        // Optional: Add more aggressive settings for better performance
        bundlePolicy: 'max-bundle',
        rtcpMuxPolicy: 'require'
    });
    
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
            handleConnectionFailure(peerId);
        } else if (connection.connectionState === 'connected') {
            showToast(`Connected to ${state.peers.get(peerId)?.name || 'device'}`, 'success');
            // Verify locality of the connection
            verifyLocality(connection, peerId).then(() => onPeerRouteChecked(peerId));
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
        updateDevicesList();
        onPeerRouteChecked(peerId);
        
        // Resume any pending transfers
        const pendingTransfer = state.activeTransfers.get(peerId);
        if (pendingTransfer && pendingTransfer.status === 'paused') {
            resumeTransfer(peerId);
        }
    };
    
    dataChannel.onclose = () => {
        console.log(`Data channel closed with ${peerId}`);
        // A replaced connection's old channel may close after the new one is
        // registered — only forget the channel if it is still the current one.
        if (state.dataChannels.get(peerId) === dataChannel) {
            state.dataChannels.delete(peerId);
        }
        updateDevicesList();
    };
    
    dataChannel.onerror = (error) => {
        console.error(`Data channel error with ${peerId}:`, error);
        showToast(`Connection error with ${state.peers.get(peerId)?.name || 'device'}`, 'error');
    };
    
    dataChannel.onmessage = (event) => {
        handleDataChannelMessage(event, peerId);
    };
}

// Handle offer
async function handleOffer(message) {
    const { fromId, offer } = message;

    // Both sides sent an offer at the same time (e.g. both have LAN-only shared
    // text on). The side with the smaller id keeps its own offer; the other drops
    // its own connection and answers instead.
    const existing = state.connections.get(fromId);
    if (existing && existing.signalingState === 'have-local-offer') {
        if (state.clientId && state.clientId < fromId) {
            return;
        }
        existing.close();
    }

    try {
        const connection = await createConnection(fromId, false);
        await connection.setRemoteDescription(new RTCSessionDescription(offer));
        
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
    
    if (connection) {
        try {
            await connection.setRemoteDescription(new RTCSessionDescription(answer));
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
        try {
            await connection.addIceCandidate(new RTCIceCandidate(candidate));
        } catch (error) {
            console.error('Error adding ICE candidate:', error);
        }
    }
}

// Initiate connection with peer
async function connectToPeer(peerId) {
    if (state.connections.has(peerId)) {
        const existingConn = state.connections.get(peerId);
        if (existingConn.connectionState === 'connected') {
            console.log(`Already connected to ${peerId}`);
            return;
        }
    }
    
    try {
        const connection = await createConnection(peerId, true);
        const offer = await connection.createOffer();
        await connection.setLocalDescription(offer);
        
        if (state.ws.readyState === WebSocket.OPEN) {
            state.ws.send(JSON.stringify({
                type: 'offer',
                targetId: peerId,
                offer: offer
            }));
        }
    } catch (error) {
        console.error('Error connecting to peer:', error);
        showToast('Failed to connect to device', 'error');
    }
}

// Locality verification
function isPrivateIP(ip) {
    if (!ip) return false;
    // IPv4 private ranges
    if (/^10\./.test(ip)) return true;
    if (/^172\.(1[6-9]|2\d|3[01])\./.test(ip)) return true;
    if (/^192\.168\./.test(ip)) return true;
    if (/^169\.254\./.test(ip)) return true; // link-local
    // IPv6 private
    if (/^fd[0-9a-f]{2}:/i.test(ip)) return true;
    if (/^fe80:/i.test(ip)) return true; // link-local
    return false;
}

function getSubnet24(ip) {
    if (!ip) return null;
    const parts = ip.split('.');
    if (parts.length === 4) return parts.slice(0, 3).join('.');
    return null;
}

async function verifyLocality(connection, peerId) {
    try {
        const stats = await connection.getStats();
        let localCandidateType = null;
        let remoteCandidateType = null;
        let localIP = null;
        let remoteIP = null;
        let selectedPairFound = false;

        stats.forEach(report => {
            if (report.type === 'candidate-pair' && report.state === 'succeeded' && report.nominated) {
                selectedPairFound = true;
                // Find local and remote candidates
                stats.forEach(r => {
                    if (r.id === report.localCandidateId) {
                        localCandidateType = r.candidateType;
                        localIP = r.address || r.ip;
                    }
                    if (r.id === report.remoteCandidateId) {
                        remoteCandidateType = r.candidateType;
                        remoteIP = r.address || r.ip;
                    }
                });
            }
        });

        if (!selectedPairFound) {
            // Fallback: check all succeeded pairs
            stats.forEach(report => {
                if (report.type === 'candidate-pair' && report.state === 'succeeded') {
                    stats.forEach(r => {
                        if (r.id === report.localCandidateId) {
                            localCandidateType = r.candidateType;
                            localIP = r.address || r.ip;
                        }
                        if (r.id === report.remoteCandidateId) {
                            remoteCandidateType = r.candidateType;
                            remoteIP = r.address || r.ip;
                        }
                    });
                }
            });
        }

        console.log(`Locality check for ${peerId}: local=${localCandidateType}(${localIP}), remote=${remoteCandidateType}(${remoteIP})`);

        // Check for relay - block
        if (localCandidateType === 'relay' || remoteCandidateType === 'relay') {
            state.peerLocality.set(peerId, 'blocked');
            showToast('Connection uses relay - file transfer blocked (not local)', 'error');
            updateDevicesList();
            return;
        }

        // Check for verified local: both host candidates with private IPs
        if (localCandidateType === 'host' && remoteCandidateType === 'host') {
            // If IPs are available and private, verified
            if (localIP && remoteIP && isPrivateIP(localIP) && isPrivateIP(remoteIP)) {
                state.peerLocality.set(peerId, 'verified');
                updateDevicesList();
                return;
            }
            // If IPs are empty/unavailable (browser privacy), host+host with no relay = direct local
            if (!localIP && !remoteIP) {
                state.peerLocality.set(peerId, 'verified');
                updateDevicesList();
                return;
            }
        }

        // Additional check: same subnet via localIP hints
        if (state.localIP && localIP && remoteIP && isPrivateIP(localIP) && isPrivateIP(remoteIP)) {
            const localSubnet = getSubnet24(localIP);
            const remoteSubnet = getSubnet24(remoteIP);
            if (localSubnet && remoteSubnet && localSubnet === remoteSubnet) {
                state.peerLocality.set(peerId, 'verified');
                updateDevicesList();
                return;
            }
        }

        // Unverified direct route - will need user confirmation before transfer
        state.peerLocality.set(peerId, 'unverified');
        updateDevicesList();

    } catch (err) {
        console.error('Error verifying locality:', err);
        // On error, mark as unverified rather than blocking
        state.peerLocality.set(peerId, 'unverified');
        updateDevicesList();
    }
}

async function checkLocalityBeforeTransfer(peerId) {
    const locality = state.peerLocality.get(peerId);

    if (locality === 'verified' || locality === 'confirmed') {
        return true;
    }

    if (locality === 'blocked') {
        showToast('Transfer blocked: connection route is not local (relay detected)', 'error');
        return false;
    }

    // Unverified or unknown - ask user
    return new Promise(resolve => {
        const peerName = state.peers.get(peerId)?.name || 'device';
        const dialog = document.createElement('div');
        dialog.className = 'locality-dialog';
        dialog.innerHTML = `
            <div class="locality-dialog-overlay"></div>
            <div class="locality-dialog-content">
                <h3>Unverified Route</h3>
                <p>The connection to <strong>${peerName}</strong> could not be verified as local. If you are sure both devices are on the same network, you can proceed.</p>
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
            onPeerRouteChecked(peerId);
            dialog.remove();
            resolve(true);
        };
        dialog.querySelector('.locality-dialog-overlay').onclick = () => {
            dialog.remove();
            resolve(false);
        };
    });
}

// File transfer functions
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
    const localityOk = await checkLocalityBeforeTransfer(peerId);
    if (!localityOk) {
        showToast('Transfer cancelled: route not verified as local', 'warning');
        return;
    }
    
    // Add files to queue
    for (const file of files) {
        const transferId = generateTransferId();
        const transfer = {
            id: transferId,
            file: file,
            peerId: peerId,
            direction: 'send',
            progress: 0,
            bytesTransferred: 0,
            status: 'queued',
            startTime: null,
            chunks: [],
            currentChunk: 0
        };
        
        state.transferQueue.push(transfer);
        state.activeTransfers.set(transferId, transfer);
        addTransferToUI(transfer);
    }
    
    // Process queue
    processTransferQueue();
}

// Process transfer queue
async function processTransferQueue() {
    const pendingTransfers = state.transferQueue.filter(t => t.status === 'queued');
    
    for (const transfer of pendingTransfers) {
        if (transfer.direction === 'send') {
            await startFileSend(transfer);
        }
    }
}

// Start sending file
async function startFileSend(transfer) {
    const dataChannel = state.dataChannels.get(transfer.peerId);
    
    if (!dataChannel || dataChannel.readyState !== 'open') {
        transfer.status = 'paused';
        updateTransferUI(transfer);
        return;
    }
    
    transfer.status = 'sending';
    transfer.startTime = Date.now();
    updateTransferUI(transfer);
    
    // Compute effective chunk size for this connection
    const effectiveChunkSize = getEffectiveChunkSize(transfer.peerId);
    console.log(`Using chunk size ${effectiveChunkSize} for transfer ${transfer.id}`);

    // Send file metadata as JSON (includes chunkSize so receiver knows boundaries)
    const metadata = {
        type: 'file-start',
        transferId: transfer.id,
        name: transfer.file.name,
        size: transfer.file.size,
        mimeType: transfer.file.type,
        lastModified: transfer.file.lastModified,
        chunkSize: effectiveChunkSize
    };

    await sendWithFlowControl(dataChannel, JSON.stringify(metadata));

    // Track inactivity for timeout
    transfer.lastActivity = Date.now();

    // Wrap the entire send loop in a Promise so processTransferQueue
    // truly awaits each file before starting the next one
    await new Promise((resolveTransfer, rejectTransfer) => {
        const reader = new FileReader();
        let offset = transfer.bytesTransferred;

        const readNextChunk = async () => {
            if (offset >= transfer.file.size) {
                // File transfer complete
                try {
                    await sendWithFlowControl(dataChannel, JSON.stringify({
                        type: 'file-end',
                        transferId: transfer.id,
                        totalBytes: offset
                    }));
                    transfer.status = 'completed';
                    updateTransferUI(transfer);
                    showToast(`Sent ${transfer.file.name}`, 'success');
                    resolveTransfer();
                } catch (error) {
                    transfer.status = 'failed';
                    updateTransferUI(transfer);
                    showToast(`Failed to send ${transfer.file.name}`, 'error');
                    resolveTransfer();
                }
                return;
            }

            if (transfer.status === 'paused' || transfer.status === 'cancelled') {
                resolveTransfer();
                return;
            }

            // Inactivity timeout check
            if (Date.now() - transfer.lastActivity > CONFIG.inactivityTimeout) {
                transfer.status = 'failed';
                updateTransferUI(transfer);
                showToast(`Transfer timed out: ${transfer.file.name}`, 'error');
                resolveTransfer();
                return;
            }

            const chunk = transfer.file.slice(offset, offset + effectiveChunkSize);
            reader.readAsArrayBuffer(chunk);
        };

        reader.onload = async (event) => {
            if (dataChannel.readyState !== 'open') {
                transfer.status = 'paused';
                updateTransferUI(transfer);
                resolveTransfer();
                return;
            }

            try {
                // Send only binary data (no per-chunk JSON metadata)
                await sendWithFlowControl(dataChannel, event.target.result);

                offset += event.target.result.byteLength;
                transfer.bytesTransferred = offset;
                transfer.progress = (offset / transfer.file.size) * 100;
                transfer.lastActivity = Date.now();
                updateTransferUI(transfer);

                // Continue reading immediately
                queueMicrotask(readNextChunk);
            } catch (error) {
                console.error('Error sending chunk:', error);
                transfer.status = 'failed';
                updateTransferUI(transfer);
                showToast(`Failed to send ${transfer.file.name}`, 'error');
                resolveTransfer();
            }
        };

        reader.onerror = () => {
            transfer.status = 'failed';
            updateTransferUI(transfer);
            showToast(`Failed to send ${transfer.file.name}`, 'error');
            resolveTransfer();
        };

        readNextChunk();
    });
}

// Send data with flow control to prevent buffer overflow
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

            case 'file-end':
                handleFileEnd(message, peerId);
                break;

            case 'transfer-cancel':
                handleTransferCancel(message, peerId);
                break;

            case 'transfer-resume':
                handleTransferResume(message, peerId);
                break;

            case 'text-sync':
                handleP2PTextChunk(message, peerId);
                break;
        }
    } catch (error) {
        console.error('Error handling data channel message:', error);
    }
}

// Handle binary data (actual file chunks - no per-chunk metadata needed)
function handleBinaryData(data, peerId) {
    // Find the active receiving transfer for this peer
    for (const [transferId, transfer] of state.activeTransfers) {
        if (transfer.peerId === peerId &&
            transfer.direction === 'receive' &&
            transfer.status === 'receiving') {

            // Store the binary chunk
            const chunkData = new Uint8Array(data);

            // If streaming to disk, serialize writes via a promise chain
            if (transfer.writableStream) {
                const writePromise = (transfer.writeChain || Promise.resolve())
                    .then(() => transfer.writableStream.write(chunkData))
                    .then(() => {
                        transfer.bytesReceived += chunkData.byteLength;
                        transfer.progress = (transfer.bytesReceived / transfer.fileSize) * 100;
                        transfer.lastActivity = Date.now();
                        updateTransferUI(transfer);
                    })
                    .catch(err => {
                        console.error('Error writing to stream:', err);
                        transfer.status = 'failed';
                        updateTransferUI(transfer);
                    });
                transfer.writeChain = writePromise;
            } else {
                // Accumulate in memory (fallback)
                transfer.chunks.push(chunkData);
                transfer.bytesReceived += chunkData.byteLength;
                transfer.progress = (transfer.bytesReceived / transfer.fileSize) * 100;
                transfer.lastActivity = Date.now();
                updateTransferUI(transfer);
            }
            break;
        }
    }
}

// Handle incoming file start
function handleFileStart(message, peerId) {
    const transfer = {
        id: message.transferId,
        peerId: peerId,
        direction: 'receive',
        fileName: message.name,
        fileSize: message.size,
        mimeType: message.mimeType,
        progress: 0,
        bytesReceived: 0,
        status: 'receiving',
        startTime: Date.now(),
        lastActivity: Date.now(),
        chunks: [],
        writableStream: null,
        writeChain: null
    };

    // Register transfer FIRST so incoming binary chunks are not lost
    state.activeTransfers.set(message.transferId, transfer);
    addTransferToUI(transfer);

    showToast(`Receiving ${message.name} from ${state.peers.get(peerId)?.name || 'device'}`, 'info');

    // For large files, try to set up streaming to disk asynchronously
    // Chunks will accumulate in transfer.chunks until the stream is ready,
    // then we flush them and switch to streaming mode
    if (message.size > 100 * 1024 * 1024 && typeof window.showSaveFilePicker === 'function') {
        setupStreamingReceive(transfer, message);
    } else if (message.size > 500 * 1024 * 1024) {
        showToast(`Warning: ${message.name} is very large (${formatFileSize(message.size)}). May use significant memory.`, 'warning');
    }
}

// Set up streaming receive asynchronously (chunks accumulate in memory until ready)
async function setupStreamingReceive(transfer, message) {
    try {
        const fileHandle = await window.showSaveFilePicker({
            suggestedName: message.name,
            types: [{
                description: 'File',
                accept: { [message.mimeType || 'application/octet-stream']: [] }
            }]
        });
        const writable = await fileHandle.createWritable();

        // Switch to streaming mode FIRST, so new chunks go through writeChain
        // instead of appending to transfer.chunks
        transfer.writableStream = writable;

        // Snapshot the accumulated chunks and clear the array atomically
        const pendingChunks = transfer.chunks;
        transfer.chunks = [];

        // Flush pending chunks through the write chain so they serialize
        // with any new chunks arriving via handleBinaryData
        let flushChain = Promise.resolve();
        for (const chunk of pendingChunks) {
            flushChain = flushChain.then(() => writable.write(chunk));
        }
        transfer.writeChain = flushChain;

        console.log(`Streaming large file ${message.name} directly to disk (flushed ${pendingChunks.length} buffered chunks)`);
    } catch (err) {
        // User cancelled or API not supported - stay in memory mode
        console.log('File System Access API unavailable or cancelled, using in-memory fallback');
        if (message.size > 500 * 1024 * 1024) {
            showToast(`Warning: ${message.name} is very large (${formatFileSize(message.size)}). May use significant memory.`, 'warning');
        }
    }
}

// Handle file end
async function handleFileEnd(message, peerId) {
    const transfer = state.activeTransfers.get(message.transferId);

    if (!transfer) {
        console.error('Transfer not found:', message.transferId);
        return;
    }

    if (transfer.writableStream) {
        // Wait for all pending writes to finish before closing
        try {
            if (transfer.writeChain) {
                await transfer.writeChain;
            }
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

    // Integrity check: verify total bytes if sender included it
    if (message.totalBytes && transfer.bytesReceived !== message.totalBytes) {
        console.error(`Size mismatch: received ${transfer.bytesReceived}, expected ${message.totalBytes}`);
        transfer.status = 'failed';
        updateTransferUI(transfer);
        showToast(`Transfer corrupted: ${transfer.fileName} (size mismatch)`, 'error');
        return;
    }

    transfer.status = 'completed';
    updateTransferUI(transfer);

    showToast(`Received ${transfer.fileName}`, 'success');
}

// Resume transfer
function resumeTransfer(transferId) {
    const transfer = state.activeTransfers.get(transferId);
    
    if (!transfer || transfer.status !== 'paused') {
        return;
    }
    
    if (transfer.direction === 'send') {
        transfer.status = 'queued';
        processTransferQueue();
    } else {
        // Send resume request
        const dataChannel = state.dataChannels.get(transfer.peerId);
        if (dataChannel && dataChannel.readyState === 'open') {
            dataChannel.send(JSON.stringify({
                type: 'transfer-resume',
                transferId: transfer.id,
                offset: transfer.bytesReceived
            }));
        }
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
    
    // Notify peer
    const dataChannel = state.dataChannels.get(transfer.peerId);
    if (dataChannel && dataChannel.readyState === 'open') {
        dataChannel.send(JSON.stringify({
            type: 'transfer-cancel',
            transferId: transfer.id
        }));
    }
    
    // Remove from queue
    const index = state.transferQueue.indexOf(transfer);
    if (index > -1) {
        state.transferQueue.splice(index, 1);
    }
    
    // Remove from UI after delay
    setTimeout(() => {
        removeTransferFromUI(transfer.id);
        state.activeTransfers.delete(transfer.id);
    }, 3000);
}

// Handle transfer cancel from peer
function handleTransferCancel(message, peerId) {
    const transfer = state.activeTransfers.get(message.transferId);
    
    if (transfer) {
        transfer.status = 'cancelled';
        updateTransferUI(transfer);
        showToast(`Transfer cancelled by ${state.peers.get(peerId)?.name || 'device'}`, 'warning');
    }
}

// Handle transfer resume from peer
function handleTransferResume(message, peerId) {
    const transfer = state.activeTransfers.get(message.transferId);
    
    if (transfer && transfer.direction === 'send') {
        transfer.bytesTransferred = message.offset;
        transfer.status = 'queued';
        processTransferQueue();
    }
}

// Handle peer disconnect
function handlePeerDisconnect(peerId) {
    state.peers.delete(peerId);
    state.peerLocality.delete(peerId);
    state.sharedText.p2pIncoming.delete(peerId);

    // Close connection
    const connection = state.connections.get(peerId);
    if (connection) {
        connection.close();
        state.connections.delete(peerId);
    }

    // Remove data channel
    state.dataChannels.delete(peerId);
    
    // Pause active transfers
    state.activeTransfers.forEach(transfer => {
        if (transfer.peerId === peerId && (transfer.status === 'sending' || transfer.status === 'receiving')) {
            transfer.status = 'paused';
            updateTransferUI(transfer);
        }
    });
    
    updateDevicesList();
    showToast(`${state.peers.get(peerId)?.name || 'Device'} disconnected`, 'info');
}

// Handle connection failure
function handleConnectionFailure(peerId) {
    console.log(`Connection failed with ${peerId}, attempting reconnection...`);
    
    // Pause active transfers
    state.activeTransfers.forEach(transfer => {
        if (transfer.peerId === peerId && (transfer.status === 'sending' || transfer.status === 'receiving')) {
            transfer.status = 'paused';
            updateTransferUI(transfer);
        }
    });
    
    // Attempt reconnection
    setTimeout(() => {
        if (state.peers.has(peerId)) {
            connectToPeer(peerId);
        }
    }, CONFIG.reconnectDelay);
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
        }

        let statusText = 'Click to connect';
        if (isConnected && locality === 'blocked') {
            statusText = 'Blocked (not local)';
        } else if (isConnected) {
            statusText = 'Click to send files';
        }

        deviceCard.innerHTML = `
            <div class="device-icon">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <rect x="5" y="2" width="14" height="20" rx="2" ry="2"></rect>
                    <line x1="12" y1="18" x2="12" y2="18"></line>
                </svg>
            </div>
            <div class="device-name">${peer.name} ${localityIndicator}</div>
            <div class="device-status">${statusText}</div>
        `;
        
        deviceCard.addEventListener('click', () => {
            if (!isConnected) {
                connectToPeer(peerId);
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
    document.getElementById('previewArea').innerHTML = '';
    document.getElementById('sendFilesBtn').style.display = 'none';
    
    dropZone.classList.remove('hidden');
}

// Handle file selection
function handleFileSelection(files, peerId) {
    console.log(files)
    if (files.length === 0) return;
    
    const peer = state.peers.get(peerId);
    const previewArea = document.getElementById('previewArea');
    const sendFilesBtn = document.getElementById('sendFilesBtn');
    
    // Clear previous previews
    previewArea.innerHTML = '';
    
    // Create previews
    Array.from(files).forEach(file => {
        const preview = document.createElement('div');
        preview.className = 'file-preview';
        
        if (file.type.startsWith('image/')) {
            const img = document.createElement('img');
            img.src = URL.createObjectURL(file);
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
            <div class="file-name">${file.name}</div>
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
                <div class="transfer-filename">${fileName}</div>
                <div class="transfer-details">
                    ${transfer.direction === 'send' ? 'To' : 'From'} ${peerName} • ${formatFileSize(fileSize)}
                </div>
                <div class="transfer-speed" id="speed-${transfer.id}"></div>
            </div>
            <div class="transfer-status ${transfer.status}">${transfer.status}</div>
        </div>
        <div class="progress-bar">
            <div class="progress-fill" style="width: ${transfer.progress}%"></div>
        </div>
        <div class="progress-text" id="progress-${transfer.id}">0% • 0 B / ${formatFileSize(fileSize)}</div>
        <div class="transfer-actions">
            ${transfer.status === 'paused' ? 
                `<button onclick="resumeTransfer('${transfer.id}')">Resume</button>` : ''}
            ${transfer.status === 'sending' || transfer.status === 'receiving' ? 
                `<button onclick="pauseTransfer('${transfer.id}')">Pause</button>` : ''}
            ${transfer.status !== 'completed' && transfer.status !== 'cancelled' ? 
                `<button onclick="cancelTransfer('${transfer.id}')">Cancel</button>` : ''}
        </div>
    `;
    
    transferQueue.appendChild(transferItem);
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
    
    statusElement.className = `transfer-status ${transfer.status}`;
    statusElement.textContent = transfer.status;
    progressFill.style.width = `${transfer.progress}%`;
    
    // Update progress text
    const transferred = transfer.bytesTransferred || transfer.bytesReceived || 0;
    const total = transfer.file?.size || transfer.fileSize || 0;
    progressText.textContent = `${Math.round(transfer.progress)}% • ${formatFileSize(transferred)} / ${formatFileSize(total)}`;
    
    // Calculate and show speed
    if (transfer.startTime && (transfer.status === 'sending' || transfer.status === 'receiving')) {
        const elapsed = (Date.now() - transfer.startTime) / 1000; // seconds
        const speed = transferred / elapsed; // bytes per second
        const remaining = (total - transferred) / speed; // seconds
        
        speedElement.textContent = `${formatFileSize(speed)}/s • ${formatTime(remaining)} remaining`;
    } else {
        speedElement.textContent = '';
    }
    
    // Update actions
    let actionsHTML = '';
    if (transfer.status === 'paused') {
        actionsHTML = `<button onclick="resumeTransfer('${transfer.id}')">Resume</button>`;
    } else if (transfer.status === 'sending' || transfer.status === 'receiving') {
        actionsHTML = `<button onclick="pauseTransfer('${transfer.id}')">Pause</button>`;
    }
    
    if (transfer.status !== 'completed' && transfer.status !== 'cancelled') {
        actionsHTML += `<button onclick="cancelTransfer('${transfer.id}')">Cancel</button>`;
    }
    
    actionsDiv.innerHTML = actionsHTML;
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
    
    let draggedFiles = [];
    let targetPeerId = null;
    
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
        targetPeerId = peerId;
        draggedFiles = files;
        
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

        console.log(peerId)
        console.log(files)
        console.log(state.pendingFiles)

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
        previewArea.innerHTML = '';
        draggedFiles = [];
        targetPeerId = null;
        state.selectedPeer = null;
        // state.pendingFiles.clear();
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

    // LAN-only is a per-device preference, off by default
    try {
        state.sharedText.lanOnly = localStorage.getItem('sharedTextLanOnly') === '1';
    } catch (_) { /* storage unavailable — stay off */ }
    if (ts.lanToggle) {
        ts.lanToggle.checked = state.sharedText.lanOnly;
        ts.lanToggle.addEventListener('change', () => setLanOnlyText(ts.lanToggle.checked));
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

function updateSharedStatus(force) {
    if (state.sharedText.lanOnly) {
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
    ts.hint.textContent = state.sharedText.lanOnly
        ? 'LAN only: text goes directly device-to-device over the encrypted P2P connection and never through the server. Only devices verified to be on your local network receive it.'
        : 'Types live to every device in this room. Formatting is preserved — paste code, copy it back byte-for-byte anywhere.';
}

// ── LAN-only mode ──
// Shared text travels over the same WebRTC data channel as files instead of the
// server relay, and only to peers whose route was verified local (or confirmed
// by the user). Receiving still works from both paths.
function setLanOnlyText(on) {
    const st = state.sharedText;
    st.lanOnly = !!on;
    try {
        localStorage.setItem('sharedTextLanOnly', st.lanOnly ? '1' : '0');
    } catch (_) { /* not persisted — fine */ }
    if (ts.lanToggle) ts.lanToggle.checked = st.lanOnly;
    updateSharedHint();
    updateSharedStatus(true);

    if (st.lanOnly) {
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
        if (dc && dc.readyState === 'open' && (locality === 'verified' || locality === 'confirmed')) {
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
    if (!state.sharedText.lanOnly) return;
    state.peers.forEach((_, peerId) => {
        const conn = state.connections.get(peerId);
        if (conn && !['failed', 'closed', 'disconnected'].includes(conn.connectionState)) return;
        connectToPeer(peerId);
    });
}

// Called when a peer's route gets classified and when its data channel opens —
// these happen in either order, so catch-up is sent once both are done.
function onPeerRouteChecked(peerId) {
    if (!state.sharedText.lanOnly || !ts.textarea) return;
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

    decodeSharedText(inc.parts.join(''), message.enc).then(text => {
        if (message.kind === 'hello' && ts.textarea.value && ts.textarea.value !== text) {
            // Catch-up from a peer: keep whichever text is newer. If ours is
            // newer, answer with it so both sides converge.
            if (theirTime < st.updatedAt) {
                if (st.lanOnly) sendSharedTextP2P('hello', [peerId]);
                return;
            }
            if (theirTime === st.updatedAt) return;
        }

        // The sender keeps this text inside the LAN — follow suit, otherwise our
        // next edit would carry it to the server.
        if (!st.lanOnly) {
            setLanOnlyText(true);
            showToast(`Shared text switched to LAN only (${peerName} shares it only on the local network)`, 'info');
        }
        applyRemoteText(text, message.language, peerName, theirTime || Date.now());
    }).catch(err => {
        console.error('Shared text P2P decode failed:', err);
    });
}

function setSharedStatus(text, cls) {
    if (!ts.status) return;
    ts.status.textContent = text;
    ts.status.className = 'ts-status' + (cls ? ' ' + cls : '');
}

function escapeHtml(str) {
    return str.replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
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
        const lanOnly = state.sharedText.lanOnly;
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
        applyRemoteText(text, inc.language, inc.fromName);
    }).catch(err => {
        console.error('Shared text decode failed:', err);
    });
}

// Apply fully-assembled remote text without echoing it back. Programmatic
// .value assignment does not fire 'input', so no re-send loop occurs.
function applyRemoteText(text, language, fromName, updatedAt) {
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
        setSharedStatus('Updated by ' + fromName, 'remote');
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
window.pauseTransfer = (transferId) => {
    const transfer = state.activeTransfers.get(transferId);
    if (transfer) {
        transfer.status = 'paused';
        updateTransferUI(transfer);
    }
};
