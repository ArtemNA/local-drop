// Configuration
const CONFIG = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' }
    ],
    chunkSize: 1 * 1024 * 1024, // 1MB chunks for better throughput
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
                message.peers.forEach(peer => {
                    state.peers.set(peer.id, peer);
                });
                
                // Hide room section, show devices
                document.getElementById('roomSection').style.display = 'none';
                document.getElementById('devicesList').parentElement.style.display = 'block';
                updateDevicesList();
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
            
            state.roomCode = message.roomCode;
            state.inRoom = true;
            
            // Add existing peers in room
            message.peers.forEach(peer => {
                state.peers.set(peer.id, peer);
            });
            
            hideRoomSection();
            showRoomInfo(message.roomCode);
            updateDevicesList();
            
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
            verifyLocality(connection, peerId);
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
        
        // Resume any pending transfers
        const pendingTransfer = state.activeTransfers.get(peerId);
        if (pendingTransfer && pendingTransfer.status === 'paused') {
            resumeTransfer(peerId);
        }
    };
    
    dataChannel.onclose = () => {
        console.log(`Data channel closed with ${peerId}`);
        state.dataChannels.delete(peerId);
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
        if (localCandidateType === 'host' && remoteCandidateType === 'host' &&
            isPrivateIP(localIP) && isPrivateIP(remoteIP)) {
            state.peerLocality.set(peerId, 'verified');
            updateDevicesList();
            return;
        }

        // Additional check: same subnet via localIP hints
        if (state.localIP && isPrivateIP(localIP) && isPrivateIP(remoteIP)) {
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
    
    // Send file metadata as JSON (includes chunkSize so receiver knows boundaries)
    const metadata = {
        type: 'file-start',
        transferId: transfer.id,
        name: transfer.file.name,
        size: transfer.file.size,
        mimeType: transfer.file.type,
        lastModified: transfer.file.lastModified,
        chunkSize: CONFIG.chunkSize
    };

    await sendWithFlowControl(dataChannel, JSON.stringify(metadata));

    // Track inactivity for timeout
    transfer.lastActivity = Date.now();

    // Read and send file in chunks (no per-chunk JSON metadata)
    const reader = new FileReader();
    let offset = transfer.bytesTransferred;

    const readNextChunk = async () => {
        if (offset >= transfer.file.size) {
            // File transfer complete
            await sendWithFlowControl(dataChannel, JSON.stringify({
                type: 'file-end',
                transferId: transfer.id,
                totalBytes: offset
            }));

            transfer.status = 'completed';
            updateTransferUI(transfer);
            showToast(`Sent ${transfer.file.name}`, 'success');
            return;
        }

        if (transfer.status === 'paused' || transfer.status === 'cancelled') {
            return;
        }

        // Inactivity timeout check
        if (Date.now() - transfer.lastActivity > CONFIG.inactivityTimeout) {
            transfer.status = 'failed';
            updateTransferUI(transfer);
            showToast(`Transfer timed out: ${transfer.file.name}`, 'error');
            return;
        }

        const chunk = transfer.file.slice(offset, offset + CONFIG.chunkSize);
        reader.readAsArrayBuffer(chunk);
    };

    reader.onload = async (event) => {
        if (dataChannel.readyState !== 'open') {
            transfer.status = 'paused';
            updateTransferUI(transfer);
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
        }
    };

    reader.onerror = () => {
        transfer.status = 'failed';
        updateTransferUI(transfer);
        showToast(`Failed to send ${transfer.file.name}`, 'error');
    };

    readNextChunk();
}

// Send data with flow control to prevent buffer overflow
async function sendWithFlowControl(dataChannel, data) {
    if (dataChannel.readyState !== 'open') {
        throw new Error('DataChannel is not open');
    }

    while (dataChannel.bufferedAmount > CONFIG.bufferThreshold) {
        await new Promise(resolve => {
            dataChannel.bufferedAmountLowThreshold = CONFIG.bufferThreshold;
            dataChannel.onbufferedamountlow = () => {
                dataChannel.onbufferedamountlow = null;
                resolve();
            };
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
        const el = document.getElementById('appVersion');
        if (el && data.version) {
            const d = new Date(data.startedAt);
            const pad = n => String(n).padStart(2, '0');
            const stamp = `${d.getFullYear()}.${pad(d.getMonth()+1)}.${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
            el.textContent = `v${data.version} · deploy ${stamp}`;
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
