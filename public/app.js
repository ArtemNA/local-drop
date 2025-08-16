// Configuration
const CONFIG = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' }
    ],
    chunkSize: 16384, // 16KB chunks for DataChannel
    reconnectDelay: 3000,
    reconnectMaxAttempts: 5,
    pingInterval: 30000,
    fileTransferTimeout: 300000 // 5 minutes
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
    isReconnecting: false
};

// Initialize application
document.addEventListener('DOMContentLoaded', () => {
    initializeWebSocket();
    setupEventListeners();
    setupDragAndDrop();
    loadDeviceName();
    
    // Check if room code in URL params (for sharing)
    const urlParams = new URLSearchParams(window.location.search);
    const roomFromUrl = urlParams.get('room');
    if (roomFromUrl) {
        document.getElementById('roomInput').value = roomFromUrl;
    }
});

// WebSocket connection
function initializeWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}`;
    
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
            if (message.needsRoom) {
                // Show room selection UI
                showRoomSection();
            } else if (message.peers) {
                // Legacy mode (local network only)
                message.peers.forEach(peer => {
                    state.peers.set(peer.id, peer);
                });
                updateDevicesList();
            }
            break;
            
        case 'room-created':
            handleRoomCreated(message);
            break;
            
        case 'room-joined':
            handleRoomJoined(message);
            break;
            
        case 'peer-joined':
            state.peers.set(message.peer.id, message.peer);
            updateDevicesList();
            showToast(`${message.peer.name} joined the room`, 'info');
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

// Handle room joined
function handleRoomJoined(message) {
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
    
    showToast(`Joined room: ${message.roomCode}`, 'success');
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
    
    const connection = new RTCPeerConnection({ iceServers: CONFIG.iceServers });
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
        }
    };
    
    // Create data channel only if initiator
    if (isInitiator) {
        const dataChannel = connection.createDataChannel('fileTransfer', {
            ordered: true,
            maxRetransmits: 3
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
    
    // Send file metadata
    const metadata = {
        type: 'file-start',
        transferId: transfer.id,
        name: transfer.file.name,
        size: transfer.file.size,
        mimeType: transfer.file.type,
        lastModified: transfer.file.lastModified
    };
    
    dataChannel.send(JSON.stringify(metadata));
    
    // Read and send file in chunks
    const reader = new FileReader();
    let offset = transfer.bytesTransferred;
    
    const readNextChunk = () => {
        if (offset >= transfer.file.size) {
            // File transfer complete
            dataChannel.send(JSON.stringify({
                type: 'file-end',
                transferId: transfer.id
            }));
            
            transfer.status = 'completed';
            updateTransferUI(transfer);
            showToast(`Sent ${transfer.file.name}`, 'success');
            return;
        }
        
        if (transfer.status === 'paused' || transfer.status === 'cancelled') {
            return;
        }
        
        const chunk = transfer.file.slice(offset, offset + CONFIG.chunkSize);
        reader.readAsArrayBuffer(chunk);
    };
    
    reader.onload = (event) => {
        if (dataChannel.readyState !== 'open') {
            transfer.status = 'paused';
            updateTransferUI(transfer);
            return;
        }
        
        // Send chunk with metadata
        const chunkData = {
            type: 'file-chunk',
            transferId: transfer.id,
            offset: offset,
            data: Array.from(new Uint8Array(event.target.result))
        };
        
        dataChannel.send(JSON.stringify(chunkData));
        
        offset += event.target.result.byteLength;
        transfer.bytesTransferred = offset;
        transfer.progress = (offset / transfer.file.size) * 100;
        updateTransferUI(transfer);
        
        // Continue reading
        setTimeout(readNextChunk, 0);
    };
    
    reader.onerror = () => {
        transfer.status = 'failed';
        updateTransferUI(transfer);
        showToast(`Failed to send ${transfer.file.name}`, 'error');
    };
    
    readNextChunk();
}

// Handle incoming data channel messages
function handleDataChannelMessage(event, peerId) {
    try {
        const message = JSON.parse(event.data);
        
        switch (message.type) {
            case 'file-start':
                handleFileStart(message, peerId);
                break;
                
            case 'file-chunk':
                handleFileChunk(message, peerId);
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
        chunks: []
    };
    
    state.activeTransfers.set(message.transferId, transfer);
    addTransferToUI(transfer);
    
    showToast(`Receiving ${message.name} from ${state.peers.get(peerId)?.name || 'device'}`, 'info');
}

// Handle incoming file chunk
function handleFileChunk(message, peerId) {
    const transfer = state.activeTransfers.get(message.transferId);
    
    if (!transfer) {
        console.error('Transfer not found:', message.transferId);
        return;
    }
    
    // Convert array back to Uint8Array
    const chunkData = new Uint8Array(message.data);
    transfer.chunks.push(chunkData);
    transfer.bytesReceived += chunkData.length;
    transfer.progress = (transfer.bytesReceived / transfer.fileSize) * 100;
    
    updateTransferUI(transfer);
}

// Handle file end
function handleFileEnd(message, peerId) {
    const transfer = state.activeTransfers.get(message.transferId);
    
    if (!transfer) {
        console.error('Transfer not found:', message.transferId);
        return;
    }
    
    // Combine all chunks
    const blob = new Blob(transfer.chunks, { type: transfer.mimeType });
    
    // Create download link
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = transfer.fileName;
    a.click();
    
    // Cleanup
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    
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
        
        deviceCard.innerHTML = `
            <div class="device-icon">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <rect x="5" y="2" width="14" height="20" rx="2" ry="2"></rect>
                    <line x1="12" y1="18" x2="12" y2="18"></line>
                </svg>
            </div>
            <div class="device-name">${peer.name}</div>
            <div class="device-status">${isConnected ? 'Connected' : 'Click to connect'}</div>
        `;
        
        deviceCard.addEventListener('click', () => {
            if (!isConnected) {
                connectToPeer(peerId);
            } else {
                showToast('Already connected', 'info');
            }
        });
        
        devicesList.appendChild(deviceCard);
    });
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
            </div>
            <div class="transfer-status ${transfer.status}">${transfer.status}</div>
        </div>
        <div class="progress-bar">
            <div class="progress-fill" style="width: ${transfer.progress}%"></div>
        </div>
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
    const actionsDiv = transferItem.querySelector('.transfer-actions');
    
    statusElement.className = `transfer-status ${transfer.status}`;
    statusElement.textContent = transfer.status;
    progressFill.style.width = `${transfer.progress}%`;
    
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

// Drag and Drop
function setupDragAndDrop() {
    const dropZone = document.getElementById('dropZone');
    const previewArea = document.getElementById('previewArea');
    const sendFilesBtn = document.getElementById('sendFilesBtn');
    const cancelDropBtn = document.getElementById('cancelDropBtn');
    
    let draggedFiles = [];
    let targetPeerId = null;
    
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
            document.body.classList.add('dragging');
            deviceCard.classList.add('drag-over');
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
        const files = Array.from(e.dataTransfer.files);
        
        if (files.length === 0) {
            return;
        }
        
        targetPeerId = peerId;
        draggedFiles = files;
        
        // Show drop zone with preview
        showDropZone(peerId, files);
    });
    
    // Send files button
    sendFilesBtn.addEventListener('click', async () => {
        if (draggedFiles.length > 0 && targetPeerId) {
            await sendFiles(targetPeerId, draggedFiles);
            hideDropZone();
        }
    });
    
    // Cancel button
    cancelDropBtn.addEventListener('click', () => {
        hideDropZone();
    });
    
    function showDropZone(peerId, files) {
        const peer = state.peers.get(peerId);
        const dropTargetName = document.getElementById('dropTargetName');
        dropTargetName.textContent = peer?.name || 'device';
        
        // Clear and populate preview area
        previewArea.innerHTML = '';
        
        files.forEach(file => {
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
        
        sendFilesBtn.style.display = 'inline-block';
        dropZone.classList.remove('hidden');
    }
    
    function hideDropZone() {
        dropZone.classList.add('hidden');
        previewArea.innerHTML = '';
        draggedFiles = [];
        targetPeerId = null;
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
            
            // Notify server
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
