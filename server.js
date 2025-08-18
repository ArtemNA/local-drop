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

// Track message sizes per client to detect file transfer attempts
const clientMessageStats = new Map();

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
    lastResetTime: Date.now()
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
        roomCode: normalizedRoomCode,
        deviceName: clientInfo.name,
        peers: peersInRoom
      }));
      
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
      roomCode: roomCode,
      deviceName: clientInfo.name,
      autoRoom: true,
      peers: peersInRoom
    }));
    
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
    roomCode: normalizedCode,
    peers: peersInRoom
  }));
  
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

// API endpoint to get server info
app.get('/api/info', (req, res) => {
  res.json({
    totalClients: clients.size,
    totalRooms: rooms.size,
    isPublicDeployment: PUBLIC_DEPLOY,
    autoRoomEnabled: AUTO_ROOM_MODE
  });
});

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
