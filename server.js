const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const os = require('os');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Serve static files
app.use(express.static('public'));

// Store rooms and clients
const rooms = new Map(); // roomCode -> Set of clientIds
const clients = new Map(); // clientId -> clientInfo

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

// Get clients in room
function getClientsInRoom(roomCode) {
  const roomClientIds = rooms.get(roomCode);
  if (!roomClientIds) return [];
  
  return Array.from(roomClientIds)
    .map(id => clients.get(id))
    .filter(client => client !== undefined);
}

// Message size limit for signaling (100KB should be enough for SDP/ICE)
const MAX_SIGNAL_SIZE = 100 * 1024; // 100KB

// Track message sizes per client to detect file transfer attempts
const clientMessageStats = new Map();

wss.on('connection', (ws, req) => {
  const clientId = uuidv4();
  const clientIp = req.socket.remoteAddress;
  
  // Initialize client info (without room initially)
  const clientInfo = {
    id: clientId,
    ws: ws,
    name: `Device-${clientId.slice(0, 6)}`,
    ip: clientIp,
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
  
  console.log(`[Connect] Client ${clientId} connected from ${clientIp}`);
  
  // Send client their ID (but not peers yet - they need to join a room first)
  ws.send(JSON.stringify({
    type: 'init',
    clientId: clientId,
    needsRoom: true
  }));
  
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
          handleCreateRoom(clientInfo);
          break;
          
        case 'join-room':
          handleJoinRoom(clientInfo, message.roomCode);
          break;
          
        case 'set-name':
          clientInfo.name = message.name.slice(0, 30); // Limit name length
          
          // Broadcast to room members only
          if (clientInfo.roomCode) {
            broadcastToRoom(clientInfo.roomCode, clientId, {
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
          // Forward signaling messages to target peer (must be in same room)
          if (message.targetId && clients.has(message.targetId)) {
            const targetClient = clients.get(message.targetId);
            
            // Check if both clients are in the same room
            if (targetClient.roomCode === clientInfo.roomCode && clientInfo.roomCode !== null) {
              targetClient.ws.send(JSON.stringify({
                ...message,
                fromId: clientId,
                fromName: clientInfo.name
              }));
            } else {
              ws.send(JSON.stringify({
                type: 'error',
                message: 'Target peer not in same room',
                code: 'PEER_NOT_IN_ROOM'
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
  // Check if room exists
  if (!rooms.has(roomCode)) {
    clientInfo.ws.send(JSON.stringify({
      type: 'error',
      message: 'Room not found',
      code: 'ROOM_NOT_FOUND'
    }));
    return;
  }
  
  // Leave current room if in one
  if (clientInfo.roomCode) {
    leaveRoom(clientInfo.id, clientInfo.roomCode);
  }
  
  // Join new room
  rooms.get(roomCode).add(clientInfo.id);
  clientInfo.roomCode = roomCode;
  
  console.log(`[Room] Client ${clientInfo.id} joined room ${roomCode}`);
  
  // Get other peers in room
  const peersInRoom = getClientsInRoom(roomCode)
    .filter(client => client.id !== clientInfo.id)
    .map(client => ({
      id: client.id,
      name: client.name
    }));
  
  // Send room info to joining client
  clientInfo.ws.send(JSON.stringify({
    type: 'room-joined',
    roomCode: roomCode,
    peers: peersInRoom
  }));
  
  // Notify other room members
  broadcastToRoom(roomCode, clientInfo.id, {
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
    isPublicDeployment: process.env.PUBLIC_DEPLOY === 'true'
  });
});

// Start server
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  const localIP = getLocalIP();
  const isPublic = process.env.PUBLIC_DEPLOY === 'true';
  
  console.log('');
  console.log('🚀 Local File Transfer Server Started');
  console.log('=====================================');
  console.log(`📡 Local:    http://localhost:${PORT}`);
  console.log(`🌐 Network:  http://${localIP}:${PORT}`);
  console.log('=====================================');
  if (isPublic) {
    console.log('🌍 Running in PUBLIC mode - Room codes required');
    console.log('📝 Users must create/join rooms to connect');
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
