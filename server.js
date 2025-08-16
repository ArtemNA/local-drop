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

// Store connected clients
const clients = new Map();

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

// Message size limit for signaling (100KB should be enough for SDP/ICE)
const MAX_SIGNAL_SIZE = 100 * 1024; // 100KB

// Track message sizes per client to detect file transfer attempts
const clientMessageStats = new Map();

wss.on('connection', (ws, req) => {
  const clientId = uuidv4();
  const clientIp = req.socket.remoteAddress;
  
  // Initialize client info
  const clientInfo = {
    id: clientId,
    ws: ws,
    name: `Device-${clientId.slice(0, 6)}`,
    ip: clientIp,
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
  
  // Send client their ID and list of peers
  ws.send(JSON.stringify({
    type: 'init',
    clientId: clientId,
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
          clients.delete(clientId);
          clientMessageStats.delete(clientId);
        }, 100);
        
        return;
      }
      
      const message = JSON.parse(data);
      
      // Validate message types (only signaling allowed)
      const allowedTypes = ['offer', 'answer', 'ice-candidate', 'set-name', 'ping'];
      if (!allowedTypes.includes(message.type)) {
        console.warn(`[Warning] Unknown message type from ${clientId}: ${message.type}`);
        return;
      }
      
      switch (message.type) {
        case 'set-name':
          clientInfo.name = message.name.slice(0, 30); // Limit name length
          broadcast(clientId, {
            type: 'peer-updated',
            peer: {
              id: clientId,
              name: clientInfo.name
            }
          });
          break;
          
        case 'offer':
        case 'answer':
        case 'ice-candidate':
          // Forward signaling messages to target peer
          if (message.targetId && clients.has(message.targetId)) {
            const targetClient = clients.get(message.targetId);
            targetClient.ws.send(JSON.stringify({
              ...message,
              fromId: clientId,
              fromName: clientInfo.name
            }));
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
    clients.delete(clientId);
    clientMessageStats.delete(clientId);
    
    // Notify other clients
    broadcast(clientId, {
      type: 'peer-left',
      peerId: clientId
    });
  });
  
  ws.on('error', (error) => {
    console.error(`[Error] WebSocket error for ${clientId}:`, error.message);
  });
});

// Broadcast message to all clients except sender
function broadcast(senderId, message) {
  clients.forEach((client, id) => {
    if (id !== senderId && client.ws.readyState === 1) {
      client.ws.send(JSON.stringify(message));
    }
  });
}

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
  console.log('📱 Open this URL on all devices in the same network');
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
