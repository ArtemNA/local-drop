# 📡 Local File Transfer - P2P File Sharing

A completely local file transfer application that allows secure P2P file sharing between devices. Works both on local networks and can be deployed publicly with room-based isolation.

## ✨ Features

- **🔒 Completely P2P**: Files are transferred directly between devices via WebRTC DataChannel, never through the server
- **🛡️ Server Protection**: Built-in detection and blocking of file transfer attempts through the server
- **📱 Cross-Platform**: Works on Mac, PC, Android, iOS - any device with a modern web browser
- **🏠 Room System**: Three modes available:
  - **Local Mode**: Auto-discovery on same network
  - **Manual Rooms**: Create/join rooms with codes
  - **Auto Rooms**: Automatic room assignment based on network
- **📂 Multiple Ways to Send**:
  - **Drag & Drop**: Drag files onto device cards
  - **Click to Select**: Click connected device to open file picker (mobile-friendly!)
- **🖼️ Image Preview**: Preview images before sending
- **📊 Progress Tracking**: Real-time progress bars with speed indicators
- **🔄 Resume Support**: Automatically resume interrupted transfers
- **📦 Queue System**: Send multiple files in a queue
- **🔌 Auto-Reconnection**: Automatically reconnects if connection is lost
- **💬 Two-Way Communication**: Both peers can send files in the same session
- **🌐 Public Deploy Ready**: Can be deployed publicly with room-based isolation

## 🚀 Quick Start

### Prerequisites

- Node.js (v14 or higher)
- npm or yarn

### Installation

1. Clone or extract this project to your local machine

2. Install dependencies:
```bash
npm install
```

### Running the Server

#### Mode 1: Local Network Only (Default)
```bash
npm start
```
- All devices on same network see each other automatically
- No room codes needed
- Perfect for home/office use

#### Mode 2: Public with Manual Rooms (Recommended for cloud)
```bash
PUBLIC_DEPLOY=true npm start
```
- Users create/join rooms manually
- Share room links: `https://your-app.com?room=ABC123`
- Complete control over who connects
- Best for public deployment

#### Mode 3: Auto Rooms (⚠️ NOT for public cloud)
```bash
# Only for private servers with local network detection
AUTO_ROOM=true npm start
```
- ⚠️ **WARNING**: Don't use with PUBLIC_DEPLOY on cloud platforms
- On cloud, all users share same server IP and end up in same room
- Only useful for private servers that can detect local network segments

### Deployment Options

#### 1. Local Network Only
- Run without any special configuration
- All devices on the same network see each other automatically
- Perfect for home/office use

#### 2. Public Deployment (Heroku, Railway, Render, etc.)

**For Heroku:**
```bash
# Create app
heroku create your-app-name

# Set environment
heroku config:set PUBLIC_DEPLOY=true

# Deploy
git push heroku public-deploy-rooms:main
```

**For Railway:**
1. Connect your GitHub repo
2. Add environment variable: `PUBLIC_DEPLOY=true`
3. Deploy automatically

**For Render:**
1. Create new Web Service
2. Connect your GitHub repo
3. Add environment variable: `PUBLIC_DEPLOY=true`
4. Set build command: `npm install`
5. Set start command: `npm start`

## 📖 How to Use

### Sending Files

**Method 1: Click to Select (Mobile-Friendly)**
1. Connect to a device by clicking it
2. Click the connected device again
3. Choose files using the file picker
4. Preview and send

**Method 2: Drag & Drop (Desktop)**
1. Connect to a device
2. Drag files from your file explorer
3. Drop them onto the connected device
4. Preview and send

### Room Systems

#### Auto-Room Mode (Simplest)
- Devices from same network automatically join same room
- No setup required - just open the app
- Perfect for offices, schools, or homes

#### Manual Room Mode
1. **Create a Room**:
   - Click "Create New Room"
   - Share the 6-character code or link
   
2. **Join a Room**:
   - Enter the room code
   - Or use the shared link

#### Local Network Mode
- Open app on all devices in same Wi-Fi
- Devices appear automatically
- No room codes needed

## 🔐 Security & Privacy

### Room Isolation
- Each room is completely isolated
- Room codes are randomly generated (1.6 billion combinations)
- No data is stored on the server
- Rooms are automatically deleted when empty

### Network Detection
When deployed publicly, the app:
- Uses room codes to group users
- Doesn't detect or validate local networks
- Allows secure file sharing between any devices with the same room code
- Perfect for sharing between different locations

### P2P Security
- All file transfers happen directly between devices
- WebRTC provides built-in encryption
- Server never sees file content
- Automatic blocking of server transfer attempts

## 🏗️ Architecture

### Public Deployment Mode
```
┌──────────┐    Room: ABC123    ┌──────────┐
│ Device A │◄───────────────────►│  Server  │
│Location 1│     (WebSocket)     │  Public  │
└─────┬────┘                     └────┬─────┘
      │                                │
      │         P2P DataChannel        │
      │         (File Transfer)        │
      │                                │
      └────────────────────────────────┤
                                 ┌─────┴────┐
                                 │ Device B │
                                 │Location 2│
                                 └──────────┘
```

### Components

- **Server (`server.js`)**: 
  - WebSocket signaling server
  - Room management
  - Device discovery within rooms
  - Connection brokering
  - Transfer protection

- **Client (`public/app.js`)**: 
  - Room creation/joining
  - WebRTC connection management
  - File chunking and transfer
  - UI updates and progress tracking
  - Reconnection logic

## 🛠️ Configuration

### Environment Variables

- `PORT`: Server port (default: 3000)
- `PUBLIC_DEPLOY`: Set to `true` for public deployment with rooms

### Client Configuration
Edit `CONFIG` object in `public/app.js`:

```javascript
const CONFIG = {
    iceServers: [...],        // STUN/TURN servers
    chunkSize: 16384,         // Size of each chunk (16KB default)
    reconnectDelay: 3000,     // Delay before reconnection attempt
    reconnectMaxAttempts: 5,  // Maximum reconnection attempts
    pingInterval: 30000,      // Keep-alive ping interval
    fileTransferTimeout: 300000 // File transfer timeout (5 min)
};
```

## 📱 Browser Support

- Chrome/Edge 79+
- Firefox 72+
- Safari 14.1+
- Opera 66+
- Chrome Android 79+
- Safari iOS 14.5+

## 🐛 Troubleshooting

### Large Files (20MB+)

The latest version includes optimizations for large file transfers:
- **Flow control** prevents buffer overflow
- **Binary transfer** for better performance  
- **Automatic speed adjustment** based on network conditions

If you experience issues, see [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for detailed solutions.

### Quick Fixes

**Slow transfer?**
- Use wired connection instead of Wi-Fi
- Close other bandwidth-heavy applications
- Try Chrome/Edge for best performance

**Connection failed?**
- Check firewall settings
- Disable VPN temporarily
- Ensure both devices are in the same room (for public deployment)

### Room Issues

**Can't join room:**
- Ensure room code is entered correctly (6 characters, case-insensitive)
- Check if room still exists (rooms expire when empty)
- Try creating a new room

**Devices not appearing in room:**
- Verify both devices joined the same room
- Check the room code in the header
- Refresh both devices

### Connection Issues

**Connection failed:**
1. Check firewall settings
2. For public deployments, ensure WebRTC ports aren't blocked
3. Try using a TURN server for restrictive networks

**Transfer stuck:**
1. Check if the data channel is still open
2. Try pausing and resuming the transfer
3. Cancel and retry if necessary

## 📄 License

MIT License - feel free to use and modify

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## 🔮 Future Enhancements

- [ ] Password-protected rooms
- [ ] Room expiration settings
- [ ] File encryption options
- [ ] Transfer history
- [ ] Mobile app versions
- [ ] TURN server integration for restrictive networks
- [ ] Room participant limit settings

## 🙏 Credits

Built with:
- Node.js & Express
- WebSocket (ws)
- WebRTC DataChannel
- Pure JavaScript (no frameworks)

---

**Note**: For production deployments, consider adding:
- HTTPS support (required for WebRTC in modern browsers)
- TURN servers for NAT traversal
- Rate limiting
- Room expiration policies
