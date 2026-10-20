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
- **⏸️ Pause & Resume**: Pause from either device; transfers continue automatically after a brief connection drop
- **🏠 LAN only** (default): Files and shared text go only to devices verified to be on your local network
- **📝 Shared Text**: Live shared clipboard with syntax highlighting. With LAN only it goes directly between devices and never through the server
- **🔑 Verify**: Compare a 6-digit code on both devices to rule out a man-in-the-middle
- **💾 Large Files**: Very large files can be streamed straight to disk ("Save to disk") in supported browsers
- **📦 Queue System**: Send multiple files in a queue
- **🔌 Auto-Reconnection**: Automatically reconnects if connection is lost
- **💬 Two-Way Communication**: Both peers can send files in the same session
- **🌐 Public Deploy Ready**: Can be deployed publicly with room-based isolation

## 🚀 Quick Start

### Prerequisites

- Node.js (v18 or higher)
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

#### Mode 2: Public with Manual Rooms
```bash
PUBLIC_DEPLOY=true npm start
```
- Users create/join rooms manually
- Share room links: `https://your-app.com?room=ABC123`
- Complete control over who connects
- Best for most public deployments

#### Mode 3: Public with Auto Rooms (Network Detection)
```bash
PUBLIC_DEPLOY=true AUTO_ROOM=true npm start
```
- Devices from same local network auto-join same room
- "Same network" = same public IP as seen by the server (devices behind one router/NAT)
- Perfect for offices, schools, cafes
- Users can still create/join manual rooms if needed
- Room codes are derived from the IP with a server secret (`AUTO_ROOM_SECRET`), so they can't be guessed from an address

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
- Room codes are randomly generated (about 2.2 billion combinations)
- Room creation and shared text through the server are rate-limited
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

### Verifying a Connection
The encryption keys of a peer-to-peer connection are exchanged through the
server, so a compromised server could try to sit in the middle. Press
**Verify** on a connected device: both devices show a 6-digit code derived
from the keys they actually use plus a random value from each side
(committed before it is revealed, so it can't be brute-forced). Same code on
both screens means a direct connection — press **Codes match** on each device.

- Each device keeps one certificate (IndexedDB), so verified devices are
  remembered and shown as verified on later connections.
- **Codes differ** closes the connection and stops automatic reconnects.
- This protects the connection itself. It can't protect against a server that
  serves modified app code — for that, run the server yourself (local mode).

### LAN only (default)
One switch at the top of the page. When on, files and shared text go only to
(and are only accepted from) devices whose connection is verified as local.
Turn it off to also send over the internet — still encrypted and directly
between the devices.

A connection counts as local when the route WebRTC actually uses is direct
(no NAT traversal, no relay) and either both addresses are private /
link-local, both are in the same IPv6 /64, or — if the browser hides the
addresses — the round-trip time is LAN-like (≤ 10 ms) or the server saw both
devices behind the same network and the RTT is ≤ 30 ms.

### Shared Text
- **LAN only** (default): text travels over the same encrypted peer-to-peer
  connection as files, only to devices whose connection is verified as local.
  The server never sees it. Text received this way is never relayed through
  the server, even from a device with LAN only off.
- With **LAN only** off, text is relayed through the server so it also works
  between different networks. The server keeps the latest text in memory for
  devices that join later (until the room empties); it is protected in transit
  (HTTPS/WSS) but readable by the server.
- Through the server, texts are limited to about 10MB compressed.

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
- `AUTO_ROOM`: With `PUBLIC_DEPLOY`, put devices from the same network into the same room
- `AUTO_ROOM_SECRET`: Key for auto-room codes (default: random on every start)
- `TRUSTED_PROXY_HOPS`: Number of reverse proxies in front of the app (default: 1)
- `MAINTENANCE_MODE`: Set to `true` to show the maintenance page

### Client Configuration
Edit `CONFIG` object in `public/app.js`:

```javascript
const CONFIG = {
    iceServers: [...],                // STUN/TURN servers
    chunkSize: 64 * 1024,             // Fallback chunk size (adapted to the connection, up to 256KB)
    bufferThreshold: 2 * 1024 * 1024, // Pause sending above this much buffered data
    reconnectDelay: 3000,             // Base delay before a reconnection attempt
    reconnectMaxAttempts: 5,          // Maximum server reconnection attempts
    pingInterval: 30000               // Keep-alive ping interval
};
```

## 🧪 Tests

```bash
npm test
```

Starts the server in each mode and checks it over WebSocket/HTTP (input
validation, limits, rooms). No extra dependencies.

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
