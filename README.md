# 📡 Local File Transfer - P2P File Sharing

A completely local file transfer application that allows secure P2P file sharing between devices on the same Wi-Fi network using WebRTC DataChannel.

## ✨ Features

- **🔒 Completely P2P**: Files are transferred directly between devices via WebRTC DataChannel, never through the server
- **🛡️ Server Protection**: Built-in detection and blocking of file transfer attempts through the server
- **📱 Cross-Platform**: Works on Mac, PC, Android, iOS - any device with a modern web browser
- **🎯 Drag & Drop**: Simply drag files onto a device to send them
- **🖼️ Image Preview**: Preview images before sending
- **📊 Progress Tracking**: Real-time progress bars for all transfers
- **🔄 Resume Support**: Automatically resume interrupted transfers
- **📦 Queue System**: Send multiple files in a queue
- **🔌 Auto-Reconnection**: Automatically reconnects if connection is lost
- **💬 Two-Way Communication**: Both peers can send files in the same session

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

1. Start the server:
```bash
npm start
```

Or for development with auto-restart:
```bash
npm run dev
```

2. The server will display:
   - Local URL: `http://localhost:3000`
   - Network URL: `http://YOUR_IP:3000`

3. Open the Network URL on all devices you want to transfer files between

## 📖 How to Use

### Connecting Devices

1. Open the app URL on all devices in the same Wi-Fi network
2. Each device will automatically appear in the "Available Devices" section
3. Click on a device to establish a P2P connection

### Sending Files

**Method 1: Drag & Drop**
1. Drag files from your file explorer
2. Drop them onto the device you want to send to
3. Preview the files and click "Send Files"

**Method 2: Click to Select** (coming soon)
1. Click on a connected device
2. Select files using the file picker
3. Confirm and send

### Managing Transfers

- **Pause**: Click pause button to temporarily stop a transfer
- **Resume**: Click resume to continue a paused transfer
- **Cancel**: Click cancel to stop and remove a transfer
- **Auto-Resume**: If connection is lost, transfers automatically pause and resume when reconnected

### Changing Device Name

1. Click on your device name in the header
2. Enter a new name
3. Click Save

## 🔐 Security Features

### Server Transfer Protection

The server actively monitors for file transfer attempts and will:
- Detect messages larger than 100KB (normal signaling is <10KB)
- Track rapid small messages that might be chunked files
- Immediately disconnect clients attempting to transfer files through the server
- Only allow WebRTC signaling messages (offer, answer, ICE candidates)

### P2P Security

- All file transfers happen directly between devices
- No files are stored on the server
- Connections use WebRTC's built-in encryption
- Works only within your local network

## 🏗️ Architecture

```
┌──────────┐      Signaling      ┌──────────┐
│ Device A │◄────────────────────►│  Server  │
└─────┬────┘     (WebSocket)      └────┬─────┘
      │                                 │
      │         Signaling              │
      │◄───────────────────────────────►│
      │                                 │
      │                           ┌─────┴────┐
      │      P2P DataChannel      │ Device B │
      └───────────────────────────►└──────────┘
           (File Transfer)
```

### Components

- **Server (`server.js`)**: 
  - WebSocket signaling server
  - Device discovery
  - Connection brokering
  - Transfer protection

- **Client (`public/app.js`)**: 
  - WebRTC connection management
  - File chunking and transfer
  - UI updates and progress tracking
  - Reconnection logic

## 🛠️ Configuration

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

### Devices Not Appearing

1. Ensure all devices are on the same Wi-Fi network
2. Check firewall settings - allow port 3000
3. Try using the IP address instead of localhost
4. Disable VPN if active

### Connection Failed

1. Refresh both devices
2. Check browser console for errors
3. Ensure WebRTC is not blocked by browser extensions
4. Try a different browser

### Transfer Stuck

1. Check if the data channel is still open
2. Try pausing and resuming the transfer
3. Cancel and retry if necessary

## 📄 License

MIT License - feel free to use and modify

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## 🔮 Future Enhancements

- [ ] File picker UI for non-drag-drop sending
- [ ] Folder transfer support
- [ ] Transfer history
- [ ] Encryption options
- [ ] QR code for easy device connection
- [ ] Transfer speed optimization
- [ ] Mobile app versions

## 🙏 Credits

Built with:
- Node.js & Express
- WebSocket (ws)
- WebRTC DataChannel
- Pure JavaScript (no frameworks)

---

**Note**: This application is designed for local network use only. Do not expose the server to the public internet without additional security measures.
