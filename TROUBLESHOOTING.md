# 🔧 Troubleshooting Guide

## Large File Transfer Issues

### Problem: "RTCDataChannel send queue is full" error

**Solution implemented in latest version:**

1. **Flow Control**: The app now monitors `bufferedAmount` and waits for the buffer to clear before sending more data
2. **Binary Transfer**: Files are now sent as binary data instead of JSON-encoded arrays (much more efficient)
3. **Optimized Settings**:
   - Chunk size: up to 256KB, adapted to what the connection allows (64KB fallback)
   - Buffer threshold: 2MB (prevents overflow)
   - Sending resumes on the `bufferedamountlow` event (no polling)

### If you still experience issues:

#### 1. Reduce chunk size for slow networks
Edit `CONFIG.chunkSize` in `public/app.js`:
```javascript
chunkSize: 32 * 1024, // 32KB for slower connections
```

#### 2. Lower buffer threshold for stability
Edit `CONFIG.bufferThreshold` in `public/app.js`:
```javascript
bufferThreshold: 4 * 1024 * 1024, // 4MB for more conservative buffering
```

#### 3. Use TURN server for better connectivity
If transfers fail or are very slow, you might need a TURN server:

**Free TURN servers** (for testing):
```javascript
iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { 
        urls: 'turn:openrelay.metered.ca:80',
        username: 'openrelayproject',
        credential: 'openrelayproject'
    }
]
```

**Your own TURN server** (recommended for production):
- [Coturn](https://github.com/coturn/coturn) - Open source TURN server
- [Twilio TURN](https://www.twilio.com/docs/stun-turn) - Managed service
- [Xirsys](https://xirsys.com/) - Managed service

## Performance Optimization

### For best performance with large files:

1. **Use Chrome/Edge**: Best WebRTC DataChannel implementation
2. **Wired connection**: Ethernet is more stable than Wi-Fi
3. **Same network segment**: Avoid multiple router hops
4. **Close other apps**: Free up bandwidth and CPU

### Network requirements:

- **Minimum**: 1 Mbps upload/download
- **Recommended**: 10+ Mbps for files over 100MB
- **Latency**: <100ms between peers for best experience

## Browser-Specific Issues

### Safari/iOS
- Requires Safari 14.5+ 
- May have lower transfer speeds
- Consider using Chrome on iOS if available

### Firefox
- Good WebRTC support but may be slower than Chrome
- Check that WebRTC is enabled in `about:config`

### Mobile browsers
- Keep screen on during transfer
- Disable battery optimization for browser
- Use "Desktop mode" for better compatibility

## Connection Issues

### "Failed to establish connection"

1. **Check firewall**: Allow UDP ports 1024-65535
2. **Disable VPN**: Can interfere with local connections
3. **Router settings**: Enable UPnP if available
4. **Corporate networks**: May block WebRTC entirely

### "Connection lost during transfer"

If the peer-to-peer connection drops while both pages stay open, the app
reconnects (up to 3 attempts) and the transfer continues from where it
stopped. You can also press **Pause** / **Resume** on either device.

If it can't reconnect:

1. Click the device again to reconnect, then press **Resume**
2. If a page was refreshed or closed, the transfer can't be resumed
   (the device rejoins with a new identity) — cancel it and send the file again

## Server Deployment Issues

### For public deployment

1. **HTTPS required**: WebRTC needs secure context
   ```bash
   # Use reverse proxy with SSL (nginx, Caddy)
   # Or deploy to platforms with automatic HTTPS (Heroku, Render)
   ```

2. **WebSocket support**: Ensure your hosting supports WebSocket
   - ✅ Heroku, Railway, Render, Fly.io
   - ❌ Some shared hosting providers

3. **Environment variables**:
   ```bash
   PUBLIC_DEPLOY=true  # Enable room system
   PORT=3000          # Or your platform's port
   ```

## Debug Mode

To enable detailed logging, open browser console and run:
```javascript
localStorage.setItem('debug', 'true');
location.reload();
```

To disable:
```javascript
localStorage.removeItem('debug');
location.reload();
```

## Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| "DataChannel not ready" | Connection not established | Wait for connection or click device to connect |
| "Room not found" | Room code invalid or expired | Create new room or verify code |
| "Server transfer blocked" | Attempting to send via server | This is normal - files must transfer P2P |
| "Transfer timeout" | File transfer took too long | Check network, try smaller files first |

## Testing

### Test with small files first
1. Start with files under 1MB
2. Then try 10MB
3. Then try 100MB+

### Create test files
```bash
# Windows
fsutil file createnew test_10mb.bin 10485760
fsutil file createnew test_100mb.bin 104857600

# Linux/Mac
dd if=/dev/zero of=test_10mb.bin bs=1M count=10
dd if=/dev/zero of=test_100mb.bin bs=1M count=100
```

## Getting Help

1. Check browser console for errors (F12)
2. Try the debug mode (see above)
3. Test with a different browser
4. Test with smaller files
5. Create an issue on GitHub with:
   - Browser versions
   - File size that fails
   - Console error messages
   - Network type (Wi-Fi/Ethernet)
