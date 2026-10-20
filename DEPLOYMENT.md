# 🚀 Deployment Guide

## Quick Deploy to Popular Platforms

### Heroku
```bash
# Create app
heroku create your-app-name

# Set as public deployment
heroku config:set PUBLIC_DEPLOY=true

# Deploy
git push heroku public-deploy-rooms:main

# Open app
heroku open
```

### Railway
1. Fork/Import this repository to GitHub
2. Connect Railway to your GitHub repo
3. Select `public-deploy-rooms` branch
4. Add environment variable:
   - `PUBLIC_DEPLOY` = `true`
5. Deploy

### Render
1. Create new Web Service
2. Connect GitHub repository
3. Branch: `public-deploy-rooms`
4. Build Command: `npm install`
5. Start Command: `npm start`
6. Environment Variables:
   - `PUBLIC_DEPLOY` = `true`

### Vercel (Static + Serverless)
⚠️ Vercel doesn't support WebSocket, use Railway or Render instead

### Fly.io
```bash
# Install flyctl
# Create fly.toml
fly launch

# Set environment
fly secrets set PUBLIC_DEPLOY=true

# Deploy
fly deploy
```

## Room System Usage

### Creating Rooms
1. Open app: `https://your-app.com`
2. Click "Create New Room"
3. Share the 6-character code

### Direct Room Links
Share direct links for instant access:
```
https://your-app.com?room=ABC123
```

When users open this link, they automatically join room ABC123

### API Endpoints
- `GET /api/info` - Server statistics
  ```json
  {
    "totalClients": 5,
    "totalRooms": 2,
    "isPublicDeployment": true
  }
  ```

## Testing Your Deployment

### 1. Basic Connection Test
- Open app in two different browsers
- Create a room in first browser
- Join same room in second browser
- Devices should see each other

### 2. Direct Link Test
- Create a room and copy the URL with `?room=XXXXXX`
- Open in incognito/private window
- Should auto-join the same room

### 3. File Transfer Test
- Connect two devices in same room
- Click connected device to select files
- Or drag & drop files onto device
- Check transfer completes

## Common Issues

### "Cannot create room"
- Ensure `PUBLIC_DEPLOY=true` is set
- Check server logs for errors

### "Devices don't see each other"
- Verify both in same room (check room code in header)
- Try refreshing both browsers
- Check WebSocket connection in browser console

### "Transfer fails"
- Check if both devices are connected (green status)
- Try smaller files first (<10MB)
- Ensure good internet connection

### "Everyone in same room" (with `AUTO_ROOM=true`)
- Auto rooms group devices by the public IP the server sees
- Behind a reverse proxy that IP comes from `X-Forwarded-For`; if every user
  lands in one room, the server is seeing the proxy's address — set
  `TRUSTED_PROXY_HOPS` to the number of proxies in front of the app (default 1)

## Security Considerations

### For Production
1. **Add HTTPS** (required for WebRTC)
   - Use platform's built-in SSL
   - Or add Cloudflare in front

2. **Built-in limits** (no setup needed)
   - Room codes: 6 letters/digits; at most 20 new rooms per IP per 10 minutes
   - Shared text through the server: 10MB per text (compressed), 30MB per
     client per minute, 200MB for all rooms together
   - WebSocket messages over 1MB are rejected

3. **Possible additions**
   - Max users per room
   - Password-protected rooms (future feature)

4. **Monitor usage**
   - Track room creation rate
   - Monitor bandwidth usage
   - Set up alerts for suspicious activity

## Environment Variables Reference

| Variable | Values | Description |
|----------|--------|-------------|
| `PORT` | Number | Server port (default: 3000) |
| `PUBLIC_DEPLOY` | true/false | Enable room system for public deployment |
| `AUTO_ROOM` | true/false | With `PUBLIC_DEPLOY`: devices on the same network (same public IP) join the same room automatically |
| `AUTO_ROOM_SECRET` | String | Key for auto-room codes; set it to keep codes stable across restarts (default: random per start) |
| `TRUSTED_PROXY_HOPS` | Number | Reverse proxies in front of the app, used to read the client IP from `X-Forwarded-For` (default: 1) |
| `MAINTENANCE_MODE` | true/false | Show the maintenance page to all visitors |
| `CLIENT_IDLE_TIMEOUT_MS` | Number | Drop clients that sent nothing for this long, e.g. pages frozen by the browser (default: 150000) |

## Recommended Platforms

| Platform | WebSocket | Free Tier | Auto-SSL | Ease |
|----------|-----------|-----------|----------|------|
| Railway | ✅ | ✅ 500hrs | ✅ | ⭐⭐⭐⭐⭐ |
| Render | ✅ | ✅ 750hrs | ✅ | ⭐⭐⭐⭐⭐ |
| Heroku | ✅ | ❌ | ✅ | ⭐⭐⭐⭐ |
| Fly.io | ✅ | ✅ Limited | ✅ | ⭐⭐⭐ |
| Vercel | ❌ | ✅ | ✅ | ⭐⭐⭐⭐⭐ |
| Netlify | ❌ | ✅ | ✅ | ⭐⭐⭐⭐⭐ |

**Best choice**: Railway or Render for easy WebSocket support

## Support

- Check [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for common issues
- Create GitHub issue for bugs
- Test locally first: `PUBLIC_DEPLOY=true npm start`
