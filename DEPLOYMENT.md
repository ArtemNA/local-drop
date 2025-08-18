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

### "Everyone in same room"
- Do NOT use `AUTO_ROOM=true` with public deployment
- This puts all users in one room (server IP is same for all)

## Security Considerations

### For Production
1. **Add HTTPS** (required for WebRTC)
   - Use platform's built-in SSL
   - Or add Cloudflare in front

2. **Add rate limiting**
   ```javascript
   // In server.js
   const rateLimit = require('express-rate-limit');
   app.use(rateLimit({
     windowMs: 15 * 60 * 1000, // 15 minutes
     max: 100 // limit each IP to 100 requests
   }));
   ```

3. **Add room limits**
   - Max users per room
   - Room expiration after inactivity
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
| `AUTO_ROOM` | true/false | ⚠️ DO NOT use with PUBLIC_DEPLOY |

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
