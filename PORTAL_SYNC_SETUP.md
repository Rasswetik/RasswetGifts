# Portal Auto Price Sync Setup

## Overview
Automatic background synchronization of gift prices from Portal Marketplace API. Prices in `data/gifts.json` are updated automatically every N minutes without manual intervention.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORTAL_SYNC_ENABLED` | `1` | Enable/disable auto-sync (1=enabled, 0=disabled) |
| `PORTAL_SYNC_INTERVAL_MINUTES` | `10` | Sync frequency in minutes (recommended: 10-15) |
| `PORTAL_AUTH_TOKEN` | preset | Telegram Mini App auth for Portal API access |

## How It Works

1. **Startup**: Background thread starts 30-120 seconds after app initialization
2. **Periodic Sync**: Every N minutes (configurable), the thread:
   - Fetches latest floor prices from Portal API
   - Converts TON prices to stars (100 stars = 1 TON)
   - Updates `data/gifts.json` with new prices
   - Logs results (success count, any errors)
3. **Error Handling**: 
   - Automatic retry with exponential backoff
   - Logs warnings after 3 consecutive failures
   - Continues indefinitely (no crashes)

## Deployment

### Enable Auto Sync (Default)
```bash
# Just deploy — auto-sync starts by default
# Runs every 10 minutes on startup delay
```

### Configure Interval
```bash
# Sync every 5 minutes (more frequent)
PORTAL_SYNC_INTERVAL_MINUTES=5

# Sync every 30 minutes (less frequent)
PORTAL_SYNC_INTERVAL_MINUTES=30
```

### Disable Auto Sync
```bash
PORTAL_SYNC_ENABLED=0
```

## Manual Sync (Optional)
Even with auto-sync enabled, you can still manually trigger:
```bash
curl -X POST http://localhost:5000/api/portal/sync-prices
```

## Logs

### Success Example
```
✅ Portal sync successful: 125/150 gifts updated
```

### Error Example with Backoff
```
⚠️ Portal sync failed: Connection timeout
🚨 Portal sync failed 3 times, backing off...
```

## Troubleshooting

### Prices Not Updating
1. Check `PORTAL_AUTH_TOKEN` is valid Telegram Mini App auth
2. Check Portal API is accessible (test with manual endpoint)
3. Review logs for error messages
4. Verify `data/gifts.json` file exists and is writable

### High Frequency Sync Needed?
- Min interval: 1 minute
- Max recommended: 30 minutes
- Watch Portal API rate limits (document if needed)

### Disable for Maintenance
```bash
# Set env var before deployment
PORTAL_SYNC_ENABLED=0
```

## Price Update Flow

```
Portal API → _portal_sync_floors() → data/gifts.json
                                   ↓
                            [Updated prices ready]
                                   ↓
                         Next /api/gifts call uses new prices
```

## Notes

- Thread-safe implementation (no blocking)
- Automatic retry on failures
- Logs to application logger (visible in deployment console)
- No impact on user gameplay or server performance
- First sync intentionally delayed 30-120s after startup for stability
