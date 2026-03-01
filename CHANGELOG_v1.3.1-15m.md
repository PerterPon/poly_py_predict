# Changelog v1.3.1-15m

## Bulletproof Wallet Detection
- **Profile-based detection:** Auto-Detect now uses Polymarket's profile API instead of slow brute-force probing. One API call instantly detects your wallet type (MetaMask, Email/Google, proxy) — no more guessing.
- **Wrong-key detection:** If you export the wrong MetaMask account's key, the bot tells you exactly what went wrong (e.g. "This key controls 0xABC but your proxy is owned by 0xDEF").
- **No-profile detection:** If the key doesn't match any Polymarket account, you get a clear error instead of a confusing $0 balance.
- **Balance preview:** See your CLOB balance, on-chain USDC, and open positions before saving config.
- **Server-side validation:** Even if browser JS is bypassed, the save endpoint validates and auto-detects everything — bad config can never reach your .env file.

## Min Order Size Fix
- Lowered the internal min_shares default from 5 to 1. Polymarket has no official minimum order size — the CLOB rejects truly undersized orders on its own.
