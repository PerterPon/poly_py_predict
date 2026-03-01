# Setup Guide — Crypto15min PolyTrader

> **New to all of this?** Don't worry. This guide walks you through every step, assumes zero crypto experience, and explains everything along the way. If you get stuck, check the [FAQ & Troubleshooting](#14-faq--troubleshooting) section at the bottom.

---

## Quick Start (5 steps, ~5 minutes)

| Step | What to do |
|------|------------|
| **1** | **Choose how to run it** — [Railway](https://railway.com?referralCode=polytrader) (easiest, no server needed), OR get a VPS — [Vultr](https://www.vultr.com/?ref=9869565-9J), [Hetzner](https://hetzner.com), [DigitalOcean](https://m.do.co/c/da83985bd90c) ($5-10/mo). Or run on Windows with Docker Desktop. |
| **2** | **Download & unzip** — `wget <your-download-link> && unzip crypto15min-polytrader.zip && cd crypto15min-polytrader` |
| **3** | **Run the installer** — `bash setup.sh` (auto-installs Docker if missing, builds the bot, starts it) |
| **4** | **Open the Setup Wizard** — Go to `http://YOUR-SERVER-IP:8603` in your browser. Fill in your password, private key, and wallet type. |
| **5** | **Fund your wallet** — Send $10-50 USDC + $1 of POL to the wallet address shown on the dashboard. Done! |

> **That's it.** The bot trains its LightGBM ensemble automatically and starts trading within minutes. Start in **Paper mode** first to verify everything works, then switch to **Live** from the dashboard.

<details>
<summary>Where do I get my private key?</summary>

- **MetaMask users**: Settings → Security → Reveal Private Key
- **Email / Google users**: Go to [reveal.magic.link](https://reveal.magic.link), sign in with the same email you used for Polymarket, and copy the key. The Setup Wizard will also ask for your **Funder Address** (your Polymarket profile wallet address, not your private key).
</details>

---

---

## Deployment Options — Pick Your Path

There are three ways to run Crypto15min PolyTrader. Choose the one that fits you best:

| Option | Best for | Cost | Effort |
|--------|----------|------|--------|
| **A — Railway (cloud, one-click)** | Complete beginners | ~$5/mo | Easiest |
| **B — VPS with Docker** | All users, 24/7 reliability | ~$4-6/mo | Moderate |
| **C — Windows PC / Windows VPS** | Windows users, no Linux | Free (own PC) or ~$6/mo | Moderate |

### Option A — Deploy on Railway (recommended for beginners)

Railway runs your bot in the cloud without you managing any server.

1. Sign up at **[railway.com](https://railway.com?referralCode=polytrader)** (using this link supports the project)
2. Create a new project, select "Empty Project", and drag-and-drop this unzipped folder into the Railway dashboard.
3. Set the required environment variables (Railway will prompt you):
   - `C5_POLY_PRIVATE_KEY` — your wallet private key
   - `C5_POLY_SIGNATURE_TYPE` — `1` for Email/Google, `0` for MetaMask direct
   - `C5_POLY_FUNDER_ADDRESS` — your Polymarket profile address (if Email/Google)
   - `C5_DASHBOARD_PASSWORD` — any password you choose
4. Railway builds and starts the bot automatically
5. Click the generated URL to open your dashboard — done!

### Option B — VPS with Docker (recommended for reliability)

A VPS is a small cloud computer that runs 24/7. This is the most reliable option.

**Recommended providers (all confirmed working with Polymarket):**

| Provider | Location | Price | Link |
|----------|----------|-------|------|
| Vultr | Amsterdam, Frankfurt, etc. | ~$6/mo | [vultr.com](https://www.vultr.com/?ref=9869565-9J) |
| Hetzner | Helsinki, Finland | ~$4/mo | [hetzner.com/cloud](https://hetzner.com/cloud) |
| DigitalOcean | Amsterdam, Netherlands | ~$6/mo | [digitalocean.com](https://m.do.co/c/da83985bd90c) |
| QuantVPS | Multiple locations | ~$5/mo | [quantvps.com](https://www.quantvps.com/?via=polytraderbot) |
| Time4VPS | Lithuania, Europe | ~$4/mo | [time4vps.com](https://www.time4vps.com/?affid=8480) |
| Contabo | Bucharest, Romania | ~$4/mo | [contabo.com](https://contabo.com) |

> **Important:** Do NOT use a US-based VPS — Polymarket blocks US IP addresses. Use Europe or Canada.

SSH into your VPS, download the ZIP, run `bash setup.sh`, and follow the Setup Wizard.

### Option C — Windows (PC or Windows VPS)

You can run the bot on a Windows computer or a Windows VPS without any Linux knowledge.

**On your own Windows PC:**
1. Install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/) (free)
2. Enable WSL 2 when prompted
3. Extract the bot ZIP, open PowerShell in that folder
4. Run: `docker compose up -d --build`
5. Open `http://localhost:8603` in your browser

**On a Windows VPS (e.g. [Vultr Windows ~$6/mo](https://www.vultr.com/?ref=9869565-9J)):**
1. RDP into your Windows VPS (use Remote Desktop Connection — built into Windows)
2. Inside the VPS, install Docker Desktop (same steps as above)
3. Follow the same Docker steps

> **Note:** For 24/7 trading on your own PC, make sure your computer never sleeps (Power Settings → "Never sleep").

---

## Table of contents

1. [What is Docker and why do I need it?](#1-what-is-docker-and-why-do-i-need-it)
2. [Installing Docker](#2-installing-docker)
3. [Starting the bot](#3-starting-the-bot)
4. [The Setup Wizard](#4-the-setup-wizard)
5. [Dashboard overview](#5-dashboard-overview)
6. [Understanding the modes: Paper → Dry-run → Live](#6-understanding-the-modes-paper--dry-run--live)
7. [Getting your Polymarket private key](#7-getting-your-polymarket-private-key)
    - [7b) Email/Google users: signature type + funder address](#7b-email--google-users-signature-type--funder-address)
8. [Funding your wallet (USDC + gas)](#8-funding-your-wallet-usdc--gas)
9. [Trading strategies explained](#9-trading-strategies-explained)
10. [Bet sizing (Fixed / % / Kelly)](#10-bet-sizing-fixed----kelly)
11. [Settlement, redemption & getting money out](#11-settlement-redemption--getting-money-out)
12. [Updating to a new version](#12-updating-to-a-new-version)
13. [Advanced settings](#13-advanced-settings)
14. [FAQ & Troubleshooting](#14-faq--troubleshooting)

---

## 1) What is Docker and why do I need it?

**Docker** is a free tool that runs apps inside isolated "containers." Think of it like this:

- Without Docker: you'd need to install Python, a bunch of libraries, configure paths, deal with version conflicts...
- With Docker: you run **one command** and everything works. The bot runs inside its own mini-computer.

**You don't need to know how Docker works.** Just install it and let it do its thing.

---

## 2) Installing Docker

### Windows

1. Go to [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)
2. Click **Download for Windows**
3. Run the installer (accept all defaults)
4. **Important:** When asked, enable **WSL 2** (Windows Subsystem for Linux) — check the box
5. Restart your computer when prompted
6. Open **Docker Desktop** from the Start menu
7. Wait for it to fully start — you'll see a **whale icon** in the system tray (bottom-right near the clock)
8. When the whale icon stops animating, Docker is ready

**To verify it works,** open PowerShell and type:
```powershell
docker version
```
You should see version numbers. If you see an error, Docker isn't running yet.

> **Common Windows issue:** If you see "WSL 2 installation is incomplete", open PowerShell **as Administrator** (right-click → Run as Administrator) and run: `wsl --install`, then restart your PC.

### Mac

1. Go to [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)
2. Click **Download for Mac** (choose Apple Silicon or Intel based on your Mac)
3. Open the downloaded `.dmg` file → drag Docker to Applications
4. Open **Docker** from Applications
5. Wait for "Docker Desktop is running" in the menu bar

**To verify:** Open Terminal and type `docker version`.

### Linux (Ubuntu / Debian / most VPS)

Run this single command:
```bash
curl -fsSL https://get.docker.com | sh
```

Then add your user to the Docker group (so you don't need `sudo` every time):
```bash
sudo usermod -aG docker $USER
```
**Log out and log back in** for this to take effect.

**To verify:** Type `docker version` and `docker compose version`.

> **VPS users:** Docker is often pre-installed. Just run `docker version` to check. If it works, skip this step.

---

## 3) Starting the bot

### Step 1: Get the bot files

- Download the ZIP from Whop
- Unzip it into a folder (e.g., `crypto15min-polytrader` on your Desktop, or `/root/crypto15min-polytrader` on a VPS)

### Step 2: Open a terminal in the bot folder

- **Windows:** Open the folder in File Explorer, click the address bar, type `powershell`, press Enter
- **Mac:** Open Terminal, type `cd ` (with a space), then drag the folder into the Terminal window, press Enter
- **Linux/VPS:** `cd /path/to/crypto15min-polytrader`

### Step 3: Run the start command

**Linux / Mac:**
```bash
bash setup.sh
```

**Windows (PowerShell):**
```powershell
docker compose up -d --build
```

> **First time?** This will take **2-5 minutes** to download dependencies. You'll see lots of text scrolling — that's normal. Subsequent starts take ~5 seconds.

### Step 4: Open the dashboard

- **Local:** Open your browser → go to [http://localhost:8603](http://localhost:8603)
- **VPS:** Open your browser → go to `http://YOUR_SERVER_IP:8603`

Replace `YOUR_SERVER_IP` with your actual VPS IP address (e.g., `http://123.45.67.89:8603`).

> **Port 8603** is the default for the 15-minute bot. If you also run the 5-minute bot (port 8602) or Copy Bot (port 8502), they won't conflict.

---

## 4) The Setup Wizard

On first run, the dashboard shows a **Setup Wizard** with two steps:

### Step 1 of 2: Create your password

- Type a password for your dashboard
- This protects the dashboard from random people who find your URL
- **Avoid special characters** like `$`, `"`, `\`, and `#` in your password — these can cause login issues because the `.env` file uses them as control characters. Stick to letters, numbers, and simple symbols like `!`, `@`, `-`, `_`.
- Click **Save and continue**

> **About the setup token:** The wizard URL has a `?token=...` part. This is a one-time security code that prevents random visitors from setting your password. If the token is wrong, just open `/setup` again — it shows the correct token link.

### Step 2 of 2: Polymarket settings (optional)

You can skip this entirely and come back later. If you want to set it up now:

- **Private key:** Your Polymarket wallet private key (see [Section 7](#7-getting-your-polymarket-private-key))
- **Wallet type:** MetaMask/browser wallet, or Email/Google
- **Funder address (only for Email/Google or proxy wallets):** Your Polymarket profile (proxy) address (must be a wallet address like `0x...`, not your private key, and it should be **different** from the wallet address derived from your private key)
- **Max spend per trade (USDC):** How much the bot can spend per 15-minute trade. Start small (e.g. $5).

Click **Save and launch** — the bot starts running!

> **Important:** If you skip wallet setup now, you can add/fix it anytime by going to `/reconfigure` (Setup Wizard Step 2).

---

## 5) Dashboard overview

The dashboard has several sections:

### Main area (center)

- **Signal:** Shows the current prediction (UP or DOWN) with confidence percentage
- **BTC Price chart:** Live Bitcoin price, updated every 10 seconds
- **Equity curve:** Your paper (simulated) or live trading balance over time
- **Trade log:** Recent trades with outcome (win/loss) and amounts
- **P&L Summary Cards:** Realized P&L, Unrealized P&L, Win Rate, and Avg Win/Avg Loss — auto-refreshes every 30 seconds
- **Ledger Library:** Auto-generated Excel ledgers (monthly, annual, all-time) — download or rebuild from the dashboard

### Sidebar (left)

- **Mode buttons:** PAPER / DRY-RUN / LIVE — click to switch
- **Status:** Shows if the bot is running, paused, or error
- **Wallet info:** Your wallet address and balances (USDC + gas)
- **Settings:** Click to expand trading settings
- **Paper Balance** (dry-run only): Shows your virtual USDC balance, realized P&L, win rate, and a "Reset Paper" button

### "How It Works" tab (top)

This is an **in-dashboard guide** with:
- Getting started steps
- Settings explained (what each toggle does)
- FAQ (frequently asked questions)
- Quick reference table
- Advanced settings

**Read this tab** — it explains everything you see on the dashboard.

---

## 6) Understanding the modes: Paper → Dry-run → Live

Always progress through modes in this order:

| Mode | What happens | Risk |
|------|-------------|------|
| **Paper** | Bot makes predictions, records fake trades. No real money involved. | None |
| **Dry-run** | Bot finds real Polymarket markets, logs what it *would* trade, but doesn't place orders. **Paper PnL** tracks a virtual balance. | None |
| **Live** | Bot places **real orders** with **real money** on Polymarket. | Real |

**Recommended progression:**

1. **Paper for 1-2 days:** Watch the predictions, check accuracy
2. **Dry-run for a few hours:** Verify it's finding the right markets
3. **Live with $10-20:** Start tiny, verify everything works
4. **Scale up gradually** if you're comfortable

---

## 7) Getting your Polymarket private key

Your Polymarket account has a hidden crypto wallet behind the scenes. To let the bot trade on your behalf, you need its private key.

### How to get it:

1. Log into [polymarket.com](https://polymarket.com) in your browser
2. Go to [reveal.magic.link/polymarket](https://reveal.magic.link/polymarket)
3. It will ask you to verify your identity (email/social login)
4. You'll see a long string starting with `0x...` — that's your private key
5. **Copy it carefully** — do not share it with anyone

### Important safety rules:

- This key controls your entire Polymarket wallet
- The bot stores it **only on your server** in `config/.env`
- **Use a dedicated bot wallet** — never use your main wallet's key
- Only put small amounts in the bot wallet ($10-50 to start)
- Never paste your key into websites, DMs, or emails

---

### 7b) Email / Google users: signature type + funder address

If you log into Polymarket with **email or Google** (not MetaMask), you must use wallet type **Email / Google** and set your Polymarket profile address as funder.

**Very important:**
- `Private key` field = key from `reveal.magic.link`
- `Funder address` field = your **wallet address** from your Polymarket profile (`0x` + 40 hex chars)
- Do **not** paste your private key into the funder field
- Do **not** paste the **same address** as your derived wallet address into the funder field (proxy/funder must be different)

#### Option A: From the dashboard (easiest)

1. Log into your dashboard
2. Go to `/reconfigure` (this re-opens the Setup Wizard)
3. Set **Wallet type** to **Email / Google (MagicLink)**
4. Paste your **Funder Address** (your Polymarket profile address — the one shown on polymarket.com when you click your profile)
5. Click **Save** — changes apply immediately, no restart needed

#### Option B: Manual edit (if you prefer the command line)

```bash
sudo nano config/.env
```

Add these two lines:

```dotenv
C5_POLY_SIGNATURE_TYPE=1
C5_POLY_FUNDER_ADDRESS=0xYOUR_POLYMARKET_PROFILE_ADDRESS
```

Then restart: `docker compose restart`

> **How do I know which type I am?**
> - Log in with **email or Google** → type `1`
> - Connect a **crypto wallet** (MetaMask, Coinbase Wallet, etc.) → type `0` (default, no change needed)
>
> **This is the #1 most common setup issue.** If you see $0 balance but have funds on Polymarket, this is almost certainly the fix.

---

## 8) Funding your wallet (USDC + gas)

You need two things in your bot wallet:

### A) USDC (the trading money)

USDC is a stablecoin worth exactly $1. It's what Polymarket uses for all trades.

**How to get USDC into your bot wallet:**

1. **Find your bot wallet address** — it's shown on the dashboard sidebar under "Wallet." It's a long string starting with `0x...`
2. **Buy USDC** on a crypto exchange:
   - [Coinbase](https://coinbase.com) (easiest for US users)
   - [Kraken](https://kraken.com) (easy, worldwide)
   - [Binance](https://binance.com) (worldwide, lowest fees)
3. **Withdraw USDC** to your bot wallet address, selecting **Polygon** as the network

   **CRITICAL:** When withdrawing, you MUST select **Polygon** (sometimes called "Polygon POS") as the network. Do NOT select Ethereum or any other network. **Wrong network = lost funds.**

4. Start with **$10-50** — you can always add more later

### B) Gas (POL / MATIC — the transaction fee token)

Every trade on Polygon costs a tiny fee (fractions of a cent). You pay this fee with a token called **POL** (formerly called MATIC).

**How to get POL/MATIC:**

1. Buy POL or MATIC on the same exchange you used for USDC
2. Withdraw **$1-3 worth** of POL/MATIC to the **same bot wallet address** on **Polygon**
3. That's enough for hundreds of trades

> **If you see a "Low gas" warning** on the dashboard → send more POL/MATIC to your bot wallet.

---

## 9) Trading strategies explained

The 15-minute bot has three strategies. You can use any combination:

### Delta-First mode (default ON — recommended)

The bot's default strategy. **Delta-first makes the snipe pass the primary entry mechanism** — ML directional trades are disabled by default.

- **How it works:** The ML model still trains and displays predictions on the dashboard, but it does NOT trigger trades. Instead, the bot relies on the Chainlink oracle delta. When BTC moves enough before window close, it trades in that direction.
- **Delta pricing (gate):** When `C5_DELTA_PRICING=true` (default), the bot gates snipe trades based on the live ask vs. the delta magnitude. If the market asks more than the tier allows, the trade is skipped:
  - Delta < 0.01% → $0.52 gate
  - Delta < 0.02% → $0.58 gate
  - Delta < 0.05% → $0.68 gate
  - Delta < 0.10% → $0.82 gate
  - Delta >= 0.10% → $0.97 gate
  - All tier prices are tunable via `C5_DELTA_PRICE_T1` through `C5_DELTA_PRICE_T5` in `.env`
- **To disable:** Set `C5_DELTA_FIRST=false` in `.env` or toggle off in dashboard → Trading → Delta-first. This re-enables ML trades.

### Snipe Mode (on by default — highest accuracy)

- **How it works:** Waits until ~20 seconds before the 15-minute window closes, then checks if BTC has *already* moved up or down from the window open price (using the **Chainlink oracle** — the exact same price feed Polymarket uses to resolve markets). If the move is large enough, trades in that direction.
- **Why it works:** With only 20 seconds left, the direction is mostly locked in.
- **Accuracy:** ~80-90%+ when triggered
- **Smart sizing:** Snipe trades automatically bet 2.5x normal size (configurable via `C5_POLY_SNIPE_BET_MULTIPLIER`). Higher accuracy = bigger bets make sense.
- **Trade-off:** Tokens cost more near close ($0.70-0.95 instead of ~$0.50), so profit per trade is smaller
- **Toggle in dashboard:** Sidebar → Trading → Snipe mode → Save

### ML Prediction (disabled when Delta-First is on)

A **LightGBM + ExtraTrees ensemble** (not CNN-LSTM like the 5-minute bot) analyzes 42+ technical indicators from BTC price history and predicts whether the next 15-minute candle will go UP or DOWN.

- **Accuracy:** ~52-55%
- **How it works:** Retrains every 30 minutes on fresh data. Makes a prediction at the start of each 15-minute window.
- **When it trades:** Only when `C5_DELTA_FIRST=false` and confidence exceeds your threshold.

### Multi-entry per window (15min exclusive feature)

The 15-minute bot can enter the same window **multiple times** as conditions change:

1. **ML signal fires** at window open (if Delta-First is off)
2. **Delta signal fires** mid-window as BTC moves
3. **Snipe signal fires** 20 seconds before close

Each entry stacks at a different price. Controlled by `C5_POLY_MAX_ENTRIES_PER_WINDOW` (default: 3).

### Arb-first mode (optional, risk-free when possible)

Watches the Polymarket orderbook and buys BOTH UP and DOWN shares when the combined cost is briefly below $1.00.

- **How it works:** If UP costs $0.48 and DOWN costs $0.49 = $0.97 total. Since one side MUST pay $1.00, you profit $0.03 per dollar regardless of outcome.
- **Accuracy:** 100% when triggered (guaranteed profit)
- **Trade-off:** Extremely rare.
- **Enable in dashboard:** Sidebar → Trading → Arb-first → Save

---

## 10) Bet sizing (Fixed / % / Kelly)

In the dashboard sidebar under **Trading → Bet mode**, you choose how the bot decides how much to trade:

| Mode | How it works | Best for |
|------|-------------|----------|
| **Fixed $** | Every trade uses the same dollar amount (your "Max $/trade" setting) | Beginners. Simple and predictable. |
| **% of balance** | Each trade uses a percentage of your current balance (still capped by max $) | Growing accounts. Trades get bigger as you win. |
| **Kelly** | Math formula that sizes trades based on model confidence vs market price. Multiplied by a safety fraction (default 0.25 = quarter Kelly). | Advanced users. Maximizes long-term growth in theory. |

**Recommendation:** Start with **Fixed $** at a small amount ($2-5). Once you're comfortable, switch to **% of balance** (5-10%) for compound growth.

---

## 11) Settlement, redemption & getting money out

### How Polymarket wins work

When a 15-minute window resolves (the outcome is determined):

1. If the bot was **right**, your winning shares are worth $1.00 each
2. The shares need to be **redeemed** (converted back to USDC)
3. The bot can do this automatically if **Auto-redeem** is enabled (on by default)

### Getting your money out of the bot

Your funds are always in **your** wallet — the bot can never lock them. To withdraw:

1. **Pause the bot** — click the pause button in the dashboard
2. **Close open positions** — use the Polymarket website or MetaMask
3. **Transfer USDC** — send it from your bot wallet to your personal wallet or exchange
4. **Convert to cash** — sell USDC on your exchange for real dollars

> **Quick method with MetaMask:** Install the MetaMask browser extension, import your bot wallet's private key, and you can manage the wallet directly.

---

## 12) Updating to a new version

This bot does not have one-click updates. To update manually:

1. Download the new ZIP from Whop
2. Unzip into a **new folder** (don't overwrite the old one)
3. Copy your settings and data from the old folder to the new one:
   - `config/.env` (your settings & private key)
   - `logs/` folder (trade history & state)
   - `data/` folder (cached candles)
4. Stop the old container: `docker compose down`
5. In the **new** folder, build and start: `docker compose up -d --build`

> **Your settings and trade history carry over** as long as you copy `config/.env` and `logs/`.

---

## 13) Advanced settings

These are for experienced users. Skip this section if you're just getting started.

### 15-minute specific settings

| Setting | Default | What it does |
| ------- | ------- | ------------ |
| `C5_GRANULARITY_SECONDS` | `900` | Window duration in seconds (900 = 15 minutes). Do not change. |
| `C5_POLY_TRADE_LEAD_SECONDS` | `120` | Seconds after window open to place the main trade. |
| `C5_SNIPE_LEAD_SECONDS` | `20` | Seconds before window close to fire the snipe. |
| `C5_POLY_SNIPE_BET_MULTIPLIER` | `2.5` | Snipe trades bet 2.5x normal size. |
| `C5_RETRAIN_MINUTES` | `30` | Model retrains every 30 minutes. |
| `C5_POLY_MAX_ENTRIES_PER_WINDOW` | `3` | Max entries in one 15-minute window. |
| `C5_DELTA_FIRST` | `true` | ML trades disabled; snipe/delta only. |
| `C5_DELTA_PRICING` | `true` | Gate snipe trades by ask vs delta magnitude. |

### Order execution

| Setting | Default | What it does |
| ------- | ------- | ------------ |
| `C5_POLY_MAX_USDC_PER_TRADE` | `5.0` | Max USDC per trade. Start small. |
| `C5_POLY_EDGE_MIN` | `0.03` | Min edge for ML trades. Snipe bypasses this. |
| `C5_CONFIDENCE_THRESHOLD` | `0.58` | Min model confidence to trade. |
| `C5_POLY_MIN_BOOK_USDC` | `20` | Skip trades when orderbook depth below this. |
| `C5_POLY_FILL_MAX_ATTEMPTS` | `3` | Retry attempts if order doesn't fill. |
| `C5_POLY_FILL_WAIT_SEC` | `20` | Seconds to wait for fill before retrying. |

### Market quality filters

| Setting | Default | What it does |
| ------- | ------- | ------------ |
| `C5_MQ_MAX_SPREAD_BPS` | `120` | Skip when spread exceeds this (basis points). |
| `C5_MQ_MIN_DEPTH_USDC` | `15` | Skip when orderbook depth below this. |
| `C5_MQ_DEPTH_CAP_BPS` | `30` | Spread cap to check depth against. |
| `C5_MQ_EDGE_SPREAD_MULT` | `0.10` | Extra required edge when spreads widen. |

### Risk rails (circuit breakers)

| Setting | Default | What it does |
| ------- | ------- | ------------ |
| `C5_RISK_DAILY_LOSS_PCT` | `10` | Pause trading after losing 10% of balance in 24h |
| `C5_RISK_CONSEC_LOSS_LIMIT` | `3` | Pause after 3 consecutive losses |
| `C5_RISK_UNFILLED_RATIO` | `0.5` | Pause if >50% of recent orders go unfilled |
| `C5_RISK_UNFILLED_LOOKBACK` | `20` | How many recent orders to check for fill rate |
| `C5_RISK_AUTO_RESUME_MINUTES` | `45` | Auto-resume after 45 min of pause (0 = manual only) |

Set any value to `0` to disable that specific rail.

---

## 14) FAQ & Troubleshooting

### Installation problems

**Q: "docker: command not found"**
Docker isn't installed. Go back to [Section 2](#2-installing-docker) and follow the steps for your operating system.

**Q: "Docker daemon is not running"**
Docker Desktop needs to be open. Find it in your Start menu (Windows) or Applications (Mac) and open it. Wait for the icon to stop animating before trying again.

**Q: "Permission denied" when running Docker on Linux**
Run: `sudo usermod -aG docker $USER` then log out and back in.

**Q: Windows says "WSL 2 installation is incomplete"**
Open PowerShell as Administrator and run: `wsl --install`. Restart your PC.

**Q: "container name already in use" error**
A previous container with the same name still exists. Remove it first:
```bash
docker rm -f crypto15min-polytrader
docker compose up -d --build
```

### Dashboard problems

**Q: I can't access the dashboard**
1. Check the container is running: `docker ps` (you should see `crypto15min-polytrader`)
2. Make sure you're using the right port: `http://YOUR_IP:8603`
3. Check your firewall allows port 8603
4. If on a VPS: check the hosting provider's firewall/security group settings

**Q: The Setup Wizard keeps appearing**
Complete the wizard by following the `?token=...` URL it shows you.

**Q: I changed settings but nothing happened**
Restart the container: `docker compose restart`

**Q: Dashboard data never updates**
The bot works on 15-minute windows. Wait up to 15 minutes for a new window cycle. If still stuck, check logs: `docker logs crypto15min-polytrader --tail 20`

### Polymarket API Errors

**Q: "400 Bad Request" on /auth/api-key**
Almost always caused by:
1. **Clock Skew (Most Common):** Your VPS clock is out of sync.
   * **Fix (Linux):** Run `sudo chronyd -q` or `sudo ntpdate pool.ntp.org`
   * **Fix (Windows):** Open Command Prompt as Admin → `w32tm /resync`
2. **Wrong Signature Type:** MetaMask wallet but set type=1, or Email/Google but set type=0.
   * **Fix:** Dashboard → Settings → Wallet → re-run Auto-Detect
3. **Wrong Funder Address:** Email/Google login but didn't provide the correct Proxy address.

### Trading problems

**Q: Why are my wins so small?**
Expected in Snipe mode. Snipe fires near window close when tokens cost $0.70-0.95 per share. Payout is $1.00. Profit = $0.05-0.30 per share. High accuracy + many trades compounds over time.

**Q: The bot skips every window / never trades**
Normal — the bot is being selective. Reasons it might skip:
- Delta too small (BTC didn't move enough in this window)
- Confidence below threshold
- Orderbook too thin (thin book guard)
- Spread too wide (market quality filter)
- Balance too low for minimum order size

**Q: Dashboard shows $0 balance but I have funds on Polymarket**
Almost always a **signature type mismatch**. If you signed up with email/Google:
1. Go to `/reconfigure` → Setup Wizard Step 2
2. Set Wallet type to **Email / Google (MagicLink)**
3. Paste your **Funder Address** (Polymarket profile address)
4. Click Save

> This is the **#1 most common issue**.

**Q: Can I run this alongside the 5-minute bot?**
Yes! They use different ports (8603 for 15min, 8602 for 5min) and different Docker containers. Both can run on the same VPS simultaneously.

### Wallet & funding problems

**Q: I sent USDC but the bot doesn't see it**
1. Did you send on **Polygon** network? (Not Ethereum!)
2. Wait 1-2 minutes for the transaction to confirm
3. Check your wallet on [polygonscan.com](https://polygonscan.com)

**Q: I sent USDC on the wrong network (Ethereum instead of Polygon)**
The bot can't access funds on Ethereum. Bridge them to Polygon using [portal.polygon.technology](https://portal.polygon.technology/bridge).

### General questions

**Q: What are the minimum VPS specs?**
1 CPU, 1 GB RAM, 20 GB storage. Ubuntu 22.04 or 24.04 recommended.

**Q: I'm in the US — do I need a VPN?**
Polymarket restricts US IP addresses. Use a non-US VPS (Europe or Canada).

**Q: How do I completely stop the bot?**
Run `docker compose down` in the bot folder. Settings and data are preserved.

**Q: How do I reset everything and start fresh?**
1. Stop: `docker compose down`
2. Delete `config/.env`, `logs/state.json`, and `logs/runtime_config.json`
3. Start: `docker compose up -d --build`
4. Complete the Setup Wizard again

**Q: Is this guaranteed to make money?**
**No.** No trading bot can guarantee profits. Markets are unpredictable. Only trade money you can afford to lose. Start small and scale up based on real results.

---

## Still stuck?

Join the **Discord community** (link in your Whop purchase) and ask for help. Include:

1. What you're trying to do
2. What error you see (screenshot or copy-paste)
3. Your operating system (Windows/Mac/Linux)
4. Output of `docker logs crypto15min-polytrader --tail 30`
