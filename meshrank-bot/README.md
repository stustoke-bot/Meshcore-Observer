# Meshrank Bot (run locally)

Run this **on your PC** with your **MeshCore radio plugged in via USB**. It connects to your radio and to **meshrank.net** (observer on the net). When someone sends **"test"** on **#test**, the bot waits 30 seconds, fetches hops/observers from meshrank.net, and replies with a link.

No need to run the full Meshcore-Observer stack — observer data comes from **https://meshrank.net**.

---

## 1. Install Node.js (once)

- Download **LTS** from https://nodejs.org/
- Run the installer (leave “Add to PATH” checked)
- Close and reopen Command Prompt after installing

---

## 2. Get this folder on your PC

**Option A – Download and install in one go (recommended)**

1. Install **Node.js** (https://nodejs.org/) and **Git** (https://git-scm.com/download/win) if you don’t have them.
2. Open Command Prompt and go to the folder where you want the project (e.g. your user folder):
   ```bat
   cd C:\Users\pc
   ```
3. Download the repo and install the bot (run these two lines):
   ```bat
   git clone https://github.com/stustoke-bot/Meshcore-Observer.git
   cd Meshcore-Observer\meshrank-bot && npm install
   ```
4. Run the bot (replace COM3 with your port):
   ```bat
   node bot.js --port COM3
   ```

   Or use the all-in-one script: copy **install-and-run.bat** into a folder, double-click it (it will clone, install, then ask for your COM port and start the bot).

**Option B – Copy the folder**

Copy the **meshrank-bot** folder to your computer (e.g. `C:\Users\pc\meshrank-bot` or your Desktop).

---

## 3. Find your COM port (Windows)

- Open **Device Manager** → **Ports (COM & LPT)**
- Find the port for your MeshCore device (e.g. **COM3**, **COM4**)

---

## 4. Run the bot

**Option A – Double‑click or use default COM3**

```bat
run.bat
```

**Option B – Specify your COM port**

```bat
run.bat COM4
```

**Option C – Run manually**

```bat
cd meshrank-bot
npm install
node bot.js --port COM3
```

Replace `COM3` with your actual port.

---

## What you should see

- `Installing dependencies (first run)...` only on first run
- `Connected to Companion node.`
- `Meshrank bot is online (using https://meshrank.net).`

When someone sends **test** on **#test**, after ~30 seconds the bot replies with something like:

**@[STu] Hi! I got 2 Hops via 3 Observers. see more here https://meshrank.net/msg/56463**

---

## Optional

- **Different port:** `run.bat COM5` or `node bot.js --port COM5`
- **Help:** `node bot.js --help`
- **Longer delay:** `node bot.js --port COM3 --delay 15000`
- **Stream mode (server-driven replies):** `node bot.js --port COM3 --stream --token <BOT_TOKEN>`

Stream mode listens to `https://meshrank.net/api/bot-stream` and sends replies based on server-side matching.
Set `MESHRANK_BOT_TOKEN` on the server and pass the same token with `--token` (or `MESHRANK_TOKEN` env var).
