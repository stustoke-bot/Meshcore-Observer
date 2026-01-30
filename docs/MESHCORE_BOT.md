# MeshCore auto-responder bot

Runs on your **PC** (where the MeshCore radio is connected via USB). When someone sends **"Test"** in **#test**, the bot waits **10 seconds** (so meshrank can see the message), then fetches hops, observers, path, and share link from the observer server and replies on the mesh.

## How to get this working

1. **Hardware**  
   Connect your MeshCore companion radio to this PC via USB. It will show up as a serial port (e.g. `/dev/ttyUSB0`, `/dev/cu.usbmodem*` on macOS, or `COM3` on Windows).

2. **Install dependencies** (once, from repo root):
   ```bash
   cd Meshcore-Observer
   npm install
   ```

3. **Data source (observer network)**  
   The bot needs an API that has your observer data (message routes, observer count, share codes):
   - **Option A – meshrank.net**  
     Use the default: no `--server` needed. Ensure meshrank.net is ingesting from your observers and that `https://meshrank.net/msg/{code}` works for share links.
   - **Option B – Your own observer server**  
     Run the Meshcore-Observer demo server (ingests from observers, serves the APIs), then point the bot at it:
     ```bash
     # Terminal 1: from Meshcore-Observer repo root
     npm run server
     # Terminal 2: run bot against it (use your serial port)
     npm run bot -- --port /dev/ttyUSB0 --server http://localhost:3000
     ```
     Use your server’s real URL if it’s on another host.

4. **Find the serial port**  
   - Linux: `ls /dev/ttyUSB* /dev/ttyACM*`  
   - macOS: `ls /dev/cu.usbmodem*`  
   - Windows: Device Manager → Ports (COM & LPT), or try `COM3`, `COM4`, etc.

5. **Run the bot**  
   From **Meshcore-Observer** (repo root):
   ```bash
   npm run bot -- --port /dev/ttyUSB0
   ```
   Or directly:
   ```bash
   node meshcore-bot.js --port /dev/ttyUSB0
   ```
   Windows example:
   ```bash
   npm run bot -- --port COM3
   ```
   You should see: `Connected to Companion node.` and `Meshcore bot is online.`

6. **Test**  
   From another mesh device (or app), send a message containing **test** on the **#test** channel. After ~10 seconds the bot should reply on #test with something like:  
   *@[STu] Hi! I got 2 Hops via 3 Observers. see more here https://meshrank.net/msg/56463*

If the message isn’t in the observer DB yet after 10s, hops/observers may be 0 and the share link might not resolve; you can increase the delay with `--delay 15000` (15s) or higher.

## Behaviour

1. Listens on the configured serial port for channel messages.
2. When a message in `#test` contains "test" (case-insensitive), the bot:
   - Waits **10 seconds** (configurable with `--delay`).
   - Calls meshrank.net: `/api/message-routes`, `/api/message-debug`, `/api/routes/<hash>/share`.
   - Sends a reply: *"@[Sender] Hi! I got X Hops via Y Observers. see more here https://meshrank.net/msg/12345"*.

## Run (from repo root)

```bash
node meshcore-bot.js --port /dev/ttyUSB0
```

**Windows:**

```bash
node meshcore-bot.js --port COM3
```

**Options:**

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--port` | `-s` | *(required)* | Serial port (e.g. `/dev/ttyUSB0`, `COM3`) |
| `--channel` | `-c` | `#test` | Channel to listen and reply in |
| `--delay` | `-d` | `10000` | Delay in ms before replying (default 10s) |
| `--server` | `-u` | `https://meshrank.net` | Meshrank base URL |
| `--template` | `-t` | `@[{sender}] Hi! I got {hops} Hops via {observers} Observers. see more here {shareLink}` | Reply template. Placeholders: `{sender}`, `{channel}`, `{hops}`, `{observers}`, `{path}`, `{shareLink}` |
| `--token` | `-k` | `MESHRANK_TOKEN` | Bearer token for meshrank API (optional; share may work without) |
| `--auto` | `-a` | `true` | Enable auto-respond on "test" |

**Example with custom delay:**

```bash
node meshcore-bot.js --port /dev/ttyUSB0 --delay 12000
```

## Dependencies

Uses `@liamcottle/meshcore.js` and `yargs` (see repo `package.json`). From repo root: `npm install` once.

## Note

The **share link** (https://meshrank.net/s/xxxxx) is created by POSTing to meshrank; if the message is not yet in meshrank’s DB after 10s, the reply will still show hops/observers/path but the link may not resolve. You can increase `--delay` if needed.
