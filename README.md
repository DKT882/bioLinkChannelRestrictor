# 🛡️ Telegram BioLink Channel Restrictor Bot

A high-performance, asynchronous Telegram bot built with modern **pyTelegramBotAPI (`AsyncTeleBot`)** and **aiohttp**, designed specifically to run seamlessly on the **Render Free Plan**.

It actively protects Telegram groups against spam bots, promotion accounts, and spammers by inspecting members' profile bios for URLs, website links, bio-link aggregators, and channel handles (`@channel`).

---

## ⚡ Key Highlights & Architecture

- **Zero External Databases**: 100% in-memory Python dictionary state management, perfectly suited for lightweight container instances with zero operational overhead.
- **Render Free Tier Optimized**: Runs an internal asynchronous `aiohttp` web server bound to `os.environ.get("PORT", 8080)` with `/` and `/healthz` endpoints to pass Render's mandatory HTTP health checks.
- **High-Throughput Caching**:
  - **Admin Cache**: In-memory monotonic TTL caching (180s) prevents rate limits on Telegram's API when checking administrative privileges.
  - **Bio Scan Cache**: Monotonic TTL caching (300s) ensures active group chatters do not trigger repeated `get_chat(user_id)` lookups.
  - **Memory Guard**: Periodic background cleanup task purges expired cache records every 10 minutes to maintain memory footprint strictly under 50MB (safely below Render's 512MB free tier cap).
- **Passive by Default**: The bot remains completely inert when added to a group until an administrator explicitly sends `/start`.

---

## 📋 Features

### 1. State Management & Activation
- **In-Memory Tracking**: Groups are stored in `active_groups[chat_id]`.
### 1. Zero-Setup Default Activation & Persistence
- **Always Active by Default**: The bot protects all groups immediately upon joining. No `/start` command or manual setup is needed after joining or after Render redeployments.
- **Default Protection**:
  - Messages from members who have links, handles, or connected channels are **deleted**.
  - Messages from **admins** who have links, handles, or connected channels are **also deleted**.
- **Settings Persistence**: Saves configurations and exemption lists to `settings.json` so custom settings survive redeployments.

### 2. Administrator Feature Controls
- **Admin Command Cleanup**: All admin/owner command messages are automatically deleted within 3 seconds to maintain a clean chat history.
- **`/setaction <delete|warn|mute|ban>`**: Configures the punishment applied when a user/admin has a bio link or connected channel:
  - `delete`: Deletes violating message & sends notification to contact `@i_am_turtle` (Default).
  - `warn`: Deletes message and warns user in chat.
  - `mute`: Deletes message and mutes user from sending messages.
  - `ban`: Deletes message and permanently bans user from group.
- **`/toggleadmin`**: Toggles whether group administrators are exempted (Default: `False` / admins are scanned and deleted too).
- **`/setmsg <text>`**: Customizes the bio deletion notice. Supports `{mention}`, `{name}`, `{id}` placeholders.
- **`/resetmsg`**: Restores the bio deletion notice to the default message directing to `@i_am_turtle`.
- **`/getmsg`**: Previews the active bio deletion notice message.
- **`/status`**: Displays a real-time configuration dashboard of the group.
- **`/help`**: Lists available commands and usage instructions.

### 3. Exemption Commands (`/free` & `/unfree`)
- **Restricted To**: **Group Owner (Creator)** and **Anonymous Admin** only.
- **Two Distinct Exemption Tiers**:
  1. **Bio Link Exemption (`/free` & `/unfree`)**:
     - Allows a user/admin whose profile bio contains URLs, handles, or connected channels to chat.
     - *Does NOT grant permission to send links/forwards in the group.*
     - Usage: `/free` (reply to message), `/free @username`, or `/free <user_id>`.
     - Revocation: `/unfree` (reply to message) or `/unfree @username`.
  2. **In-Chat Link & Forward Permission (`/free link` & `/unfree link`)**:
     - Authorizes a trusted member or admin to send web links, inline links, forwards, and bot mentions.
     - Usage: `/free link` (reply to message), `/free link @username`, or `/free link <user_id>`.
     - Revocation: `/unfree link` (reply to message) or `/unfree link @username`.

### 4. Anti-Spam / Filter Engine
- **Check 1: In-Chat Link & Forward Filter**:
  - Intercepts all messages containing URLs, inline links (`[click here](...)`), external `@bot` mentions, markdown/HTML table links, and **forwarded messages** from `{user, bot, channel, group}`.
  - Unless exempted via `/free link` (or Group Owner / Anon Admin), the message is **silently deleted immediately**.
  - **No response mention message is sent** for chat link violations (preventing chat spam).
- **Check 2: Profile Bio & Connected Channel Filter**:
  - Inspects `personal_chat` (connected channel) and `chat.bio` / `business_intro`.
  - Violating messages are deleted immediately.
  - An informative response notice is sent tagging the user and directing them to contact `@i_am_turtle` for approval (auto-deleted after 15s).
- **Check 3: Administrator Filtering**:
  - Admins are scanned by default unless `/toggleadmin` is toggled.
  - Group Owner and Anonymous Admin are always exempt.

---

## 🚀 Deployment Guide: Render Free Plan

### Method 1: Deploy with Git Repository
1. Push this repository to GitHub or GitLab.
2. Log in to [Render.com](https://render.com) and click **New +** -> **Web Service**.
3. Connect your repository.
4. Configure the service:
   - **Name**: `biolink-channel-restrictor`
   - **Environment**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python main.py`
   - **Plan**: `Free`
5. Under **Environment Variables**, add:
   - `BOT_TOKEN`: Your Telegram Bot token from [@BotFather](https://t.me/BotFather).
   - `PORT`: `8080` (Render also sets this automatically).
   - `MONGO_URI`: *(Optional but recommended)* Your MongoDB Atlas connection URI (`mongodb+srv://...`).
6. Click **Create Web Service**. Render will build and deploy the container. Render's health checks will query `http://0.0.0.0:8080/health` and verify the service is running.

### Method 2: Render Blueprint (`render.yaml`)
If deploying via Render Blueprints, Render will automatically detect `render.yaml` in the root of the repository and configure all settings. You only need to enter your `BOT_TOKEN` and optional `MONGO_URI`.

---

## 🗄️ Database Architecture & MongoDB Schema

The bot operates a **Hybrid Write-Through Cache**:
* **Read Throughput**: In-memory Python dictionaries for $O(1)$ sub-millisecond lookups on every message.
* **Durability**: Persistent MongoDB Atlas sync for zero data loss on restarts/redeployments.

### 1. `groups` Collection (Settings & Exemptions)
- `_id`: `int64` (Telegram `chat_id` as clustered primary key for $O(1)$ lookups)
- `title`: `string` (Telegram group title)
- `activated`: `bool` (Default: `true`)
- `action`: `string` ("delete" | "warn" | "mute" | "ban", Default: `"delete"`)
- `ignore_admins`: `bool` (Default: `false`)
- `free_list`: `Array[int64]` (Indexed array of bio-exempt user IDs)
- `free_links`: `Array[int64]` (Indexed array of link-authorized user IDs)
- `custom_bio_msg`: `string | null` (Customizable bio deletion response notice)
- `exemptions_detail`: `Array[Object]` (`user_id`, `username`, `type: 'bio'|'link'`, `freed_by`, `freed_at`)
- `updated_at`: `ISODate`

### 2. `moderation_logs` Collection (Audit Trail)
- `_id`: `ObjectId`
- `chat_id`: `int64` (Indexed)
- `user_id`: `int64` (Violating user ID)
- `username`: `string`
- `first_name`: `string`
- `violation_type`: `"connected_channel"` | `"bio_link"` | `"chat_link"`
- `violation_detail`: Matched URL, forward type, or connected channel handle
- `action_taken`: `"delete"` | `"warn"` | `"mute"` | `"ban"`
- `created_at`: `ISODate` (**90-Day TTL Index**: Automatically purges old logs to conserve free tier storage!)

### 3. `bot_metrics` Collection (Global Counters)
- `_id`: `"global_metrics"`
- `total_violations_handled`: `int64`
- `total_connected_channels_caught`: `int64`
- `total_bio_links_caught`: `int64`
- `total_chat_links_caught`: `int64`
- `last_heartbeat`: `ISODate`

---

> 💡 **Keep-Alive Tip for Render Free Tier**:
> Render's free web services sleep after 15 minutes of inactivity if no incoming HTTP traffic is detected. To keep your Telegram bot alive 24/7, set up a free monitor on [UptimeRobot](https://uptimerobot.com) or [Cron-job.org](https://cron-job.org) pinging your Render URL (e.g. `https://your-bot.onrender.com/healthz`) every 5 to 10 minutes.

---

## 🛠️ Local Development & Testing

### 1. Clone & Install Dependencies
```bash
git clone https://github.com/your-username/bioLinkChannelRestrictor.git
cd bioLinkChannelRestrictor

# Create and activate virtual environment
python -m venv venv
# On Windows:
.\venv\Scripts\activate
# On Linux/macOS:
source venv/bin/activate

# Install requirements
pip install -r requirements.txt
```

### 2. Configure Environment
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```
Edit `.env` and insert your Telegram bot token:
```env
BOT_TOKEN=1234567890:ABCdefGhIJKlmNoPQRsTUVwxyZ
PORT=8080
```

### 3. Run Unit & Integration Tests
```bash
python test_bot.py
```

### 4. Run the Bot Locally
```bash
python main.py
```

You should see logs indicating:
```text
Render health check HTTP server listening on 0.0.0.0:8080
Bot connected successfully as @YourBotName (ID: 123456789)
Starting Telegram Bot non-blocking infinity polling...
```

You can verify the HTTP health check locally by visiting in your browser:
👉 **[http://localhost:8080/health](http://localhost:8080/health)**

- **In a Web Browser**: Displays a dark-mode status dashboard showing Bot Connection, Uptime, Monitored Groups, and Cache metrics.
- **In Postman or cURL**: Returns formatted JSON status:
```bash
curl http://localhost:8080/health
```
Response:
```json
{
  "status": "UP",
  "service": "BioLinkChannelRestrictorBot",
  "telegram_bot": {
    "status": "ONLINE",
    "username": "YourBotUsername",
    "id": 123456789
  },
  "stats": {
    "active_groups": 2,
    "cached_admins_chats": 2,
    "cached_user_bios": 15
  },
  "uptime": "0d 2h 15m 30s",
  "uptime_seconds": 8130,
  "timestamp": "2026-10-28 15:52:00 UTC"
}
```
*(Tip: In a browser, you can also view the raw JSON at `http://localhost:8080/health?format=json`)*
```

---

## 🤖 Group Bot Permissions Required
For the bot to work properly, grant it the following administrator privileges in your Telegram group:
1. **Delete Messages** *(Required for all actions: delete, warn, mute, ban)*
2. **Restrict Users** *(Required if using `mute` or `ban`)*
