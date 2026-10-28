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
- **Passive State**: Stays inactive upon joining a group chat. It does not delete or process any messages until activated.
- **Activation (`/start`)**: Initialized exclusively by group administrators with default settings:
  - `activated = True`
  - `action = "delete"`
  - `ignore_admins = True`

### 2. Administrator Feature Controls
- **`/setaction <delete|warn|mute|ban>`**: Configures the punishment applied when a user has a bio link:
  - `delete`: Silently deletes the violating message.
  - `warn`: Deletes the message and sends a warning in the group (auto-deleted after 12s to prevent chat clutter).
  - `mute`: Deletes the message and restricts the user from sending messages in the chat.
  - `ban`: Deletes the message and permanently bans the user from the group.
- **`/toggleadmin`**: Toggles whether group administrators and anonymous admins are exempted (Default: `True` / exempt).
- **`/status`**: Displays a real-time configuration dashboard of the group.
- **`/help`**: Lists available commands and usage instructions.

### 3. Exemption Command (`/free` & `/unfree`)
- **`/free`**:
  - Reply to any message: `/free`
  - Mention handle: `/free @username`
  - Direct user ID: `/free 123456789`
  - Adds the user's ID to `active_groups[chat_id]["free_list"]`. Exempted users bypass all bio checks permanently.
- **`/unfree`**:
  - Removes a user from the chat's exemption list.

### 4. Anti-Spam / Filter Engine
- Intercepts all incoming and edited messages across all media types (text, photos, stickers, animations, videos, voices, etc.).
- **Fast Check 1**: Checks if sender is Admin or Anonymous Admin AND `ignore_admins` is enabled (`O(1)` memory check).
- **Fast Check 2**: Checks if sender is on the chat's `free_list` (`O(1)` set lookup).
- **Bio Scan**: Fetches Full Info profile asynchronously (`bot.get_chat(user_id)`). Scans the `bio` attribute using a comprehensive regex covering `http://`, `https://`, `t.me/`, `.com`, `.org`, link aggregators (`bio.link`, `linktr.ee`), and `@usernames`.
- **Action Trigger**: If a promotional link or handle is detected in their bio, executes the configured action.

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
6. Click **Create Web Service**. Render will build and deploy the container. Render's health checks will query `http://0.0.0.0:8080/` and verify the service is running.

### Method 2: Render Blueprint (`render.yaml`)
If deploying via Render Blueprints, Render will automatically detect `render.yaml` in the root of the repository and configure all settings. You only need to enter your `BOT_TOKEN`.

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

You can verify the HTTP health check locally by visiting:
[http://localhost:8080/](http://localhost:8080/) or running:
```bash
curl http://localhost:8080/healthz
```
Response:
```json
{
  "status": "healthy",
  "service": "BioLinkChannelRestrictorBot",
  "active_groups": 0,
  "cached_admins_chats": 0,
  "cached_user_bios": 0,
  "uptime_seconds": 15,
  "timestamp": 1793200000
}
```

---

## 🤖 Group Bot Permissions Required
For the bot to work properly, grant it the following administrator privileges in your Telegram group:
1. **Delete Messages** *(Required for all actions: delete, warn, mute, ban)*
2. **Restrict Users** *(Required if using `mute` or `ban`)*
