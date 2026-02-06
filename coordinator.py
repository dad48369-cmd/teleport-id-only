# coordinator_v3.py - With proxy support for Roblox API
from flask import Flask, request, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import time
import threading
import requests
import json
import random
from collections import defaultdict

app = Flask(__name__)
limiter = Limiter(get_remote_address, app=app, default_limits=["1000 per minute"])

# ==================== PROXY CONFIGURATION ====================
# Add your proxies here (HTTP/HTTPS)
PROXIES = ["http://r08ckvvpewe9q6j-country-any:8EA3JqY0mv2wQH6@resi.rainproxy.io:9090",
           "http://r08ckvvpewe9q6j-country-any:8EA3JqY0mv2wQH6@resi.rainproxy.io:9090",
           "http://r08ckvvpewe9q6j-country-any:8EA3JqY0mv2wQH6@resi.rainproxy.io:9090",

    None,  # First try direct (your VPS might work sometimes)
    # "http://user:pass@ip:port",
    # "http://ip:port",
    # Add more proxies as needed
]

current_proxy_index = 0

def get_proxy():
    """Rotate through proxies"""
    global current_proxy_index
    if not PROXIES:
        return None
    proxy = PROXIES[current_proxy_index]
    current_proxy_index = (current_proxy_index + 1) % len(PROXIES)
    return proxy

# ==================== CONFIGURATION ====================
ROBLOX_PLACE_ID = "109983668079237"
SERVER_LOCK_TIME = 180  # 3 minutes
BOT_TIMEOUT = 60

# State
available_servers = []
claimed_servers = {}
active_bots = {}
server_find_counts = defaultdict(int)
last_refresh = 0
consecutive_errors = 0

def fetch_roblox_servers_with_retry():
    """Fetch with proxy rotation on 429"""
    global consecutive_errors
    
    for attempt in range(len(PROXIES) * 2):  # Try each proxy twice
        proxy = get_proxy()
        proxies = {"http": proxy, "https": proxy} if proxy else None
        
        url = f"https://games.roblox.com/v1/games/{ROBLOX_PLACE_ID}/servers/Public?limit=100"
        
        try:
            print(f"[FETCH] Attempt {attempt+1}/{len(PROXIES)*2} via {proxy or 'DIRECT'}")
            r = requests.get(url, proxies=proxies, timeout=15)
            
            if r.status_code == 200:
                consecutive_errors = 0
                data = r.json()
                servers = []
                for s in data.get("data", []):
                    servers.append({
                        "id": s["id"],
                        "players": s.get("playing", 0),
                        "ping": s.get("ping", 999)
                    })
                print(f"[SUCCESS] Got {len(servers)} servers via {proxy or 'DIRECT'}")
                return servers, data.get("nextPageCursor")
                
            elif r.status_code == 429:
                print(f"[RATE LIMIT] 429 via {proxy or 'DIRECT'}, rotating...")
                consecutive_errors += 1
                time.sleep(1)
                continue  # Try next proxy
                
            else:
                print(f"[ERROR] Status {r.status_code}")
                consecutive_errors += 1
                
        except Exception as e:
            print(f"[ERROR] {proxy or 'DIRECT'} failed: {str(e)[:50]}")
            consecutive_errors += 1
            continue
    
    return [], None

def fetch_all_servers():
    """Fetch multiple pages with proxy rotation"""
    all_servers = []
    cursor = ""
    
    for page in range(3):  # Max 300 servers to avoid rate limits
        servers, next_cursor = fetch_roblox_servers_with_retry()
        
        if not servers:
            break
            
        all_servers.extend(servers)
        
        if not next_cursor:
            break
            
        cursor = next_cursor
        time.sleep(0.5)  # Small delay between pages
    
    return all_servers

def maintenance_loop():
    """Background refresh with smart rate limit handling"""
    global available_servers, last_refresh, consecutive_errors
    
    while True:
        try:
            current_time = time.time()
            
            # Clean expired claims
            expired = []
            for sid, data in list(claimed_servers.items()):
                if current_time > data["expires"]:
                    expired.append(sid)
                    del claimed_servers[sid]
            
            if expired:
                print(f"[CLEANUP] Expired: {len(expired)}")
            
            # Adaptive refresh rate
            refresh_interval = 10 if consecutive_errors > 5 else 20
            
            if current_time - last_refresh > refresh_interval:
                fresh = fetch_all_servers()
                last_refresh = current_time
                
                claimed_ids = set(claimed_servers.keys())
                available_servers = [s for s in fresh if s["id"] not in claimed_ids]
                
                print(f"[STATUS] Available: {len(available_servers)}, Claimed: {len(claimed_servers)}, Errors: {consecutive_errors}")
                
                # Reset error counter on success
                if len(fresh) > 0:
                    consecutive_errors = 0
            
        except Exception as e:
            print(f"[MAINTENANCE ERROR] {e}")
        
        time.sleep(5)

@app.route("/request-server", methods=["POST"])
@limiter.limit("100/second")
def request_server():
    global available_servers
    
    data = request.json or {}
    bot_id = data.get("bot_id")
    current_server = data.get("current_server")
    
    if not bot_id:
        return jsonify({"error": "No bot_id"}), 400
    
    current_time = time.time()
    
    # Release current
    if current_server and current_server in claimed_servers:
        if claimed_servers[current_server]["bot_id"] == bot_id:
            del claimed_servers[current_server]
            print(f"[RELEASE] {bot_id[:20]}...")
    
    # Try to get server (with multiple attempts)
    best_server = None
    attempts = 0
    
    while not best_server and attempts < 3:
        # Refresh if empty
        if not available_servers:
            fresh = fetch_all_servers()
            claimed_ids = set(claimed_servers.keys())
            available_servers = [s for s in fresh if s["id"] not in claimed_ids]
            print(f"[REFRESH] Got {len(available_servers)} available")
        
        # Find unclaimed
        for server in available_servers:
            if server["id"] not in claimed_servers:
                best_server = server
                break
        
        if not best_server:
            time.sleep(1)
            attempts += 1
    
    if not best_server:
        return jsonify({"error": "No servers", "retry_in": 2}), 503
    
    # Claim it
    server_id = best_server["id"]
    claimed_servers[server_id] = {
        "bot_id": bot_id,
        "expires": current_time + SERVER_LOCK_TIME
    }
    
    available_servers = [s for s in available_servers if s["id"] != server_id]
    
    active_bots[bot_id] = {
        "last_seen": current_time,
        "current_server": server_id
    }
    
    print(f"[ASSIGN] {bot_id[:20]}... -> {server_id[:8]} (queue: {len(available_servers)})")
    
    return jsonify({
        "server_id": server_id,
        "expires_in": SERVER_LOCK_TIME,
        "queue": len(available_servers)
    })

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    data = request.json or {}
    bot_id = data.get("bot_id")
    server_id = data.get("server_id")
    found = data.get("found", 0)
    
    if bot_id:
        active_bots[bot_id] = {
            "last_seen": time.time(),
            "current_server": server_id
        }
        if found > 0:
            server_find_counts[server_id] = server_find_counts.get(server_id, 0) + found
    
    return jsonify({"status": "ok"})

@app.route("/release", methods=["POST"])
def release():
    data = request.json or {}
    server_id = data.get("server_id")
    bot_id = data.get("bot_id")
    
    if server_id and server_id in claimed_servers:
        if claimed_servers[server_id]["bot_id"] == bot_id:
            del claimed_servers[server_id]
            return jsonify({"ok": True})
    
    return jsonify({"ok": False})

@app.route("/stats", methods=["GET"])
def stats():
    current_time = time.time()
    
    # Clean dead bots
    dead = [b for b, d in active_bots.items() if current_time - d["last_seen"] > BOT_TIMEOUT]
    for b in dead:
        s = active_bots[b].get("current_server")
        if s and s in claimed_servers and claimed_servers[s]["bot_id"] == b:
            del claimed_servers[s]
        del active_bots[b]
    
    return jsonify({
        "available_servers": len(available_servers),
        "claimed_servers": len(claimed_servers),
        "active_bots": len(active_bots),
        "total_finds": sum(server_find_counts.values()),
        "proxies_available": len(PROXIES),
        "rate_limit_errors": consecutive_errors,
        "last_refresh": last_refresh
    })

if __name__ == "__main__":
    print("[INIT] Coordinator starting...")
    print(f"[INIT] {len(PROXIES)} proxies configured")
    
    # Test fetch on startup
    test = fetch_all_servers()
    print(f"[INIT] Test fetch: {len(test)} servers")
    
    threading.Thread(target=maintenance_loop, daemon=True).start()
    
    app.run(host="0.0.0.0", port=5000, threaded=True)