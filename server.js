// Relay WebSocket server: ESP32 (/device) <-> เกม Unity (/game?device=<MAC>)
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;

const devices = new Map();   // mac -> ws ของ ESP32
const games = new Map();     // mac -> Set<ws> ของเกมที่ดูอุปกรณ์นั้น
const lastSensor = new Map(); // mac -> ข้อความ sensor ล่าสุด (ส่งให้เกมที่เพิ่งต่อเข้ามา)

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("relay ok\n");   // ใช้เป็น health check ของแพลตฟอร์มโฮสต์
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, "http://localhost");
  if (url.pathname !== "/device" && url.pathname !== "/game") {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    ws.role = url.pathname === "/device" ? "device" : "game";
    ws.mac = (url.searchParams.get("device") || "").toUpperCase();
    wss.emit("connection", ws, req);
  });
});

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(typeof obj === "string" ? obj : JSON.stringify(obj));
}

function notifyGames(mac, obj) {
  const set = games.get(mac);
  if (set) for (const g of set) send(g, obj);
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));

  if (ws.role === "game") {
    if (!ws.mac) return ws.close(1008, "missing device");
    if (!games.has(ws.mac)) games.set(ws.mac, new Set());
    games.get(ws.mac).add(ws);
    console.log(`[game] join ${ws.mac}`);

    send(ws, { type: "device_status", online: devices.has(ws.mac) });
    if (lastSensor.has(ws.mac)) send(ws, lastSensor.get(ws.mac));

    ws.on("message", (data) => {
      const dev = devices.get(ws.mac);   // เกม -> ESP32
      if (dev) send(dev, data.toString());
    });

    ws.on("close", () => {
      const set = games.get(ws.mac);
      if (set) {
        set.delete(ws);
        if (set.size === 0) games.delete(ws.mac);
      }
      console.log(`[game] leave ${ws.mac}`);
    });
    return;
  }

  // ===== role = device (ESP32) =====
  ws.on("message", (data) => {
    const text = data.toString();
    let msg;
    try { msg = JSON.parse(text); } catch { return; }

    if (msg.type === "register" && msg.mac) {
      ws.mac = String(msg.mac).toUpperCase();
      const old = devices.get(ws.mac);
      if (old && old !== ws) old.terminate();   // ถ้ามีตัวเก่าค้าง ตัดทิ้ง
      devices.set(ws.mac, ws);
      console.log(`[device] register ${ws.mac}`);
      notifyGames(ws.mac, { type: "device_status", online: true });
      return;
    }

    if (!ws.mac) return;   // ยังไม่ register ไม่รับ
    if (msg.type === "sensor") lastSensor.set(ws.mac, text);
    notifyGames(ws.mac, text);
  });

  ws.on("close", () => {
    if (ws.mac && devices.get(ws.mac) === ws) {
      devices.delete(ws.mac);
      lastSensor.delete(ws.mac);
      notifyGames(ws.mac, { type: "device_status", online: false });
      console.log(`[device] offline ${ws.mac}`);
    }
  });
});

// ตรวจจับ client ที่หายเงียบ (ทุก 30 วิ)
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log(`relay listening on ${PORT}`));
