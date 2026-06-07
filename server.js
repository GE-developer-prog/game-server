 /**
 * NjaLudo WebSocket Server
 * ────────────────────────
 * Pure message-relay server. No game logic lives here.
 * All game authority stays with the host client.
 *
 * Deploy free on Railway / Render / Fly.io
 *   railway up  OR  render deploy  OR  fly launch
 *
 * Local dev:
 *   npm install ws
 *   node server.js
 */

const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;

/**
 * rooms Map structure:
 * {
 *   "ABC123": {
 *     hostId: "socket-id",
 *     players: [
 *       { id: "socket-id", name: "Naruto", color: null, isHost: true }
 *     ]
 *   }
 * }
 */
const rooms  = new Map();   // roomCode → room object
const sockets = new Map();  // socket → { id, roomCode, name, color, isHost }

let _idCounter = 0;
function genId() { return `s${++_idCounter}_${Date.now()}`; }

function genCode() {
  const CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c = '';
  for (let i = 0; i < 6; i++) c += CHARS[Math.floor(Math.random() * CHARS.length)];
  return c;
}

function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

/** Send to every socket in a room except optional excludeWs */
function broadcast(roomCode, msg, excludeWs) {
  const room = rooms.get(roomCode);
  if (!room) return;
  const data = JSON.stringify(msg);
  for (const [ws, info] of sockets) {
    if (info.roomCode === roomCode && ws !== excludeWs && ws.readyState === WebSocket.OPEN) {
      ws.send(data);
    }
  }
}

/** Send only to the host of a room */
function sendToHost(roomCode, msg) {
  const room = rooms.get(roomCode);
  if (!room) return;
  for (const [ws, info] of sockets) {
    if (info.roomCode === roomCode && info.isHost && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg));
      return;
    }
  }
}

/** Build the lobby player list for LOBBY_STATE messages */
function lobbyPlayers(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return [];
  return room.players.map(p => ({ name: p.name, color: p.color, isHost: p.isHost }));
}

// ── SERVER ──────────────────────────────────────────────────────────────────

const wss = new WebSocketServer({ port: PORT });
console.log(`[Server] NjaLudo WS server listening on :${PORT}`);

wss.on('connection', (ws) => {
  const id = genId();
  sockets.set(ws, { id, roomCode: null, name: null, color: null, isHost: false });
  console.log(`[+] Connected: ${id}  (total: ${sockets.size})`);

  // ── Heartbeat ──
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }
    const info = sockets.get(ws);
    if (!info) return;

    switch (msg.type) {

      // ── HOST creates a room ──────────────────────────────────────────────
      case 'CREATE_ROOM': {
        const name = (msg.name || '').trim().slice(0, 16);
        if (!name) { send(ws, { type: 'ERROR', reason: 'Name required' }); return; }

        // Generate a unique code
        let code;
        let tries = 0;
        do { code = genCode(); tries++; } while (rooms.has(code) && tries < 20);

        if (rooms.has(code)) { send(ws, { type: 'ERROR', reason: 'Could not generate unique room code. Try again.' }); return; }

        info.name    = name;
        info.isHost  = true;
        info.roomCode = code;

        rooms.set(code, {
          hostId:      id,
          gameStarted: false,
          players: [{ id, name, color: null, isHost: true }]
        });

        send(ws, { type: 'ROOM_CREATED', roomCode: code, name });
        console.log(`[Room] Created ${code} by ${name}`);
        break;
      }

      // ── JOINER joins a room ──────────────────────────────────────────────
      case 'JOIN_ROOM': {
        const code = (msg.roomCode || '').toUpperCase().trim();
        const name = (msg.name || '').trim().slice(0, 16);

        if (!name)  { send(ws, { type: 'REJECTED', reason: 'Name required' }); return; }
        if (!code)  { send(ws, { type: 'REJECTED', reason: 'Room code required' }); return; }

        const room = rooms.get(code);
        if (!room)  { send(ws, { type: 'REJECTED', reason: 'Room not found. Check the code.' }); return; }
        if (room.gameStarted) { send(ws, { type: 'REJECTED', reason: 'Game already started.' }); return; }
        if (room.players.length >= 4) { send(ws, { type: 'REJECTED', reason: 'Room is full (max 4 players).' }); return; }
        if (room.players.find(p => p.name === name)) { send(ws, { type: 'REJECTED', reason: 'Name already taken. Use a different name.' }); return; }

        info.name     = name;
        info.isHost   = false;
        info.roomCode = code;

        room.players.push({ id, name, color: null, isHost: false });

        // Tell the joiner they're in
        send(ws, {
          type:     'JOINED',
          roomCode: code,
          name,
          players:  lobbyPlayers(code)
        });

        // Tell everyone (including host) a player joined
        broadcast(code, {
          type:    'PLAYER_JOINED',
          name,
          players: lobbyPlayers(code)
        });

        console.log(`[Room] ${name} joined ${code}  (${room.players.length}/4)`);
        break;
      }

      // ── TOKEN selection (joiner → host via server) ───────────────────────
      case 'TOKEN_SELECT': {
        if (!info.roomCode) return;
        // Forward to host only; host will validate and broadcast TOKEN_LOCKED
        sendToHost(info.roomCode, { type: 'TOKEN_SELECT', payload: msg.payload });
        break;
      }

      // ── HOST broadcasts token locked to everyone else ────────────────────
      case 'TOKEN_LOCKED': {
        if (!info.isHost || !info.roomCode) return;
        // Update server-side player record
        const room = rooms.get(info.roomCode);
        if (room) {
          const p = room.players.find(pl => pl.name === msg.payload.name);
          if (p) p.color = msg.payload.color;
        }
        broadcast(info.roomCode, { type: 'TOKEN_LOCKED', payload: msg.payload }, ws);
        // Also send updated lobby state
        broadcast(info.roomCode, { type: 'LOBBY_STATE', payload: { players: lobbyPlayers(info.roomCode), roomCode: info.roomCode } });
        break;
      }

      // ── HOST broadcasts lobby state ──────────────────────────────────────
      case 'LOBBY_STATE': {
        if (!info.isHost || !info.roomCode) return;
        broadcast(info.roomCode, { type: 'LOBBY_STATE', payload: { players: lobbyPlayers(info.roomCode), roomCode: info.roomCode } }, ws);
        break;
      }

      // ── HOST starts the game ─────────────────────────────────────────────
      case 'GAME_START': {
        if (!info.isHost || !info.roomCode) return;
        const room = rooms.get(info.roomCode);
        if (room) room.gameStarted = true;
        broadcast(info.roomCode, { type: 'GAME_START', payload: msg.payload }, ws);
        console.log(`[Room] Game started in ${info.roomCode}`);
        break;
      }

      // ── GENERIC RELAY messages ───────────────────────────────────────────
      // These are forwarded as-is.  The host is the authority for game logic.
      // Joiners send to host; host broadcasts to everyone.

      case 'DICE_ROLL_REQUEST': {
        // Joiner → host
        if (!info.roomCode) return;
        sendToHost(info.roomCode, { type: 'DICE_ROLL_REQUEST', payload: msg.payload });
        break;
      }

      case 'MOVE_REQUEST': {
        // Joiner → host
        if (!info.roomCode) return;
        sendToHost(info.roomCode, { type: 'MOVE_REQUEST', payload: msg.payload });
        break;
      }

      // Host → everyone: game state messages
      case 'DICE_RESULT':
      case 'GAME_STATE_SYNC':
      case 'NEXT_TURN':
      case 'PLAYER_WON':
      case 'GAME_OVER':
      case 'VIDEO_PLAY':
      case 'CHAT_ALERT': {
        if (!info.isHost || !info.roomCode) return;
        broadcast(info.roomCode, { type: msg.type, payload: msg.payload }, ws);
        break;
      }

      // Chat: sender → server → everyone else in room
      case 'CHAT_MSG': {
        if (!info.roomCode) return;
        broadcast(info.roomCode, { type: 'CHAT_MSG', payload: msg.payload }, ws);
        break;
      }

      // Voice speaking indicator: sender → everyone else
      case 'VOICE_SPEAKING': {
        if (!info.roomCode) return;
        broadcast(info.roomCode, { type: 'VOICE_SPEAKING', payload: msg.payload }, ws);
        break;
      }

      // Kick: host → specific target (by name)
      
     case 'KICK_PLAYER': {
      if (!info.isHost || !info.roomCode) return;
      const targetName = msg.payload.target;
      for (const [targetWs, targetInfo] of sockets) {
    if (targetInfo.roomCode === info.roomCode && targetInfo.name === targetName) {
      send(targetWs, { type: 'KICK_PLAYER', payload: { target: targetName } });
      targetInfo.roomCode = null; // ✅ ADD THIS LINE
      break;
    }
  }
  // Remove from room
  const room = rooms.get(info.roomCode);
  if (room) room.players = room.players.filter(p => p.name !== targetName);
  broadcast(info.roomCode, { type: 'LOBBY_STATE', payload: { players: lobbyPlayers(info.roomCode), roomCode: info.roomCode } }, ws);
  break;
}

      // Player voluntarily quits → forward to host
      case 'PLAYER_QUIT': {
        if (!info.roomCode) return;
        sendToHost(info.roomCode, { type: 'PLAYER_QUIT', payload: msg.payload });
        broadcast(info.roomCode, { type: 'CHAT_ALERT', payload: { msg: `${info.name} left the game`, color: info.color, alertType: 'quit' } }, ws);
        cleanupSocket(ws, false);
        break;
      }

      // Host closes / leaves
      case 'HOST_LEFT':
      case 'ROOM_CLOSED': {
        if (!info.roomCode) return;
        broadcast(info.roomCode, { type: msg.type, payload: msg.payload }, ws);
        cleanupRoom(info.roomCode);
        break;
      }

      case 'PING':
        send(ws, { type: 'PONG', payload: msg.payload });
        break;

      default:
        // Unknown message — ignore silently
        break;
    }
  });

  ws.on('close', () => {
    const info = sockets.get(ws);
    if (info) {
      console.log(`[-] Disconnected: ${info.name || info.id}  room: ${info.roomCode || 'none'}`);
      cleanupSocket(ws, true);
    }
    sockets.delete(ws);
  });

  ws.on('error', (err) => {
    console.warn(`[WS Error] ${err.message}`);
  });
});

// ── Cleanup helpers ──────────────────────────────────────────────────────────

function cleanupSocket(ws, wasDisconnect) {
  const info = sockets.get(ws);
  if (!info || !info.roomCode) return;

  const room = rooms.get(info.roomCode);
  if (!room) return;

  if (info.isHost) {
    // Host gone — notify all joiners and destroy room
    broadcast(info.roomCode, {
      type:    wasDisconnect ? 'HOST_LEFT' : 'ROOM_CLOSED',
      payload: { name: info.name }
    }, ws);
    cleanupRoom(info.roomCode);
  } else {
    // Joiner gone — remove from room, notify host
    room.players = room.players.filter(p => p.id !== info.id);
    sendToHost(info.roomCode, {
      type:    wasDisconnect ? 'PLAYER_DISCONNECTED' : 'PLAYER_QUIT',
      payload: { name: info.name, color: info.color }
    });
    broadcast(info.roomCode, {
      type:    'CHAT_ALERT',
      payload: { msg: `${info.name} ${wasDisconnect ? 'lost connection' : 'left'}`, color: info.color, alertType: wasDisconnect ? 'disconnect' : 'quit' }
    }, ws);
    // Broadcast updated lobby state
    broadcast(info.roomCode, {
      type:    'LOBBY_STATE',
      payload: { players: lobbyPlayers(info.roomCode), roomCode: info.roomCode }
    });
    if (room.players.length === 0) cleanupRoom(info.roomCode);
  }
}

function cleanupRoom(code) {
  rooms.delete(code);
  console.log(`[Room] Closed ${code}`);
}

// ── Heartbeat interval — ping all connected sockets every 20s ───────────────
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 20000);

// ── Graceful shutdown ────────────────────────────────────────────────────────
process.on('SIGTERM', () => {
  console.log('[Server] SIGTERM — shutting down');
  wss.close(() => process.exit(0));
});
