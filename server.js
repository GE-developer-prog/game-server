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
 *       { id: "socket-id", name: "Naruto", color: null, isHost: true, peerId: null }
 *     ]
 *   }
 * }
 */
const rooms   = new Map();  // roomCode → room object
const sockets = new Map();  // socket   → { id, roomCode, name, color, isHost }

let _idCounter = 0;
function genId() { return `s${++_idCounter}_${Date.now()}`; }

function genCode() {
  const CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c = '';
  for (let i = 0; i < 6; i++) c += CHARS[Math.floor(Math.random() * CHARS.length)];
  return c;
}

function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
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

// FIX 1: lobbyPlayers now includes peerId so voice chat peer IDs reach all clients
function lobbyPlayers(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return [];
  return room.players.map(p => ({
    name:   p.name,
    color:  p.color,
    isHost: p.isHost,
    peerId: p.peerId || null,
  }));
}

// ── SERVER ───────────────────────────────────────────────────────────────────

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

      // ── HOST creates a room ─────────────────────────────────────────────
      case 'CREATE_ROOM': {
        const name = (msg.name || '').trim().slice(0, 16);
        if (!name) { send(ws, { type: 'ERROR', reason: 'Name required' }); return; }

        let code, tries = 0;
        do { code = genCode(); tries++; } while (rooms.has(code) && tries < 20);
        if (rooms.has(code)) { send(ws, { type: 'ERROR', reason: 'Could not generate unique room code. Try again.' }); return; }

        info.name     = name;
        info.isHost   = true;
        info.roomCode = code;

        // FIX 2: store peerId from host
        rooms.set(code, {
          hostId:      id,
          gameStarted: false,
          players: [{ id, name, color: null, isHost: true, peerId: msg.peerId || null }],
        });

        send(ws, { type: 'ROOM_CREATED', roomCode: code, name });
        console.log(`[Room] Created ${code} by ${name}`);
        break;
      }

      // ── JOINER joins a room ─────────────────────────────────────────────
      case 'JOIN_ROOM': {
        const code = (msg.roomCode || '').toUpperCase().trim();
        const name = (msg.name || '').trim().slice(0, 16);

        if (!name) { send(ws, { type: 'REJECTED', reason: 'Name required' }); return; }
        if (!code) { send(ws, { type: 'REJECTED', reason: 'Room code required' }); return; }

        const room = rooms.get(code);
        if (!room)                { send(ws, { type: 'REJECTED', reason: 'Room not found. Check the code.' }); return; }
        if (room.gameStarted)     { send(ws, { type: 'REJECTED', reason: 'Game already started.' }); return; }
        if (room.players.length >= 4) { send(ws, { type: 'REJECTED', reason: 'Room is full (max 4 players).' }); return; }
        if (room.players.find(p => p.name === name)) { send(ws, { type: 'REJECTED', reason: 'Name already taken. Use a different name.' }); return; }

        info.name     = name;
        info.isHost   = false;
        info.roomCode = code;

        // FIX 3: store peerId from joiner
        room.players.push({ id, name, color: null, isHost: false, peerId: msg.peerId || null });

        send(ws, { type: 'JOINED', roomCode: code, name, players: lobbyPlayers(code) });
        broadcast(code, { type: 'PLAYER_JOINED', name, players: lobbyPlayers(code) });

        console.log(`[Room] ${name} joined ${code}  (${room.players.length}/4)`);
        break;
      }

      // ── TOKEN selection (joiner → host via server) ──────────────────────
      case 'TOKEN_SELECT': {
        if (!info.roomCode) return;
        sendToHost(info.roomCode, { type: 'TOKEN_SELECT', payload: msg.payload });
        break;
      }

      // ── HOST broadcasts token locked to everyone else ───────────────────
      case 'TOKEN_LOCKED': {
        if (!info.isHost || !info.roomCode) return;
        const room = rooms.get(info.roomCode);
        if (room) {
          const p = room.players.find(pl => pl.name === msg.payload.name);
          if (p) p.color = msg.payload.color;
        }
        // FIX 4: sync color back onto the socket info for the player who picked
        for (const [ws2, info2] of sockets) {
          if (info2.roomCode === info.roomCode && info2.name === msg.payload.name) {
            info2.color = msg.payload.color;
            break;
          }
        }
        broadcast(info.roomCode, { type: 'TOKEN_LOCKED', payload: msg.payload }, ws);
        broadcast(info.roomCode, { type: 'LOBBY_STATE', payload: { players: lobbyPlayers(info.roomCode), roomCode: info.roomCode } });
        break;
      }

      // ── HOST broadcasts lobby state ─────────────────────────────────────
      case 'LOBBY_STATE': {
        if (!info.isHost || !info.roomCode) return;
        broadcast(info.roomCode, { type: 'LOBBY_STATE', payload: { players: lobbyPlayers(info.roomCode), roomCode: info.roomCode } }, ws);
        break;
      }

      // ── HOST regenerates room code (only allowed before any joiner joins) ──
      case 'REGEN_CODE': {
        if (!info.isHost || !info.roomCode) return;
        const oldRoom = rooms.get(info.roomCode);
        if (!oldRoom) return;

        // Block regen if any joiner is already present
        const joiners = oldRoom.players.filter(p => !p.isHost);
        if (joiners.length > 0) {
          send(ws, { type: 'ERROR', reason: 'Cannot regenerate code after a player has joined.' });
          return;
        }

        // Generate a new unique code
        let newCode, tries = 0;
        do { newCode = genCode(); tries++; } while (rooms.has(newCode) && tries < 20);
        if (rooms.has(newCode)) {
          send(ws, { type: 'ERROR', reason: 'Could not generate unique room code. Try again.' });
          return;
        }

        // Move room to new code, clean up old entry
        const oldCode = info.roomCode;
        rooms.set(newCode, oldRoom);
        rooms.delete(oldCode);

        // Update host socket info
        info.roomCode = newCode;

        send(ws, { type: 'CODE_REGENERATED', roomCode: newCode });
        console.log(`[Room] Code regenerated ${oldCode} → ${newCode} by ${info.name}`);
        break;
      }

      // ── HOST starts the game ────────────────────────────────────────────
      case 'GAME_START': {
        if (!info.isHost || !info.roomCode) return;
        const room = rooms.get(info.roomCode);

        // SPEC: Game cannot start if only host is present
        const joiners = room ? room.players.filter(p => !p.isHost) : [];
        if (joiners.length === 0) {
          send(ws, { type: 'ERROR', reason: 'Cannot start — no players have joined yet.' });
          return;
        }

        if (room) room.gameStarted = true;
        broadcast(info.roomCode, { type: 'GAME_START', payload: msg.payload }, ws);
        console.log(`[Room] Game started in ${info.roomCode}`);
        break;
      }

      // ── GENERIC RELAY messages ──────────────────────────────────────────

      case 'DICE_ROLL_REQUEST': {
        if (!info.roomCode) return;
        sendToHost(info.roomCode, { type: 'DICE_ROLL_REQUEST', payload: msg.payload });
        break;
      }

      case 'MOVE_REQUEST': {
        if (!info.roomCode) return;
        sendToHost(info.roomCode, { type: 'MOVE_REQUEST', payload: msg.payload });
        break;
      }

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

      // ADD THIS BLOCK RIGHT HERE ↓
        case 'VIDEO_DONE': {
          if (!info.roomCode) return;
          sendToHost(info.roomCode, { type: 'VIDEO_DONE', payload: { name: info.name } });
          break;
        }

      case 'CHAT_MSG': {
        if (!info.roomCode) return;
        broadcast(info.roomCode, { type: 'CHAT_MSG', payload: msg.payload }, ws);
        break;
      }

      case 'VOICE_SPEAKING': {
        if (!info.roomCode) return;
        broadcast(info.roomCode, { type: 'VOICE_SPEAKING', payload: msg.payload }, ws);
        break;
      }

      case 'KICK_PLAYER': {
        if (!info.isHost || !info.roomCode) return;
        const targetName = msg.payload.target;
        for (const [targetWs, targetInfo] of sockets) {
          if (targetInfo.roomCode === info.roomCode && targetInfo.name === targetName) {
            send(targetWs, { type: 'KICK_PLAYER', payload: { target: targetName } });
            targetInfo.roomCode = null;
            break;
          }
        }
        const room = rooms.get(info.roomCode);
        if (room) room.players = room.players.filter(p => p.name !== targetName);
        broadcast(info.roomCode, { type: 'LOBBY_STATE', payload: { players: lobbyPlayers(info.roomCode), roomCode: info.roomCode } }, ws);
        break;
      }

      // FIX 5: PLAYER_QUIT — removed manual sendToHost call here to avoid
      // double quit message. cleanupSocket handles notifying the host.
      case 'PLAYER_QUIT': {
        if (!info.roomCode) return;
        broadcast(info.roomCode, {
          type:    'CHAT_ALERT',
          payload: { msg: `${info.name} left the game`, color: info.color, alertType: 'quit' },
        }, ws);
        // FIX 6: null out roomCode before cleanupSocket so it doesn't double-fire
        const quitRoomCode = info.roomCode;
        info.roomCode = null;
        cleanupSocket(ws, false, quitRoomCode);
        break;
      }

      case 'HOST_LEFT':
      case 'ROOM_CLOSED': {
        if (!info.roomCode) return;
        broadcast(info.roomCode, { type: msg.type, payload: msg.payload }, ws);
        cleanupRoom(info.roomCode);
        info.roomCode = null;
        break;
      }

      case 'PING':
        send(ws, { type: 'PONG', payload: msg.payload });
        break;

      default:
        break;
    }
  });

  ws.on('close', () => {
    const info = sockets.get(ws);
    if (info) {
      console.log(`[-] Disconnected: ${info.name || info.id}  room: ${info.roomCode || 'none'}`);
      cleanupSocket(ws, true, info.roomCode);
    }
    sockets.delete(ws);
  });

  ws.on('error', (err) => {
    console.warn(`[WS Error] ${err.message}`);
  });
});

// ── Cleanup helpers ───────────────────────────────────────────────────────────

// FIX 5+6: cleanupSocket now accepts explicit roomCode so it works correctly
// even after info.roomCode has been nulled (PLAYER_QUIT path).
function cleanupSocket(ws, wasDisconnect, roomCode) {
  if (!roomCode) return;

  const info = sockets.get(ws);
  const room = rooms.get(roomCode);
  if (!room) return;

  if (info && info.isHost) {
    // Host gone — notify all joiners and destroy room
    broadcast(roomCode, {
      type:    wasDisconnect ? 'HOST_LEFT' : 'ROOM_CLOSED',
      payload: { name: info ? info.name : 'Host' },
    }, ws);
    cleanupRoom(roomCode);
  } else {
    // Joiner gone — remove from room, notify host once
    const playerName  = info ? info.name  : null;
    const playerColor = info ? info.color : null;
    const playerId    = info ? info.id    : null;

    if (playerId) room.players = room.players.filter(p => p.id !== playerId);

    // Single notification to host (fixes double-quit bug)
    sendToHost(roomCode, {
      type:    wasDisconnect ? 'PLAYER_DISCONNECTED' : 'PLAYER_QUIT',
      payload: { name: playerName, color: playerColor },
    });

    broadcast(roomCode, {
      type:    'CHAT_ALERT',
      payload: {
        msg:       `${playerName} ${wasDisconnect ? 'lost connection' : 'left'}`,
        color:     playerColor,
        alertType: wasDisconnect ? 'disconnect' : 'quit',
      },
    }, ws);

    broadcast(roomCode, {
      type:    'LOBBY_STATE',
      payload: { players: lobbyPlayers(roomCode), roomCode },
    });

    // SPEC: End rule — if no joiners remain and game is in progress, end game for host
    const remainingJoiners = room.players.filter(p => !p.isHost);
    if (remainingJoiners.length === 0 && room.gameStarted) {
      sendToHost(roomCode, {
        type:    'GAME_OVER',
        payload: { reason: 'all_players_left' },
      });
      console.log(`[Room] All joiners left ${roomCode} — GAME_OVER sent to host`);
    }

    if (room.players.length === 0) cleanupRoom(roomCode);
  }
}

function cleanupRoom(code) {
  rooms.delete(code);
  console.log(`[Room] Closed ${code}`);
}

// ── Heartbeat — ping all connected sockets every 20s ─────────────────────────
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 20000);

// ── Graceful shutdown ─────────────────────────────────────────────────────────
process.on('SIGTERM', () => {
  console.log('[Server] SIGTERM — shutting down');
  wss.close(() => process.exit(0));
});
