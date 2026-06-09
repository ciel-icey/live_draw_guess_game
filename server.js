const express = require('express');
const crypto = require('crypto');
const fs = require('fs');
const http = require('http');
const https = require('https');
const path = require('path');
const { Server } = require('socket.io');

const app = express();

const HTTPS_ENABLED = /^(1|true|yes)$/i.test(process.env.HTTPS || process.env.USE_HTTPS || '');
const SSL_CERT_FILE = process.env.SSL_CERT_FILE || path.join(__dirname, '.cert', 'localhost.crt');
const SSL_KEY_FILE = process.env.SSL_KEY_FILE || path.join(__dirname, '.cert', 'localhost.key');

let protocol = 'http';
let server;

if (HTTPS_ENABLED) {
  try {
    if (!fs.existsSync(SSL_CERT_FILE) || !fs.existsSync(SSL_KEY_FILE)) {
      throw new Error(`HTTPS certificate files not found: ${SSL_CERT_FILE}, ${SSL_KEY_FILE}`);
    }

    server = https.createServer({
      cert: fs.readFileSync(SSL_CERT_FILE),
      key: fs.readFileSync(SSL_KEY_FILE)
    }, app);
    protocol = 'https';
  } catch (err) {
    console.warn(`HTTPS unavailable, falling back to HTTP: ${err.message}`);
  }
}

if (!server) {
  server = http.createServer(app);
}
const io = new Server(server, {
  cors: { origin: "*" },
  pingTimeout: 60000,
  pingInterval: 25000
});

app.use(express.static('public'));
app.use(express.json({ limit: '1mb' }));

const MAX_PLAYERS = 20;
const MAX_SPECTATORS = 30;
const LOG_DIR = path.join(__dirname, 'logs');

function getLogDateKey(date = new Date()) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function getPlayerLogPath(date = new Date()) {
  return path.join(LOG_DIR, `player-activity-${getLogDateKey(date)}.log`);
}

function formatDuration(ms) {
  if (!Number.isFinite(ms) || ms < 0) return null;
  const totalSeconds = Math.floor(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
}

function logPlayerActivity(action, details = {}) {
  const now = new Date();
  const entry = {
    time: now.toISOString(),
    action,
    username: details.username || '',
    roomId: details.roomId || '',
    roomName: details.roomName || '',
    role: details.role || '',
    socketId: details.socketId || '',
    reason: details.reason || '',
    duration: details.duration || null
  };
  const line = JSON.stringify(entry) + '\n';
  fs.mkdir(LOG_DIR, { recursive: true }, (mkdirErr) => {
    if (mkdirErr) {
      console.warn(`写入玩家日志失败，无法创建目录: ${mkdirErr.message}`);
      return;
    }
    fs.appendFile(getPlayerLogPath(now), line, (writeErr) => {
      if (writeErr) console.warn(`写入玩家日志失败: ${writeErr.message}`);
    });
  });
}

const defaultWords = ['苹果', '香蕉', '猫', '狗', '太阳', '月亮', '星星', '电脑', '手机', '书本',
  '汽车', '飞机', '房子', '树', '花', '鱼', '鸟', '蛋糕', '冰淇淋', '篮球', '足球', '雨伞',
  '眼镜', '手表', '书包', '铅笔', '橡皮', '桌子', '椅子', '电视', '冰箱', '空调', '洗衣机',
  '吉他', '钢琴', '小提琴', '跑步', '游泳', '跳舞', '唱歌', '画画', '吃饭', '睡觉', '喝水'];

const rooms = {};

function generateRoomId() {
  let id;
  do { id = Math.random().toString(36).substring(2, 8).toUpperCase(); }
  while (rooms[id]);
  return id;
}

function createReconnectToken() {
  return crypto.randomBytes(16).toString('hex');
}

function createRoom(roomName) {
  const roomId = generateRoomId();
  rooms[roomId] = {
    id: roomId,
    name: roomName || '房间' + roomId,
    hostId: null,
    players: [],
    playerTokens: {},
    disconnectedPlayers: {},
    gameState: 'waiting',
    currentDrawerIndex: 0,
    currentWord: '',
    currentRound: 1,
    timer: 60,
    timerInterval: null,
    scores: {},
    guessedPlayers: [],
    canvasHistory: [],
    gameSettings: { drawTime: 60, selectWordTime: 15, totalRounds: 3 },
    customWords: [],
    spectatorCorrectCounts: {},
    currentSpectatorGuessed: new Set()
  };
  return roomId;
}

function getRoomBySocket(socket) {
  for (const roomId in rooms) {
    if (rooms[roomId].players.some(p => p.id === socket.id)) {
      return rooms[roomId];
    }
  }
  return null;
}

// 定期清理空房间和超时断线玩家
setInterval(() => {
  const now = Date.now();
  for (const roomId in rooms) {
    const room = rooms[roomId];
    let changed = false;

    for (const id in room.disconnectedPlayers) {
      if (now - room.disconnectedPlayers[id].disconnectTime > 5 * 60 * 1000) {
        const disconnected = room.disconnectedPlayers[id];
        logPlayerActivity('logout', {
          username: disconnected.player?.name,
          roomId,
          roomName: room.name,
          role: disconnected.player?.isSpectator ? 'spectator' : 'player',
          socketId: id,
          reason: 'disconnect_timeout',
          duration: formatDuration(now - (disconnected.player?.joinedAt || disconnected.disconnectTime))
        });
        delete room.disconnectedPlayers[id];
        delete room.scores[id];
        delete room.spectatorCorrectCounts[id];
        delete room.playerTokens[id];
        room.players = room.players.filter(p => p.id !== id);
        changed = true;
      }
    }

    const hasOnlinePlayer = room.players.some(p => !room.disconnectedPlayers[p.id]);
    if (!hasOnlinePlayer) {
      delete rooms[roomId];
      continue;
    }

    if (changed) {
      io.to(roomId).emit('playerLeft', {
        hostId: getHostId(room),
        players: room.players.filter(p => !room.disconnectedPlayers[p.id])
      });
      io.emit('roomListUpdate', getRoomListForBroadcast());
    }
  }
}, 60000);

function getActivePlayers(room) {
  return room.players.filter(p => !p.isSpectator && !room.disconnectedPlayers[p.id]);
}

function getHostId(room) {
  const active = getActivePlayers(room);
  if (room.hostId && active.some(p => p.id === room.hostId)) return room.hostId;
  room.hostId = active.length > 0 ? active[0].id : null;
  return room.hostId;
}

function getRandomWords(room, count) {
  const list = room.customWords.length > 0 ? room.customWords : defaultWords;
  const shuffled = [...list].sort(() => 0.5 - Math.random());
  return shuffled.slice(0, Math.min(count, shuffled.length));
}

// 规范化答案：去除所有空格并转小写
function normalizeAnswer(text) {
  return text.replace(/\s+/g, '').toLowerCase();
}

// 检查答案是否匹配当前词（支持别名）
function isWordMatched(message, currentWord) {
  const normalizedMsg = normalizeAnswer(message);
  const aliases = currentWord.split('/');
  return aliases.some(alias => normalizeAnswer(alias) === normalizedMsg);
}

// ========== 游戏流程 ==========
function startGame(room, socket) {
  if (room.gameState === 'finished') return;
  const active = getActivePlayers(room);
  
  if (active.length < 2) {
    if (socket) {
      socket.emit('errorMessage', '人数不足！至少需要 2 名非观战玩家才能开始游戏。');
    }
    return;
  }

  room.gameState = 'selectingWord';
  room.currentRound = 1;
  room.currentDrawerIndex = 0;
  room.scores = {};
  room.canvasHistory = [];
  room.spectatorCorrectCounts = {};
  active.forEach(p => room.scores[p.id] = 0);

  io.to(room.id).emit('gameStarted', {
    settings: room.gameSettings, players: room.players, scores: room.scores, hostId: getHostId(room)
  });
  
  io.emit('roomListUpdate', getRoomListForBroadcast());
  
  setTimeout(() => startWordSelection(room), 2000);
}

function startWordSelection(room) {
  if (room.gameState === 'finished') return;
  if (room.currentRound > room.gameSettings.totalRounds) {
    endGame(room);
    return;
  }

  while (room.currentDrawerIndex < room.players.length &&
    (room.disconnectedPlayers[room.players[room.currentDrawerIndex].id] ||
      room.players[room.currentDrawerIndex].isSpectator)) {
    room.currentDrawerIndex++;
  }

  if (room.currentDrawerIndex >= room.players.length) {
    room.currentDrawerIndex = 0;
    room.currentRound++;
    setTimeout(() => startWordSelection(room), 1000);
    return;
  }

  const drawer = room.players[room.currentDrawerIndex];
  const wordOptions = getRandomWords(room, 4);

  room.timer = room.gameSettings.selectWordTime;
  room.gameState = 'selectingWord';
  room.canvasHistory = [];

  io.to(room.id).emit('wordSelectionStart', {
    round: room.currentRound,
    totalRounds: room.gameSettings.totalRounds,
    drawerId: drawer.id,
    drawerName: drawer.name,
    currentTimer: room.timer
  });

  // 发送给画手时，只显示每个词的主名称（第一个/之前的部分）
  const wordChoices = wordOptions.map(full => ({
    display: full.includes('/') ? full.split('/')[0] : full,
    full
  }));
  io.to(drawer.id).emit('wordOptions', wordChoices);

  clearInterval(room.timerInterval);
  room.timerInterval = setInterval(() => {
    if (room.gameState === 'finished') return;
    room.timer--;
    io.to(room.id).emit('timerUpdate', room.timer);
    if (room.timer <= 0) {
      selectWord(room, wordOptions[0]);
    }
  }, 1000);
}

function selectWord(room, word) {
  if (room.gameState === 'finished') return;
  clearInterval(room.timerInterval);
  room.currentWord = word;
  room.timer = room.gameSettings.drawTime;
  room.gameState = 'playing';
  room.guessedPlayers = [];
  room.currentSpectatorGuessed = new Set();

  const drawerId = room.players[room.currentDrawerIndex].id;
  const drawerName = room.players[room.currentDrawerIndex].name;

  room.players.forEach(p => {
    if (room.disconnectedPlayers[p.id]) return;
    if (p.id === drawerId) {
      io.to(p.id).emit('roundStart', {
        round: room.currentRound, totalRounds: room.gameSettings.totalRounds,
        drawerId, drawerName, word: room.currentWord,
        isDrawer: true, isSpectator: false, currentTimer: room.timer, canvasHistory: room.canvasHistory
      });
    } else {
      io.to(p.id).emit('roundStart', {
        round: room.currentRound, totalRounds: room.gameSettings.totalRounds,
        drawerId, drawerName, word: '',
        isDrawer: false, isSpectator: p.isSpectator, currentTimer: room.timer, canvasHistory: room.canvasHistory
      });
    }
  });

  room.timerInterval = setInterval(() => {
    if (room.gameState === 'finished') return;
    room.timer--;
    io.to(room.id).emit('timerUpdate', room.timer);
    if (room.timer <= 0) {
      endTurn(room);
    }
  }, 1000);
}

function endTurn(room) {
  if (room.gameState === 'finished') return;
  clearInterval(room.timerInterval);

  const drawer = room.players[room.currentDrawerIndex];
  const n = room.guessedPlayers.length;
  const drawerScore = n * (n + 1) / 2;
  if (drawer && room.scores[drawer.id] !== undefined) {
    room.scores[drawer.id] += drawerScore;
  }

  io.to(room.id).emit('turnEnd', {
    word: room.currentWord, guessedPlayers: room.guessedPlayers,
    drawerScore, scores: room.scores
  });

  room.currentDrawerIndex++;
  if (room.currentDrawerIndex >= room.players.length) {
    room.currentDrawerIndex = 0;
    room.currentRound++;
  }
  setTimeout(() => startWordSelection(room), 4000);
}

function endGame(room) {
  clearInterval(room.timerInterval);
  room.gameState = 'finished';

  const onlinePlayers = room.players.filter(p => !room.disconnectedPlayers[p.id]);
  const playerRankings = onlinePlayers
    .filter(p => !p.isSpectator)
    .map(p => ({ id: p.id, name: p.name, score: room.scores[p.id] || 0 }))
    .sort((a, b) => b.score - a.score);

  const spectatorRankings = onlinePlayers
    .filter(p => p.isSpectator)
    .map(p => ({ id: p.id, name: p.name, correctCount: room.spectatorCorrectCounts[p.id] || 0 }))
    .sort((a, b) => b.correctCount - a.correctCount);

  io.to(room.id).emit('gameEnd', { rankings: playerRankings, spectatorRankings });
  for (const id in room.disconnectedPlayers) {
    delete room.scores[id];
    delete room.spectatorCorrectCounts[id];
    delete room.playerTokens[id];
    room.players = room.players.filter(p => p.id !== id);
  }
  room.disconnectedPlayers = {};
  
  io.emit('roomListUpdate', getRoomListForBroadcast());
}

function restartGame(room) {
  room.gameState = 'waiting';
  clearInterval(room.timerInterval);
  room.currentRound = 1;
  room.currentDrawerIndex = 0;
  room.scores = {};
  room.canvasHistory = [];
  room.spectatorCorrectCounts = {};
  getActivePlayers(room).forEach(p => room.scores[p.id] = 0);

  io.to(room.id).emit('gameRestarted', {
    players: room.players,
    settings: room.gameSettings,
    hostId: getHostId(room),
    hasCustomWords: room.customWords.length > 0,
    wordCount: room.customWords.length
  });
  
  io.emit('roomListUpdate', getRoomListForBroadcast());
}

function getRoomListForBroadcast() {
  return Object.values(rooms).map(room => ({
    id: room.id, name: room.name,
    playerCount: room.players.filter(p => !p.isSpectator && !room.disconnectedPlayers[p.id]).length,
    spectatorCount: room.players.filter(p => p.isSpectator && !room.disconnectedPlayers[p.id]).length,
    gameState: room.gameState
  }));
}

// 移除玩家
function removePlayerFromRoom(socket, room, isDisconnect = false) {
  const idx = room.players.findIndex(p => p.id === socket.id);
  if (idx === -1) return;

  const player = room.players[idx];
  const eventAt = Date.now();
  logPlayerActivity(isDisconnect ? 'disconnect' : 'logout', {
    username: player.name,
    roomId: room.id,
    roomName: room.name,
    role: player.isSpectator ? 'spectator' : 'player',
    socketId: socket.id,
    reason: isDisconnect ? 'socket_disconnect' : 'leave_room',
    duration: formatDuration(eventAt - (player.joinedAt || eventAt))
  });
  if (isDisconnect) {
    room.disconnectedPlayers[socket.id] = {
      player,
      oldScores: room.scores[socket.id],
      oldSpectatorCorrectCount: room.spectatorCorrectCounts[socket.id],
      reconnectToken: room.playerTokens[socket.id],
      canvasHistory: [...room.canvasHistory], disconnectTime: Date.now()
    };
    if (room.gameState === 'playing' && room.players[room.currentDrawerIndex]?.id === socket.id) {
      io.to(room.id).emit('chat', { name: '系统', message: `画手 ${player.name} 断线了，等待重连中，本轮计时继续`, isSystem: true });
    }
  } else {
    room.players.splice(idx, 1);
    delete room.scores[socket.id];
    delete room.spectatorCorrectCounts[socket.id];
    delete room.playerTokens[socket.id];
    if (room.hostId === socket.id) {
      room.hostId = null;
      getHostId(room);
    }
  }

  io.to(room.id).emit('playerLeft', {
    playerId: socket.id,
    players: room.players.filter(p => !room.disconnectedPlayers[p.id]),
    hostId: getHostId(room)
  });
  io.emit('roomListUpdate', getRoomListForBroadcast());
}

// ===================== Socket 事件 =====================
io.on('connection', (socket) => {
  console.log('用户连接:', socket.id);

  socket.on('reconnectAttempt', (payload, legacyOldId) => {
    const isPayloadObject = payload && typeof payload === 'object';
    const roomId = isPayloadObject ? payload.roomId : payload;
    const oldId = isPayloadObject ? payload.playerId : legacyOldId;
    const reconnectToken = isPayloadObject ? payload.reconnectToken : null;
    const room = rooms[roomId];
    if (!room) {
      socket.emit('reconnectFailed', { reason: 'roomNotFound', message: '房间不存在或已结束' });
      return;
    }

    const disconnected = room.disconnectedPlayers[oldId];
    if (!disconnected) {
      socket.emit('reconnectFailed', { reason: 'playerNotFound', message: '未找到可恢复的断线玩家' });
      return;
    }

    if (!reconnectToken || disconnected.reconnectToken !== reconnectToken) {
      socket.emit('reconnectFailed', { reason: 'tokenMismatch', message: '重连凭证无效' });
      return;
    }

    if (disconnected) {
      const { player, oldScores, oldSpectatorCorrectCount } = disconnected;
      delete room.disconnectedPlayers[oldId];

      const playerIndex = room.players.findIndex(p => p.id === oldId);
      if (playerIndex === -1) {
        socket.emit('reconnectFailed', { reason: 'playerNotFound', message: '玩家已被移出房间' });
        return;
      }

      room.players[playerIndex].id = socket.id;
      if (room.hostId === oldId) room.hostId = socket.id;
      logPlayerActivity('reconnect', {
        username: room.players[playerIndex].name,
        roomId: room.id,
        roomName: room.name,
        role: room.players[playerIndex].isSpectator ? 'spectator' : 'player',
        socketId: socket.id,
        reason: 'reconnect_success',
        duration: formatDuration(Date.now() - (room.players[playerIndex].joinedAt || Date.now()))
      });

      if (!player.isSpectator) room.scores[socket.id] = oldScores || 0;
      delete room.scores[oldId];
      if (player.isSpectator) room.spectatorCorrectCounts[socket.id] = oldSpectatorCorrectCount || 0;
      delete room.spectatorCorrectCounts[oldId];

      if (room.guessedPlayers.includes(oldId)) {
        room.guessedPlayers = room.guessedPlayers.map(id => id === oldId ? socket.id : id);
      }
      if (room.currentSpectatorGuessed.has(oldId)) {
        room.currentSpectatorGuessed.delete(oldId);
        room.currentSpectatorGuessed.add(socket.id);
      }

      const newReconnectToken = createReconnectToken();
      room.playerTokens[socket.id] = newReconnectToken;
      delete room.playerTokens[oldId];

      socket.join(roomId);
      if (player.isSpectator) socket.join(roomId + ':spectators');

      io.to(roomId).emit('playerReconnected', {
        oldId, newPlayer: room.players[playerIndex],
        players: room.players.filter(p => !room.disconnectedPlayers[p.id]),
        scores: room.scores,
        hostId: getHostId(room),
        isCurrentDrawer: playerIndex === room.currentDrawerIndex
      });

      socket.emit('reconnectSuccess', {
        player: room.players[playerIndex],
        gameState: room.gameState,
        players: room.players.filter(p => !room.disconnectedPlayers[p.id]),
        settings: room.gameSettings,
        scores: room.scores,
        isHost: getHostId(room) === socket.id,
        currentRound: room.currentRound,
        totalRounds: room.gameSettings.totalRounds,
        currentDrawer: room.players[room.currentDrawerIndex]?.name || '',
        currentDrawerId: room.players[room.currentDrawerIndex]?.id || '',
        currentWord: (room.gameState === 'playing' && room.players[room.currentDrawerIndex]?.id === socket.id) ? room.currentWord : '',
        currentTimer: room.timer,
        isDrawer: room.players[room.currentDrawerIndex]?.id === socket.id,
        isSpectator: player.isSpectator,
        hasCustomWords: room.customWords.length > 0,
        wordCount: room.customWords.length,
        canvasHistory: room.canvasHistory,
        roomId: room.id,
        reconnectToken: newReconnectToken
      });
    }
  });

  socket.on('getRoomList', () => socket.emit('roomList', getRoomListForBroadcast()));

  socket.on('createRoom', (roomName, playerName) => {
    const oldRoom = getRoomBySocket(socket);
    if (oldRoom) removePlayerFromRoom(socket, oldRoom, false);

    const roomId = createRoom(roomName);
    const reconnectToken = createReconnectToken();
    const player = { id: socket.id, name: playerName, isSpectator: false, joinedAt: Date.now() };
    rooms[roomId].players.push(player);
    rooms[roomId].hostId = socket.id;
    rooms[roomId].scores[socket.id] = 0;
    rooms[roomId].playerTokens[socket.id] = reconnectToken;
    socket.join(roomId);

    socket.emit('roomJoined', {
      roomId, roomName: rooms[roomId].name, isHost: true, player,
      players: rooms[roomId].players, hostId: socket.id, hasCustomWords: false, wordCount: 0,
      reconnectToken
    });
    socket.emit('reconnectInfo', { roomId, playerId: socket.id, reconnectToken });
    logPlayerActivity('login', {
      username: player.name,
      roomId,
      roomName: rooms[roomId].name,
      role: 'player',
      socketId: socket.id,
      reason: 'create_room'
    });
    io.emit('roomListUpdate', getRoomListForBroadcast());
  });

  socket.on('joinRoom', (roomId, playerName, asSpectator = false) => {
    const room = rooms[roomId];
    if (!room) {
      socket.emit('errorMessage', { type: 'notFound', message: '房间不存在' });
      return;
    }

    const existing = room.players.find(p => p.name === playerName);
    if (existing) {
      socket.emit('errorMessage', { type: 'nameConflict', message: `房间内已有玩家叫“${playerName}”，请修改昵称` });
      return;
    }

    const oldRoom = getRoomBySocket(socket);
    if (oldRoom && oldRoom.id !== roomId) {
      removePlayerFromRoom(socket, oldRoom, false);
    }

    if (room.gameState !== 'waiting' || asSpectator) asSpectator = true;
    else if (getActivePlayers(room).length >= MAX_PLAYERS) {
      asSpectator = true;
      socket.emit('chat', { name: '系统', message: '玩家已满，自动转为观战', isSystem: true });
    }
    if (asSpectator && room.players.filter(p => p.isSpectator && !room.disconnectedPlayers[p.id]).length >= MAX_SPECTATORS) {
      socket.emit('errorMessage', { type: 'full', message: '观众人数已满' });
      return;
    }

    const player = { id: socket.id, name: playerName, isSpectator: asSpectator, joinedAt: Date.now() };
    const reconnectToken = createReconnectToken();
    room.players.push(player);
    if (!asSpectator) room.scores[socket.id] = 0;
    else room.spectatorCorrectCounts[socket.id] = 0;
    room.playerTokens[socket.id] = reconnectToken;

    socket.join(roomId);
    if (asSpectator) socket.join(roomId + ':spectators');
    getHostId(room);

    const eventName = asSpectator ? 'spectatorJoined' : 'playerJoined';
    io.to(roomId).emit(eventName, {
      player,
      players: room.players.filter(p => !room.disconnectedPlayers[p.id]),
      hostId: getHostId(room),
      ...(asSpectator ? {} : { hasCustomWords: room.customWords.length > 0, wordCount: room.customWords.length })
    });

    socket.emit('stateUpdate', {
      gameState: room.gameState,
      players: room.players.filter(p => !room.disconnectedPlayers[p.id]),
      settings: room.gameSettings,
      isHost: getHostId(room) === socket.id,
      hasCustomWords: room.customWords.length > 0,
      wordCount: room.customWords.length,
      roomId: room.id,
      reconnectToken
    });
    socket.emit('reconnectInfo', { roomId: room.id, playerId: socket.id, reconnectToken });
    logPlayerActivity('login', {
      username: player.name,
      roomId: room.id,
      roomName: room.name,
      role: asSpectator ? 'spectator' : 'player',
      socketId: socket.id,
      reason: asSpectator ? 'join_as_spectator' : 'join_room'
    });

    if (room.gameState === 'playing' || room.gameState === 'selectingWord') {
      socket.emit('gameInProgress', {
        gameState: room.gameState,
        currentRound: room.currentRound,
        totalRounds: room.gameSettings.totalRounds,
        currentDrawer: room.players[room.currentDrawerIndex]?.name || '',
        currentDrawerId: room.players[room.currentDrawerIndex]?.id || '',
        currentTimer: room.timer,
        canvasHistory: room.canvasHistory,
        isSpectator: asSpectator
      });
    }

    io.emit('roomListUpdate', getRoomListForBroadcast());
  });

  socket.on('leaveRoom', () => {
    const room = getRoomBySocket(socket);
    if (room) removePlayerFromRoom(socket, room, false);
  });

  socket.on('switchToSpectator', () => {
    const room = getRoomBySocket(socket);
    if (!room) return;
    const idx = room.players.findIndex(p => p.id === socket.id);
    if (idx === -1 || room.players[idx].isSpectator) return;

    const wasHost = (socket.id === room.hostId);
    room.players[idx].isSpectator = true;
    delete room.scores[socket.id];
    room.spectatorCorrectCounts[socket.id] = 0;
    socket.join(room.id + ':spectators');

    if (wasHost) {
      room.hostId = null;
      getHostId(room);
    }

    io.to(room.id).emit('playerSwitched', {
      player: room.players[idx],
      players: room.players.filter(p => !room.disconnectedPlayers[p.id]),
      isNowPlayer: false,
      hostId: getHostId(room)
    });
  });

  socket.on('switchToPlayer', () => {
    const room = getRoomBySocket(socket);
    if (!room) return;
    const idx = room.players.findIndex(p => p.id === socket.id);
    if (idx === -1 || !room.players[idx].isSpectator) return;

    room.players[idx].isSpectator = false;
    room.scores[socket.id] = 0;
    socket.leave(room.id + ':spectators');
    if (!room.hostId) room.hostId = socket.id;

    io.to(room.id).emit('playerSwitched', {
      player: room.players[idx],
      players: room.players.filter(p => !room.disconnectedPlayers[p.id]),
      isNowPlayer: true,
      hostId: getHostId(room)
    });
  });

  socket.on('updateSettings', (newSettings) => {
    const room = getRoomBySocket(socket);
    if (!room || getHostId(room) !== socket.id) return;
    room.gameSettings = {
      ...room.gameSettings,
      drawTime: Math.max(30, Math.min(360, Number(newSettings.drawTime) || room.gameSettings.drawTime)),
      selectWordTime: Math.max(10, Math.min(30, Number(newSettings.selectWordTime) || room.gameSettings.selectWordTime)),
      totalRounds: Math.max(1, Math.min(10, Number(newSettings.totalRounds) || room.gameSettings.totalRounds))
    };
    io.to(room.id).emit('settingsUpdated', room.gameSettings);
  });

  socket.on('updateWordList', (words) => {
    const room = getRoomBySocket(socket);
    if (!room || getHostId(room) !== socket.id) return;
    room.customWords = words;
    socket.emit('wordListUpdated', { count: room.customWords.length, isUsingCustom: true });
  });

  socket.on('startGame', () => {
    const room = getRoomBySocket(socket);
    if (room && getHostId(room) === socket.id) startGame(room, socket);
  });

  socket.on('restartGame', () => {
    const room = getRoomBySocket(socket);
    if (room && getHostId(room) === socket.id) restartGame(room);
  });

  socket.on('forceEndGame', () => {
    const room = getRoomBySocket(socket);
    if (!room || getHostId(room) !== socket.id) return;
    if (room.gameState === 'playing' || room.gameState === 'selectingWord') {
      io.to(room.id).emit('chat', { name: '系统', message: '⚠️ 房主强制结束了游戏', isSystem: true });
      endGame(room);
    }
  });

  socket.on('selectWord', (word) => {
    const room = getRoomBySocket(socket);
    if (!room || room.gameState !== 'selectingWord') return;
    if (room.players[room.currentDrawerIndex]?.id === socket.id) {
      selectWord(room, word);
    }
  });

  // 笔画开始标记
  socket.on('startStroke', () => {
    const room = getRoomBySocket(socket);
    if (!room) return;
    room.canvasHistory.push({ type: 'startStroke' });
    socket.broadcast.to(room.id).emit('startStroke');
  });

  // 笔画结束标记
  socket.on('endStroke', () => {
    const room = getRoomBySocket(socket);
    if (!room) return;
    room.canvasHistory.push({ type: 'endStroke' });
    socket.broadcast.to(room.id).emit('endStroke');
  });

  socket.on('draw', (data) => {
    const room = getRoomBySocket(socket);
    if (!room) return;
    room.canvasHistory.push({ type: 'draw', data });
    socket.broadcast.to(room.id).emit('draw', data);
  });

  socket.on('fill', (data) => {
    const room = getRoomBySocket(socket);
    if (!room) return;
    room.canvasHistory.push({ type: 'fill', data });
    socket.broadcast.to(room.id).emit('fill', data);
  });

  socket.on('undo', () => {
    const room = getRoomBySocket(socket);
    if (!room || room.players[room.currentDrawerIndex]?.id !== socket.id) return;
    if (room.canvasHistory.length === 0) return;

    // 撤回一整笔：若末尾是 fill 直接移除；若是 endStroke 则一直移除直到 startStroke（含）
    const history = room.canvasHistory;
    let popped = false;
    const last = history[history.length - 1];

    if (last.type === 'fill') {
      history.pop();
      popped = true;
    } else if (last.type === 'endStroke') {
      history.pop(); // 移除 endStroke
      while (history.length > 0 && history[history.length - 1].type !== 'startStroke') {
        history.pop();
      }
      if (history.length > 0 && history[history.length - 1].type === 'startStroke') {
        history.pop();
        popped = true;
      }
    }

    if (popped) {
      io.to(room.id).emit('canvasHistoryUpdated', room.canvasHistory);
    }
  });

  socket.on('clearCanvas', () => {
    const room = getRoomBySocket(socket);
    if (!room) return;
    room.canvasHistory = [];
    socket.broadcast.to(room.id).emit('clearCanvas');
  });

  socket.on('chat', (message) => {
    const room = getRoomBySocket(socket);
    if (!room) return;
    const player = room.players.find(p => p.id === socket.id);
    if (!player) return;

    // 画手也可以发言（直接广播，不检查答案）
    if (room.gameState === 'playing' && room.players[room.currentDrawerIndex]?.id === socket.id) {
      io.to(room.id).emit('chat', { name: player.name, message });
      return;
    }

    // 检查是否猜中词语（忽略空格、大小写，支持别名）
    if (room.gameState === 'playing' && isWordMatched(message, room.currentWord)) {
      if (!player.isSpectator) {
        if (!room.guessedPlayers.includes(socket.id)) {
          room.guessedPlayers.push(socket.id);
          const activeCount = getActivePlayers(room).length;
          const guessOrder = room.guessedPlayers.length - 1;
          const guessScore = activeCount - 1 - guessOrder;
          room.scores[socket.id] += guessScore;

          io.to(room.id).emit('chat', {
            name: '系统', message: `🎉 ${player.name} 猜对了！获得 ${guessScore} 分`, isSystem: true
          });
          io.to(room.id).emit('scoreUpdate', room.scores);

          if (room.guessedPlayers.length >= activeCount - 1) {
            endTurn(room);
          }
        }
      } else {
        if (!room.currentSpectatorGuessed.has(socket.id)) {
          room.currentSpectatorGuessed.add(socket.id);
          room.spectatorCorrectCounts[socket.id] = (room.spectatorCorrectCounts[socket.id] || 0) + 1;

          io.to(room.id + ':spectators').emit('chat', {
            name: '系统', message: `👀 ${player.name} 猜对了`, isSystem: true
          });
          io.to(room.id + ':spectators').emit('spectatorCorrectUpdate', {
            id: socket.id, name: player.name, correctCount: room.spectatorCorrectCounts[socket.id]
          });
        }
      }
      return;
    }

    // 普通聊天消息
    if (player.isSpectator) {
      io.to(room.id + ':spectators').emit('chat', { name: player.name, message, spectatorChat: true });
    } else {
      io.to(room.id).emit('chat', { name: player.name, message });
    }
  });

  socket.on('disconnect', () => {
    const room = getRoomBySocket(socket);
    if (room) removePlayerFromRoom(socket, room, true);
  });
});

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '127.0.0.1';
const displayHost = HOST === '127.0.0.1' ? 'localhost' : HOST;
server.listen(PORT, HOST, () => console.log(`服务器运行在 ${protocol}://${displayHost}:${PORT}`));
