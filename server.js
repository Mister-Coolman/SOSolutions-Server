require('dotenv').config();

const express = require('express');
const axios = require('axios');
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');
const twilio = require('twilio');
const { DeepgramClient } = require('@deepgram/sdk');

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json({ limit: '20mb' }));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const TWILIO_API_KEY = process.env.TWILIO_API_KEY;
const TWILIO_API_SECRET = process.env.TWILIO_API_SECRET;
const TWIML_APP_SID = process.env.TWIML_APP_SID;
const TWILIO_PHONE_NUMBER = process.env.TWILIO_NUMBER;
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || process.env.NGROK_URL || '').replace(/\/$/, '');
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY;
const PORT = process.env.PORT || 3000;

if (!ACCOUNT_SID || !AUTH_TOKEN || !TWILIO_PHONE_NUMBER || !PUBLIC_BASE_URL) {
  console.error('Missing required env vars. Need at least TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER, PUBLIC_BASE_URL/NGROK_URL');
  process.exit(1);
}

const client = twilio(ACCOUNT_SID, AUTH_TOKEN);
const deepgram = new DeepgramClient(DEEPGRAM_API_KEY);

// sessionId -> session state
const sessions = Object.create(null);
/*
sessions[sessionId] = {
  sessionId,
  roomName,
  conferenceSid,
  appCallSid,
  calleeCallSid,
  appParticipantLabel,
  calleeParticipantLabel,
  appWs,
  streams: {
    [callSid]: { ws, streamSid, role, deepgramLive }
  },
  lastActivityAt
}
*/

const ttsDir = path.join(__dirname, 'tmp-tts');
fs.mkdirSync(ttsDir, { recursive: true });

function nowIso() {
  return new Date().toISOString();
}

function escapeXml(text) {
  return String(text || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function getOrCreateSession(sessionId) {
  if (!sessionId) throw new Error('sessionId required');
  if (!sessions[sessionId]) {
    sessions[sessionId] = {
      sessionId,
      roomName: `room_${sessionId}`,
      conferenceSid: null,
      appCallSid: null,
      calleeCallSid: null,
      appParticipantLabel: `app-${sessionId}`,
      calleeParticipantLabel: `callee-${sessionId}`,
      appWs: null,
      streams: {},
      lastActivityAt: Date.now(),
      callerNumber: null,
      callerLocation: null,
      introPlayed: false,
      ttsQueue: [],
      announcementPlaying: false,
    };
  }
  sessions[sessionId].lastActivityAt = Date.now();
  return sessions[sessionId];
}

function findSessionByCallSid(callSid) {
  if (!callSid) return null;
  for (const session of Object.values(sessions)) {
    if (session.appCallSid === callSid || session.calleeCallSid === callSid || session.streams[callSid]) {
      return session;
    }
  }
  return null;
}

function safeSend(ws, payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try {
      ws.send(JSON.stringify(payload));
    } catch (err) {
      console.error('WS send error:', err.message);
    }
  }
}

function notifyApp(session, payload) {
  if (!session?.appWs) return;
  safeSend(session.appWs, payload);
}

function setupHeartbeat(ws) {
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });
}

const HEARTBEAT_INTERVAL_MS = 20000;
const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      try { ws.terminate(); } catch {}
      return;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  });
}, HEARTBEAT_INTERVAL_MS);

server.on('close', () => {
  clearInterval(heartbeatInterval);
});

async function createDeepgramConnection({ sessionId, role, callSid }) {
  const dg = await deepgram.listen.v1.createConnection({
    model: 'nova-2-phonecall',
    language: 'en-US',
    encoding: 'mulaw',
    sample_rate: 8000,
    channels: 1,
    smart_format: true,
    interim_results: true,
    endpointing: 1500,
    utterances: true,
  });

  dg.on('message', (data) => {
    try {
      if (data.type !== 'Results') return;
      const alt = data.channel?.alternatives?.[0];
      const transcript = alt?.transcript;
      if (!transcript) return;

      const session = sessions[sessionId];
      if (!session) return;

      notifyApp(session, {
        type: 'transcription',
        role,
        callSid,
        isFinal: !!data.is_final,
        transcript,
        confidence: alt.confidence ?? 0,
        ts: nowIso(),
      });
    } catch (err) {
      console.error('Deepgram message handling error:', err.message);
    }
  });

  dg.on('error', (err) => {
    console.error(`Deepgram error [${sessionId}/${role}/${callSid}]`, err);
  });

  dg.on('close', () => {
    console.log(`Deepgram closed [${sessionId}/${role}/${callSid}]`);
  });

  dg.connect();
  await dg.waitForOpen();
  return dg;
}

function cleanupSessionIfEmpty(sessionId) {
  const session = sessions[sessionId];
  if (!session) return;

  const hasAppWs = !!session.appWs && session.appWs.readyState === WebSocket.OPEN;
  const hasAppCall = !!session.appCallSid;
  const hasCalleeCall = !!session.calleeCallSid;
  const hasStreams = Object.keys(session.streams || {}).length > 0;

  if (!hasAppWs && !hasAppCall && !hasCalleeCall && !hasStreams) {
    delete sessions[sessionId];
    console.log(`Deleted empty session ${sessionId}`);
  }
}

function scheduleTtsFileDelete(filePath, delayMs = 10 * 60 * 1000) {
  setTimeout(() => {
    fs.unlink(filePath, (err) => {
      if (err && err.code !== 'ENOENT') {
        console.error('Failed to delete temp TTS file:', err.message);
      }
    });
  }, delayMs);
}

// -------------------- WebSocket --------------------

wss.on('connection', (ws, req) => {
  setupHeartbeat(ws);

  const url = new URL(req.url, 'http://localhost');
  const pathname = url.pathname || '/';

  // App WebSocket: ws://host/app?sessionId=...
  if (pathname === '/app') {
    const sessionIdFromQuery = url.searchParams.get('sessionId') || null;
    if (sessionIdFromQuery) {
      const session = getOrCreateSession(sessionIdFromQuery);
      session.appWs = ws;
      notifyApp(session, { type: 'server', event: 'app-ws-connected', sessionId: session.sessionId });
    }

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return;
      }

      if (msg.type === 'register' && msg.sessionId) {
        const session = getOrCreateSession(msg.sessionId);
        session.appWs = ws;
        notifyApp(session, {
          type: 'server',
          event: 'registered',
          sessionId: session.sessionId,
          roomName: session.roomName,
          appCallSid: session.appCallSid,
          calleeCallSid: session.calleeCallSid,
          conferenceSid: session.conferenceSid,
        });
      }
    });

    ws.on('close', () => {
      for (const session of Object.values(sessions)) {
        if (session.appWs === ws) {
          session.appWs = null;
          cleanupSessionIfEmpty(session.sessionId);
          break;
        }
      }
    });

    ws.on('error', (err) => {
      console.error('App WS error:', err.message);
    });

    return;
  }

  // Twilio media stream: wss://host/media-stream
  if (pathname === '/media-stream') {
    let streamSid = null;
    let callSid = null;
    let sessionId = null;
    let role = null;
    let deepgramLive = null;
    let deepgramStarted = false;

    async function startDeepgram() {
      if (deepgramStarted) return;
      if (!sessionId || !role || !callSid) return;

      // Safety guard: only transcribe the remote callee leg
      if (role !== 'callee') return;

      deepgramStarted = true;
      deepgramLive = await createDeepgramConnection({ sessionId, role, callSid });
    }

    ws.on('message', async (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return;
      }

      switch (msg.event) {
        case 'start': {
            console.log('RAW TWILIO START EVENT:');
            console.log(JSON.stringify(msg, null, 2));
          
            streamSid = msg.start?.streamSid || null;
            callSid = msg.start?.callSid || null;
          
            const params = msg.start?.customParameters || {};
            sessionId = params.sessionId || null;
            role = params.role || null;
          
            if (!callSid) {
              console.error('Missing callSid in Twilio start event');
              return;
            }
          
            // Ignore stray streams that are not the callee transcription leg
            if (!sessionId || role !== 'callee') {
              console.log('Ignoring non-callee or untagged media stream', {
                callSid,
                streamSid,
                customParameters: params,
                tracks: msg.start?.tracks
              });
              return;
            }
          
            const session = getOrCreateSession(sessionId);
            session.streams[callSid] = { ws, streamSid, role, deepgramLive: null };
            session.calleeCallSid = callSid;
          
            notifyApp(session, {
              type: 'server',
              event: 'stream-started',
              sessionId,
              role,
              callSid,
              streamSid,
            });
          
            try {
              await startDeepgram();
              session.streams[callSid].deepgramLive = deepgramLive;
            } catch (err) {
              console.error('Deepgram startup error:', err.message);
            }
            break;
          }

        case 'media': {
          // Only media from the callee leg should ever be transcribed
          if (role !== 'callee') return;
          if (!deepgramLive || typeof deepgramLive.sendMedia !== 'function') return;

          try {
            const buffer = Buffer.from(msg.media.payload, 'base64');
            deepgramLive.sendMedia(buffer);
          } catch (err) {
            console.error('Deepgram sendMedia error:', err.message);
          }
          break;
        }

        case 'stop': {
          try { deepgramLive?.finish(); } catch {}
          if (sessionId && callSid && sessions[sessionId]?.streams?.[callSid]) {
            delete sessions[sessionId].streams[callSid];
          }
          break;
        }

        default:
          break;
      }
    });

    ws.on('close', () => {
      try { deepgramLive?.finish(); } catch {}
      if (sessionId && callSid && sessions[sessionId]?.streams?.[callSid]) {
        delete sessions[sessionId].streams[callSid];
        cleanupSessionIfEmpty(sessionId);
      }
    });

    ws.on('error', (err) => {
      console.error('Twilio media WS error:', err.message);
      try { deepgramLive?.finish(); } catch {}
    });

    return;
  }

  ws.close();
});

// -------------------- Twilio Voice SDK token --------------------

app.get('/token', (req, res) => {
  try {
    const identity = req.query.identity || `user_${crypto.randomUUID()}`;
    if (!TWILIO_API_KEY || !TWILIO_API_SECRET || !TWIML_APP_SID) {
      return res.status(500).json({ error: 'Missing TWILIO_API_KEY, TWILIO_API_SECRET, or TWIML_APP_SID' });
    }

    const AccessToken = twilio.jwt.AccessToken;
    const VoiceGrant = AccessToken.VoiceGrant;

    const token = new AccessToken(
      ACCOUNT_SID,
      TWILIO_API_KEY,
      TWILIO_API_SECRET,
      { identity, ttl: 3600 }
    );

    token.addGrant(new VoiceGrant({
      outgoingApplicationSid: TWIML_APP_SID,
      incomingAllow: true,
    }));

    res.json({
      identity,
      token: token.toJwt(),
    });
  } catch (err) {
    console.error('Token error:', err);
    res.status(500).json({ error: err.message });
  }
});

// -------------------- Session bootstrap --------------------

app.post('/start-session', async (req, res) => {
    const { sessionId, to } = req.body;
    if (!sessionId || !to) {
      return res.status(400).json({ error: 'sessionId and to are required' });
    }
  
    try {
      const session = getOrCreateSession(sessionId);
  
      const calleeJoinUrl =
        `${PUBLIC_BASE_URL}/twiml/callee-join?sessionId=${encodeURIComponent(sessionId)}`;
  
      console.log('Creating callee call with URL:', calleeJoinUrl);
  
      const calleeCall = await client.calls.create({
        to,
        from: TWILIO_PHONE_NUMBER,
        url: calleeJoinUrl,
        statusCallback: `${PUBLIC_BASE_URL}/call-status`,
        statusCallbackMethod: 'POST',
        statusCallbackEvent: ['initiated', 'ringing', 'answered', 'completed'],
      });
  
      session.calleeCallSid = calleeCall.sid;
      session.callerNumber = req.body.callerNumber || null;
      session.callerLocation = req.body.location || null;

      res.json({
        ok: true,
        sessionId,
        roomName: session.roomName,
        calleeCallSid: calleeCall.sid,
      });
    } catch (err) {
      console.error('start-session error:', err);
      res.status(500).json({ error: err.message });
    }
  });

// -------------------- TwiML endpoints --------------------

// App joins the conference as a live participant.
// No media stream here, so the app user's own voice is not transcribed.
app.post('/twiml/app-join', (req, res) => {
    console.log('APP JOIN HIT');
    console.log('app-join body:', req.body);
    console.log('app-join query:', req.query);
  
    const sessionId = req.body.sessionId || req.query.sessionId;
  
    if (!sessionId) {
      console.log('APP JOIN MISSING sessionId');
      return res.type('text/xml').send('<Response><Say>Missing session ID.</Say></Response>');
    }
  
    const session = getOrCreateSession(sessionId);
    const VoiceResponse = twilio.twiml.VoiceResponse;
    const twiml = new VoiceResponse();
  
    const dial = twiml.dial();
    dial.conference(
      {
        participantLabel: session.appParticipantLabel,
        startConferenceOnEnter: true,
        endConferenceOnExit: true,
        beep: false,
        waitUrl: '',
        statusCallback: `${PUBLIC_BASE_URL}/conference-events`,
        statusCallbackMethod: 'POST',
        statusCallbackEvent: 'start end join leave mute hold modify speaker announcement announcement-end',
      },
      session.roomName
    );
  
    const xml = twiml.toString();
    console.log('APP TWIML:\n', xml);
  
    res.type('text/xml').send(xml);
  });
// Callee joins the conference and ONLY this leg is streamed for transcription.
app.post('/twiml/callee-join', (req, res) => {
  const sessionId = req.body.sessionId || req.query.sessionId;
  if (!sessionId) {
    return res.type('text/xml').send('<Response><Say>Missing session ID.</Say></Response>');
  }

  const session = getOrCreateSession(sessionId);
  const VoiceResponse = twilio.twiml.VoiceResponse;
  const twiml = new VoiceResponse();

  const start = twiml.start();
  const stream = start.stream({
    url: `wss://${new URL(PUBLIC_BASE_URL).host}/media-stream`,
    name: `callee-${sessionId}`,
    track: 'inbound_track',
  });
  stream.parameter({ name: 'sessionId', value: sessionId });
  stream.parameter({ name: 'role', value: 'callee' });

  const dial = twiml.dial();
  dial.conference(
    {
      participantLabel: session.calleeParticipantLabel,
      startConferenceOnEnter: true,
      endConferenceOnExit: false,
      beep: false,
      statusCallback: `${PUBLIC_BASE_URL}/conference-events`,
      statusCallbackMethod: 'POST',
      statusCallbackEvent: 'join leave mute hold modify speaker announcement announcement-end',
    },
    session.roomName
  );
  const xml = twiml.toString();
  console.log('CALLEE TWIML:\n', xml);

  res.type('text/xml').send(twiml.toString());
});

// -------------------- TTS queue --------------------
/*
 * Each queue item: { announceUrl, targetCallSids, text, target }
 * Audio is generated eagerly (before enqueue) so there's no delay when dequeued.
 * drainTtsQueue fires the next item only when the previous announcement finishes
 * (Twilio sends the 'announcement' conference event on completion).
 */

async function drainTtsQueue(session) {
  if (session.announcementPlaying || session.ttsQueue.length === 0) return;
  if (!session.conferenceSid) return;

  const item = session.ttsQueue.shift();
  session.announcementPlaying = true;

  notifyApp(session, {
    type: 'speaking',
    state: 'start',
    sessionId: session.sessionId,
    target: item.target || 'callee',
    text: item.text || '',
    ts: nowIso(),
  });

  // Fallback: if the 'announcement' conference event never arrives (Twilio doesn't
  // guarantee it in all configurations), unblock the queue after a generous timeout.
  // Estimate: ~60 words/min ElevenLabs speech + 1.5s pause + 5s buffer = ~90s max.
  // Use item text length to scale: ~150ms per word, minimum 20s.
  const wordCount = (item.text || '').split(/\s+/).length;
  const fallbackMs = Math.max(45000, wordCount * 150 + 7000);
  const fallbackTimer = setTimeout(() => {
    if (session.announcementPlaying) {
      console.warn(`[TTS] Session ${session.sessionId}: 'announcement' event never arrived after ${fallbackMs}ms — unblocking queue`);
      session.announcementPlaying = false;
      if (session.ttsQueue.length > 0) {
        drainTtsQueue(session).catch(e => console.error('drainTtsQueue fallback retry:', e.message));
      } else {
        notifyApp(session, { type: 'speaking', state: 'stop', sessionId: session.sessionId, ts: nowIso() });
      }
    }
  }, fallbackMs);

  try {
    for (const callSid of item.targetCallSids) {
      await client
        .conferences(session.conferenceSid)
        .participants(callSid)
        .update({ announceUrl: item.announceUrl, announceMethod: 'GET' });
    }
    console.log(`[TTS] Playing for session ${session.sessionId} (fallback in ${fallbackMs}ms): "${(item.text || '').slice(0, 60)}"`);
  } catch (err) {
    clearTimeout(fallbackTimer);
    console.error('drainTtsQueue announce error:', err.message);
    session.announcementPlaying = false;
    // Skip failed item and try the next one
    drainTtsQueue(session).catch(e => console.error('drainTtsQueue retry error:', e.message));
  }
}

async function enqueueTts(session, item) {
  session.ttsQueue.push(item);
  console.log(`TTS enqueued for session ${session.sessionId} (queue depth: ${session.ttsQueue.length}, playing: ${session.announcementPlaying})`);
  await drainTtsQueue(session);
}

// -------------------- Intro message --------------------

async function playIntroMessage(session) {
  if (!session.conferenceSid || !session.calleeCallSid) return;

  const parts = [
    'This call is from a hearing-impaired person using the SOSolutions app.',
  ];
  if (session.callerNumber) {
    parts.push(`Their callback number is ${session.callerNumber}.`);
  }
  if (session.callerLocation) {
    parts.push(`They are located at ${session.callerLocation}.`);
  }
  parts.push('Please do not hang up.');

  const text = parts.join(' ');

  const elevenRes = await fetch(
    'https://api.elevenlabs.io/v1/text-to-speech/JBFqnCBsd6RMkjVDRZzb',
    {
      method: 'POST',
      headers: {
        'xi-api-key': ELEVENLABS_API_KEY,
        'Content-Type': 'application/json',
        Accept: 'audio/mpeg',
      },
      body: JSON.stringify({ text, model_id: 'eleven_multilingual_v2' }),
    }
  );

  if (!elevenRes.ok) {
    throw new Error(`ElevenLabs error ${elevenRes.status}: ${await elevenRes.text()}`);
  }

  const audioBuffer = Buffer.from(await elevenRes.arrayBuffer());
  if (!audioBuffer.length) throw new Error('ElevenLabs returned empty audio');

  const fileName = `${crypto.randomUUID()}.mp3`;
  const filePath = path.join(ttsDir, fileName);
  fs.writeFileSync(filePath, audioBuffer);
  scheduleTtsFileDelete(filePath);

  const announceUrl = `${PUBLIC_BASE_URL}/tts-twiml/${encodeURIComponent(fileName)}`;

  await enqueueTts(session, {
    announceUrl,
    targetCallSids: [session.calleeCallSid],
    text,
    target: 'callee',
  });

  console.log(`Intro message enqueued for session ${session.sessionId}`);
}

// -------------------- Status callbacks --------------------

app.post('/conference-events', (req, res) => {
  const {
    ConferenceSid,
    FriendlyName,
    CallSid,
    StatusCallbackEvent,
    ParticipantLabel,
  } = req.body;

  const roomName = FriendlyName || '';
  const sessionId = roomName.startsWith('room_') ? roomName.slice(5) : null;

  if (sessionId) {
    const session = getOrCreateSession(sessionId);
    session.conferenceSid = ConferenceSid || session.conferenceSid;

    if (ParticipantLabel === session.appParticipantLabel) {
      session.appCallSid = CallSid || session.appCallSid;
    }
    if (ParticipantLabel === session.calleeParticipantLabel) {
      session.calleeCallSid = CallSid || session.calleeCallSid;
    }

    if (
      StatusCallbackEvent === 'participant-join' &&
      ParticipantLabel === session.calleeParticipantLabel &&
      !session.introPlayed
    ) {
      session.introPlayed = true;
      playIntroMessage(session).catch(err =>
        console.error('playIntroMessage error:', err.message)
      );
    }

    // Twilio fires 'announcement-end' when audio finishes playing — advance queue or clear indicator
    if (StatusCallbackEvent === 'announcement-end') {
      console.log(`[TTS] 'announcement-end' event received for session ${sessionId} — unblocking queue (depth: ${session.ttsQueue.length})`);
      session.announcementPlaying = false;
      if (session.ttsQueue.length > 0) {
        drainTtsQueue(session).catch(err =>
          console.error('drainTtsQueue (announcement) error:', err.message)
        );
      } else {
        notifyApp(session, {
          type: 'speaking',
          state: 'stop',
          sessionId,
          ts: nowIso(),
        });
      }
    }

    notifyApp(session, {
      type: 'conference',
      sessionId,
      conferenceSid: session.conferenceSid,
      callSid: CallSid,
      participantLabel: ParticipantLabel,
      event: StatusCallbackEvent,
      ts: nowIso(),
    });
  }

  console.log(`[conf-event] ${StatusCallbackEvent} | room=${FriendlyName} | label=${ParticipantLabel} | callSid=${CallSid}`);
  res.sendStatus(200);
});

app.post('/call-status', (req, res) => {
  const { CallSid, CallStatus } = req.body;
  const session = findSessionByCallSid(CallSid);

  if (session) {
    notifyApp(session, {
      type: 'call-status',
      sessionId: session.sessionId,
      callSid: CallSid,
      status: CallStatus,
      ts: nowIso(),
    });

    if (CallSid === session.appCallSid && CallStatus === 'completed') {
      session.appCallSid = null;
    }
    if (CallSid === session.calleeCallSid && CallStatus === 'completed') {
      session.calleeCallSid = null;
    }

    cleanupSessionIfEmpty(session.sessionId);
  }

  res.sendStatus(200);
});

// -------------------- Conference participant controls --------------------

app.post('/mute', async (req, res) => {
  const { sessionId, target = 'app', muted = true } = req.body;
  const session = sessions[sessionId];
  if (!session?.conferenceSid) {
    return res.status(409).json({ error: 'Conference not ready' });
  }

  const targetCallSid =
    target === 'callee' ? session.calleeCallSid :
    target === 'app' ? session.appCallSid :
    null;

  if (!targetCallSid) {
    return res.status(404).json({ error: `No ${target} participant found` });
  }

  try {
    await client
      .conferences(session.conferenceSid)
      .participants(targetCallSid)
      .update({ muted: !!muted });

    res.json({ ok: true, sessionId, target, muted: !!muted });
  } catch (err) {
    console.error('mute error:', err);
    res.status(500).json({ error: err.message });
  }
});

// -------------------- ElevenLabs TTS via conference announcement --------------------

app.post('/speak', async (req, res) => {
  const { sessionId, text, target = 'callee' } = req.body;

  if (!sessionId || !text) {
    return res.status(400).json({ error: 'sessionId and text are required' });
  }

  const session = sessions[sessionId];
  if (!session?.conferenceSid) {
    return res.status(409).json({ error: 'Conference not ready' });
  }

  try {
    const elevenRes = await fetch(
      'https://api.elevenlabs.io/v1/text-to-speech/JBFqnCBsd6RMkjVDRZzb',
      {
        method: 'POST',
        headers: {
          'xi-api-key': ELEVENLABS_API_KEY,
          'Content-Type': 'application/json',
          'Accept': 'audio/mpeg',
        },
        body: JSON.stringify({
          text,
          model_id: 'eleven_multilingual_v2',
        }),
      }
    );

    if (!elevenRes.ok) {
      const errText = await elevenRes.text();
      throw new Error(`ElevenLabs error ${elevenRes.status}: ${errText}`);
    }

    const audioBuffer = Buffer.from(await elevenRes.arrayBuffer());
    if (!audioBuffer.length) {
      throw new Error('ElevenLabs returned empty audio');
    }

    const fileId = crypto.randomUUID();
    const fileName = `${fileId}.mp3`;
    const filePath = path.join(ttsDir, fileName);
    fs.writeFileSync(filePath, audioBuffer);
    scheduleTtsFileDelete(filePath);

    const announceUrl = `${PUBLIC_BASE_URL}/tts-twiml/${encodeURIComponent(fileName)}`;

    const targetCallSids = [];
    if (target === 'callee' || target === 'both') {
      if (session.calleeCallSid) targetCallSids.push(session.calleeCallSid);
    }
    if (target === 'app' || target === 'both') {
      if (session.appCallSid) targetCallSids.push(session.appCallSid);
    }

    if (!targetCallSids.length) {
      return res.status(404).json({ error: 'No target participant is currently connected' });
    }

    await enqueueTts(session, {
      announceUrl,
      targetCallSids,
      text,
      target,
    });

    res.json({
      ok: true,
      sessionId,
      target,
      targets: targetCallSids,
      announceUrl,
      queued: session.announcementPlaying,
    });
  } catch (err) {
    console.error('speak error:', err);
    notifyApp(session, {
      type: 'speaking',
      state: 'error',
      sessionId,
      target,
      error: err.message,
      ts: nowIso(),
    });
    res.status(500).json({ error: err.message });
  }
});

app.get('/tts/:file', (req, res) => {
  const fileName = path.basename(req.params.file);
  const filePath = path.join(ttsDir, fileName);

  if (!fs.existsSync(filePath)) {
    return res.sendStatus(404);
  }

  res.type('audio/mpeg');
  res.sendFile(filePath);
});

app.get('/tts-twiml/:file', (req, res) => {
  const fileName = path.basename(req.params.file);
  const url = `${PUBLIC_BASE_URL}/tts/${encodeURIComponent(fileName)}`;
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Pause length="1.5"/>
  <Play>${escapeXml(url)}</Play>
</Response>`;
  res.type('text/xml').send(xml);
});

// -------------------- Fireworks passthrough --------------------

app.post('/fireworks/chat', async (req, res) => {
  try {
    const response = await axios.post(
      'https://api.fireworks.ai/inference/v1/chat/completions',
      req.body,
      {
        headers: {
          Authorization: `Bearer ${process.env.FIREWORKS_API_KEY}`,
          'Content-Type': 'application/json',
        },
      }
    );

    res.json(response.data);
  } catch (err) {
    console.error(err.response?.data || err);
    res.status(500).json({ error: 'Fireworks request failed' });
  }
});

// -------------------- Health/debug --------------------

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    sessions: Object.keys(sessions).length,
    time: nowIso(),
  });
});

app.get('/sessions/:sessionId', (req, res) => {
  const session = sessions[req.params.sessionId];
  if (!session) return res.status(404).json({ error: 'Not found' });

  res.json({
    sessionId: session.sessionId,
    roomName: session.roomName,
    conferenceSid: session.conferenceSid,
    appCallSid: session.appCallSid,
    calleeCallSid: session.calleeCallSid,
    hasAppWs: !!session.appWs && session.appWs.readyState === WebSocket.OPEN,
    streams: Object.fromEntries(
      Object.entries(session.streams).map(([callSid, s]) => [
        callSid,
        { streamSid: s.streamSid, role: s.role }
      ])
    ),
    lastActivityAt: session.lastActivityAt,
  });
});

// -------------------- Startup --------------------

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Public base URL: ${PUBLIC_BASE_URL}`);
});