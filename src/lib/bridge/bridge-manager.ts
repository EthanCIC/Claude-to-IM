/**
 * Bridge Manager — singleton orchestrator for the multi-IM bridge system.
 *
 * Manages adapter lifecycles, routes inbound messages through the
 * conversation engine, and coordinates permission handling.
 *
 * Uses globalThis to survive Next.js HMR in development.
 */

import type { BridgeStatus, InboundMessage, OutboundMessage, StreamingPreviewState, UserRole, FileAttachment, ChannelAddress } from './types.js';
import type { InterruptedTask, ActiveTaskInfo } from './host.js';
import { createAdapter, getRegisteredTypes } from './channel-adapter.js';
import type { BaseChannelAdapter } from './channel-adapter.js';
// Side-effect import: triggers self-registration of all adapter factories
import './adapters/index.js';
import * as router from './channel-router.js';
import * as engine from './conversation-engine.js';
import * as broker from './permission-broker.js';
import { deliver, deliverRendered } from './delivery-layer.js';
import { markdownToTelegramChunks } from './markdown/telegram.js';
import { markdownToDiscordChunks } from './markdown/discord.js';
import { getBridgeContext } from './context.js';
import { escapeHtml } from './adapters/telegram-utils.js';
import {
  validateWorkingDirectory,
  validateSessionId,
  isDangerousInput,
  sanitizeInput,
  validateMode,
} from './security/validators.js';

const GLOBAL_KEY = '__bridge_manager__';

// ── Per-user OAuth auth card ─────────────────────────────────

import type { OAuthManager } from './auth/oauth-manager.js';
import type { BridgeStore } from './host.js';

/**
 * Send an OAuth authorization card to the user.
 * Group chat: button opens bot DM (OAuth flow happens there).
 * DM: button links directly to the OAuth authorize URL.
 */
async function sendAuthCard(
  adapter: BaseChannelAdapter,
  address: ChannelAddress,
  oauthManager: OAuthManager,
  store: BridgeStore,
): Promise<void> {
  const meta = store.getGroupMetadata?.(address.chatId);
  const isDM = meta?.chatType === 'p2p' || !address.isGroup;

  // Access the Lark REST client (adapter-specific)
  const restClient = (adapter as any).restClient;
  if (!restClient) return;

  if (isDM && address.userId) {
    // DM: generate OAuth URL directly
    const webhookUrl = store.getSetting('bridge_webhook_url') || process.env.CTI_WEBHOOK_URL || '';
    if (!webhookUrl) {
      await deliver(adapter, {
        address,
        text: 'OAuth not configured. Ask the admin to set CTI_WEBHOOK_URL.',
        parseMode: 'plain',
      });
      return;
    }

    const redirectUri = `${webhookUrl}/oauth/callback`;
    const { url } = oauthManager.generateAuthUrl(address.userId, redirectUri);

    const cardJson = JSON.stringify({
      config: { wide_screen_mode: true },
      header: {
        template: 'blue',
        title: { tag: 'plain_text', content: 'Authorize Claude Account' },
      },
      elements: [
        { tag: 'markdown', content: 'Click below to authorize your Claude account. After authorization, you can start using this bot.' },
        { tag: 'hr' },
        {
          tag: 'action',
          actions: [{
            tag: 'button',
            text: { tag: 'plain_text', content: 'Authorize' },
            type: 'primary',
            multi_url: { url, pc_url: url, android_url: url, ios_url: url },
          }],
        },
      ],
    });

    try {
      await restClient.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: { receive_id: address.chatId, msg_type: 'interactive', content: cardJson },
      });
    } catch (err) {
      console.warn('[bridge-manager] Failed to send auth card:', err instanceof Error ? err.message : err);
    }
  } else {
    // Group chat: direct user to DM the bot
    const botOpenId = (adapter as any).botOpenId || '';
    const deepLink = botOpenId
      ? `https://applink.larksuite.com/client/chat/open?openId=${botOpenId}`
      : '';

    const elements: any[] = [
      { tag: 'markdown', content: 'You need to authorize your Claude account before using this bot. Please send me a direct message to complete authorization.' },
    ];
    if (deepLink) {
      elements.push({
        tag: 'action',
        actions: [{
          tag: 'button',
          text: { tag: 'plain_text', content: 'Open DM' },
          type: 'primary',
          multi_url: { url: deepLink, pc_url: deepLink, android_url: deepLink, ios_url: deepLink },
        }],
      });
    }

    const cardJson = JSON.stringify({
      config: { wide_screen_mode: true },
      header: {
        template: 'blue',
        title: { tag: 'plain_text', content: 'Authorization Required' },
      },
      elements,
    });

    try {
      await restClient.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: { receive_id: address.chatId, msg_type: 'interactive', content: cardJson },
      });
    } catch (err) {
      console.warn('[bridge-manager] Failed to send auth card:', err instanceof Error ? err.message : err);
    }
  }
}

// ── Message envelope helpers ─────────────────────────────────

/** Strip brackets and collapse whitespace so user text can't break the header. */
function sanitizeHeaderPart(value: string): string {
  return value.replace(/[[\]]/g, (ch) => (ch === '[' ? '(' : ')'))
    .replace(/[\r\n]+/g, ' ')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

/** Format elapsed duration between two messages. Returns `undefined` when invalid/negative. */
function formatElapsed(durationMs: number): string | undefined {
  if (!Number.isFinite(durationMs) || durationMs < 0) return undefined;
  const sec = Math.floor(durationMs / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m`;
  const hr = Math.floor(min / 60);
  if (hr < 48) return `${hr}h`;
  const day = Math.floor(hr / 24);
  return `${day}d`;
}

const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'] as const;

/** Format a unix-ms timestamp as `Wed 2026-03-19 23:50 CST` in host-local time. */
function formatTimestamp(ts: number): string {
  const d = new Date(ts);
  const wday = WEEKDAYS[d.getDay()];
  const yyyy = d.getFullYear();
  const MM = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  // Extract short timezone name (e.g. "CST", "PST")
  const tz = new Intl.DateTimeFormat('en-US', { timeZoneName: 'short' })
    .formatToParts(d)
    .find(p => p.type === 'timeZoneName')?.value ?? '';
  return `${wday} ${yyyy}-${MM}-${dd} ${hh}:${mm} ${tz}`;
}

interface EnvelopeParams {
  channelType: string;
  isGroup: boolean;
  displayName: string | undefined;
  body: string;
  timestamp: number;
  previousTimestamp?: number;
}

/** Build `[Channel From +elapsed Weekday Timestamp] Body` envelope string. */
function formatEnvelope(params: EnvelopeParams): string {
  const channel = sanitizeHeaderPart(params.channelType.charAt(0).toUpperCase() + params.channelType.slice(1));
  const from = params.isGroup
    ? 'GROUP'
    : sanitizeHeaderPart(params.displayName || 'DM');
  const elapsed = (params.previousTimestamp != null)
    ? formatElapsed(params.timestamp - params.previousTimestamp)
    : undefined;
  const elapsedStr = elapsed ? ` +${elapsed}` : '';
  const ts = formatTimestamp(params.timestamp);
  const header = `[${channel} ${from}${elapsedStr} ${ts}]`;
  const body = params.isGroup && params.displayName
    ? `${sanitizeHeaderPart(params.displayName)}: ${params.body}`
    : params.body;
  return `${header} ${body}`;
}

// ── Observe mode (group chat context buffer) ──────────────────

/** Max observe messages to buffer per chat before oldest are dropped. */
const OBSERVE_BUFFER_MAX = 50;

/** Per-chat buffer of observe-only messages (group chats without @mention). */
interface ObserveEntry {
  timestamp: number;
  channelType: string;
  displayName: string | undefined;
  text: string;
}
const observeBuffers = new Map<string, ObserveEntry[]>();

/** Last message timestamp per chat for elapsed calculation. */
const lastMessageTimestamps = new Map<string, number>();

/**
 * Add an observe-only message to the per-chat buffer.
 * Stores raw fields so the caller can format envelopes when draining.
 */
function bufferObserveMessage(
  chatId: string,
  displayName: string | undefined,
  _userId: string | undefined,
  text: string,
  timestamp: number,
  channelType: string,
): void {
  let buf = observeBuffers.get(chatId);
  if (!buf) {
    buf = [];
    observeBuffers.set(chatId, buf);
  }
  buf.push({ timestamp, channelType, displayName, text });
  // Ring buffer: drop oldest when full
  if (buf.length > OBSERVE_BUFFER_MAX) {
    buf.splice(0, buf.length - OBSERVE_BUFFER_MAX);
  }
}

/**
 * Drain and return all buffered observe entries for a chat.
 * Returns null if buffer is empty.
 */
function drainObserveBuffer(chatId: string): ObserveEntry[] | null {
  const buf = observeBuffers.get(chatId);
  if (!buf || buf.length === 0) return null;
  const entries = [...buf];
  observeBuffers.delete(chatId);
  return entries;
}

function hasObserveBuffer(chatId: string): boolean {
  const buf = observeBuffers.get(chatId);
  return !!buf && buf.length > 0;
}


// ── User role resolution ───────────────────────────────────────

/**
 * Resolve the user's permission role from bridge settings.
 * Returns undefined when no role config exists (legacy mode — no restrictions).
 */
function resolveUserRole(channelType: string, userId?: string): UserRole | undefined {
  const { store } = getBridgeContext();
  const adminsRaw = store.getSetting('bridge_admins');
  if (!adminsRaw) return undefined; // no config → legacy
  if (!userId) return 'regular';
  const key = `${channelType}:${userId}`;
  const list = adminsRaw.split(',').map(s => s.trim()).filter(Boolean);
  if (list.includes(key) || list.includes(userId)) return 'admin';
  return 'regular';
}

// ── Streaming preview helpers ──────────────────────────────────

/** Generate a non-zero random 31-bit integer for use as draft_id. */
function generateDraftId(): number {
  return (Math.floor(Math.random() * 0x7FFFFFFE) + 1); // 1 .. 2^31-1
}

interface StreamConfig {
  intervalMs: number;
  minDeltaChars: number;
  maxChars: number;
}

/** Default stream config per channel type. */
const STREAM_DEFAULTS: Record<string, StreamConfig> = {
  telegram: { intervalMs: 700, minDeltaChars: 20, maxChars: 3900 },
  discord: { intervalMs: 1500, minDeltaChars: 40, maxChars: 1900 },
  feishu: { intervalMs: 500, minDeltaChars: 20, maxChars: 28000 },
  lark: { intervalMs: 500, minDeltaChars: 20, maxChars: 28000 },
};

function getStreamConfig(channelType = 'telegram'): StreamConfig {
  const { store } = getBridgeContext();
  const defaults = STREAM_DEFAULTS[channelType] || STREAM_DEFAULTS.telegram;
  const prefix = `bridge_${channelType}_stream_`;
  const intervalMs = parseInt(store.getSetting(`${prefix}interval_ms`) || '', 10) || defaults.intervalMs;
  const minDeltaChars = parseInt(store.getSetting(`${prefix}min_delta_chars`) || '', 10) || defaults.minDeltaChars;
  const maxChars = parseInt(store.getSetting(`${prefix}max_chars`) || '', 10) || defaults.maxChars;
  return { intervalMs, minDeltaChars, maxChars };
}

/** Fire-and-forget: send a preview draft. Only degrades on permanent failure.
 *  Stores the resulting Promise on state.lastFlushPromise so the final flush
 *  can be awaited before deciding whether to fall back to deliverResponse.
 *  Uses flushInFlight to prevent concurrent PATCHes from the same session.
 *  Uses generation guard to prevent stale callbacks from corrupting state (ETH-98). */
function flushPreview(
  adapter: BaseChannelAdapter,
  state: StreamingPreviewState,
  config: StreamConfig,
): void {
  if (state.degraded || !adapter.sendPreview) return;
  if (state.flushInFlight) return; // Another PATCH is in-flight — skip, trailing timer will retry

  const text = state.pendingText.length > config.maxChars
    ? state.pendingText.slice(0, config.maxChars) + '...'
    : state.pendingText;

  // Don't update lastSentText/lastSentAt here — wait for PATCH confirmation (ETH-91 bug 2)
  state.flushInFlight = true;
  const gen = state.generation; // Capture generation to detect segment resets (ETH-98)

  const promise = adapter.sendPreview(state.chatId, text, state.draftId).then(result => {
    // Only update state if we're still in the same segment (ETH-98).
    // If finalizePreviewSegment ran while this was in-flight, generation
    // will have incremented and we must not overwrite the new segment's state.
    const sameSegment = state.generation === gen;
    if (sameSegment) state.flushInFlight = false;
    if (result === 'degrade') { state.degraded = true; return false; }
    if (result === 'skip') return false;
    // PATCH confirmed — only update tracking state if still in same segment
    if (sameSegment) {
      state.lastSentText = text;
      state.lastSentAt = Date.now();
    }
    state.previewEverDelivered = true;
    return true; // 'sent'
  }).catch(() => {
    if (state.generation === gen) state.flushInFlight = false;
    return false;
  });

  state.lastFlushPromise = promise;
}

/**
 * Finalize the current streaming preview segment: flush pending text,
 * end the preview card, and reset state for the next segment.
 * Used when a tool_use or permission_request splits the response.
 *
 * ETH-98: When a flush is in-flight (e.g. the initial card CREATE hasn't
 * returned yet), we can't synchronously PATCH the remaining text.  Instead,
 * we capture the old segment's state, reset immediately so the next segment
 * starts cleanly, and chain a deferred PATCH + endPreview onto the in-flight
 * promise.  The generation guard in flushPreview prevents the old promise's
 * .then() from corrupting the new segment's state.
 */
function finalizePreviewSegment(
  adapter: BaseChannelAdapter,
  ps: StreamingPreviewState,
  cfg: StreamConfig,
  chatId: string,
): void {
  if (ps.degraded) return;
  if (ps.throttleTimer) {
    clearTimeout(ps.throttleTimer);
    ps.throttleTimer = null;
  }

  // Capture current segment's state before resetting
  const segPendingText = ps.pendingText;
  const segLastSentText = ps.lastSentText;
  const segDraftId = ps.draftId;
  const segLastSentAt = ps.lastSentAt;
  const hadInFlight = ps.flushInFlight;
  const prevPromise = ps.lastFlushPromise;
  const needsFlush = segPendingText && segPendingText !== segLastSentText;

  // Reset state immediately so the next segment starts cleanly (ETH-98)
  ps.textOffset += segPendingText.length;
  ps.draftId = generateDraftId();
  ps.lastSentText = '';
  ps.lastSentAt = 0;
  ps.pendingText = '';
  ps.flushInFlight = false; // New segment starts without in-flight
  ps.lastFlushPromise = null;
  ps.generation++;

  if (hadInFlight && prevPromise) {
    // In-flight flush (e.g. CREATE) — chain deferred PATCH + endPreview
    prevPromise.then(() => {
      if (needsFlush && adapter.sendPreview) {
        const patchText = segPendingText.length > cfg.maxChars
          ? segPendingText.slice(0, cfg.maxChars) + '...'
          : segPendingText;
        return adapter.sendPreview(chatId, patchText, segDraftId);
      }
      return undefined;
    }).then(() => {
      adapter.endPreview?.(chatId, segDraftId);
    }).catch(() => {
      // Best-effort — old card may be incomplete but new segment continues
    });
  } else {
    // No in-flight — synchronous path
    if (needsFlush) {
      // Direct sendPreview (not through flushPreview to avoid state confusion)
      if (adapter.sendPreview) {
        const patchText = segPendingText.length > cfg.maxChars
          ? segPendingText.slice(0, cfg.maxChars) + '...'
          : segPendingText;
        adapter.sendPreview(chatId, patchText, segDraftId)
          .then(() => adapter.endPreview?.(chatId, segDraftId))
          .catch(() => {});
      }
    } else if (segLastSentAt > 0) {
      adapter.endPreview?.(chatId, segDraftId);
    }
  }
}

// ── Channel-aware rendering dispatch ──────────────────────────

import type { SendResult } from './types.js';

/**
 * Render response text and deliver via the appropriate channel format.
 * Telegram: Markdown → HTML chunks via deliverRendered.
 * Other channels: plain text via deliver (no HTML).
 */
async function deliverResponse(
  adapter: BaseChannelAdapter,
  address: ChannelAddress,
  responseText: string,
  sessionId: string,
  replyToMessageId?: string,
): Promise<SendResult> {
  if (adapter.channelType === 'telegram') {
    const chunks = markdownToTelegramChunks(responseText, 4096);
    if (chunks.length > 0) {
      return deliverRendered(adapter, address, chunks, { sessionId, replyToMessageId });
    }
    return { ok: true };
  }
  if (adapter.channelType === 'discord') {
    // Discord: native markdown, chunk at 2000 chars with fence repair
    const chunks = markdownToDiscordChunks(responseText, 2000);
    for (let i = 0; i < chunks.length; i++) {
      const result = await deliver(adapter, {
        address,
        text: chunks[i].text,
        parseMode: 'Markdown',
        replyToMessageId,
      }, { sessionId });
      if (!result.ok) return result;
    }
    return { ok: true };
  }
  if (adapter.channelType === 'feishu' || adapter.channelType === 'lark') {
    // Feishu: pass markdown through for adapter to format as post/card
    return deliver(adapter, {
      address,
      text: responseText,
      parseMode: 'Markdown',
      replyToMessageId,
    }, { sessionId });
  }
  // Generic fallback: deliver as plain text (deliver() handles chunking internally)
  return deliver(adapter, {
    address,
    text: responseText,
    parseMode: 'plain',
    replyToMessageId,
  }, { sessionId });
}

interface AdapterMeta {
  lastMessageAt: string | null;
  lastError: string | null;
}

interface BridgeManagerState {
  adapters: Map<string, BaseChannelAdapter>;
  adapterMeta: Map<string, AdapterMeta>;
  running: boolean;
  startedAt: string | null;
  loopAborts: Map<string, AbortController>;
  activeTasks: Map<string, AbortController>;
  /** Per-session processing chains for concurrency control */
  sessionLocks: Map<string, Promise<void>>;
  autoStartChecked: boolean;
}

function getState(): BridgeManagerState {
  const g = globalThis as unknown as Record<string, BridgeManagerState>;
  if (!g[GLOBAL_KEY]) {
    g[GLOBAL_KEY] = {
      adapters: new Map(),
      adapterMeta: new Map(),
      running: false,
      startedAt: null,
      loopAborts: new Map(),
      activeTasks: new Map(),
      sessionLocks: new Map(),
      autoStartChecked: false,
    };
  }
  // Backfill sessionLocks for states created before this field existed
  if (!g[GLOBAL_KEY].sessionLocks) {
    g[GLOBAL_KEY].sessionLocks = new Map();
  }
  return g[GLOBAL_KEY];
}

/** Notify host about active task changes (for runtime file sync). */
function syncActiveTasksToHost(): void {
  const { lifecycle } = getBridgeContext();
  if (!lifecycle.onActiveTasksChanged) return;

  const state = getState();
  const allBindings = router.listBindings();
  const sessionToBinding = new Map<string, ReturnType<typeof router.listBindings>[number]>();
  for (const b of allBindings) {
    sessionToBinding.set(b.codepilotSessionId, b);
  }

  const tasks: ActiveTaskInfo[] = [];
  for (const [sessionId] of state.activeTasks) {
    const binding = sessionToBinding.get(sessionId);
    if (binding) {
      tasks.push({
        codepilotSessionId: binding.codepilotSessionId,
        chatId: binding.chatId,
        channelType: binding.channelType,
      });
    }
  }
  lifecycle.onActiveTasksChanged(tasks);
}

/**
 * Process a function with per-session serialization.
 * Different sessions run concurrently; same-session requests are serialized.
 */
function processWithSessionLock(sessionId: string, fn: () => Promise<void>): Promise<void> {
  const state = getState();
  const prev = state.sessionLocks.get(sessionId) || Promise.resolve();
  const current = prev.then(fn, fn);
  state.sessionLocks.set(sessionId, current);
  // Cleanup when the chain completes.
  // Suppress rejection on the cleanup chain — callers handle errors on `current` directly.
  current.finally(() => {
    if (state.sessionLocks.get(sessionId) === current) {
      state.sessionLocks.delete(sessionId);
    }
  }).catch(() => {});
  return current;
}

/**
 * Start the bridge system.
 * Checks feature flags, registers enabled adapters, starts polling loops.
 */
export async function start(): Promise<void> {
  const state = getState();
  if (state.running) return;

  const { store, lifecycle } = getBridgeContext();

  const bridgeEnabled = store.getSetting('remote_bridge_enabled') === 'true';
  if (!bridgeEnabled) {
    console.log('[bridge-manager] Bridge not enabled (remote_bridge_enabled != true)');
    return;
  }

  // Iterate all registered adapter types and create those that are enabled
  for (const channelType of getRegisteredTypes()) {
    const settingKey = `bridge_${channelType}_enabled`;
    if (store.getSetting(settingKey) !== 'true') continue;

    const adapter = createAdapter(channelType);
    if (!adapter) continue;

    const configError = adapter.validateConfig();
    if (!configError) {
      registerAdapter(adapter);
    } else {
      console.warn(`[bridge-manager] ${channelType} adapter not valid:`, configError);
    }
  }

  // Start all registered adapters, track how many succeeded
  let startedCount = 0;
  for (const [type, adapter] of state.adapters) {
    try {
      await adapter.start();
      console.log(`[bridge-manager] Started adapter: ${type}`);
      startedCount++;
    } catch (err) {
      console.error(`[bridge-manager] Failed to start adapter ${type}:`, err);
    }
  }

  // Only mark as running if at least one adapter started successfully
  if (startedCount === 0) {
    console.warn('[bridge-manager] No adapters started successfully, bridge not activated');
    state.adapters.clear();
    state.adapterMeta.clear();
    return;
  }

  // Mark running BEFORE starting consumer loops — runAdapterLoop checks
  // state.running in its while-condition, so it must be true first.
  state.running = true;
  state.startedAt = new Date().toISOString();

  // Notify host that bridge is starting (e.g., suppress competing polling)
  lifecycle.onBridgeStart?.();

  // Now start the consumer loops (state.running is already true)
  for (const [, adapter] of state.adapters) {
    if (adapter.isRunning()) {
      runAdapterLoop(adapter);
    }
  }

  console.log(`[bridge-manager] Bridge started with ${startedCount} adapter(s)`);
}

/**
 * Stop the bridge system gracefully.
 */
/**
 * Notify all chats with in-flight tasks that the bot is restarting (ETH-78).
 * Best-effort: failures are logged but do not block shutdown.
 * Must be called BEFORE stop() so adapters are still alive.
 */
export async function notifyShutdown(): Promise<void> {
  const state = getState();
  const { lifecycle } = getBridgeContext();
  if (state.activeTasks.size === 0) return;

  const allBindings = router.listBindings();

  // Reverse-map codepilotSessionId → binding
  const sessionToBinding = new Map<string, ReturnType<typeof router.listBindings>[number]>();
  for (const b of allBindings) {
    sessionToBinding.set(b.codepilotSessionId, b);
  }

  const notifications: Promise<void>[] = [];
  const interruptedTasks: InterruptedTask[] = [];

  for (const [sessionId] of state.activeTasks) {
    const binding = sessionToBinding.get(sessionId);
    if (!binding) continue;

    const adapter = state.adapters.get(binding.channelType);
    if (!adapter) continue;

    // Don't clear typing indicator — let it persist through restart.
    // Stale emoji is acceptable; next normal message cycle resets it.

    // Collect interrupted task info for recovery after restart
    interruptedTasks.push({
      codepilotSessionId: binding.codepilotSessionId,
      chatId: binding.chatId,
      channelType: binding.channelType,
      timestamp: Date.now(),
    });

    notifications.push(
      deliver(adapter, {
        address: { channelType: binding.channelType, chatId: binding.chatId },
        text: 'Bot 正在重啟，稍等。',
        parseMode: 'plain',
      }).then(() => {}, (err) => {
        console.warn(`[bridge-manager] Shutdown notification failed for ${binding.chatId}:`, err);
      }),
    );
  }

  if (notifications.length > 0) {
    console.log(`[bridge-manager] Sending shutdown notifications to ${notifications.length} active chat(s)...`);
    await Promise.race([
      Promise.allSettled(notifications),
      new Promise(resolve => setTimeout(resolve, 5000)),
    ]);
  }

  // Persist interrupted tasks for auto-recovery after restart
  if (interruptedTasks.length > 0) {
    lifecycle.onInterruptedTasks?.(interruptedTasks);
  }
}

/**
 * Recover tasks that were interrupted by a previous SIGTERM.
 * Resumes each session with a "continue" prompt so the LLM completes its response.
 * Runs sequentially to avoid resource contention.
 */
export async function recoverInterruptedTasks(tasks: InterruptedTask[]): Promise<void> {
  const state = getState();
  if (!state.running) return;

  for (const task of tasks) {
    const adapter = state.adapters.get(task.channelType);
    if (!adapter) {
      console.warn(`[bridge-manager] Recovery skip: no adapter for ${task.channelType}`);
      continue;
    }

    // Resolve binding — if binding is gone, skip
    const binding = router.resolve({
      channelType: task.channelType,
      chatId: task.chatId,
    });
    if (!binding?.sdkSessionId) {
      console.warn(`[bridge-manager] Recovery skip: no session for ${task.chatId}`);
      continue;
    }

    // Use session lock to serialize with any incoming messages
    await processWithSessionLock(binding.codepilotSessionId, async () => {
      const taskAbort = new AbortController();
      state.activeTasks.set(binding.codepilotSessionId, taskAbort);
      syncActiveTasksToHost();

      try {
        console.log(`[bridge-manager] Recovering task for ${task.chatId}...`);
        // Recovery permission handler: immediately deny AskUserQuestion and any
        // other tool that bypasses role checks and goes straight to forwardAndWait.
        // Without this, PendingPermissions' 5-minute timeout blocks recovery.
        const recoveryPermHandler = async (perm: { permissionRequestId: string }) => {
          const { permissions } = getBridgeContext();
          permissions.resolvePendingPermission(perm.permissionRequestId, {
            behavior: 'deny',
            message: 'Auto-denied during recovery (no interactive user)',
          });
        };
        // Use 'regular' role so non-whitelisted tools are silently denied
        // instead of falling into legacy-mode forwardAndWait (which hangs
        // for 5 minutes when no one resolves the permission request).
        // recoveryPermHandler is still needed for AskUserQuestion, which
        // always goes to forwardAndWait regardless of role.
        const result = await engine.processMessage(
          binding,
          'continue',
          recoveryPermHandler,
          taskAbort.signal,
          undefined,  // files
          undefined,  // onPartialText
          undefined,  // onToolUse
          'regular' as UserRole,
        );

        const isNoOp = !result.responseText || result.responseText.trim() === 'No response requested.';
        console.log(`[bridge-manager] Recovery response: ${result.responseText?.length ?? 0} chars, hasError=${result.hasError}, sdkSessionId=${result.sdkSessionId?.slice(0, 8) ?? 'none'}`);

        // Always deliver as new message — don't PATCH the interrupted card.
        // The interrupted card keeps its partial content; recovery appears below it.
        if (!isNoOp) {
          await deliverResponse(
            adapter,
            { channelType: task.channelType, chatId: task.chatId },
            result.responseText,
            binding.codepilotSessionId,
          );
        }

        // Update sdkSessionId
        if (binding.id) {
          const update = computeSdkSessionUpdate(result.sdkSessionId, result.hasError);
          if (update !== null) {
            getBridgeContext().store.updateChannelBinding(binding.id, { sdkSessionId: update });
          }
        }

        if (result.hasError) {
          console.warn(`[bridge-manager] Recovery error for ${task.chatId}: ${result.errorMessage}`);
          await deliver(adapter, {
            address: { channelType: task.channelType, chatId: task.chatId },
            text: '重啟後嘗試恢復上次的回覆失敗，請重新 @ 我。',
            parseMode: 'plain',
          }).catch(() => {});
        } else {
          console.log(`[bridge-manager] Recovery successful for ${task.chatId}`);
        }
      } catch (err) {
        console.error(`[bridge-manager] Recovery failed for ${task.chatId}:`, err);
      } finally {
        state.activeTasks.delete(binding.codepilotSessionId);
        syncActiveTasksToHost();
      }
    });
  }
}

export async function stop(): Promise<void> {
  const state = getState();
  if (!state.running) return;

  const { lifecycle } = getBridgeContext();

  state.running = false;

  // Abort all event loops
  for (const [, abort] of state.loopAborts) {
    abort.abort();
  }
  state.loopAborts.clear();

  // Stop all adapters
  for (const [type, adapter] of state.adapters) {
    try {
      await adapter.stop();
      console.log(`[bridge-manager] Stopped adapter: ${type}`);
    } catch (err) {
      console.error(`[bridge-manager] Error stopping adapter ${type}:`, err);
    }
  }

  state.adapters.clear();
  state.adapterMeta.clear();
  state.startedAt = null;

  // Clear all pending permission expiry timers
  broker.clearAllPermissionTimers();

  // Clear observe buffers and timestamp tracking (in-memory, no need to persist)
  observeBuffers.clear();
  lastMessageTimestamps.clear();

  // Notify host that bridge stopped
  lifecycle.onBridgeStop?.();

  console.log('[bridge-manager] Bridge stopped');
}

/**
 * Lazy auto-start: checks bridge_auto_start setting once and starts if enabled.
 * Called from POST /api/bridge with action 'auto-start' (triggered by Electron on startup).
 */
export function tryAutoStart(): void {
  const state = getState();
  if (state.autoStartChecked) return;
  state.autoStartChecked = true;

  if (state.running) return;

  const { store } = getBridgeContext();
  const autoStart = store.getSetting('bridge_auto_start');
  if (autoStart !== 'true') return;

  start().catch(err => {
    console.error('[bridge-manager] Auto-start failed:', err);
  });
}

/**
 * Get the current bridge status.
 */
export function getStatus(): BridgeStatus {
  const state = getState();
  return {
    running: state.running,
    startedAt: state.startedAt,
    adapters: Array.from(state.adapters.entries()).map(([type, adapter]) => {
      const meta = state.adapterMeta.get(type);
      return {
        channelType: adapter.channelType,
        running: adapter.isRunning(),
        connectedAt: state.startedAt,
        lastMessageAt: meta?.lastMessageAt ?? null,
        error: meta?.lastError ?? null,
      };
    }),
  };
}

/**
 * Register a channel adapter.
 */
export function registerAdapter(adapter: BaseChannelAdapter): void {
  const state = getState();
  state.adapters.set(adapter.channelType, adapter);
}

/**
 * Run the event loop for a single adapter.
 * Messages for different sessions are dispatched concurrently;
 * messages for the same session are serialized via session locks.
 */
function runAdapterLoop(adapter: BaseChannelAdapter): void {
  const state = getState();
  const abort = new AbortController();
  state.loopAborts.set(adapter.channelType, abort);

  (async () => {
    while (state.running && adapter.isRunning()) {
      try {
        const msg = await adapter.consumeOne();
        if (!msg) continue; // Adapter stopped

        // Callback queries, commands, observe-only, and pending text answers
        // are lightweight — process inline (bypass session lock).
        // Text answers for "Other" MUST bypass the lock because the lock
        // holder is waiting for this answer.
        if (msg.observeOnly || msg.callbackData || msg.text.trim().startsWith('/')
            || broker.hasPendingTextAnswer(msg.address.chatId)) {
          await handleMessage(adapter, msg);
        } else {
          const binding = router.resolve(msg.address);
          // Fire-and-forget into session lock — loop continues to accept
          // messages for other sessions immediately.
          processWithSessionLock(binding.codepilotSessionId, () =>
            handleMessage(adapter, msg),
          ).catch(err => {
            console.error(`[bridge-manager] Session ${binding.codepilotSessionId.slice(0, 8)} error:`, err);
          });
        }
      } catch (err) {
        if (abort.signal.aborted) break;
        const errMsg = err instanceof Error ? err.message : String(err);
        console.error(`[bridge-manager] Error in ${adapter.channelType} loop:`, err);
        // Track last error per adapter
        const meta = state.adapterMeta.get(adapter.channelType) || { lastMessageAt: null, lastError: null };
        meta.lastError = errMsg;
        state.adapterMeta.set(adapter.channelType, meta);
        // Brief delay to prevent tight error loops
        await new Promise(r => setTimeout(r, 1000));
      }
    }
  })().catch(err => {
    if (!abort.signal.aborted) {
      const errMsg = err instanceof Error ? err.message : String(err);
      console.error(`[bridge-manager] ${adapter.channelType} loop crashed:`, err);
      const meta = state.adapterMeta.get(adapter.channelType) || { lastMessageAt: null, lastError: null };
      meta.lastError = errMsg;
      state.adapterMeta.set(adapter.channelType, meta);
    }
  });
}

/**
 * Handle a single inbound message.
 */
async function handleMessage(
  adapter: BaseChannelAdapter,
  msg: InboundMessage,
): Promise<void> {
  const { store } = getBridgeContext();

  // Update lastMessageAt for this adapter
  const adapterState = getState();
  const meta = adapterState.adapterMeta.get(adapter.channelType) || { lastMessageAt: null, lastError: null };
  meta.lastMessageAt = new Date().toISOString();
  adapterState.adapterMeta.set(adapter.channelType, meta);

  // Acknowledge the update offset after processing completes (or fails).
  // This ensures the adapter only advances its committed offset once the
  // message has been fully handled, preventing message loss on crash.
  const ack = () => {
    if (msg.updateId != null && adapter.acknowledgeUpdate) {
      adapter.acknowledgeUpdate(msg.updateId);
    }
  };

  // Handle callback queries (permission buttons or question answers)
  if (msg.callbackData) {
    // Only admins can interact with permission/question callbacks
    if (msg.callbackData.startsWith('perm:') ||
        msg.callbackData.startsWith('askq:') ||
        msg.callbackData.startsWith('askq_other:')) {
      const clickerRole = resolveUserRole(adapter.channelType, msg.address.userId);
      if (clickerRole !== undefined && clickerRole !== 'admin') {
        await deliver(adapter, {
          address: msg.address,
          text: 'Only admins can respond to permission requests.',
          parseMode: 'plain',
        });
        ack();
        return;
      }
    }

    let handled = false;
    let resolveAction = '';
    if (msg.callbackData.startsWith('askq_other:')) {
      handled = broker.handleOtherCallback(msg.callbackData, msg.address.chatId);
    } else if (msg.callbackData.startsWith('askq:')) {
      handled = broker.handleQuestionCallback(msg.callbackData, msg.address.chatId, msg.callbackMessageId);
      if (handled) resolveAction = 'answered';
    } else {
      handled = broker.handlePermissionCallback(msg.callbackData, msg.address.chatId, msg.callbackMessageId);
      if (handled) {
        const parts = msg.callbackData.split(':');
        resolveAction = parts[1] || '';
      }
    }

    // Audit: permission decision
    if (handled) {
      const permId = msg.callbackData.startsWith('askq:')
        ? msg.callbackData.split(':')[1]
        : msg.callbackData.startsWith('askq_other:')
          ? msg.callbackData.split(':')[1]
          : msg.callbackData.split(':').slice(2).join(':');
      store.insertAuditLog({
        channelType: adapter.channelType,
        chatId: msg.address.chatId,
        direction: 'inbound',
        messageId: msg.messageId,
        summary: `[PERM] ${resolveAction || 'answered'} permId=${permId.slice(0, 12)} by=${msg.address.userId?.slice(0, 8) || 'unknown'}`,
      });
    }

    // Patch the original card so all group members see the resolved state
    if (handled && resolveAction && adapter.resolvePermissionCard && msg.callbackMessageId) {
      const permId = msg.callbackData.startsWith('askq:')
        ? msg.callbackData.split(':')[1]
        : msg.callbackData.split(':').slice(2).join(':');
      const link = getBridgeContext().store.getPermissionLink(permId);
      adapter.resolvePermissionCard(
        msg.address.chatId,
        msg.callbackMessageId,
        resolveAction,
        link?.toolName,
        link?.toolInput,
      ).catch((err) => {
        console.warn('[bridge-manager] Failed to patch permission card:', err instanceof Error ? err.message : err);
      });
    }

    if (handled && !adapter.resolvePermissionCard) {
      // Send text confirmation for adapters without card patching
      const confirmMsg: OutboundMessage = {
        address: msg.address,
        text: 'Permission response recorded.',
        parseMode: 'plain',
      };
      await deliver(adapter, confirmMsg);
    }
    ack();
    return;
  }

  // Allow control/info commands even without @mention (observe-only) — these
  // should work without waking the LLM.
  const rawText = msg.text.trim();
  if (msg.observeOnly) {
    const cmdText = rawText.replace(/^@\S+\s+/, '');
    if (cmdText === '/stop' || cmdText === '/cancel' || cmdText === '/help') {
      await handleCommand(adapter, msg, cmdText);
      ack();
      return;
    }
    bufferObserveMessage(msg.address.chatId, msg.address.displayName, msg.address.userId, rawText, msg.timestamp, adapter.channelType);
    lastMessageTimestamps.set(msg.address.chatId, msg.timestamp);
    ack();
    return;
  }
  const hasAttachments = msg.attachments && msg.attachments.length > 0;

  // Check if this chat has a pending "Other" text answer (admin-only when roles configured)
  if (rawText && broker.hasPendingTextAnswer(msg.address.chatId)) {
    const answererRole = resolveUserRole(adapter.channelType, msg.address.userId);
    if (answererRole === undefined || answererRole === 'admin') {
      const pendingPermId = broker.getPendingTextAnswerPermId(msg.address.chatId);
      const answered = broker.handleTextAnswer(msg.address.chatId, rawText);
      if (answered) {
        // Patch the original question card so all members see it's answered
        if (pendingPermId && adapter.resolvePermissionCard) {
          const link = getBridgeContext().store.getPermissionLink(pendingPermId);
          if (link?.messageId) {
            adapter.resolvePermissionCard(
              msg.address.chatId, link.messageId, 'answered', link.toolName, link.toolInput,
            ).catch((err) => {
              console.warn('[bridge-manager] Failed to patch question card:', err instanceof Error ? err.message : err);
            });
          }
        }
        await deliver(adapter, {
          address: msg.address,
          text: 'Answer recorded.',
          parseMode: 'plain',
          replyToMessageId: msg.messageId,
        });
        ack();
        return;
      }
    }
    // Regular user's message is not a permission answer — fall through to normal processing
  }

  // Handle image-only download failures — surface error to user instead of silently dropping
  if (!rawText && !hasAttachments) {
    if (!hasObserveBuffer(msg.address.chatId)) {
      const rawData = msg.raw as { imageDownloadFailed?: boolean; failedCount?: number } | undefined;
      if (rawData?.imageDownloadFailed) {
        await deliver(adapter, {
          address: msg.address,
          text: `Failed to download ${rawData.failedCount ?? 1} image(s). Please try sending again.`,
          parseMode: 'plain',
          replyToMessageId: msg.messageId,
        });
      }
      ack();
      return;
    }
  }

  // Check for IM commands (before sanitization — commands are validated individually)
  // In group chats, messages arrive as "@BotName /stop" — strip the leading @mention
  // so the slash command is recognized.
  const commandText = rawText.replace(/^@\S+\s+/, '');
  if (commandText.startsWith('/')) {
    await handleCommand(adapter, msg, commandText);
    ack();
    return;
  }

  // Sanitize general message text before routing to conversation engine
  const { text, truncated } = sanitizeInput(rawText);
  if (truncated) {
    console.warn(`[bridge-manager] Input truncated from ${rawText.length} to ${text.length} chars for chat ${msg.address.chatId}`);
    store.insertAuditLog({
      channelType: adapter.channelType,
      chatId: msg.address.chatId,
      direction: 'inbound',
      messageId: msg.messageId,
      summary: `[TRUNCATED] Input truncated from ${rawText.length} chars`,
    });
  }

  if (!text && !hasAttachments && !hasObserveBuffer(msg.address.chatId)) { ack(); return; }

  // Regular message — route to conversation engine
  const binding = router.resolve(msg.address);

  // Audit: task start
  store.insertAuditLog({
    channelType: adapter.channelType,
    chatId: msg.address.chatId,
    direction: 'inbound',
    messageId: msg.messageId,
    summary: `[TASK_START] session=${binding.codepilotSessionId.slice(0, 8)} sdk=${binding.sdkSessionId?.slice(0, 8) || 'none'}`,
  });

  // Notify adapter that message processing is starting (e.g., typing indicator)
  adapter.onMessageStart?.(msg.address.chatId, msg.messageId);

  // Create an AbortController so /stop can cancel this task externally
  const taskAbort = new AbortController();
  const state = getState();
  state.activeTasks.set(binding.codepilotSessionId, taskAbort);
  syncActiveTasksToHost();

  // ── Streaming preview setup ──────────────────────────────────
  let previewState: StreamingPreviewState | null = null;
  const caps = adapter.getPreviewCapabilities?.(msg.address.chatId) ?? null;
  if (caps?.supported) {
    previewState = {
      draftId: generateDraftId(),
      chatId: msg.address.chatId,
      lastSentText: '',
      lastSentAt: 0,
      degraded: false,
      flushInFlight: false,
      throttleTimer: null,
      pendingText: '',
      textOffset: 0,
      lastFlushPromise: null,
      previewEverDelivered: false,
      generation: 0,
    };
  }

  const streamCfg = previewState ? getStreamConfig(adapter.channelType) : null;

  // Build the onPartialText callback (or undefined if preview not supported)
  const onPartialText = (previewState && streamCfg) ? (fullText: string) => {
    const ps = previewState!;
    const cfg = streamCfg!;
    if (ps.degraded) return;

    // Slice from textOffset to get only the current segment's text
    const segmentText = fullText.slice(ps.textOffset);

    // Truncate to maxChars + ellipsis
    ps.pendingText = segmentText.length > cfg.maxChars
      ? segmentText.slice(0, cfg.maxChars) + '...'
      : segmentText;

    const delta = ps.pendingText.length - ps.lastSentText.length;
    const elapsed = Date.now() - ps.lastSentAt;

    if (delta < cfg.minDeltaChars && ps.lastSentAt > 0) {
      // Not enough new content — schedule trailing-edge timer if not already set
      if (!ps.throttleTimer) {
        ps.throttleTimer = setTimeout(() => {
          ps.throttleTimer = null;
          if (!ps.degraded) flushPreview(adapter, ps, cfg);
        }, cfg.intervalMs);
      }
      return;
    }

    if (elapsed < cfg.intervalMs && ps.lastSentAt > 0) {
      // Too soon — schedule trailing-edge timer to ensure latest text is sent
      if (!ps.throttleTimer) {
        ps.throttleTimer = setTimeout(() => {
          ps.throttleTimer = null;
          if (!ps.degraded) flushPreview(adapter, ps, cfg);
        }, cfg.intervalMs - elapsed);
      }
      return;
    }

    // Clear any pending trailing-edge timer and flush immediately
    if (ps.throttleTimer) {
      clearTimeout(ps.throttleTimer);
      ps.throttleTimer = null;
    }
    flushPreview(adapter, ps, cfg);

    // If flush was skipped (another PATCH in-flight), schedule trailing timer
    // so the pending text gets sent after the in-flight PATCH completes.
    if (ps.flushInFlight && !ps.throttleTimer) {
      ps.throttleTimer = setTimeout(() => {
        ps.throttleTimer = null;
        if (!ps.degraded) flushPreview(adapter, ps, cfg);
      }, cfg.intervalMs);
    }
  } : undefined;

  try {
    // Pass permission callback so requests are forwarded to IM immediately
    // during streaming (the stream blocks until permission is resolved).
    // Use text or empty string for image-only messages (prompt is still required by streamClaude)
    const rawPrompt = text || (hasAttachments ? 'Describe this image.' : '');

    // Build message envelope with timestamp, elapsed, and channel metadata
    const observeEntries = drainObserveBuffer(msg.address.chatId);
    const prevTs = lastMessageTimestamps.get(msg.address.chatId);

    const currentEnvelope = formatEnvelope({
      channelType: adapter.channelType,
      isGroup: msg.address.isGroup ?? false,
      displayName: msg.address.displayName,
      body: rawPrompt,
      timestamp: msg.timestamp,
      previousTimestamp: prevTs,
    });

    let promptText: string;
    if (observeEntries && observeEntries.length > 0) {
      const historyLines = observeEntries.map((entry, i) => {
        const entryPrevTs = i === 0 ? prevTs : observeEntries[i - 1].timestamp;
        return formatEnvelope({
          channelType: entry.channelType,
          isGroup: true,
          displayName: entry.displayName,
          body: entry.text,
          timestamp: entry.timestamp,
          previousTimestamp: entryPrevTs,
        });
      });
      promptText = `[Chat messages since your last reply - for context]\n${historyLines.join('\n')}\n[Current message - respond to this]\n${currentEnvelope}`;
    } else {
      promptText = currentEnvelope;
    }

    // Update timestamp for next message's elapsed calculation
    lastMessageTimestamps.set(msg.address.chatId, msg.timestamp);

    // Observe attachments are now disk-first (paths in text), only current message has inline attachments
    const allAttachments: FileAttachment[] = hasAttachments ? msg.attachments! : [];

    const userRole = resolveUserRole(adapter.channelType, msg.address.userId);

    // ── Per-user OAuth check ──
    const senderOpenId = msg.address.userId;
    let resolvedOAuthToken: string | undefined;
    const { oauth: oauthMgr } = getBridgeContext();

    if (oauthMgr && senderOpenId) {
      let storedToken = store.getAuthToken?.(senderOpenId) ?? null;

      if (storedToken && !oauthMgr.isTokenValid(storedToken)) {
        // Token expired — try refresh
        if (storedToken.refresh_token) {
          try {
            storedToken = await oauthMgr.refreshToken(storedToken);
            store.setAuthToken?.(senderOpenId, storedToken);
          } catch {
            storedToken = null;
          }
        } else {
          storedToken = null;
        }
      }

      if (!storedToken) {
        // No valid token — send auth card and skip processing
        await sendAuthCard(adapter, msg.address, oauthMgr, store);
        return;
      }

      // Proactive refresh if close to expiry (< 5 min)
      if (oauthMgr.needsRefresh(storedToken, 300) && storedToken.refresh_token) {
        try {
          const refreshed = await oauthMgr.refreshToken(storedToken);
          store.setAuthToken?.(senderOpenId, refreshed);
          resolvedOAuthToken = refreshed.access_token;
        } catch {
          resolvedOAuthToken = storedToken.access_token;
        }
      } else {
        resolvedOAuthToken = storedToken.access_token;
      }
    }

    const result = await engine.processMessage(binding, promptText, async (perm) => {
      // ── Finalize current preview segment before permission card ──
      if (previewState && streamCfg) {
        finalizePreviewSegment(adapter, previewState, streamCfg, msg.address.chatId);
      }

      // Forward permission request to IM
      await broker.forwardPermissionRequest(
        adapter,
        msg.address,
        perm.permissionRequestId,
        perm.toolName,
        perm.toolInput,
        binding.codepilotSessionId,
        perm.suggestions,
        msg.messageId,
      );
    }, taskAbort.signal, allAttachments.length > 0 ? allAttachments : undefined, onPartialText,
    // onToolUse: split preview at tool boundaries
    (previewState && streamCfg) ? (_toolName: string) => {
      if (!previewState || previewState.degraded) return;
      if (previewState.lastSentAt > 0 || previewState.pendingText.length > 0) {
        finalizePreviewSegment(adapter, previewState, streamCfg!, msg.address.chatId);
      }
    } : undefined, userRole, resolvedOAuthToken);

    // ── User-initiated abort: skip delivery & error notification ──
    // sdkSessionId preserved (not cleared), session stays alive.
    // /stop handler already replied "已停止當前任務。" so no extra notification needed.
    // `return` inside try triggers the finally block for preview cleanup.
    if (result.isAbort) {
      store.insertAuditLog({
        channelType: adapter.channelType,
        chatId: msg.address.chatId,
        direction: 'inbound',
        messageId: msg.messageId,
        summary: `[ABORT] Task stopped by user, session preserved`,
      });
      if (binding.id && result.sdkSessionId) {
        store.updateChannelBinding(binding.id, { sdkSessionId: result.sdkSessionId });
      }
      return;
    }

    // Send response text — render via channel-appropriate format.
    // If streaming preview was active and not degraded, the preview card already
    // contains the final text — skip the redundant deliverResponse.
    //
    // Before deciding, ensure the final text is up to date:
    // 1. Wait for any in-flight flush to complete
    // 2. If pending text differs from last sent text, do one final flush
    // 3. Only trust preview delivery if final text matches (ETH-95)
    if (previewState && streamCfg && !previewState.degraded) {
      // Wait for in-flight flush before attempting final flush
      if (previewState.flushInFlight && previewState.lastFlushPromise) {
        await previewState.lastFlushPromise.catch(() => {});
      }
      // Final flush if pending text differs from what was confirmed sent
      if (previewState.pendingText && previewState.pendingText !== previewState.lastSentText) {
        flushPreview(adapter, previewState, streamCfg);
        if (previewState.lastFlushPromise) {
          await previewState.lastFlushPromise.catch(() => {});
        }
      }
    }
    const previewHandledDelivery = previewState
      && !previewState.degraded
      && previewState.previewEverDelivered
      && previewState.pendingText === previewState.lastSentText; // verify final text matches (ETH-95)
    if (previewHandledDelivery) {
      console.log(`[bridge-manager] Response delivered via streaming preview to ${msg.address.chatId}`);
    } else if (previewState && !previewState.degraded && previewState.previewEverDelivered
      && previewState.pendingText !== previewState.lastSentText) {
      console.warn(`[bridge-manager] Streaming preview final text not confirmed for ${msg.address.chatId}, falling back to deliverResponse`);
    } else if (previewState && !previewState.degraded && !previewState.previewEverDelivered) {
      console.warn(`[bridge-manager] Streaming preview never delivered for ${msg.address.chatId}, falling back to deliverResponse`);
    }
    // ETH-144: Diagnostic logging when response contains URLs
    const urlPattern = /https?:\/\/\S+/;
    const responseHasUrl = result.responseText && urlPattern.test(result.responseText);
    if (responseHasUrl) {
      const urls = result.responseText.match(/https?:\/\/\S+/g) || [];
      const pendingHasUrl = previewState?.pendingText ? urlPattern.test(previewState.pendingText) : false;
      const lastSentHasUrl = previewState?.lastSentText ? urlPattern.test(previewState.lastSentText) : false;
      console.log(`[bridge-manager][ETH-144] URL in response for ${msg.address.chatId}:`, JSON.stringify({
        urls,
        previewHandledDelivery: !!previewHandledDelivery,
        pendingHasUrl,
        lastSentHasUrl,
        degraded: previewState?.degraded ?? null,
        previewEverDelivered: previewState?.previewEverDelivered ?? null,
        pendingLen: previewState?.pendingText?.length ?? 0,
        lastSentLen: previewState?.lastSentText?.length ?? 0,
        textMatch: previewState ? previewState.pendingText === previewState.lastSentText : null,
        generation: previewState?.generation ?? null,
        responseLen: result.responseText.length,
      }));
      if (previewHandledDelivery && !lastSentHasUrl) {
        console.error(`[bridge-manager][ETH-144] BUG: preview "handled" delivery but lastSentText missing URL! pendingText tail: ${JSON.stringify(previewState!.pendingText!.slice(-300))}`);
      }
    }
    // Filter out Claude Code internal "no-op" responses that should never reach IM.
    // "No response requested." is a CLI convention (synthetic or LLM-generated) that
    // the CLI UI hides; the bridge must do the same.
    const isNoOpResponse = result.responseText.trim() === 'No response requested.';
    if (result.responseText && !previewHandledDelivery && !isNoOpResponse) {
      await deliverResponse(adapter, msg.address, result.responseText, binding.codepilotSessionId, msg.messageId);
    }
    // Error notification — independent check, never swallowed by responseText (ETH-78)
    if (result.hasError) {
      const errorText = result.errorMessage || 'Unknown error';
      store.insertAuditLog({
        channelType: adapter.channelType,
        chatId: msg.address.chatId,
        direction: 'outbound',
        messageId: msg.messageId,
        summary: `[ERROR] ${errorText.slice(0, 150)}`,
      });
      const errorResponse: OutboundMessage = {
        address: msg.address,
        text: `<b>Error:</b> ${escapeHtml(errorText)}\n\n下次訊息將嘗試繼續當前對話。如需重置，請發送 /session reset`,
        parseMode: 'HTML',
        replyToMessageId: msg.messageId,
      };
      await deliver(adapter, errorResponse);
    }

    // Persist the actual SDK session ID for future resume (ETH-185).
    // Errors no longer clear the session — the JSONL transcript survives and
    // the next query() can resume. Users can /session reset if needed.
    if (binding.id) {
      try {
        const update = computeSdkSessionUpdate(result.sdkSessionId, result.hasError);
        if (update !== null) {
          const prev = binding.sdkSessionId || '(empty)';
          if (update !== binding.sdkSessionId) {
            console.log(`[bridge-manager] sdkSessionId change for ${msg.address.chatId}: ${prev} → ${update || '(cleared)'}`);
            store.insertAuditLog({
              channelType: adapter.channelType,
              chatId: msg.address.chatId,
              direction: 'outbound',
              messageId: msg.messageId,
              summary: `[SDK_SESSION] ${prev.slice(0, 8)} → ${update ? update.slice(0, 8) : '(cleared)'}${result.hasError ? ' (error reset)' : ''}`,
            });
          }
          store.updateChannelBinding(binding.id, { sdkSessionId: update });
        }
      } catch { /* best effort */ }
    }
  } finally {
    // Clean up preview state — flush any buffered text before ending
    if (previewState) {
      if (previewState.throttleTimer) {
        clearTimeout(previewState.throttleTimer);
        previewState.throttleTimer = null;
      }
      // Wait for any in-flight flush before attempting final cleanup flush
      if (previewState.flushInFlight && previewState.lastFlushPromise) {
        await previewState.lastFlushPromise.catch(() => {});
      }
      // Final flush to ensure the preview card has the latest text
      if (streamCfg && !previewState.degraded && previewState.pendingText !== previewState.lastSentText) {
        flushPreview(adapter, previewState, streamCfg);
      }
      // Await the final flush before ending the preview card (ETH-80)
      if (previewState.lastFlushPromise) {
        await previewState.lastFlushPromise.catch(() => {});
      }
      adapter.endPreview?.(msg.address.chatId, previewState.draftId);
    }

    state.activeTasks.delete(binding.codepilotSessionId);
    syncActiveTasksToHost();
    store.insertAuditLog({
      channelType: adapter.channelType,
      chatId: msg.address.chatId,
      direction: 'outbound',
      messageId: msg.messageId,
      summary: `[TASK_END] session=${binding.codepilotSessionId.slice(0, 8)}`,
    });
    // Notify adapter that message processing ended
    adapter.onMessageEnd?.(msg.address.chatId, msg.messageId);
    // Commit the offset only after full processing (success or failure)
    ack();
  }
}

/**
 * Handle IM slash commands.
 */
async function handleCommand(
  adapter: BaseChannelAdapter,
  msg: InboundMessage,
  text: string,
): Promise<void> {
  const { store } = getBridgeContext();

  // Extract command and args (handle /command@botname format)
  const parts = text.split(/\s+/);
  const command = parts[0].split('@')[0].toLowerCase();
  const args = parts.slice(1).join(' ').trim();

  // Run dangerous-input detection on the full command text
  const dangerCheck = isDangerousInput(text);
  if (dangerCheck.dangerous) {
    store.insertAuditLog({
      channelType: adapter.channelType,
      chatId: msg.address.chatId,
      direction: 'inbound',
      messageId: msg.messageId,
      summary: `[BLOCKED] Dangerous input detected: ${dangerCheck.reason}`,
    });
    console.warn(`[bridge-manager] Blocked dangerous command input from chat ${msg.address.chatId}: ${dangerCheck.reason}`);
    await deliver(adapter, {
      address: msg.address,
      text: `Command rejected: invalid input detected.`,
      parseMode: 'plain',
      replyToMessageId: msg.messageId,
    });
    return;
  }

  let response = '';

  switch (command) {
    case '/start':
      response = [
        '<b>CodePilot Bridge</b>',
        '',
        'Send any message to interact with Claude.',
        '',
        '<b>Commands:</b>',
        '/new [path] - Start new session',
        '/bind &lt;session_id&gt; - Bind to existing session',
        '/cwd /path - Change working directory',
        '/mode plan|code|ask - Change mode',
        '/status - Show current status',
        '/sessions - List recent sessions',
        '/stop, /cancel - 停止當前任務',
        '/perm allow|allow_session|deny &lt;id&gt; - Respond to permission',
        '/help - Show this help',
      ].join('\n');
      break;

    case '/new': {
      let workDir: string | undefined;
      if (args) {
        const validated = validateWorkingDirectory(args);
        if (!validated) {
          response = 'Invalid path. Must be an absolute path without traversal sequences.';
          break;
        }
        workDir = validated;
      }
      const binding = router.createBinding(msg.address, workDir);
      store.insertAuditLog({
        channelType: adapter.channelType,
        chatId: msg.address.chatId,
        direction: 'inbound',
        messageId: msg.messageId,
        summary: `[CMD] /new → session=${binding.codepilotSessionId.slice(0, 8)} cwd=${binding.workingDirectory || '~'}`,
      });
      response = `New session created.\nSession: <code>${binding.codepilotSessionId.slice(0, 8)}...</code>\nCWD: <code>${escapeHtml(binding.workingDirectory || '~')}</code>`;
      break;
    }

    case '/bind': {
      if (!args) {
        response = 'Usage: /bind &lt;session_id&gt;';
        break;
      }
      if (!validateSessionId(args)) {
        response = 'Invalid session ID format. Expected a 32-64 character hex/UUID string.';
        break;
      }
      const binding = router.bindToSession(msg.address, args);
      if (binding) {
        store.insertAuditLog({
          channelType: adapter.channelType,
          chatId: msg.address.chatId,
          direction: 'inbound',
          messageId: msg.messageId,
          summary: `[CMD] /bind → session=${args.slice(0, 8)}`,
        });
        response = `Bound to session <code>${args.slice(0, 8)}...</code>`;
      } else {
        response = 'Session not found.';
      }
      break;
    }

    case '/cwd': {
      if (!args) {
        response = 'Usage: /cwd /path/to/directory';
        break;
      }
      const validatedPath = validateWorkingDirectory(args);
      if (!validatedPath) {
        response = 'Invalid path. Must be an absolute path without traversal sequences or special characters.';
        break;
      }
      const binding = router.resolve(msg.address);
      router.updateBinding(binding.id, { workingDirectory: validatedPath });
      store.insertAuditLog({
        channelType: adapter.channelType,
        chatId: msg.address.chatId,
        direction: 'inbound',
        messageId: msg.messageId,
        summary: `[CMD] /cwd → ${validatedPath}`,
      });
      response = `Working directory set to <code>${escapeHtml(validatedPath)}</code>`;
      break;
    }

    case '/mode': {
      if (!validateMode(args)) {
        response = 'Usage: /mode plan|code|ask';
        break;
      }
      const binding = router.resolve(msg.address);
      router.updateBinding(binding.id, { mode: args });
      store.insertAuditLog({
        channelType: adapter.channelType,
        chatId: msg.address.chatId,
        direction: 'inbound',
        messageId: msg.messageId,
        summary: `[CMD] /mode → ${args}`,
      });
      response = `Mode set to <b>${args}</b>`;
      break;
    }

    case '/status': {
      const binding = router.resolve(msg.address);
      response = [
        '<b>Bridge Status</b>',
        '',
        `Session: <code>${binding.codepilotSessionId.slice(0, 8)}...</code>`,
        `CWD: <code>${escapeHtml(binding.workingDirectory || '~')}</code>`,
        `Mode: <b>${binding.mode}</b>`,
        `Model: <code>${binding.model || 'default'}</code>`,
      ].join('\n');
      break;
    }

    case '/sessions': {
      const bindings = router.listBindings(adapter.channelType);
      if (bindings.length === 0) {
        response = 'No sessions found.';
      } else {
        const lines = ['<b>Sessions:</b>', ''];
        for (const b of bindings.slice(0, 10)) {
          const active = b.active ? 'active' : 'inactive';
          lines.push(`<code>${b.codepilotSessionId.slice(0, 8)}...</code> [${active}] ${escapeHtml(b.workingDirectory || '~')}`);
        }
        response = lines.join('\n');
      }
      break;
    }

    case '/cancel':
    case '/stop': {
      const binding = router.resolve(msg.address);
      const st = getState();
      const taskAbort = st.activeTasks.get(binding.codepilotSessionId);
      if (taskAbort) {
        taskAbort.abort();
        st.activeTasks.delete(binding.codepilotSessionId);
        syncActiveTasksToHost();
        store.insertAuditLog({
          channelType: adapter.channelType,
          chatId: msg.address.chatId,
          direction: 'inbound',
          messageId: msg.messageId,
          summary: `[CMD] ${command} → task aborted, session=${binding.codepilotSessionId.slice(0, 8)}`,
        });
        response = '已停止當前任務。';
      } else {
        response = 'No task is currently running.';
      }
      break;
    }

    case '/perm': {
      // Admin-only: text-based permission approval fallback
      const permRole = resolveUserRole(adapter.channelType, msg.address.userId);
      if (permRole !== undefined && permRole !== 'admin') {
        response = 'Only admins can use this command.';
        break;
      }
      // Usage: /perm allow <id> | /perm allow_session <id> | /perm deny <id>
      const permParts = args.split(/\s+/);
      const permAction = permParts[0];
      const permId = permParts.slice(1).join(' ');
      if (!permAction || !permId || !['allow', 'allow_session', 'deny'].includes(permAction)) {
        response = 'Usage: /perm allow|allow_session|deny &lt;permission_id&gt;';
        break;
      }
      const callbackData = `perm:${permAction}:${permId}`;
      const handled = broker.handlePermissionCallback(callbackData, msg.address.chatId);
      if (handled) {
        // Patch the original permission card for all group members
        if (adapter.resolvePermissionCard) {
          const link = getBridgeContext().store.getPermissionLink(permId);
          if (link?.messageId) {
            adapter.resolvePermissionCard(
              msg.address.chatId, link.messageId, permAction, link?.toolName, link?.toolInput,
            ).catch((err) => {
              console.warn('[bridge-manager] Failed to patch permission card via /perm:', err instanceof Error ? err.message : err);
            });
          }
        }
        store.insertAuditLog({
          channelType: adapter.channelType,
          chatId: msg.address.chatId,
          direction: 'inbound',
          messageId: msg.messageId,
          summary: `[CMD] /perm ${permAction} → permId=${permId.slice(0, 12)}`,
        });
        response = `Permission ${permAction}: recorded.`;
      } else {
        response = `Permission not found or already resolved.`;
      }
      break;
    }

    case '/help':
      response = [
        '<b>CodePilot Bridge Commands</b>',
        '',
        '/new [path] - Start new session',
        '/bind &lt;session_id&gt; - Bind to existing session',
        '/cwd /path - Change working directory',
        '/mode plan|code|ask - Change mode',
        '/status - Show current status',
        '/sessions - List recent sessions',
        '/stop, /cancel - 停止當前任務',
        '/perm allow|allow_session|deny &lt;id&gt; - Respond to permission request',
        '/help - Show this help',
      ].join('\n');
      break;

    default:
      response = `Unknown command: ${escapeHtml(command)}\nType /help for available commands.`;
  }

  if (response) {
    await deliver(adapter, {
      address: msg.address,
      text: response,
      parseMode: 'HTML',
      replyToMessageId: msg.messageId,
    });
  }
}

// ── SDK Session Update Logic ─────────────────────────────────

/**
 * Compute the sdkSessionId value to persist after a conversation result.
 * Returns the new value to write, or null if no update is needed.
 *
 * Rules (ETH-185):
 * - If result has sdkSessionId → save the new ID (regardless of error)
 * - If result has error but NO sdkSessionId → no update (preserve previous ID)
 * - Otherwise → no update needed
 *
 * Errors (quota exceeded, rate limit, transient failures) should NOT clear
 * the session. The JSONL transcript is intact and the next query() can resume.
 * Users can manually reset via /session reset if the session is truly broken.
 */
export function computeSdkSessionUpdate(
  sdkSessionId: string | null | undefined,
  hasError: boolean,
): string | null {
  if (sdkSessionId) {
    return sdkSessionId;
  }
  return null;
}

// ── Test-only export ─────────────────────────────────────────
// Exposed so integration tests can exercise handleMessage directly
// without wiring up the full adapter loop.
/** @internal */
export const _testOnly = { handleMessage };
