/**
 * Feishu (Lark) Adapter — implements BaseChannelAdapter for Feishu Bot API.
 *
 * Uses the official @larksuiteoapi/node-sdk WSClient for real-time event
 * subscription and REST Client for message sending / resource downloading.
 * Routes messages through an internal async queue (same pattern as Telegram).
 *
 * Rendering strategy (aligned with Openclaw):
 * - Code blocks / tables → interactive card (schema 2.0 markdown)
 * - Other text → post (msg_type: 'post') with md tag
 * - Permission prompts → interactive card with action buttons
 *
 * card.action.trigger events are handled via EventDispatcher (Openclaw pattern):
 * button clicks are converted to synthetic text messages and routed through
 * the normal /perm command processing pipeline.
 */

import crypto from 'crypto';
import * as lark from '@larksuiteoapi/node-sdk';
import type {
  ChannelType,
  InboundMessage,
  OutboundMessage,
  PreviewCapabilities,
  SendResult,
} from '../types.js';
import type { FileAttachment } from '../types.js';
import { BaseChannelAdapter, registerAdapterFactory } from '../channel-adapter.js';
import { getBridgeContext } from '../context.js';
import {
  htmlToFeishuMarkdown,
  preprocessFeishuMarkdown,
  hasComplexMarkdown,
  buildCardContent,
  buildPostContent,
  countMarkdownTables,
  splitAtTableBoundary,
  MAX_CARD_TABLES,
} from '../markdown/feishu.js';

/** Max number of message_ids to keep for dedup. */
const DEDUP_MAX = 1000;

/** Max file download size for in-memory buffers (20 MB). */
const MAX_FILE_SIZE = 20 * 1024 * 1024;

/** Max file download size for disk-based downloads (200 MB). */
const MAX_FILE_SIZE_DISK = 200 * 1024 * 1024;

/** Feishu emoji type for typing indicator (same as Openclaw). */
const TYPING_EMOJI = 'Typing';

/** Shape of the SDK's im.message.receive_v1 event data. */
type FeishuMessageEventData = {
  sender: {
    sender_id?: {
      open_id?: string;
      union_id?: string;
      user_id?: string;
    };
    sender_type: string;
    tenant_key?: string;
  };
  message: {
    message_id: string;
    chat_id: string;
    chat_type: string;
    message_type: string;
    content: string;
    create_time: string;
    mentions?: Array<{
      key: string;
      id: { open_id?: string; union_id?: string; user_id?: string };
      name: string;
    }>;
    parent_id?: string;
    root_id?: string;
  };
};


/** MIME type guesses by message_type. */
const MIME_BY_TYPE: Record<string, string> = {
  image: 'image/png',
  file: 'application/octet-stream',
  audio: 'audio/ogg',
  video: 'video/mp4',
};

/** MIME type by file extension (common types for file attachments). */
const MIME_BY_EXT: Record<string, string> = {
  pdf: 'application/pdf',
  doc: 'application/msword',
  docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  xls: 'application/vnd.ms-excel',
  xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  ppt: 'application/vnd.ms-powerpoint',
  pptx: 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
  txt: 'text/plain',
  csv: 'text/csv',
  json: 'application/json',
  md: 'text/markdown',
  zip: 'application/zip',
  png: 'image/png',
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  gif: 'image/gif',
  webp: 'image/webp',
  mp4: 'video/mp4',
  ogg: 'audio/ogg',
  media: 'application/octet-stream',
};

export class FeishuAdapter extends BaseChannelAdapter {
  readonly channelType: ChannelType = 'feishu';

  protected running = false;
  private queue: InboundMessage[] = [];
  private waiters: Array<(msg: InboundMessage | null) => void> = [];
  private wsClient: lark.WSClient | null = null;
  protected restClient: lark.Client | null = null;
  private seenMessageIds = new Map<string, boolean>();
  protected botOpenId: string | null = null;
  /** All known bot IDs (open_id, user_id, union_id) for mention matching. */
  private botIds = new Set<string>();
  /** Track last incoming message ID per chat (fallback for typing indicator). */
  private lastIncomingMessageId = new Map<string, string>();
  /** Cache: open_id → display name (resolved via Group Member API or Contact API). */
  private userNameCache = new Map<string, string>();
  /** Cache: chat_id set of chats whose members have been loaded. */
  private membersCachedChats = new Set<string>();
  /** Cache: chat_id set of chats whose metadata has been loaded. */
  private metadataCachedChats = new Set<string>();
  /** Disable Contact API calls after first failure (missing scopes). */
  private contactApiDisabled = false;
  /** Track active typing reaction IDs per message for cleanup. */
  private typingReactions = new Map<string, string>();
  /** Track preview message IDs per chat for streaming updates. */
  private previewMessages = new Map<string, string>();
  /** Guard: in-flight card creation per chat (prevents duplicate creates). */
  private previewCreating = new Set<string>();
  /** Chats where preview has permanently degraded (API not supported). */
  private previewDegraded = new Set<string>();
  /** Queued card content to PATCH after in-flight create resolves. */
  private previewQueuedUpdate = new Map<string, string>();
  /** Generation counter per chat — incremented by endPreview to invalidate in-flight creates. */
  private previewGeneration = new Map<string, number>();
  /** Overflow text when preview content exceeds Lark card table limit. Sent as follow-up on endPreview. */
  private previewOverflow = new Map<string, string>();
  /** Cache: messageId → final text content for bot-sent messages (for quote resolution). */
  private botMessageContentCache = new Map<string, string>();
  private static readonly BOT_CONTENT_CACHE_MAX = 200;
  /** Timestamp of the last PATCH sent (across all sessions) for global rate limiting. */
  private lastPatchAt = 0;
  /** Reverse cache: lowercase display name (and first-name fragments) → open_id for outbound @mention. */
  private nameToIdCache = new Map<string, string>();
  /** Minimum interval (ms) between PATCHes across all sessions to reduce API rate limit collisions. */
  private static readonly MIN_PATCH_INTERVAL = 200;
  /** Max retry attempts for rate-limited PATCHes. */
  private static readonly PATCH_MAX_RETRIES = 2;

  protected setting(key: string): string {
    return getBridgeContext().store.getSetting(`bridge_${this.channelType}_${key}`) || '';
  }

  // ── Lifecycle ───────────────────────────────────────────────

  async start(): Promise<void> {
    if (this.running) return;

    const configError = this.validateConfig();
    if (configError) {
      console.warn('[feishu-adapter] Cannot start:', configError);
      return;
    }

    const appId = this.setting('app_id');
    const appSecret = this.setting('app_secret');
    const domainSetting = this.setting('domain') || 'feishu';
    const domain = domainSetting === 'lark'
      ? lark.Domain.Lark
      : lark.Domain.Feishu;

    // Create REST client
    this.restClient = new lark.Client({
      appId,
      appSecret,
      domain,
    });

    // Resolve bot identity for @mention detection
    await this.resolveBotIdentity(appId, appSecret, domain);

    this.running = true;

    // Preload members from all groups in the background (best-effort)
    this.preloadAllChatMembers().catch(() => {});

    // WebSocket mode
    const dispatcher = new lark.EventDispatcher({}).register({
      'im.message.receive_v1': async (data) => {
        await this.handleIncomingEvent(data as FeishuMessageEventData);
      },
      'im.chat.member.user.added_v1': async (data) => {
        await this.handleMemberChange(data);
      },
      'im.chat.member.user.deleted_v1': async (data) => {
        await this.handleMemberChange(data);
      },
    });

    this.wsClient = new lark.WSClient({
      appId,
      appSecret,
      domain,
    });
    this.wsClient.start({ eventDispatcher: dispatcher });

    console.log('[feishu-adapter] Started in ws mode (botOpenId:', this.botOpenId || 'unknown', ')');
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;

    // Close WebSocket connection (SDK exposes close())
    if (this.wsClient) {
      try {
        this.wsClient.close({ force: true });
      } catch (err) {
        console.warn('[feishu-adapter] WSClient close error:', err instanceof Error ? err.message : err);
      }
      this.wsClient = null;
    }

    this.restClient = null;

    // Reject all waiting consumers
    for (const waiter of this.waiters) {
      waiter(null);
    }
    this.waiters = [];

    // Clear state
    this.seenMessageIds.clear();
    this.lastIncomingMessageId.clear();
    this.typingReactions.clear();
    this.previewMessages.clear();
    this.previewCreating.clear();
    this.previewDegraded.clear();
    this.previewQueuedUpdate.clear();
    this.previewGeneration.clear();
    this.previewOverflow.clear();
    this.botMessageContentCache.clear();
    this.userNameCache.clear();
    this.contactApiDisabled = false;
    this.lastPatchAt = 0;

    console.log('[feishu-adapter] Stopped');
  }

  isRunning(): boolean {
    return this.running;
  }

  // ── Sender Identity ────────────────────────────────────────

  /** Sentinel value: word key maps to multiple users — skip transformation. */
  private static readonly AMBIGUOUS_NAME = '__ambiguous__';

  /**
   * Update both forward (open_id → name) and reverse (name → open_id) caches.
   * Reverse cache indexes full name and individual name words (for first-name @mentions).
   * Collisions on word keys are marked ambiguous (e.g. two "Allen"s → @Allen won't transform).
   */
  private addToNameCache(openId: string, name: string): void {
    this.userNameCache.set(openId, name);
    const lower = name.toLowerCase();
    this.nameToIdCache.set(lower, openId);
    for (const word of lower.split(/\s+/)) {
      if (word.length < 2) continue;
      const existing = this.nameToIdCache.get(word);
      if (!existing) {
        this.nameToIdCache.set(word, openId);
      } else if (existing !== openId && existing !== FeishuAdapter.AMBIGUOUS_NAME) {
        // Collision: two different users share the same word — mark ambiguous
        this.nameToIdCache.set(word, FeishuAdapter.AMBIGUOUS_NAME);
      }
    }
  }

  /**
   * Load all members of a chat and populate userNameCache.
   * Uses Group Member API (im.v1.chat.members) which only requires IM permissions,
   * unlike Contact API which needs contact:contact.base:readonly.
   */
  private async loadChatMembers(chatId: string): Promise<void> {
    if (!this.restClient || this.membersCachedChats.has(chatId)) return;

    try {
      const chatMembers: Array<{ id: string; name: string }> = [];
      let pageToken: string | undefined;
      do {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const resp: any = await this.restClient.im.chatMembers.get({
          path: { chat_id: chatId },
          params: {
            member_id_type: 'open_id',
            page_size: 100,
            ...(pageToken ? { page_token: pageToken } : {}),
          },
        });
        const items = resp?.data?.items;
        if (Array.isArray(items)) {
          for (const member of items) {
            if (member.member_id && member.name) {
              this.addToNameCache(member.member_id, member.name);
              if (!this.botIds.has(member.member_id)) {
                chatMembers.push({ id: member.member_id, name: member.name });
              }
            }
          }
        }
        pageToken = resp?.data?.page_token || undefined;
      } while (pageToken);

      this.membersCachedChats.add(chatId);
      // Publish to store so conversation-engine can inject into system prompt
      const { store } = getBridgeContext();
      store.setGroupMembers?.(chatId, chatMembers);

      // On-demand metadata load for chats not seen during preload
      if (!this.metadataCachedChats.has(chatId)) {
        this.loadChatMetadata(chatId).catch(() => {});
      }
    } catch (err) {
      console.warn(
        '[feishu-adapter] Failed to load chat members for chatId:', chatId,
        err instanceof Error ? err.message : err,
      );
    }
  }

  /**
   * Load chat metadata (name, description, type) via im.chat.get.
   * Called on-demand for chats not seen during preload.
   */
  protected async loadChatMetadata(chatId: string): Promise<void> {
    if (this.metadataCachedChats.has(chatId) || !this.restClient) return;
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const resp: any = await this.restClient.im.chat.get({
        path: { chat_id: chatId },
      });
      const name = resp?.data?.name || '';
      const description = resp?.data?.description || '';
      const chatType = resp?.data?.chat_type || '';
      this.metadataCachedChats.add(chatId);
      const { store } = getBridgeContext();
      store.setGroupMetadata?.(chatId, { name, description, chatType });
    } catch (err) {
      console.warn(
        '[feishu-adapter] Failed to load chat metadata for chatId:', chatId,
        err instanceof Error ? err.message : err,
      );
    }
  }

  /**
   * Handle member added/deleted events — invalidate cache and reload.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  protected async handleMemberChange(data: any): Promise<void> {
    const chatId = data?.chat_id;
    if (!chatId) return;
    // Invalidate so next loadChatMembers does a full reload
    this.membersCachedChats.delete(chatId);
    await this.loadChatMembers(chatId);
    console.log(`[feishu-adapter] Member change in ${chatId}, reloaded ${this.userNameCache.size} users`);
  }

  /**
   * Preload members from all groups the bot is in (best-effort, runs in background).
   * Ensures userNameCache is warm for cross-group name resolution (e.g. merge_forward).
   */
  protected async preloadAllChatMembers(): Promise<void> {
    if (!this.restClient) return;
    try {
      let pageToken: string | undefined;
      const chatIds: string[] = [];
      do {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const resp: any = await this.restClient.im.chat.list({
          params: {
            page_size: 100,
            ...(pageToken ? { page_token: pageToken } : {}),
          },
        });
        const items = resp?.data?.items;
        const { store } = getBridgeContext();
        if (Array.isArray(items)) {
          for (const chat of items) {
            if (chat.chat_id) {
              chatIds.push(chat.chat_id);
              // Extract metadata from list response (free, no extra API call)
              if (!this.metadataCachedChats.has(chat.chat_id)) {
                const name = chat.name || '';
                const description = chat.description || '';
                const chatType = chat.chat_type || '';
                this.metadataCachedChats.add(chat.chat_id);
                store.setGroupMetadata?.(chat.chat_id, { name, description, chatType });
              }
            }
          }
        }
        pageToken = resp?.data?.page_token || undefined;
      } while (pageToken);

      for (const chatId of chatIds) {
        await this.loadChatMembers(chatId);
      }
      console.log(`[feishu-adapter] Preloaded members from ${chatIds.length} chats (${this.userNameCache.size} users cached)`);
    } catch (err) {
      console.warn('[feishu-adapter] Failed to preload chat members:', err instanceof Error ? err.message : err);
    }
  }

  /**
   * Resolve a user's display name.
   * 1. Check cache (populated by loadChatMembers or previous lookups)
   * 2. Fall back to Contact API if available
   */
  private async resolveUserDisplayName(userId: string, chatId?: string): Promise<string | null> {
    if (!userId || !this.restClient) return null;

    // Skip bot app_ids (cli_*) — not valid open_ids for Contact API
    if (userId.startsWith('cli_')) return null;

    // Try loading chat members first if we haven't yet
    if (chatId && !this.membersCachedChats.has(chatId)) {
      await this.loadChatMembers(chatId);
    }

    const cached = this.userNameCache.get(userId);
    if (cached) return cached;

    // Fall back to Contact API for users not in the chat member list (e.g. DMs)
    if (!this.contactApiDisabled) {
      try {
        const resp = await this.restClient.contact.user.get({
          path: { user_id: userId },
          params: { user_id_type: 'open_id' },
        });
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const name = (resp as any)?.data?.user?.name;
        if (name) {
          this.addToNameCache(userId, name);
          return name;
        }
      } catch (err) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const errCode = (err as any)?.response?.data?.code ?? (err as any)?.code;
        if (errCode === 41050 || errCode === 99992351) {
          // Per-user permission issue — don't disable globally
          console.warn('[feishu-adapter] Contact API: no authority for userId:', userId);
        } else {
          console.warn(
            '[feishu-adapter] Contact API failed for userId:', userId,
            '— disabling Contact API fallback.',
            err instanceof Error ? err.message : err,
          );
          this.contactApiDisabled = true;
        }
      }
    }

    return null;
  }

  /**
   * Fetch the content of a quoted (replied-to) message by its message_id.
   * Returns a formatted string like "[Replying to 張三: 原始訊息內容]", or null on failure.
   */
  private async fetchQuotedContent(parentId: string, chatId?: string): Promise<string | null> {
    // Check local cache first (bot-sent messages whose PATCH content isn't in API)
    const cached = this.botMessageContentCache.get(parentId);
    if (cached) {
      const truncated = cached.length > 200 ? cached.slice(0, 200) + '…' : cached;
      return `[Replying to bot: ${truncated}]`;
    }

    if (!this.restClient) return null;
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const resp = await (this.restClient as any).im.message.get({
        path: { message_id: parentId },
        params: { card_msg_content_type: 'raw_card_content' },
      });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const item = (resp as any)?.data?.items?.[0];
      if (!item) return null;

      const msgType: string = item.msg_type || '';
      const rawContent: string = item.body?.content || '';
      const senderId: string | undefined = item.sender?.id;
      console.log('[feishu-adapter] fetchQuotedContent: msgType:', msgType, 'contentLen:', rawContent.length, 'msgId:', parentId);

      // Parse message content based on type
      let content: string;
      if (msgType === 'text') {
        try {
          const parsed = JSON.parse(rawContent);
          content = parsed.text || rawContent;
        } catch {
          content = rawContent;
        }
      } else if (msgType === 'post') {
        const { extractedText } = this.parsePostContent(rawContent, item.mentions);
        content = extractedText || '[post]';
      } else if (msgType === 'image') {
        content = '[image]';
      } else if (msgType === 'file') {
        try {
          const parsed = JSON.parse(rawContent);
          content = `[file: ${parsed.file_name || 'unknown'}]`;
        } catch {
          content = '[file]';
        }
      } else if (msgType === 'audio') {
        content = '[audio]';
      } else if (msgType === 'video') {
        content = '[video]';
      } else if (msgType === 'sticker') {
        content = '[sticker]';
      } else if (msgType === 'folder') {
        try {
          const parsed = JSON.parse(rawContent);
          content = `[folder: ${parsed.file_name || 'unknown'}]`;
        } catch {
          content = '[folder]';
        }
      } else if (msgType === 'merge_forward') {
        content = '[forwarded conversation]';
      } else if (msgType === 'interactive') {
        // Try raw_card_content json_card first (ETH-89)
        // Lark API sometimes returns incomplete card on first attempt (ETH-96), retry once
        console.log('[feishu-adapter] fetchQuotedContent: interactive card, msgId:', parentId);
        content = this.extractCardContent(item) || '';
        if (!content) {
          await new Promise(r => setTimeout(r, 300));
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const retry = await (this.restClient as any).im.message.get({
            path: { message_id: parentId },
            params: { card_msg_content_type: 'raw_card_content' },
          });
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const retryItem = (retry as any)?.data?.items?.[0];
          content = (retryItem ? this.extractCardContent(retryItem) : null) || '[interactive]';
        }
      } else {
        content = `[${msgType || 'unknown'}]`;
      }

      // Truncate very long quoted content
      if (content.length > 200) {
        content = content.slice(0, 200) + '…';
      }

      // Resolve sender name — bot IDs start with "cli_", skip Contact API
      let senderName: string | undefined;
      if (senderId && senderId.startsWith('cli_')) {
        senderName = 'bot';
      } else if (senderId) {
        senderName = await this.resolveUserDisplayName(senderId, chatId) || undefined;
      }

      if (senderName) {
        return `[Replying to ${senderName}: ${content}]`;
      }
      return `[Replying to: ${content}]`;
    } catch (err) {
      console.warn(
        '[feishu-adapter] Failed to fetch quoted message:',
        parentId,
        err instanceof Error ? err.message : err,
      );
      return null;
    }
  }

  /**
   * Extract display name from message mentions array (fallback when Contact API unavailable).
   * Lark includes the sender's name in mentions if they @mention the bot.
   */
  private extractSenderNameFromMentions(
    mentions: FeishuMessageEventData['message']['mentions'],
    userId: string,
  ): string | null {
    if (!mentions) return null;
    for (const m of mentions) {
      if (m.id.open_id === userId || m.id.user_id === userId || m.id.union_id === userId) {
        return m.name || null;
      }
    }
    return null;
  }

  // ── Queue ───────────────────────────────────────────────────

  consumeOne(): Promise<InboundMessage | null> {
    const queued = this.queue.shift();
    if (queued) return Promise.resolve(queued);

    if (!this.running) return Promise.resolve(null);

    return new Promise<InboundMessage | null>((resolve) => {
      this.waiters.push(resolve);
    });
  }

  protected enqueue(msg: InboundMessage): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(msg);
    } else {
      this.queue.push(msg);
    }
  }

  // ── Typing indicator (Openclaw-style reaction) ─────────────

  /**
   * Add a "Typing" emoji reaction to the user's message.
   * Called by bridge-manager via onMessageStart().
   */
  onMessageStart(chatId: string, messageId?: string): void {
    const msgId = messageId ?? this.lastIncomingMessageId.get(chatId);
    if (!msgId || !this.restClient) return;

    // Fire-and-forget — typing indicator is non-critical
    this.restClient.im.messageReaction.create({
      path: { message_id: msgId },
      data: { reaction_type: { emoji_type: TYPING_EMOJI } },
    }).then((res) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const reactionId = (res as any)?.data?.reaction_id;
      if (reactionId) {
        this.typingReactions.set(msgId, reactionId);
      }
    }).catch((err) => {
      // Non-critical — don't log rate limit errors
      const code = (err as { code?: number })?.code;
      if (code !== 99991400 && code !== 99991403) {
        console.warn('[feishu-adapter] Typing indicator failed:', err instanceof Error ? err.message : err);
      }
    });
  }

  /**
   * Remove the "Typing" emoji reaction from the user's message.
   * Called by bridge-manager via onMessageEnd().
   */
  onMessageEnd(chatId: string, messageId?: string): void {
    const msgId = messageId ?? this.lastIncomingMessageId.get(chatId);
    if (!msgId || !this.restClient) return;

    const reactionId = this.typingReactions.get(msgId);
    if (!reactionId) return;

    this.typingReactions.delete(msgId);

    // Fire-and-forget — failure is fine (reaction may already be gone)
    this.restClient.im.messageReaction.delete({
      path: { message_id: msgId, reaction_id: reactionId },
    }).catch(() => { /* ignore */ });
  }

  // ── Streaming preview ──────────────────────────────────────

  getPreviewCapabilities(chatId: string): PreviewCapabilities | null {
    // Global kill switch
    if (this.setting('stream_enabled') === 'false') return null;

    // Already degraded for this chat
    if (this.previewDegraded.has(chatId)) return null;

    return { supported: true, privateOnly: false };
  }

  /**
   * PATCH a preview card with global throttle + retry on rate limit (230020).
   * Enforces MIN_PATCH_INTERVAL between PATCHes across all sessions to reduce
   * API rate limit collisions, and retries up to PATCH_MAX_RETRIES times.
   */
  private async patchPreviewCard(messageId: string, cardJson: string): Promise<void> {
    for (let attempt = 0; attempt <= FeishuAdapter.PATCH_MAX_RETRIES; attempt++) {
      // Global throttle: wait if another PATCH was sent too recently
      const elapsed = Date.now() - this.lastPatchAt;
      if (elapsed < FeishuAdapter.MIN_PATCH_INTERVAL) {
        await new Promise(r => setTimeout(r, FeishuAdapter.MIN_PATCH_INTERVAL - elapsed));
      }
      this.lastPatchAt = Date.now();

      try {
        await this.restClient!.im.message.patch({
          path: { message_id: messageId },
          data: { content: cardJson },
        });
        if (attempt > 0) {
          console.log(`[feishu-adapter] PATCH succeeded after ${attempt} retry(s) for ${messageId}`);
        }
        return; // success
      } catch (err) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const code = (err as any)?.code;
        if (code === 230020 && attempt < FeishuAdapter.PATCH_MAX_RETRIES) {
          // Rate limited — backoff and retry
          const backoff = 300 * (attempt + 1);
          console.warn(`[feishu-adapter] PATCH rate limited (230020), retry ${attempt + 1} in ${backoff}ms`);
          await new Promise(r => setTimeout(r, backoff));
          continue;
        }
        throw err; // re-throw for caller to handle
      }
    }
  }

  async sendPreview(chatId: string, text: string, _draftId: number): Promise<'sent' | 'skip' | 'degrade'> {
    if (!this.restClient) return 'skip';

    // Apply the same markdown preprocessing used for final messages
    let processed = preprocessFeishuMarkdown(text);
    // Transform @Name patterns to Lark at-mention tags
    processed = this.transformOutboundMentions(processed);

    // Guard against Lark's card table limit (~10 tables → ErrCode 11310).
    // If the streamed content already exceeds the safe limit, truncate and
    // stash the overflow for delivery in endPreview().
    if (countMarkdownTables(processed) > MAX_CARD_TABLES) {
      const { head, tail } = splitAtTableBoundary(processed, MAX_CARD_TABLES);
      processed = head + '\n\n*(continued in next message…)*';
      this.previewOverflow.set(chatId, tail);
    } else {
      // Content fits — clear any previously stashed overflow (content may
      // have been trimmed by the LLM between updates).
      this.previewOverflow.delete(chatId);
    }

    // Build card JSON with update_multi enabled for PATCH support
    const cardJson = JSON.stringify({
      schema: '2.0',
      config: { wide_screen_mode: true, update_multi: true },
      body: {
        elements: [
          { tag: 'markdown', content: processed },
        ],
      },
    });

    const existingMsgId = this.previewMessages.get(chatId);

    try {
      if (existingMsgId) {
        // Update existing preview card via PATCH (with retry + global throttle)
        await this.patchPreviewCard(existingMsgId, cardJson);
        // Cache latest text for quote resolution (PATCH doesn't update im.message.get)
        this.cacheBotMessageContent(existingMsgId, text);
        return 'sent';
      } else {
        // Guard: if a create is already in-flight, queue the update
        if (this.previewCreating.has(chatId)) {
          this.previewQueuedUpdate.set(chatId, cardJson);
          return 'sent';  // optimistic — queued PATCH will apply after create resolves
        }

        // Send new preview card
        this.previewCreating.add(chatId);
        const genBefore = this.previewGeneration.get(chatId) || 0;
        try {
          const res = await this.restClient.im.message.create({
            params: { receive_id_type: 'chat_id' },
            data: {
              receive_id: chatId,
              msg_type: 'interactive',
              content: cardJson,
            },
          });
          if (res?.data?.message_id) {
            console.log(`[feishu-adapter] Sent preview to ${chatId}, msgId: ${res.data.message_id}`);
            // Seed throttle so the first PATCH respects MIN_PATCH_INTERVAL
            // (prevents race where PATCH arrives before Lark finishes card init)
            this.lastPatchAt = Date.now();
            // Cache content for quote resolution
            this.cacheBotMessageContent(res.data.message_id, text);
            // Only adopt this card for future updates if endPreview hasn't been
            // called during the create. If generation changed, the card belongs to
            // a finalized segment — don't track it for future PATCHes.
            const genAfter = this.previewGeneration.get(chatId) || 0;
            if (genAfter === genBefore) {
              this.previewMessages.set(chatId, res.data.message_id);
            }
            // Don't immediately PATCH queued content — let the next natural
            // flushPreview cycle deliver it through the throttled pipeline.
            // Immediate PATCH after CREATE can race with Lark's internal card
            // init, causing invisible cards (ETH-158).
            return 'sent';
          }
          return 'skip';
        } finally {
          this.previewCreating.delete(chatId);
          this.previewQueuedUpdate.delete(chatId);
        }
      }
    } catch (err) {
      console.warn('[feishu-adapter] sendPreview error:', err instanceof Error ? err.message : err);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const code = (err as any)?.code;
      // Permanent failures: degrade
      if (code === 230001 || code === 230002 || code === 230099 || code === 11310) {
        this.previewDegraded.add(chatId);
        return 'degrade';
      }
      // Transient (rate limit after retries, network) — skip this update
      return 'skip';
    }
  }

  getPreviewMessageId(chatId: string): string | undefined {
    return this.previewMessages.get(chatId);
  }

  seedPreviewMessageId(chatId: string, messageId: string): void {
    this.previewMessages.set(chatId, messageId);
  }

  endPreview(chatId: string, _draftId: number): void {
    // Keep the preview card as the final message — bridge-manager skips
    // deliverResponse when preview was active, so this card IS the response.
    console.log(`[feishu-adapter] Preview finalized for ${chatId}`);
    this.previewMessages.delete(chatId);
    this.previewCreating.delete(chatId);
    // Increment generation so in-flight creates don't re-adopt stale card IDs
    this.previewGeneration.set(chatId, (this.previewGeneration.get(chatId) || 0) + 1);
    this.previewQueuedUpdate.delete(chatId);

    // Deliver any overflow content that was truncated from the preview card
    // due to the Lark card table limit.
    const overflow = this.previewOverflow.get(chatId);
    if (overflow) {
      this.previewOverflow.delete(chatId);
      // Fire-and-forget: endPreview is synchronous (void return)
      this.sendOverflowMessages(chatId, overflow).catch(err => {
        console.warn('[feishu-adapter] Failed to send overflow:', err instanceof Error ? err.message : err);
      });
    }
  }

  /**
   * Send overflow content that didn't fit in the preview card.
   * Recursively splits if the overflow itself exceeds the table limit.
   */
  private async sendOverflowMessages(chatId: string, text: string): Promise<void> {
    let remaining = text;
    while (remaining) {
      const tableCount = countMarkdownTables(remaining);
      if (tableCount > MAX_CARD_TABLES) {
        const { head, tail } = splitAtTableBoundary(remaining, MAX_CARD_TABLES);
        await this.sendAsCard(chatId, head);
        remaining = tail;
      } else {
        // Last chunk — use normal send path (card or post based on content)
        if (hasComplexMarkdown(remaining)) {
          await this.sendAsCard(chatId, remaining);
        } else {
          await this.sendAsPost(chatId, remaining);
        }
        break;
      }
    }
  }

  // ── Outbound @mention ─────────────────────────────────────

  /**
   * Transform @Name patterns in outbound text to Lark at-mention syntax.
   * Protects code blocks and inline code from transformation.
   * - @all / @所有人 → <at id=all></at>
   * - @Name (matching a cached user) → <at id=OPEN_ID>Name</at>
   */
  protected transformOutboundMentions(text: string): string {
    if (this.nameToIdCache.size === 0) return text;

    // Build dynamic regex from cached names, longest first (greedy matching).
    // This ensures "@Ethan Chen" matches the full name before trying "@Ethan".
    const entries = [...this.nameToIdCache.entries()]
      .filter(([, id]) => id !== FeishuAdapter.AMBIGUOUS_NAME)
      .sort((a, b) => b[0].length - a[0].length);
    if (entries.length === 0) return text;

    const escaped = entries.map(([name]) => name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
    // Combined pattern: inline code (preserve) | @(all|所有人|name1|name2|...) followed by non-word
    const mentionRegex = new RegExp(
      `(\`[^\`]+\`)|@(all|所有人|${escaped.join('|')})(?!\\w)`,
      'gi',
    );

    // Split by fenced code blocks (odd indices = code blocks, skip them)
    const parts = text.split(/(```[\s\S]*?```)/g);
    for (let i = 0; i < parts.length; i += 1) {
      if (i % 2 === 1) continue;
      parts[i] = parts[i]!.replace(
        mentionRegex,
        (match, inlineCode: string | undefined, name: string | undefined) => {
          if (inlineCode) return match;
          if (!name) return match;
          const lower = name.toLowerCase();
          if (lower === 'all' || name === '所有人') {
            return '<at id=all></at>';
          }
          const openId = this.nameToIdCache.get(lower);
          if (openId) {
            return `<at id=${openId}>${name}</at>`;
          }
          return match;
        },
      );
    }
    return parts.join('');
  }

  // ── Send ────────────────────────────────────────────────────

  async send(message: OutboundMessage): Promise<SendResult> {
    if (!this.restClient) {
      return { ok: false, error: 'Feishu client not initialized' };
    }

    let text = message.text;

    // Convert HTML to markdown for Feishu rendering (e.g. command responses)
    if (message.parseMode === 'HTML') {
      text = htmlToFeishuMarkdown(text);
    }

    // Preprocess markdown for Claude responses
    if (message.parseMode === 'Markdown') {
      text = preprocessFeishuMarkdown(text);
    }

    // Transform @Name patterns to Lark at-mention tags
    text = this.transformOutboundMentions(text);

    // If there are inline buttons, route to appropriate card type
    if (message.inlineButtons && message.inlineButtons.length > 0) {
      if (message.questionMeta) {
        return this.sendQuestionCard(message.address.chatId, text, message.inlineButtons, message.questionMeta);
      }
      return this.sendPermissionCard(message.address.chatId, text, message.inlineButtons, message.permissionMeta);
    }

    // Rendering strategy (aligned with Openclaw):
    // - Code blocks / tables → interactive card (schema 2.0 markdown)
    // - Other text → post (md tag)
    if (hasComplexMarkdown(text)) {
      return this.sendAsCard(message.address.chatId, text);
    }
    return this.sendAsPost(message.address.chatId, text);
  }

  /**
   * Send text as an interactive card (schema 2.0 markdown).
   * Used for code blocks and tables — card renders them properly.
   */
  private async sendAsCard(chatId: string, text: string): Promise<SendResult> {
    const cardContent = buildCardContent(text);

    try {
      const res = await this.restClient!.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'interactive',
          content: cardContent,
        },
      });

      if (res?.data?.message_id) {
        console.log(`[feishu-adapter] Sent card to ${chatId}, msgId: ${res.data.message_id}`);
        this.cacheBotMessageContent(res.data.message_id, text);
        return { ok: true, messageId: res.data.message_id };
      }
      console.warn('[feishu-adapter] Card send failed:', res?.msg, res?.code);
    } catch (err) {
      console.warn('[feishu-adapter] Card send error, falling back to post:', err instanceof Error ? err.message : err);
    }

    // Fallback to post
    return this.sendAsPost(chatId, text);
  }

  /**
   * Send text as a post message (msg_type: 'post') with md tag.
   * Used for simple text — renders bold, italic, inline code, links.
   */
  private async sendAsPost(chatId: string, text: string): Promise<SendResult> {
    const postContent = buildPostContent(text);

    try {
      const res = await this.restClient!.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'post',
          content: postContent,
        },
      });

      if (res?.data?.message_id) {
        console.log(`[feishu-adapter] Sent post to ${chatId}, msgId: ${res.data.message_id}`);
        this.cacheBotMessageContent(res.data.message_id, text);
        return { ok: true, messageId: res.data.message_id };
      }
      console.warn('[feishu-adapter] Post send failed:', res?.msg, res?.code);
    } catch (err) {
      console.warn('[feishu-adapter] Post send error, falling back to text:', err instanceof Error ? err.message : err);
    }

    // Final fallback: plain text
    try {
      const res = await this.restClient!.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'text',
          content: JSON.stringify({ text }),
        },
      });
      if (res?.data?.message_id) {
        return { ok: true, messageId: res.data.message_id };
      }
      return { ok: false, error: res?.msg || 'Send failed' };
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : 'Send failed' };
    }
  }

  // ── Permission card (with real action buttons) ─────────────

  /**
   * Send a permission card with real Feishu card action buttons.
   * Button clicks trigger card.action.trigger events handled by handleCardActionEvent().
   * Feishu card action callbacks require HTTP webhook (not supported via WSClient).
   * CodePilot is a desktop app without a public endpoint, so we send a
   * well-formatted card with /perm text commands instead of clickable buttons.
   * The user replies with the /perm command to approve/deny.
   */
  protected async sendPermissionCard(
    chatId: string,
    text: string,
    inlineButtons: import('../types.js').InlineButton[][],
    _permissionMeta?: import('../types.js').PermissionCardMeta,
  ): Promise<SendResult> {
    if (!this.restClient) {
      return { ok: false, error: 'Feishu client not initialized' };
    }

    // Build /perm command lines from inline buttons
    const permCommands = inlineButtons.flat().map((btn) => {
      if (btn.callbackData.startsWith('perm:')) {
        const parts = btn.callbackData.split(':');
        const action = parts[1];
        const permId = parts.slice(2).join(':');
        return `\`/perm ${action} ${permId}\``;
      }
      return btn.text;
    });

    // Schema 2.0 card with markdown — permission info + copyable commands
    const cardContent = [
      text,
      '',
      '---',
      '**Reply with one of these commands:**',
      ...permCommands,
    ].join('\n');

    const cardJson = JSON.stringify({
      schema: '2.0',
      config: { wide_screen_mode: true },
      header: {
        template: 'orange',
        title: { tag: 'plain_text', content: '🔐 Permission Required' },
      },
      body: {
        elements: [
          { tag: 'markdown', content: cardContent },
        ],
      },
    });

    try {
      const res = await this.restClient.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'interactive',
          content: cardJson,
        },
      });
      if (res?.data?.message_id) {
        return { ok: true, messageId: res.data.message_id };
      }
      console.warn('[feishu-adapter] Permission card send failed:', res?.msg);
    } catch (err) {
      console.warn('[feishu-adapter] Permission card error:', err instanceof Error ? err.message : err);
    }

    // Fallback: plain text
    const plainCommands = inlineButtons.flat().map((btn) => {
      if (btn.callbackData.startsWith('perm:')) {
        const parts = btn.callbackData.split(':');
        return `/perm ${parts[1]} ${parts.slice(2).join(':')}`;
      }
      return btn.text;
    });
    const fallbackText = text + '\n\nReply with:\n' + plainCommands.join('\n');

    try {
      const res = await this.restClient.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'text',
          content: JSON.stringify({ text: fallbackText }),
        },
      });
      if (res?.data?.message_id) {
        return { ok: true, messageId: res.data.message_id };
      }
      return { ok: false, error: res?.msg || 'Send failed' };
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : 'Send failed' };
    }
  }

  // ── Question card (AskUserQuestion) ─────────────────────────

  /**
   * Send an AskUserQuestion card. Feishu WS mode cannot handle card callbacks,
   * so we render the question as formatted text with /perm commands as fallback.
   */
  protected async sendQuestionCard(
    chatId: string,
    _text: string,
    inlineButtons: import('../types.js').InlineButton[][],
    questionMeta: import('../types.js').QuestionCardMeta,
  ): Promise<SendResult> {
    if (!this.restClient) {
      return { ok: false, error: 'Feishu client not initialized' };
    }

    const q = questionMeta.questions[0];
    const permId = questionMeta.permissionRequestId;

    // Build card content with question and options
    const lines: string[] = [];
    lines.push(`**${q.question}**`);
    lines.push('');
    for (const opt of q.options) {
      lines.push(`• **${opt.label}** — ${opt.description}`);
    }
    lines.push('');
    lines.push('---');

    // Check if buttons are askq (native options) or perm (fallback)
    const hasAskqButtons = inlineButtons.flat().some(btn => btn.callbackData.startsWith('askq:'));
    if (hasAskqButtons) {
      // Feishu can't handle askq callbacks, fall back to /perm allow
      lines.push('**Reply with:** `/perm allow ' + permId + '`');
    } else {
      // Already using perm buttons
      const cmds = inlineButtons.flat().map(btn => {
        const parts = btn.callbackData.split(':');
        return '`/perm ' + parts[1] + ' ' + parts.slice(2).join(':') + '`';
      });
      lines.push('**Reply with:**');
      lines.push(...cmds);
    }

    const cardJson = JSON.stringify({
      schema: '2.0',
      config: { wide_screen_mode: true },
      header: {
        template: 'blue',
        title: { tag: 'plain_text', content: `❓ ${q.header}` },
      },
      body: {
        elements: [
          { tag: 'markdown', content: lines.join('\n') },
        ],
      },
    });

    try {
      const res = await this.restClient.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: {
          receive_id: chatId,
          msg_type: 'interactive',
          content: cardJson,
        },
      });
      if (res?.data?.message_id) {
        return { ok: true, messageId: res.data.message_id };
      }
      console.warn('[feishu-adapter] Question card send failed:', res?.msg);
    } catch (err) {
      console.warn('[feishu-adapter] Question card error:', err instanceof Error ? err.message : err);
    }

    // Fallback: send as text
    return this.sendPermissionCard(chatId, lines.join('\n'), inlineButtons);
  }

  // ── Config & Auth ───────────────────────────────────────────

  validateConfig(): string | null {
    const enabled = this.setting('enabled');
    if (enabled !== 'true') return `bridge_${this.channelType}_enabled is not true`;

    const appId = this.setting('app_id');
    if (!appId) return `bridge_${this.channelType}_app_id not configured`;

    const appSecret = this.setting('app_secret');
    if (!appSecret) return `bridge_${this.channelType}_app_secret not configured`;

    return null;
  }

  isAuthorized(userId: string, chatId: string): boolean {
    const allowedUsers = this.setting('allowed_users');
    if (!allowedUsers) {
      // No restriction configured — allow all
      return true;
    }

    const allowed = allowedUsers
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);

    if (allowed.length === 0) return true;

    return allowed.includes(userId) || allowed.includes(chatId);
  }

  // ── Incoming event handler ──────────────────────────────────

  protected async handleIncomingEvent(data: FeishuMessageEventData): Promise<void> {
    try {
      await this.processIncomingEvent(data);
    } catch (err) {
      console.error(
        '[feishu-adapter] Unhandled error in event handler:',
        err instanceof Error ? err.stack || err.message : err,
      );
    }
  }

  private async processIncomingEvent(data: FeishuMessageEventData): Promise<void> {
    const msg = data.message;
    const sender = data.sender;

    // [P1] Filter out bot messages to prevent self-triggering loops
    if (sender.sender_type === 'bot') return;

    // Dedup by message_id
    if (this.seenMessageIds.has(msg.message_id)) return;
    this.addToDedup(msg.message_id);

    const chatId = msg.chat_id;
    // [P2] Complete sender ID fallback chain: open_id > user_id > union_id
    const userId = sender.sender_id?.open_id
      || sender.sender_id?.user_id
      || sender.sender_id?.union_id
      || '';
    const isGroup = msg.chat_type === 'group';

    // Authorization check
    if (!this.isAuthorized(userId, chatId)) {
      console.warn('[feishu-adapter] Unauthorized message from userId:', userId, 'chatId:', chatId);
      return;
    }

    let observeOnly = false;

    // Group chat policy
    if (isGroup) {
      const policy = this.setting('group_policy') || 'open';

      if (policy === 'disabled') {
        console.log('[feishu-adapter] Group message ignored (policy=disabled), chatId:', chatId);
        return;
      }

      if (policy === 'allowlist') {
        const allowedGroups = (this.setting('group_allow_from'))
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean);
        if (!allowedGroups.includes(chatId)) {
          console.log('[feishu-adapter] Group message ignored (not in allowlist), chatId:', chatId);
          return;
        }
      }

      // Require @mention check — if not mentioned, observe only (store context, no LLM reply)
      const requireMention = this.setting('require_mention') !== 'false';
      if (requireMention && !this.isBotMentioned(msg.mentions)) {
        observeOnly = true;
      }
    }

    // Track last message ID per chat for typing indicator
    if (!observeOnly) {
      this.lastIncomingMessageId.set(chatId, msg.message_id);
    }

    // Resolve timestamp and sender name early — needed by disk-first download for filenames
    const timestamp = parseInt(msg.create_time, 10) || Date.now();
    let displayName: string | undefined;
    if (userId) {
      const name = await this.resolveUserDisplayName(userId, chatId)
        || this.extractSenderNameFromMentions(msg.mentions, userId)
        || undefined;
      if (name) displayName = name;
    }

    // Extract content based on message type
    const messageType = msg.message_type;
    let text = '';
    const attachments: FileAttachment[] = [];

    if (messageType === 'text') {
      text = this.parseTextContent(msg.content);
    } else if (messageType === 'image') {
      console.log('[feishu-adapter] Image message received, content:', msg.content);
      const fileKey = this.extractFileKey(msg.content);
      console.log('[feishu-adapter] Extracted fileKey:', fileKey);
      if (fileKey) {
        if (observeOnly) {
          // Disk-first: download to file, store path in text
          const destDir = this.resolveUploadDir(chatId);
          const result = await this.downloadResourceToDisk(msg.message_id, fileKey, 'image', destDir, displayName || userId || 'unknown', timestamp);
          text = result
            ? `sent a photo [image: ${result.filePath}]`
            : '[image download failed]';
        } else {
          const attachment = await this.downloadResource(msg.message_id, fileKey, 'image');
          if (attachment) {
            attachments.push(attachment);
          } else {
            text = '[image download failed]';
            try {
              getBridgeContext().store.insertAuditLog({
                channelType: this.channelType,
                chatId,
                direction: 'inbound',
                messageId: msg.message_id,
                summary: `[ERROR] Image download failed for key: ${fileKey}`,
              });
            } catch { /* best effort */ }
          }
        }
      }
    } else if (messageType === 'post') {
      const { extractedText, imageKeys } = this.parsePostContent(msg.content, msg.mentions);
      text = extractedText;
      if (observeOnly) {
        // Disk-first: download post images to file, append paths to text
        const destDir = this.resolveUploadDir(chatId);
        for (const key of imageKeys) {
          const result = await this.downloadResourceToDisk(msg.message_id, key, 'image', destDir, displayName || userId || 'unknown', timestamp);
          if (result) {
            text += ` [image: ${result.filePath}]`;
          }
        }
      } else {
        for (const key of imageKeys) {
          const attachment = await this.downloadResource(msg.message_id, key, 'image');
          if (attachment) {
            attachments.push(attachment);
          }
        }
      }
    } else if (messageType === 'merge_forward') {
      text = await this.parseMergeForward(msg.message_id, chatId);
    } else if (messageType === 'file' || messageType === 'audio' || messageType === 'video' || messageType === 'media') {
      const fileKey = this.extractFileKey(msg.content, messageType);
      if (fileKey) {
        const resourceType = messageType === 'audio' || messageType === 'video' || messageType === 'media'
          ? messageType
          : 'file';
        let fileName: string | undefined;
        try { fileName = JSON.parse(msg.content).file_name; } catch { /* ignore */ }
        if (observeOnly) {
          // Disk-first: download to file, store path in text
          const destDir = this.resolveUploadDir(chatId);
          const result = await this.downloadResourceToDisk(msg.message_id, fileKey, resourceType, destDir, displayName || userId || 'unknown', timestamp, fileName);
          text = result
            ? `sent a ${messageType} [${messageType}: ${result.filePath}]`
            : `[${messageType} download failed]`;
        } else {
          const attachment = await this.downloadResource(msg.message_id, fileKey, resourceType, fileName);
          if (attachment) {
            attachments.push(attachment);
          } else {
            text = `[${messageType} download failed]`;
            try {
              getBridgeContext().store.insertAuditLog({
                channelType: this.channelType,
                chatId,
                direction: 'inbound',
                messageId: msg.message_id,
                summary: `[ERROR] ${messageType} download failed for key: ${fileKey}`,
              });
            } catch { /* best effort */ }
          }
        }
      }
    } else if (messageType === 'interactive') {
      // Interactive card — reuse extractCardContent (wrapping msg to match expected shape)
      text = this.extractCardContent({ body: { content: msg.content } }) || '[interactive card]';
    } else if (messageType === 'folder') {
      // Lark API does not support downloading folder contents (messageResource.get returns 234003).
      // Extract folder name for context and guide user to send files individually.
      let folderName = 'unknown';
      try { folderName = JSON.parse(msg.content).file_name || 'unknown'; } catch { /* ignore */ }
      text = `[folder: ${folderName} — Lark API does not support folder downloads. Please send files individually or as a zip.]`;
    } else {
      // Unsupported type — show placeholder instead of silently dropping
      text = `[${messageType}: unsupported message type]`;
    }

    // Resolve @mention placeholders to actual names
    text = this.resolveMentionMarkers(text, msg.mentions);

    // Fetch quoted message content if this is a reply
    console.log('[feishu-adapter] parent_id:', msg.parent_id, 'root_id:', msg.root_id, 'msgId:', msg.message_id);
    if (msg.parent_id) {
      const quoted = await this.fetchQuotedContent(msg.parent_id, chatId);
      if (quoted) {
        text = `${quoted}\n\n${text}`;
      }
    }

    if (!text.trim() && attachments.length === 0 && !isGroup) return;

    const address = {
      channelType: this.channelType,
      chatId,
      userId,
      ...(displayName ? { displayName } : {}),
      ...(isGroup ? { isGroup } : {}),
    };

    // [P1] Check for /perm text command (permission approval fallback)
    const trimmedText = text.trim();
    if (trimmedText.startsWith('/perm ')) {
      const permParts = trimmedText.split(/\s+/);
      // /perm <action> <permId>
      if (permParts.length >= 3) {
        const action = permParts[1]; // allow / allow_session / deny
        const permId = permParts.slice(2).join(' ');
        const callbackData = `perm:${action}:${permId}`;

        const inbound: InboundMessage = {
          messageId: msg.message_id,
          address,
          text: trimmedText,
          timestamp,
          callbackData,
        };
        this.enqueue(inbound);
        return;
      }
    }

    const inbound: InboundMessage = {
      messageId: msg.message_id,
      address,
      text: text.trim(),
      timestamp,
      attachments: attachments.length > 0 ? attachments : undefined,
      ...(observeOnly ? { observeOnly } : {}),
    };

    const senderLabel = displayName ? `${displayName} (${userId})` : userId;
    const modeLabel = observeOnly ? ' (observe)' : '';
    console.log(`[feishu-adapter] Received ${messageType} from ${senderLabel} in ${chatId}${modeLabel}`);
    // Audit log
    try {
      const summary = attachments.length > 0
        ? `[${attachments.length} attachment(s)] ${text.slice(0, 150)}`
        : text.slice(0, 200);
      getBridgeContext().store.insertAuditLog({
        channelType: this.channelType,
        chatId,
        direction: 'inbound',
        messageId: msg.message_id,
        summary,
        userId,
        senderName: displayName,
      });
    } catch { /* best effort */ }

    this.enqueue(inbound);
  }

  // ── Content parsing ─────────────────────────────────────────

  private parseTextContent(content: string): string {
    try {
      const parsed = JSON.parse(content);
      return parsed.text || '';
    } catch {
      return content;
    }
  }

  /**
   * Extract file key from message content JSON.
   * Handles multiple key names: image_key, file_key, imageKey, fileKey.
   */
  private extractFileKey(content: string, messageType?: string): string | null {
    try {
      const parsed = JSON.parse(content);
      // For video/media, prefer file_key (actual video) over image_key (thumbnail)
      if (messageType === 'video' || messageType === 'media') {
        return parsed.file_key || parsed.fileKey || parsed.image_key || parsed.imageKey || null;
      }
      return parsed.image_key || parsed.file_key || parsed.imageKey || parsed.fileKey || null;
    } catch {
      return null;
    }
  }

  /**
   * Fetch and format sub-messages of a merge_forward message.
   * Uses GET /im/v1/messages/{message_id} which returns sub-messages
   * with upper_message_id pointing to the parent merge_forward message.
   */
  private async parseMergeForward(messageId: string, chatId: string): Promise<string> {
    if (!this.restClient) return '[forwarded conversation]';

    try {
      const subMessages = await this.fetchMergeForwardItems(messageId);
      if (subMessages.length === 0) return '[forwarded conversation (empty)]';

      // Log all sub-message types for diagnostic (ETH-96)
      console.log(`[feishu-adapter] parseMergeForward: ${subMessages.length} sub-messages, types: [${subMessages.map((m: any) => m.msg_type).join(', ')}]`);

      const lines: string[] = ['[Forwarded conversation]'];
      let hasFailedCards = false;

      for (const item of subMessages) {
        const msgType: string = item.msg_type || '';
        const rawContent: string = item.body?.content || '';
        const senderId: string | undefined = item.sender?.id;

        // Resolve sender name
        let senderName = 'Unknown';
        if (senderId && senderId.startsWith('cli_')) {
          senderName = 'bot';
        } else if (senderId) {
          senderName = await this.resolveUserDisplayName(senderId, chatId) || senderId;
        }

        // Parse content by type
        let content: string;
        if (msgType === 'text') {
          content = this.parseTextContent(rawContent);
        } else if (msgType === 'post') {
          const { extractedText } = this.parsePostContent(rawContent, item.mentions);
          content = extractedText || '[post]';
        } else if (msgType === 'image') {
          content = '[image]';
        } else if (msgType === 'file') {
          try { content = `[file: ${JSON.parse(rawContent).file_name || 'unknown'}]`; } catch { content = '[file]'; }
        } else if (msgType === 'folder') {
          try { content = `[folder: ${JSON.parse(rawContent).file_name || 'unknown'}]`; } catch { content = '[folder]'; }
        } else if (msgType === 'merge_forward') {
          content = '[nested forwarded conversation]';
        } else if (msgType === 'interactive') {
          // Extract card content via raw_card_content (ETH-89)
          content = this.extractCardContent(item) || '[interactive]';
          if (content === '[interactive]') {
            hasFailedCards = true;
            console.warn(`[feishu-adapter] parseMergeForward: interactive card extraction FAILED, msg_id: ${item.message_id}, body keys: ${item.body ? Object.keys(item.body) : 'no body'}, raw (200 chars): ${rawContent.slice(0, 200)}`);
          }
        } else {
          content = `[${msgType}]`;
        }

        lines.push(`${senderName}: ${content}`);
      }

      // Retry once if any interactive cards failed — Lark API returns
      // inconsistent card content non-deterministically (ETH-96)
      if (hasFailedCards) {
        console.log('[feishu-adapter] parseMergeForward: retrying GET due to failed card extractions');
        await new Promise(r => setTimeout(r, 300));
        const retryItems = await this.fetchMergeForwardItems(messageId);
        // Same forward → same order. Patch failed cards by index.
        for (let i = 0; i < subMessages.length && i < retryItems.length; i++) {
          if (subMessages[i].msg_type !== 'interactive') continue;
          if (!lines[i + 1]?.includes('[interactive]')) continue; // already OK
          if (retryItems[i]?.msg_type !== 'interactive') continue;
          const retried = this.extractCardContent(retryItems[i]);
          if (retried) {
            const senderPart = lines[i + 1].split(': ')[0];
            lines[i + 1] = `${senderPart}: ${retried}`;
            console.log(`[feishu-adapter] parseMergeForward: retry recovered card, length: ${retried.length}`);
          }
        }
      }

      return lines.join('\n');
    } catch (err) {
      console.error('[feishu-adapter] Failed to parse merge_forward:', err instanceof Error ? err.message : err);
      return '[forwarded conversation (failed to load)]';
    }
  }

  /** Fetch sub-messages of a merge_forward message via im.message.get. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async fetchMergeForwardItems(messageId: string): Promise<any[]> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const resp = await (this.restClient as any).im.message.get({
      path: { message_id: messageId },
      params: { card_msg_content_type: 'raw_card_content' },
    });
    const items = resp?.data?.items;
    if (!items || items.length === 0) return [];
    return items.filter((item: any) => item.upper_message_id === messageId);
  }

  /**
   * Parse rich text (post) content.
   * Extracts plain text from text elements and image keys from img elements.
   */
  private parsePostContent(
    content: string,
    mentions?: FeishuMessageEventData['message']['mentions'] | Array<{ key: string; id: string; name: string }>,
  ): { extractedText: string; imageKeys: string[] } {
    const imageKeys: string[] = [];
    const textParts: string[] = [];

    try {
      const parsed = JSON.parse(content);
      // Post content structure: { title, content: [[{tag, text/image_key}]] }
      const title = parsed.title;
      if (title) textParts.push(title);

      const paragraphs = parsed.content;
      if (Array.isArray(paragraphs)) {
        for (const paragraph of paragraphs) {
          if (!Array.isArray(paragraph)) continue;
          for (const element of paragraph) {
            if (element.tag === 'text' && element.text) {
              textParts.push(element.text);
            } else if (element.tag === 'a' && element.text) {
              textParts.push(element.text);
            } else if (element.tag === 'at') {
              // Resolve mention name from mentions array or element metadata
              // Supports both WebSocket event format ({ id: { open_id } }) and API response format ({ id: string })
              const userId = element.user_id;
              const matched = mentions?.find(m => {
                const mid = m.id;
                if (typeof mid === 'string') return mid === userId;
                return mid.open_id === userId || mid.user_id === userId || mid.union_id === userId;
              });
              if (matched?.name) {
                textParts.push(`@${matched.name}`);
              } else if (element.user_name) {
                textParts.push(`@${element.user_name}`);
              } else if (userId === 'all') {
                textParts.push('@all');
              } else {
                textParts.push('@[user]');
              }
            } else if (element.tag === 'img') {
              const key = element.image_key || element.file_key || element.imageKey;
              if (key) imageKeys.push(key);
            }
          }
          textParts.push('\n');
        }
      }
    } catch {
      // Failed to parse post content
    }

    return { extractedText: textParts.join('').trim(), imageKeys };
  }

  /**
   * Extract text content from an interactive card message item.
   * With raw_card_content param, json_card is inside body.content (not at item level).
   * Structure: item.body.content → JSON → { json_card: string, card_schema: number }
   * Falls back to v1 extractInteractiveText if json_card not found. (ETH-89)
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private extractCardContent(item: any): string | null {
    const rawContent: string = item.body?.content || '';
    if (!rawContent) { console.log('[feishu-adapter] extractCardContent: no body.content'); return null; }

    try {
      const parsed = JSON.parse(rawContent);
      console.log('[feishu-adapter] extractCardContent: body.content keys:', Object.keys(parsed), 'has json_card:', !!parsed.json_card);

      // raw_card_content wraps json_card inside body.content
      if (parsed.json_card) {
        const card = typeof parsed.json_card === 'string' ? JSON.parse(parsed.json_card) : parsed.json_card;
        console.log('[feishu-adapter] extractCardContent: json_card keys:', Object.keys(card));
        const text = this.extractInteractiveText(card);
        console.log('[feishu-adapter] extractCardContent: extracted text length:', text?.length, 'preview:', text?.slice(0, 100));
        if (text) return text;
      }

      // Fallback: v1 degraded format (elements array directly in body.content)
      const v1text = this.extractInteractiveText(parsed) || null;
      console.log('[feishu-adapter] extractCardContent: v1 fallback result:', v1text?.slice(0, 100));
      return v1text;
    } catch (e) { console.log('[feishu-adapter] extractCardContent: parse error:', e); return null; }
  }

  /**
   * Recursively extract text from an interactive card JSON structure.
   * Handles both Card v1 (legacy) and Card v2 (schema 2.0) formats.
   */
  private extractInteractiveText(card: Record<string, unknown>): string {
    const parts: string[] = [];

    // Card title — check both header.title.content (Card v2) and top-level title (Card v1)
    const header = card.header as Record<string, unknown> | undefined;
    if (header) {
      const title = header.title as Record<string, unknown> | undefined;
      if (title?.content && typeof title.content === 'string') {
        parts.push(title.content);
      }
    }
    if (typeof card.title === 'string' && card.title) {
      parts.push(card.title);
    }

    // Recursively extract text from elements
    const walk = (node: unknown): void => {
      if (!node || typeof node !== 'object') return;
      if (Array.isArray(node)) {
        for (const item of node) walk(item);
        return;
      }
      const obj = node as Record<string, unknown>;
      const tag = obj.tag as string | undefined;
      // raw_card_content nests content inside `property` (ETH-89)
      const prop = obj.property as Record<string, unknown> | undefined;

      // Text elements — check both direct fields and property-wrapped fields
      const directContent = typeof obj.content === 'string' ? obj.content : null;
      const propContent = prop && typeof prop.content === 'string' ? prop.content : null;
      const content = directContent || propContent;

      if (tag === 'text' && typeof obj.text === 'string' && obj.text.trim()) {
        parts.push(obj.text);
      } else if ((tag === 'plain_text' || tag === 'lark_md' || tag === 'markdown' || tag === 'code_span') && content?.trim()) {
        parts.push(content);
      } else if (tag === 'div') {
        // property-wrapped: { tag: 'div', property: { text: {...} } }
        const textChild = obj.text || prop?.text;
        if (textChild) walk(textChild);
        if (obj.extra) walk(obj.extra);
      } else if (tag === 'column_set' || tag === 'column') {
        // property-wrapped: { tag: 'column_set', property: { columns: [...] } }
        const cols = obj.columns || prop?.columns;
        const elems = obj.elements || prop?.elements;
        if (cols) walk(cols);
        if (elems) walk(elems);
      } else if (tag === 'button') {
        // property-wrapped: { tag: 'button', property: { text: { property: { content } }, url } }
        const textObj = (obj.text || prop?.text) as Record<string, unknown> | undefined;
        const label = textObj?.content || (textObj?.property as Record<string, unknown>)?.content || '';
        const url = obj.url || prop?.url || '';
        if (label && url) parts.push(`[${label}](${url})`);
        else if (label) parts.push(String(label));
      } else if (tag === 'img') {
        parts.push('[image]');
      } else if (tag === 'br') {
        // Line break in raw_card_content — skip
      }

      // Recurse into elements/body — check both direct and property-wrapped
      if (obj.elements) walk(obj.elements);
      if (obj.actions || prop?.actions) walk(obj.actions || prop?.actions);
      if (prop?.elements) walk(prop.elements);
      if (obj.body && typeof obj.body === 'object') {
        const body = obj.body as Record<string, unknown>;
        walk(body.elements);
        // raw_card_content wraps body elements in property
        if (body.property) walk((body.property as Record<string, unknown>).elements);
      }
    };

    walk(card.elements || card.body || card.newBody);
    return parts.join('\n').trim();
  }

  /**
   * Cache bot-sent message content for quote resolution.
   * Lark's im.message.get doesn't return PATCH-updated card content,
   * so we cache the plaintext locally.
   */
  private cacheBotMessageContent(messageId: string, text: string): void {
    if (this.botMessageContentCache.size >= FeishuAdapter.BOT_CONTENT_CACHE_MAX) {
      const firstKey = this.botMessageContentCache.keys().next().value;
      if (firstKey) this.botMessageContentCache.delete(firstKey);
    }
    this.botMessageContentCache.set(messageId, text);
  }

  // ── Bot identity ────────────────────────────────────────────

  /**
   * Resolve bot identity via the Feishu REST API /bot/v3/info/.
   * Collects all available bot IDs for comprehensive mention matching.
   */
  protected async resolveBotIdentity(
    appId: string,
    appSecret: string,
    domain: lark.Domain,
  ): Promise<void> {
    try {
      const baseUrl = domain === lark.Domain.Lark
        ? 'https://open.larksuite.com'
        : 'https://open.feishu.cn';

      const tokenRes = await fetch(`${baseUrl}/open-apis/auth/v3/tenant_access_token/internal`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ app_id: appId, app_secret: appSecret }),
        signal: AbortSignal.timeout(10_000),
      });
      const tokenData: any = await tokenRes.json();
      if (!tokenData.tenant_access_token) {
        console.warn('[feishu-adapter] Failed to get tenant access token');
        return;
      }

      const botRes = await fetch(`${baseUrl}/open-apis/bot/v3/info/`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${tokenData.tenant_access_token}` },
        signal: AbortSignal.timeout(10_000),
      });
      const botData: any = await botRes.json();
      if (botData?.bot?.open_id) {
        this.botOpenId = botData.bot.open_id;
        this.botIds.add(botData.bot.open_id);
      }
      // Also record app_id-based IDs if available
      if (botData?.bot?.bot_id) {
        this.botIds.add(botData.bot.bot_id);
      }
      if (!this.botOpenId) {
        console.warn('[feishu-adapter] Could not resolve bot open_id');
      }
    } catch (err) {
      console.warn(
        '[feishu-adapter] Failed to resolve bot identity:',
        err instanceof Error ? err.message : err,
      );
    }
  }

  // ── @Mention detection ──────────────────────────────────────

  /**
   * [P2] Check if bot is mentioned — matches against open_id, user_id, union_id.
   */
  private isBotMentioned(
    mentions?: FeishuMessageEventData['message']['mentions'],
  ): boolean {
    if (!mentions || this.botIds.size === 0) return false;
    return mentions.some((m) => {
      const ids = [m.id.open_id, m.id.user_id, m.id.union_id].filter(Boolean) as string[];
      return ids.some((id) => this.botIds.has(id));
    });
  }

  private resolveMentionMarkers(
    text: string,
    mentions?: FeishuMessageEventData['message']['mentions'],
  ): string {
    if (!mentions || mentions.length === 0) {
      return text.replace(/@_user_\d+/g, '@[user]').trim();
    }
    // Replace all @_user_N placeholders with actual names to preserve semantics
    for (const m of mentions) {
      if (m.key && m.name) {
        text = text.replace(m.key, `@${m.name}`);
      }
    }
    // Replace any unresolved placeholders with visible marker instead of silent strip
    return text.replace(/@_user_\d+/g, '@[user]').trim();
  }

  // ── Resource download ───────────────────────────────────────

  /**
   * Download a message resource (image/file/audio/video) via SDK.
   * Returns null on failure (caller decides fallback behavior).
   */
  private async downloadResource(
    messageId: string,
    fileKey: string,
    resourceType: string,
    fileName?: string,
  ): Promise<FileAttachment | null> {
    if (!this.restClient) return null;

    try {
      console.log(`[feishu-adapter] Downloading resource: type=${resourceType}, key=${fileKey}, msgId=${messageId}`);

      const res = await this.restClient.im.messageResource.get({
        path: {
          message_id: messageId,
          file_key: fileKey,
        },
        params: {
          type: resourceType === 'image' ? 'image' : 'file',
        },
      });

      if (!res) {
        console.warn('[feishu-adapter] messageResource.get returned null/undefined');
        return null;
      }

      // SDK returns { writeFile, getReadableStream, headers }
      // Try stream approach first, fall back to writeFile + read if stream fails
      let buffer: Buffer;

      try {
        const readable = res.getReadableStream();
        const chunks: Buffer[] = [];
        let totalSize = 0;

        for await (const chunk of readable) {
          const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
          totalSize += buf.length;
          if (totalSize > MAX_FILE_SIZE) {
            console.warn(`[feishu-adapter] Resource too large (>${MAX_FILE_SIZE} bytes), key: ${fileKey}`);
            return null;
          }
          chunks.push(buf);
        }
        buffer = Buffer.concat(chunks);
      } catch (streamErr) {
        // Stream approach failed — fall back to writeFile + read
        console.warn('[feishu-adapter] Stream read failed, falling back to writeFile:', streamErr instanceof Error ? streamErr.message : streamErr);

        const fs = await import('fs');
        const os = await import('os');
        const path = await import('path');
        const tmpPath = path.join(os.tmpdir(), `feishu-dl-${crypto.randomUUID()}`);
        try {
          await res.writeFile(tmpPath);
          const stat = fs.statSync(tmpPath);
          if (stat.size > MAX_FILE_SIZE) {
            console.warn(`[feishu-adapter] Resource too large (>${MAX_FILE_SIZE} bytes), key: ${fileKey}`);
            return null;
          }
          buffer = fs.readFileSync(tmpPath);
          if (buffer.length > MAX_FILE_SIZE) {
            console.warn(`[feishu-adapter] Resource too large (>${MAX_FILE_SIZE} bytes), key: ${fileKey}`);
            return null;
          }
        } finally {
          try { fs.unlinkSync(tmpPath); } catch { /* ignore cleanup errors */ }
        }
      }

      if (!buffer || buffer.length === 0) {
        console.warn('[feishu-adapter] Downloaded resource is empty, key:', fileKey);
        return null;
      }

      const base64 = buffer.toString('base64');
      const id = crypto.randomUUID();
      const fallbackExt = resourceType === 'image' ? 'png'
        : resourceType === 'audio' ? 'ogg'
        : resourceType === 'video' ? 'mp4'
        : 'bin';
      const resolvedName = fileName || `${fileKey}.${fallbackExt}`;
      const extFromName = resolvedName.includes('.') ? resolvedName.split('.').pop()! : fallbackExt;
      const mimeType = MIME_BY_EXT[extFromName] || MIME_BY_TYPE[resourceType] || 'application/octet-stream';

      console.log(`[feishu-adapter] Resource downloaded: ${buffer.length} bytes, key=${fileKey}`);

      return {
        id,
        name: resolvedName,
        type: mimeType,
        size: buffer.length,
        data: base64,
      };
    } catch (err) {
      console.error(
        `[feishu-adapter] Resource download failed (type=${resourceType}, key=${fileKey}):`,
        err instanceof Error ? err.stack || err.message : err,
      );
      return null;
    }
  }

  // ── Disk-first resource download (observe mode) ────────────

  /**
   * Resolve the upload directory for a given chat.
   * Uses the channel binding's workingDirectory, falls back to bridge_default_work_dir or $HOME.
   */
  private resolveUploadDir(chatId: string): string {
    const { store } = getBridgeContext();
    const binding = store.getChannelBinding(this.channelType, chatId);
    const baseDir = binding?.workingDirectory
      || store.getSetting('bridge_default_work_dir')
      || process.env.HOME
      || '';
    return `${baseDir}/.codepilot-uploads`;
  }

  /**
   * Download a resource directly to disk (no base64 intermediary).
   * Returns file metadata on success, null on failure.
   */
  private async downloadResourceToDisk(
    messageId: string,
    fileKey: string,
    resourceType: string,
    destDir: string,
    senderName: string,
    timestamp: number,
    fileName?: string,
  ): Promise<{ filePath: string; name: string; mimeType: string; size: number } | null> {
    if (!this.restClient) return null;

    const fs = await import('fs');
    const path = await import('path');

    try {
      console.log(`[feishu-adapter] Downloading resource to disk: type=${resourceType}, key=${fileKey}, msgId=${messageId}`);

      // Ensure dest dir exists
      fs.mkdirSync(destDir, { recursive: true });

      const res = await this.restClient.im.messageResource.get({
        path: { message_id: messageId, file_key: fileKey },
        params: { type: resourceType === 'image' ? 'image' : 'file' },
      });

      if (!res) {
        console.warn('[feishu-adapter] messageResource.get returned null/undefined');
        return null;
      }

      // Build filename: YYYYMMDD-HHmmss-sender-originalName
      const d = new Date(timestamp);
      const ts = `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}-${String(d.getHours()).padStart(2, '0')}${String(d.getMinutes()).padStart(2, '0')}${String(d.getSeconds()).padStart(2, '0')}`;
      const safeSender = senderName.replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 20) || 'unknown';
      const fallbackExt = resourceType === 'image' ? 'png'
        : resourceType === 'audio' ? 'ogg'
        : resourceType === 'video' ? 'mp4'
        : 'bin';
      const resolvedName = fileName || `${fileKey}.${fallbackExt}`;
      const destPath = path.join(destDir, `${ts}-${safeSender}-${resolvedName}`);

      // Stream to disk with size guard
      let totalSize = 0;
      try {
        const readable = res.getReadableStream();
        const writeStream = fs.createWriteStream(destPath);
        for await (const chunk of readable) {
          const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
          totalSize += buf.length;
          if (totalSize > MAX_FILE_SIZE_DISK) {
            await new Promise<void>((resolve) => { writeStream.end(() => resolve()); });
            try { fs.unlinkSync(destPath); } catch { /* cleanup */ }
            console.warn(`[feishu-adapter] Resource too large (>${MAX_FILE_SIZE_DISK} bytes), key: ${fileKey}`);
            return null;
          }
          writeStream.write(buf);
        }
        await new Promise<void>((resolve, reject) => {
          writeStream.end(() => resolve());
          writeStream.on('error', reject);
        });
      } catch (streamErr) {
        // Fallback: writeFile to temp, then rename
        console.warn('[feishu-adapter] Stream write failed, falling back to writeFile:', streamErr instanceof Error ? streamErr.message : streamErr);
        const os = await import('os');
        const tmpPath = path.join(os.tmpdir(), `feishu-dl-${crypto.randomUUID()}`);
        try {
          await res.writeFile(tmpPath);
          const stat = fs.statSync(tmpPath);
          if (stat.size > MAX_FILE_SIZE_DISK) {
            console.warn(`[feishu-adapter] Resource too large (>${MAX_FILE_SIZE_DISK} bytes), key: ${fileKey}`);
            return null;
          }
          totalSize = stat.size;
          fs.renameSync(tmpPath, destPath);
        } finally {
          try { fs.unlinkSync(tmpPath); } catch { /* ignore */ }
        }
      }

      if (totalSize === 0) {
        try { fs.unlinkSync(destPath); } catch { /* ignore */ }
        console.warn('[feishu-adapter] Downloaded resource is empty, key:', fileKey);
        return null;
      }

      const extFromName = resolvedName.includes('.') ? resolvedName.split('.').pop()! : fallbackExt;
      const mimeType = MIME_BY_EXT[extFromName] || MIME_BY_TYPE[resourceType] || 'application/octet-stream';

      console.log(`[feishu-adapter] Resource saved to disk: ${destPath} (${totalSize} bytes)`);

      return { filePath: destPath, name: resolvedName, mimeType, size: totalSize };
    } catch (err) {
      console.error(
        `[feishu-adapter] Resource disk download failed (type=${resourceType}, key=${fileKey}):`,
        err instanceof Error ? err.stack || err.message : err,
      );
      return null;
    }
  }

  // ── Utilities ───────────────────────────────────────────────

  private addToDedup(messageId: string): void {
    this.seenMessageIds.set(messageId, true);

    // LRU eviction: remove oldest entries when exceeding limit
    if (this.seenMessageIds.size > DEDUP_MAX) {
      const excess = this.seenMessageIds.size - DEDUP_MAX;
      let removed = 0;
      for (const key of this.seenMessageIds.keys()) {
        if (removed >= excess) break;
        this.seenMessageIds.delete(key);
        removed++;
      }
    }
  }
}

// Self-register so bridge-manager can create FeishuAdapter via the registry.
registerAdapterFactory('feishu', () => new FeishuAdapter());
