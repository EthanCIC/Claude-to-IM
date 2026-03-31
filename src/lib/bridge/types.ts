/**
 * Bridge system types — shared across all bridge modules.
 *
 * The bridge connects external IM channels (Telegram, Discord, Slack)
 * to CodePilot chat sessions, allowing users to interact with Claude
 * from their preferred messaging platform.
 */

// Re-export bridge-local types from host.ts so consumers can import from one place
export type { FileAttachment } from './host.js';

/** User permission role for access control. undefined = no role enforcement (legacy). */
export type UserRole = 'admin' | 'regular';

// ── Channel Types ──────────────────────────────────────────────

/**
 * Channel type identifier.
 * Extensible — any string is valid so new adapters can register without
 * modifying this definition. Well-known values: 'telegram', 'discord', 'slack'.
 */
export type ChannelType = string;

/** Unique address of a user within a channel */
export interface ChannelAddress {
  channelType: ChannelType;
  chatId: string;        // Platform-specific chat/channel identifier
  userId?: string;       // Platform-specific user identifier (optional for group chats)
  displayName?: string;  // Human-readable name for audit logs
  isGroup?: boolean;     // Whether this is a group/channel chat
}

/** Composite key for routing: channelType + chatId */
export interface SessionKey {
  channelType: ChannelType;
  chatId: string;
}

// ── Messages ───────────────────────────────────────────────────

/** Inbound message from an IM channel */
export interface InboundMessage {
  /** Platform-specific message ID (for dedup and reference) */
  messageId: string;
  /** Address of the sender */
  address: ChannelAddress;
  /** Plain text content of the message */
  text: string;
  /** Timestamp of the message (ISO string or unix epoch ms) */
  timestamp: number;
  /** If this is a callback query (inline button press), the callback data */
  callbackData?: string;
  /** For callback queries: the message ID of the original message that triggered the callback */
  callbackMessageId?: string;
  /** Platform-specific raw update object (for adapter-specific handling) */
  raw?: unknown;
  /** Adapter-specific update ID for deferred offset acknowledgement */
  updateId?: number;
  /** File attachments (images, documents) from the IM channel */
  attachments?: import('./host.js').FileAttachment[];
  /** Observe-only: store in context buffer but don't trigger LLM response (group chat without @mention) */
  observeOnly?: boolean;
}

/** Outbound message to send to an IM channel */
export interface OutboundMessage {
  /** Target address */
  address: ChannelAddress;
  /** Message text (may contain HTML for Telegram) */
  text: string;
  /** Parse mode for the text */
  parseMode?: 'HTML' | 'Markdown' | 'plain';
  /** Inline keyboard buttons */
  inlineButtons?: InlineButton[][];
  /** Structured permission metadata (for adapters that build native cards) */
  permissionMeta?: PermissionCardMeta;
  /** Structured question metadata for AskUserQuestion cards */
  questionMeta?: QuestionCardMeta;
  /** If replying to a specific message */
  replyToMessageId?: string;
}

/** Inline keyboard button for permission prompts */
export interface InlineButton {
  text: string;
  callbackData: string;
}

/** Structured metadata for permission cards (avoids HTML parsing in adapters) */
export interface PermissionCardMeta {
  toolName: string;
  toolInput: Record<string, unknown>;
}

/** Structured metadata for AskUserQuestion cards */
export interface QuestionCardMeta {
  permissionRequestId: string;
  questions: Array<{
    question: string;
    header: string;
    options: Array<{ label: string; description: string }>;
    multiSelect: boolean;
  }>;
}

/** Result of sending a message via an adapter */
export interface SendResult {
  ok: boolean;
  /** Platform-specific message ID of the sent message */
  messageId?: string;
  error?: string;
}

// ── Bindings ───────────────────────────────────────────────────

/** Links an IM chat to a CodePilot session */
export interface ChannelBinding {
  id: string;
  channelType: ChannelType;
  chatId: string;
  /** CodePilot session ID this chat is bound to */
  codepilotSessionId: string;
  /** SDK session ID for resume (cached from last conversation) */
  sdkSessionId: string;
  /** Working directory for this binding */
  workingDirectory: string;
  /** Model override for this binding */
  model: string;
  /** Chat mode */
  mode: 'code' | 'plan' | 'ask';
  /** Whether this binding is currently active */
  active: boolean;
  createdAt: string;
  updatedAt: string;
}

// ── Bridge Status ──────────────────────────────────────────────

/** Overall bridge system status */
export interface BridgeStatus {
  running: boolean;
  startedAt: string | null;
  adapters: AdapterStatus[];
}

/** Status of a single channel adapter */
export interface AdapterStatus {
  channelType: ChannelType;
  running: boolean;
  connectedAt: string | null;
  lastMessageAt: string | null;
  error: string | null;
}

// ── Audit & Dedup ──────────────────────────────────────────────

/** Audit log entry */
export interface AuditLogEntry {
  id: string;
  channelType: ChannelType;
  chatId: string;
  direction: 'inbound' | 'outbound';
  messageId: string;
  summary: string;
  /** Sender identity (for inbound messages). */
  userId?: string;
  /** Human-readable sender name (for inbound messages). */
  senderName?: string;
  createdAt: string;
}

/** Permission link: maps permissionRequestId to an IM message for callback handling */
export interface PermissionLink {
  id: string;
  permissionRequestId: string;
  channelType: ChannelType;
  chatId: string;
  messageId: string;
  createdAt: string;
}

// ── Streaming Preview ─────────────────────────────────────────

/** Capabilities of a channel adapter's streaming preview support */
export interface PreviewCapabilities {
  supported: boolean;
  privateOnly: boolean;
}

/** Mutable state for an in-flight streaming preview */
export interface StreamingPreviewState {
  draftId: number;           // non-zero 31-bit random integer, reused within one answer cycle
  chatId: string;
  lastSentText: string;      // last text actually sent as draft
  lastSentAt: number;        // timestamp (ms) of last sent draft
  degraded: boolean;         // set true after API failure → skip further previews
  throttleTimer: ReturnType<typeof setTimeout> | null;
  pendingText: string;       // latest accumulated text (may not yet be sent due to throttle)
  /** Character offset into the full accumulated text — text before this offset
   *  was already finalized in a previous preview segment. */
  textOffset: number;
  /** True while a sendPreview call is in-flight (prevents concurrent PATCHes). */
  flushInFlight: boolean;
  /** Promise from the most recent flushPreview call. Awaited before deciding fallback. */
  lastFlushPromise: Promise<boolean> | null;
  /** True once any preview PATCH has been confirmed delivered. Survives segment resets. */
  previewEverDelivered: boolean;
  /** Monotonic counter incremented on each segment finalization.
   *  Prevents stale async callbacks from corrupting the next segment's state (ETH-98). */
  generation: number;
}

// ── Config ─────────────────────────────────────────────────────

/** Platform-specific message length limits */
export const PLATFORM_LIMITS: Record<string, number> = {
  telegram: 4096,
  discord: 2000,
  slack: 40000,
  feishu: 30000,
  lark: 30000,
  qq: 2000,
};

// ── User Preferences ────────────────────────────────────────

/** Per-user preferences (model selection, etc.). */
export interface UserPreferences {
  preferred_model?: string;
  updated_at: string; // ISO timestamp
}

// ── OAuth ───────────────────────────────────────────────────

/** Stored OAuth token for per-user authentication. */
export interface OAuthToken {
  access_token: string;
  refresh_token?: string;
  /** Unix epoch seconds when the access token expires. */
  expires_at: number;
  scopes: string[];
  /** ISO timestamp of when the user completed authorization. */
  authorized_at: string;
}
