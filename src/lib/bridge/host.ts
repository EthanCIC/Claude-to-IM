/**
 * Host Interfaces — abstractions for host-application dependencies.
 *
 * These interfaces decouple the bridge system from any specific host
 * (e.g., CodePilot). A host must provide implementations of these
 * interfaces to use the bridge.
 */

import type { ChannelBinding, ChannelType } from './types.js';

// ── Bridge-local types (replacing @/types imports) ────────────

/** File attachment from an IM channel (images, documents). */
export interface FileAttachment {
  id: string;
  name: string;
  type: string; // MIME type
  size: number;
  data: string; // base64 encoded content
  filePath?: string;
}

/** Server-Sent Event from the LLM stream. */
export interface SSEEvent {
  type: SSEEventType;
  data: string;
}

export type SSEEventType =
  | 'text'
  | 'tool_use'
  | 'tool_result'
  | 'tool_output'
  | 'tool_timeout'
  | 'status'
  | 'result'
  | 'error'
  | 'permission_request'
  | 'mode_changed'
  | 'task_update'
  | 'keep_alive'
  | 'done';

/** Content block in an LLM response message. */
export type MessageContentBlock =
  | { type: 'text'; text: string }
  | { type: 'tool_use'; id: string; name: string; input: unknown }
  | { type: 'tool_result'; tool_use_id: string; content: string; is_error?: boolean }
  | { type: 'code'; language: string; code: string };

/** Token usage statistics from an LLM response. */
export interface TokenUsage {
  input_tokens: number;
  output_tokens: number;
  cache_read_input_tokens?: number;
  cache_creation_input_tokens?: number;
  cost_usd?: number;
}

/** API provider configuration (opaque to the bridge). */
export interface BridgeApiProvider {
  id: string;
  [key: string]: unknown;
}

// ── Session & Message types ──────────────────────────────────

/** Minimal session object returned by the store. */
export interface BridgeSession {
  id: string;
  working_directory: string;
  model: string;
  system_prompt?: string;
  provider_id?: string;
}

/** Minimal message object returned by the store. */
export interface BridgeMessage {
  role: string;
  content: string;
}

// ── Host Interface: Settings ─────────────────────────────────

export interface SettingsProvider {
  getSetting(key: string): string | null;
}

// ── Host Interface: Store ────────────────────────────────────

/** Input for creating an audit log entry. */
export interface AuditLogInput {
  channelType: string;
  chatId: string;
  direction: 'inbound' | 'outbound';
  messageId: string;
  summary: string;
  /** Sender identity (for inbound messages). */
  userId?: string;
  /** Human-readable sender name (for inbound messages). */
  senderName?: string;
}

/** Input for inserting a permission link. */
export interface PermissionLinkInput {
  permissionRequestId: string;
  channelType: string;
  chatId: string;
  messageId: string;
  toolName: string;
  toolInput?: Record<string, unknown>;
  suggestions: string;
  createdAt: number;
}

/** Stored permission link record. */
export interface PermissionLinkRecord {
  permissionRequestId: string;
  chatId: string;
  messageId: string;
  resolved: boolean;
  toolName: string;
  toolInput?: Record<string, unknown>;
  suggestions: string;
  createdAt: number;
}

/** Input for inserting an outbound reference. */
export interface OutboundRefInput {
  channelType: string;
  chatId: string;
  codepilotSessionId: string;
  platformMessageId: string;
  purpose: string;
}

/** Input for upserting a channel binding. */
export interface UpsertChannelBindingInput {
  channelType: string;
  chatId: string;
  codepilotSessionId: string;
  workingDirectory: string;
  model: string;
}

/**
 * Persistence layer for the bridge system.
 * All database operations are abstracted through this interface.
 */
export interface BridgeStore {
  // ── Settings ──
  getSetting(key: string): string | null;

  // ── Channel bindings ──
  getChannelBinding(channelType: string, chatId: string): ChannelBinding | null;
  upsertChannelBinding(data: UpsertChannelBindingInput): ChannelBinding;
  updateChannelBinding(id: string, updates: Partial<ChannelBinding>): void;
  listChannelBindings(channelType?: ChannelType): ChannelBinding[];

  // ── Sessions ──
  getSession(id: string): BridgeSession | null;
  createSession(
    name: string,
    model: string,
    systemPrompt?: string,
    cwd?: string,
    mode?: string,
  ): BridgeSession;
  updateSessionProviderId(sessionId: string, providerId: string): void;

  // ── Messages ──
  addMessage(sessionId: string, role: string, content: string, usage?: string | null): void;
  getMessages(sessionId: string, opts?: { limit?: number }): { messages: BridgeMessage[] };

  // ── Session locking ──
  acquireSessionLock(sessionId: string, lockId: string, owner: string, ttlSecs: number): boolean;
  renewSessionLock(sessionId: string, lockId: string, ttlSecs: number): void;
  releaseSessionLock(sessionId: string, lockId: string): void;
  setSessionRuntimeStatus(sessionId: string, status: string): void;

  // ── SDK session ──
  updateSdkSessionId(sessionId: string, sdkSessionId: string): void;
  updateSessionModel(sessionId: string, model: string): void;
  syncSdkTasks(sessionId: string, todos: unknown): void;

  // ── Provider ──
  getProvider(id: string): BridgeApiProvider | undefined;
  getDefaultProviderId(): string | null;

  // ── Audit & dedup ──
  insertAuditLog(entry: AuditLogInput): void;
  checkDedup(key: string): boolean;
  insertDedup(key: string): void;
  cleanupExpiredDedup(): void;
  insertOutboundRef(ref: OutboundRefInput): void;

  // ── Permission links ──
  insertPermissionLink(link: PermissionLinkInput): void;
  getPermissionLink(permissionRequestId: string): PermissionLinkRecord | null;
  markPermissionLinkResolved(permissionRequestId: string): boolean;

  // ── Group context ──
  /** Return per-group system prompt context for a chat, or null if none configured. */
  getGroupContext?(channelType: string, chatId: string): string | null;

  // ── Group metadata (chat name, description, type) ──
  /** Store chat metadata (name, description) for a chat. */
  setGroupMetadata?(chatId: string, metadata: { name: string; description: string; chatType: string }): void;
  /** Retrieve chat metadata, or null if not loaded. */
  getGroupMetadata?(chatId: string): { name: string; description: string; chatType: string } | null;

  // ── Group members (for outbound @mention resolution) ──
  /** Store the list of mentionable users for a chat (called by adapters after loading members). */
  setGroupMembers?(chatId: string, members: Array<{ id: string; name: string }>): void;
  /** Retrieve the mentionable users list for a chat, or null if not populated. */
  getGroupMembers?(chatId: string): Array<{ id: string; name: string }> | null;

  // ── Channel offsets (adapter watermarks) ──
  getChannelOffset(key: string): string;
  setChannelOffset(key: string, offset: string): void;
}

// ── Host Interface: LLM Provider ─────────────────────────────

/** Parameters for starting an LLM stream. */
export interface StreamChatParams {
  prompt: string;
  sessionId: string;
  sdkSessionId?: string;
  model?: string;
  systemPrompt?: string | { type: 'preset'; preset: 'claude_code'; append?: string };
  workingDirectory?: string;
  abortController?: AbortController;
  permissionMode?: string;
  provider?: BridgeApiProvider;
  conversationHistory?: Array<{ role: 'user' | 'assistant'; content: string }>;
  files?: FileAttachment[];
  onRuntimeStatusChange?: (status: string) => void;
  /** User permission role. undefined = no role enforcement (legacy). */
  userRole?: import('./types.js').UserRole;
}

export interface LLMProvider {
  /**
   * Start a streaming chat with the LLM.
   * Returns a ReadableStream of SSE-formatted strings.
   */
  streamChat(params: StreamChatParams): ReadableStream<string>;
}

// ── Host Interface: Permission Gateway ───────────────────────

/** Resolution result for a pending permission. */
export interface PermissionResolution {
  behavior: 'allow' | 'deny';
  message?: string;
  updatedPermissions?: unknown[];
  data?: Record<string, unknown>;
}

export interface PermissionGateway {
  /**
   * Resolve a pending permission request.
   * Returns true if the permission was found and resolved.
   */
  resolvePendingPermission(permissionRequestId: string, resolution: PermissionResolution): boolean;
}

// ── Host Interface: Lifecycle Hooks ──────────────────────────

export interface LifecycleHooks {
  /** Called when the bridge system starts (e.g., to suppress competing polling). */
  onBridgeStart?(): void;
  /** Called when the bridge system stops. */
  onBridgeStop?(): void;
}
