/**
 * Permission Broker — forwards Claude permission requests to IM channels
 * and handles user responses via inline buttons.
 *
 * When Claude needs tool approval, the broker:
 * 1. Formats a permission prompt with inline keyboard buttons
 * 2. Sends it via the delivery layer
 * 3. Records the link between permission ID and IM message
 * 4. When a callback arrives, resolves the permission via the gateway
 */

import type { PermissionUpdate } from '@anthropic-ai/claude-agent-sdk';
import type { ChannelAddress, OutboundMessage } from './types.js';
import type { BaseChannelAdapter } from './channel-adapter.js';
import { deliver } from './delivery-layer.js';
import { getBridgeContext } from './context.js';
import { escapeHtml } from './adapters/telegram-utils.js';

/** SDK default permission timeout (2 minutes). */
const PERMISSION_TIMEOUT_MS = 2 * 60 * 1000;

/**
 * Dedup recent permission forwards to prevent duplicate cards.
 * Key: permissionRequestId, value: timestamp. Entries expire after 30s.
 */
const recentPermissionForwards = new Map<string, number>();

/**
 * Pending text answers: when user clicks "Other" on a question card,
 * we store chatId → permissionRequestId so the next text message
 * from that chat is treated as the answer.
 */
const pendingTextAnswers = new Map<string, string>();

/**
 * Active timeout timers for permission cards.
 * Key: permissionRequestId, value: timer handle.
 */
const permissionTimers = new Map<string, ReturnType<typeof setTimeout>>();

/**
 * Forward a permission request to an IM channel as an interactive message.
 */
export async function forwardPermissionRequest(
  adapter: BaseChannelAdapter,
  address: ChannelAddress,
  permissionRequestId: string,
  toolName: string,
  toolInput: Record<string, unknown>,
  sessionId?: string,
  suggestions?: unknown[],
  replyToMessageId?: string,
): Promise<void> {
  const { store } = getBridgeContext();

  // Dedup: prevent duplicate forwarding of the same permission request
  const now = Date.now();
  if (recentPermissionForwards.has(permissionRequestId)) {
    console.warn(`[permission-broker] Duplicate forward suppressed for ${permissionRequestId}`);
    return;
  }
  recentPermissionForwards.set(permissionRequestId, now);
  // Clean up old entries
  for (const [id, ts] of recentPermissionForwards) {
    if (now - ts > 30_000) recentPermissionForwards.delete(id);
  }

  console.log(`[permission-broker] Forwarding permission request: ${permissionRequestId} tool=${toolName} channel=${adapter.channelType}`);

  // AskUserQuestion: render as question card with option buttons
  if (toolName === 'AskUserQuestion' && toolInput.questions) {
    await forwardQuestionRequest(adapter, address, permissionRequestId, toolInput, sessionId, replyToMessageId);
    return;
  }

  // Format the input summary (truncated)
  const inputStr = JSON.stringify(toolInput, null, 2);
  const truncatedInput = inputStr.length > 300
    ? inputStr.slice(0, 300) + '...'
    : inputStr;

  let result: import('./types.js').SendResult;

  if (adapter.channelType === 'qq') {
    // QQ: plain text permission prompt with copyable /perm commands (no inline buttons)
    const qqText = [
      `Permission Required`,
      ``,
      `Tool: ${toolName}`,
      truncatedInput,
      ``,
      `Reply with one of:`,
      `/perm allow ${permissionRequestId}`,
      `/perm allow_session ${permissionRequestId}`,
      `/perm deny ${permissionRequestId}`,
    ].join('\n');

    const qqMessage: OutboundMessage = {
      address,
      text: qqText,
      parseMode: 'plain',
      replyToMessageId,
    };

    result = await deliver(adapter, qqMessage, { sessionId });
  } else {
    const text = [
      `<b>Permission Required</b>`,
      ``,
      `Tool: <code>${escapeHtml(toolName)}</code>`,
      `<pre>${escapeHtml(truncatedInput)}</pre>`,
      ``,
      `Choose an action:`,
    ].join('\n');

    const buttons: import('./types.js').InlineButton[] = [
      { text: 'Allow', callbackData: `perm:allow:${permissionRequestId}` },
    ];
    // Only offer "Allow Session" when suggestions exist (auto-approve pattern)
    if (suggestions && suggestions.length > 0) {
      buttons.push({ text: 'Allow Session', callbackData: `perm:allow_session:${permissionRequestId}` });
    }
    buttons.push({ text: 'Deny', callbackData: `perm:deny:${permissionRequestId}` });

    const message: OutboundMessage = {
      address,
      text,
      parseMode: 'HTML',
      inlineButtons: [buttons],
      permissionMeta: { toolName, toolInput },
      replyToMessageId,
    };

    result = await deliver(adapter, message, { sessionId });
  }

  // Record the link so we can match callback queries back to this permission
  if (result.ok && result.messageId) {
    try {
      store.insertPermissionLink({
        permissionRequestId,
        channelType: adapter.channelType,
        chatId: address.chatId,
        messageId: result.messageId,
        toolName,
        toolInput,
        suggestions: suggestions ? JSON.stringify(suggestions) : '',
        createdAt: Date.now(),
      });
    } catch { /* best effort */ }

    // Set timeout timer to expire the card when SDK permission times out
    schedulePermissionExpiry(adapter, permissionRequestId, address.chatId, result.messageId, toolName);
  }
}

/**
 * Forward an AskUserQuestion as an interactive question card.
 * Renders option buttons so the user can select an answer directly.
 */
async function forwardQuestionRequest(
  adapter: BaseChannelAdapter,
  address: ChannelAddress,
  permissionRequestId: string,
  toolInput: Record<string, unknown>,
  sessionId?: string,
  replyToMessageId?: string,
): Promise<void> {
  const { store } = getBridgeContext();

  const questions = toolInput.questions as Array<{
    question: string;
    header: string;
    options: Array<{ label: string; description: string }>;
    multiSelect: boolean;
  }>;

  if (!questions || questions.length === 0) return;

  const q = questions[0];
  const isSingleSelectSingle = questions.length === 1 && !q.multiSelect;

  // Build option buttons for single-question single-select (most common)
  const buttons: import('./types.js').InlineButton[] = [];
  if (isSingleSelectSingle) {
    for (let i = 0; i < q.options.length; i++) {
      buttons.push({
        text: q.options[i].label,
        callbackData: `askq:${permissionRequestId}:0:${i}`,
      });
    }
    // "Other" lets user type a custom answer; "Skip" denies the tool call
    buttons.push({ text: 'Other', callbackData: `askq_other:${permissionRequestId}` });
    buttons.push({ text: 'Skip', callbackData: `perm:deny:${permissionRequestId}` });
  } else {
    // Multi-question or multiSelect: fall back to Allow/Deny
    buttons.push({ text: 'Allow', callbackData: `perm:allow:${permissionRequestId}` });
    buttons.push({ text: 'Deny', callbackData: `perm:deny:${permissionRequestId}` });
  }

  // Format question text
  const textParts = [`<b>${escapeHtml(q.header)}</b>`, '', escapeHtml(q.question)];
  if (isSingleSelectSingle) {
    // Show option descriptions
    for (const opt of q.options) {
      textParts.push(`  • <b>${escapeHtml(opt.label)}</b> — ${escapeHtml(opt.description)}`);
    }
  }
  const text = textParts.join('\n');

  const questionMeta: import('./types.js').QuestionCardMeta = {
    permissionRequestId,
    questions,
  };

  const message: OutboundMessage = {
    address,
    text,
    parseMode: 'HTML',
    inlineButtons: [buttons],
    questionMeta,
    replyToMessageId,
  };

  const result = await deliver(adapter, message, { sessionId });

  if (result.ok && result.messageId) {
    try {
      store.insertPermissionLink({
        permissionRequestId,
        channelType: adapter.channelType,
        chatId: address.chatId,
        messageId: result.messageId,
        toolName: 'AskUserQuestion',
        toolInput,
        suggestions: '',
        createdAt: Date.now(),
      });
    } catch { /* best effort */ }

    // Set timeout timer to expire the card when SDK permission times out
    schedulePermissionExpiry(adapter, permissionRequestId, address.chatId, result.messageId, 'AskUserQuestion');
  }
}

/**
 * Handle a permission callback from an inline button press.
 * Validates that the callback came from the same chat AND same message that
 * received the permission request, prevents duplicate resolution via atomic
 * DB check-and-set, and implements real allow_session semantics by passing
 * updatedPermissions (suggestions).
 *
 * Returns true if the callback was recognized and handled.
 */
export function handlePermissionCallback(
  callbackData: string,
  callbackChatId: string,
  callbackMessageId?: string,
): boolean {
  const { store, permissions } = getBridgeContext();

  // Parse callback data: perm:action:permId
  const parts = callbackData.split(':');
  if (parts.length < 3 || parts[0] !== 'perm') return false;

  const action = parts[1];
  const permissionRequestId = parts.slice(2).join(':'); // permId might contain colons

  // Look up the permission link to validate origin and check dedup
  const link = store.getPermissionLink(permissionRequestId);
  if (!link) {
    console.warn(`[permission-broker] No permission link found for ${permissionRequestId}`);
    return false;
  }

  // Security: verify the callback came from the same chat that received the request
  if (link.chatId !== callbackChatId) {
    console.warn(`[permission-broker] Chat ID mismatch: expected ${link.chatId}, got ${callbackChatId}`);
    return false;
  }

  // Security: verify the callback came from the original permission message
  if (callbackMessageId && link.messageId !== callbackMessageId) {
    console.warn(`[permission-broker] Message ID mismatch: expected ${link.messageId}, got ${callbackMessageId}`);
    return false;
  }

  // Dedup: reject if already resolved (fast path before expensive resolution)
  if (link.resolved) {
    console.warn(`[permission-broker] Permission ${permissionRequestId} already resolved`);
    return false;
  }

  // Atomically mark as resolved BEFORE calling resolvePendingPermission
  // to prevent race conditions with concurrent button clicks
  let claimed: boolean;
  try {
    claimed = store.markPermissionLinkResolved(permissionRequestId);
  } catch {
    return false;
  }

  if (!claimed) {
    // Another concurrent handler already resolved this permission
    console.warn(`[permission-broker] Permission ${permissionRequestId} already claimed by concurrent handler`);
    return false;
  }

  // Clear the expiry timer since the user responded in time
  clearPermissionTimer(permissionRequestId);

  let resolved: boolean;

  switch (action) {
    case 'allow':
      resolved = permissions.resolvePendingPermission(permissionRequestId, {
        behavior: 'allow',
      });
      break;

    case 'allow_session': {
      // Parse stored suggestions so subsequent same-tool calls auto-approve
      let updatedPermissions: PermissionUpdate[] | undefined;
      if (link.suggestions) {
        try {
          updatedPermissions = JSON.parse(link.suggestions) as PermissionUpdate[];
        } catch { /* fall through without updatedPermissions */ }
      }

      resolved = permissions.resolvePendingPermission(permissionRequestId, {
        behavior: 'allow',
        ...(updatedPermissions ? { updatedPermissions } : {}),
      });
      break;
    }

    case 'deny':
      resolved = permissions.resolvePendingPermission(permissionRequestId, {
        behavior: 'deny',
        message: 'Denied via IM bridge',
      });
      break;

    default:
      return false;
  }

  return resolved;
}

/**
 * Handle a question answer callback from an option button press.
 * Parses the callback data (askq:<permId>:<qIdx>:<optIdx>), looks up
 * the question from the stored toolInput, and resolves the permission
 * with the user's answer.
 *
 * Returns true if the callback was recognized and handled.
 */
export function handleQuestionCallback(
  callbackData: string,
  callbackChatId: string,
  callbackMessageId?: string,
): boolean {
  const { store, permissions } = getBridgeContext();

  // Parse: askq:<permId>:<qIdx>:<optIdx>
  const parts = callbackData.split(':');
  if (parts.length < 4 || parts[0] !== 'askq') return false;

  const permissionRequestId = parts[1];
  const qIdx = parseInt(parts[2], 10);
  const optIdx = parseInt(parts[3], 10);

  if (isNaN(qIdx) || isNaN(optIdx)) return false;

  const link = store.getPermissionLink(permissionRequestId);
  if (!link) {
    console.warn(`[permission-broker] No permission link found for question ${permissionRequestId}`);
    return false;
  }

  if (link.chatId !== callbackChatId) {
    console.warn(`[permission-broker] Chat ID mismatch for question callback`);
    return false;
  }

  if (callbackMessageId && link.messageId !== callbackMessageId) {
    console.warn(`[permission-broker] Message ID mismatch for question callback`);
    return false;
  }

  if (link.resolved) {
    console.warn(`[permission-broker] Question ${permissionRequestId} already resolved`);
    return false;
  }

  let claimed: boolean;
  try {
    claimed = store.markPermissionLinkResolved(permissionRequestId);
  } catch {
    return false;
  }

  if (!claimed) return false;

  // Clear the expiry timer since the user responded in time
  clearPermissionTimer(permissionRequestId);

  // Look up question and selected option from stored toolInput
  const questions = link.toolInput?.questions as Array<{
    question: string;
    options: Array<{ label: string }>;
  }> | undefined;

  if (!questions || !questions[qIdx]) {
    console.warn(`[permission-broker] Question index ${qIdx} out of range`);
    return false;
  }

  const question = questions[qIdx];
  const option = question.options?.[optIdx];
  if (!option) {
    console.warn(`[permission-broker] Option index ${optIdx} out of range`);
    return false;
  }

  // Build answers map: { questionText: selectedOptionLabel }
  const answers: Record<string, string> = {
    [question.question]: option.label,
  };

  console.log(`[permission-broker] Question answered: "${question.question}" → "${option.label}"`);

  return permissions.resolvePendingPermission(permissionRequestId, {
    behavior: 'allow',
    data: { answers },
  });
}

/**
 * Handle the "Other" button callback from a question card.
 * Stores the chatId → permissionRequestId mapping so the next text message
 * from this chat is treated as a free-text answer.
 *
 * Returns true if recognized and stored.
 */
export function handleOtherCallback(
  callbackData: string,
  callbackChatId: string,
): boolean {
  // Parse: askq_other:<permId>
  const parts = callbackData.split(':');
  if (parts.length < 2 || parts[0] !== 'askq_other') return false;

  const permissionRequestId = parts.slice(1).join(':');
  const { store } = getBridgeContext();

  const link = store.getPermissionLink(permissionRequestId);
  if (!link) {
    console.warn(`[permission-broker] No permission link found for other-callback ${permissionRequestId}`);
    return false;
  }

  if (link.chatId !== callbackChatId) return false;
  if (link.resolved) return false;

  // Store pending: next text from this chat resolves this question
  pendingTextAnswers.set(callbackChatId, permissionRequestId);
  console.log(`[permission-broker] "Other" clicked, awaiting text answer for ${permissionRequestId}`);
  return true;
}

/**
 * Check if a chat has a pending text answer (user clicked "Other").
 */
export function hasPendingTextAnswer(chatId: string): boolean {
  return pendingTextAnswers.has(chatId);
}

/**
 * Handle a free-text answer from a chat that previously clicked "Other".
 * Resolves the question permission with the typed text as the answer.
 *
 * Returns true if the text was consumed as an answer.
 */
export function handleTextAnswer(chatId: string, text: string): boolean {
  const permissionRequestId = pendingTextAnswers.get(chatId);
  if (!permissionRequestId) return false;

  // Remove pending state immediately
  pendingTextAnswers.delete(chatId);

  const { store, permissions } = getBridgeContext();

  const link = store.getPermissionLink(permissionRequestId);
  if (!link || link.resolved) {
    console.warn(`[permission-broker] Text answer arrived but permission ${permissionRequestId} already resolved`);
    return false;
  }

  let claimed: boolean;
  try {
    claimed = store.markPermissionLinkResolved(permissionRequestId);
  } catch {
    return false;
  }
  if (!claimed) return false;

  // Clear the expiry timer since the user responded in time
  clearPermissionTimer(permissionRequestId);

  // Look up the question text from stored toolInput
  const questions = link.toolInput?.questions as Array<{
    question: string;
  }> | undefined;

  const questionText = questions?.[0]?.question || 'unknown';

  const answers: Record<string, string> = {
    [questionText]: text,
  };

  console.log(`[permission-broker] Text answer for "${questionText}" → "${text}"`);

  return permissions.resolvePendingPermission(permissionRequestId, {
    behavior: 'allow',
    data: { answers },
  });
}

// ── Permission expiry timer ─────────────────────────────────

/**
 * Schedule a timer to expire a permission card after PERMISSION_TIMEOUT_MS.
 * When the timer fires (and the permission hasn't been resolved), it marks the
 * link as resolved and calls the adapter's expirePermissionCard to update the card UI.
 */
function schedulePermissionExpiry(
  adapter: BaseChannelAdapter,
  permissionRequestId: string,
  chatId: string,
  messageId: string,
  toolName: string,
): void {
  const timer = setTimeout(() => {
    permissionTimers.delete(permissionRequestId);

    const { store } = getBridgeContext();
    const link = store.getPermissionLink(permissionRequestId);

    // Already resolved by user action — nothing to do
    if (!link || link.resolved) return;

    // Mark as resolved (expired)
    try {
      store.markPermissionLinkResolved(permissionRequestId);
    } catch { /* best effort */ }

    console.log(`[permission-broker] Permission ${permissionRequestId} expired (timeout)`);

    // Update the card UI via the adapter
    if (adapter.expirePermissionCard) {
      adapter.expirePermissionCard(chatId, messageId, toolName).catch((err) => {
        console.warn('[permission-broker] Failed to expire card:', err instanceof Error ? err.message : err);
      });
    }
  }, PERMISSION_TIMEOUT_MS);

  permissionTimers.set(permissionRequestId, timer);
}

/**
 * Clear a permission expiry timer (called when the user responds in time).
 */
export function clearPermissionTimer(permissionRequestId: string): void {
  const timer = permissionTimers.get(permissionRequestId);
  if (timer) {
    clearTimeout(timer);
    permissionTimers.delete(permissionRequestId);
  }
}

/**
 * Clear all permission expiry timers (called on bridge stop).
 */
export function clearAllPermissionTimers(): void {
  for (const timer of permissionTimers.values()) {
    clearTimeout(timer);
  }
  permissionTimers.clear();
}
