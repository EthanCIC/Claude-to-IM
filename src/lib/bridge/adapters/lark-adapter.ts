/**
 * Lark Adapter — webhook + card action buttons for Lark (international).
 *
 * Extends FeishuAdapter, overriding:
 * - channelType = 'lark'
 * - start(): webhook HTTP server (EventDispatcher for messages,
 *            direct HTTP handling for card action callbacks)
 * - stop(): HTTP server shutdown
 * - sendPermissionCard(): v1 card with real action buttons
 * - sendQuestionCard(): v1 card with option buttons for AskUserQuestion
 *
 * Card action callbacks are intercepted at the HTTP level (before EventDispatcher)
 * because the Lark Open SDK does not support the new version (schema 2.0) card
 * callback response format. We parse the body ourselves, process the action,
 * and return the correct response structure per Lark docs:
 *   { card: { type: "raw", data: { ...cardJSON } } }
 *
 * Domain is fixed to lark.Domain.Lark.
 */

import http from 'node:http';
import crypto from 'node:crypto';
import * as lark from '@larksuiteoapi/node-sdk';
import type { ChannelType, InboundMessage, SendResult } from '../types.js';
import { registerAdapterFactory } from '../channel-adapter.js';
import { getBridgeContext } from '../context.js';
import { FeishuAdapter } from './feishu-adapter.js';

export class LarkAdapter extends FeishuAdapter {
  override readonly channelType: ChannelType = 'lark';

  private httpServer: http.Server | null = null;
  private encryptKey: string | undefined;

  // ── Lifecycle ───────────────────────────────────────────────

  override async start(): Promise<void> {
    if (this.running) return;

    const configError = this.validateConfig();
    if (configError) {
      console.warn('[lark-adapter] Cannot start:', configError);
      return;
    }

    const appId = this.setting('app_id');
    const appSecret = this.setting('app_secret');
    const domain = lark.Domain.Lark;

    // Create REST client
    this.restClient = new lark.Client({ appId, appSecret, domain });

    // Resolve bot identity for @mention detection
    await this.resolveBotIdentity(appId, appSecret, domain);

    this.running = true;

    // Preload members from all groups in the background (best-effort)
    this.preloadAllChatMembers().catch(() => {});

    const verificationToken = this.setting('verification_token') || undefined;
    this.encryptKey = this.setting('encrypt_key') || undefined;

    // EventDispatcher handles message events only
    const eventDispatcher = new lark.EventDispatcher({
      verificationToken,
      encryptKey: this.encryptKey,
    }).register({
      'im.message.receive_v1': async (data) => {
        await this.handleIncomingEvent(data as any);
      },
    });

    const webhookHandler = lark.adaptDefault('/webhook', eventDispatcher, { autoChallenge: true });
    const port = Number(this.setting('webhook_port')) || 9800;

    this.httpServer = http.createServer(async (req, res) => {
      // Buffer the request body for inspection
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      const bodyBuf = Buffer.concat(chunks);

      let parsed: any = null;
      try {
        parsed = JSON.parse(bodyBuf.toString('utf-8'));
      } catch { /* not JSON */ }

      // Lark sends a duplicate plaintext card action callback alongside the
      // encrypted one.  Detect by the presence of 'action' at the top level
      // without 'encrypt' or 'schema'.  Silently acknowledge — the encrypted
      // copy is the one we actually process.
      if (parsed?.action && !parsed?.encrypt && !parsed?.schema) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end('{}');
        return;
      }

      // Handle encrypted payloads
      if (parsed?.encrypt && this.encryptKey) {
        try {
          const decrypted = this.decryptBody(this.encryptKey, parsed.encrypt);
          parsed = JSON.parse(decrypted);
        } catch (err) {
          console.error('[lark-adapter] Decrypt failed:', err instanceof Error ? err.message : err);
          parsed = null;
        }
      }

      // Intercept card action callbacks — handle directly with correct response format
      if (parsed?.header?.event_type === 'card.action.trigger' && parsed?.event) {
        const cardResponse = await this.handleCardAction(parsed.event);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(cardResponse));
        return;
      }

      // For all other requests (messages, challenge), replay body through EventDispatcher
      const { Readable } = await import('node:stream');
      const proxyReq = Object.assign(
        Readable.from(bodyBuf),
        { method: req.method, url: req.url, headers: req.headers,
          httpVersion: req.httpVersion, httpVersionMajor: req.httpVersionMajor,
          httpVersionMinor: req.httpVersionMinor },
      );
      await webhookHandler(proxyReq as any, res);
    });

    this.httpServer.listen(port, () => {
      console.log(`[lark-adapter] Webhook server listening on port ${port}`);
    });

    console.log('[lark-adapter] Started in webhook mode (botOpenId:', this.botOpenId || 'unknown', ')');
  }

  override async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;

    // Close HTTP server
    if (this.httpServer) {
      await new Promise<void>((resolve) => {
        this.httpServer!.close(() => resolve());
      });
      this.httpServer = null;
    }

    this.restClient = null;

    console.log('[lark-adapter] Stopped');
  }

  // ── Lark AES-256-CBC decryption ────────────────────────────

  private decryptBody(key: string, encrypted: string): string {
    const keyHash = crypto.createHash('sha256').update(key).digest();
    const buf = Buffer.from(encrypted, 'base64');
    const iv = buf.subarray(0, 16);
    const ciphertext = buf.subarray(16);
    const decipher = crypto.createDecipheriv('aes-256-cbc', keyHash, iv);
    return decipher.update(ciphertext, undefined, 'utf-8') + decipher.final('utf-8');
  }

  // ── Card action handler ─────────────────────────────────────

  private async handleCardAction(event: any): Promise<any> {
    try {
      const action = event?.action;
      if (!action?.value) return {};

      const callbackData = action.value.callback_data;
      if (!callbackData) return {};

      // Passive expiry check: if the permission link is already resolved/expired,
      // return an expired card immediately without enqueuing
      if (callbackData.startsWith('perm:') || callbackData.startsWith('askq:') || callbackData.startsWith('askq_other:')) {
        const permId = this.extractPermIdFromCallback(callbackData);
        if (permId) {
          const link = getBridgeContext().store.getPermissionLink(permId);
          if (link?.resolved) {
            return this.buildExpiredCard(link.toolName);
          }
        }
      }

      const context = event?.context || {};
      const chatId = context.open_chat_id || '';
      const userId = event?.operator?.open_id || '';
      const messageId = context.open_message_id || '';

      const address = {
        channelType: this.channelType,
        chatId,
        userId,
      };

      const inbound: InboundMessage = {
        messageId: messageId || `card_action_${Date.now()}`,
        address,
        text: callbackData,
        timestamp: Date.now(),
        callbackData,
        callbackMessageId: messageId,
      };

      this.enqueue(inbound);

      // Handle "Other" button click (askq_other:<permId>)
      if (callbackData.startsWith('askq_other:')) {
        return this.buildOtherPromptCard();
      }

      // Handle question answer callbacks (askq:<permId>:<qIdx>:<optIdx>)
      if (callbackData.startsWith('askq:')) {
        return this.buildQuestionAnswerCard(callbackData);
      }

      // Handle permission callbacks (perm:<action>:<permId>)
      return this.buildPermissionResponseCard(callbackData);
    } catch (err) {
      console.error('[lark-adapter] Card action error:', err instanceof Error ? err.message : err);
      return {};
    }
  }

  /**
   * Extract permission ID from various callback data formats.
   */
  private extractPermIdFromCallback(callbackData: string): string | null {
    if (callbackData.startsWith('perm:')) {
      // perm:<action>:<permId>
      const parts = callbackData.split(':');
      return parts.length >= 3 ? parts.slice(2).join(':') : null;
    }
    if (callbackData.startsWith('askq_other:')) {
      // askq_other:<permId>
      return callbackData.slice('askq_other:'.length) || null;
    }
    if (callbackData.startsWith('askq:')) {
      // askq:<permId>:<qIdx>:<optIdx>
      const parts = callbackData.split(':');
      return parts.length >= 2 ? parts[1] : null;
    }
    return null;
  }

  /**
   * Build card response for permission button clicks (Allow/Deny/Allow Session).
   */
  private buildPermissionResponseCard(callbackData: string): any {
    let actionLabel = 'Processed';
    let headerTemplate = 'grey';
    if (callbackData.includes(':allow_session:')) {
      actionLabel = 'Allowed (Session)';
      headerTemplate = 'green';
    } else if (callbackData.includes(':allow:')) {
      actionLabel = 'Allowed';
      headerTemplate = 'green';
    } else if (callbackData.includes(':deny:')) {
      actionLabel = 'Denied';
      headerTemplate = 'red';
    }

    // Look up tool info from permission link
    let toolName = '';
    let toolInput: Record<string, unknown> | undefined;
    try {
      const parts = callbackData.split(':');
      if (parts.length >= 3) {
        const permId = parts.slice(2).join(':');
        const link = getBridgeContext().store.getPermissionLink(permId);
        if (link?.toolName) toolName = link.toolName;
        if (link?.toolInput) toolInput = link.toolInput;
      }
    } catch { /* best effort */ }

    const elements: any[] = [];
    if (toolName) {
      elements.push({ tag: 'markdown', content: `Tool: **${toolName}**` });
    }
    if (toolInput) {
      const lines = Object.entries(toolInput).map(([k, v]) => {
        const val = typeof v === 'string' ? v : JSON.stringify(v);
        const display = val.length > 200 ? val.slice(0, 200) + '...' : val;
        return `**${k}:** ${display}`;
      });
      elements.push({ tag: 'markdown', content: lines.join('\n') });
    }

    return {
      card: {
        type: 'raw',
        data: {
          config: { wide_screen_mode: true },
          header: {
            template: headerTemplate,
            title: { tag: 'plain_text', content: `🔐 ${actionLabel}` },
          },
          elements,
        },
      },
    };
  }

  /**
   * Build card response for question answer button clicks.
   * Shows the selected option prominently.
   */
  private buildQuestionAnswerCard(callbackData: string): any {
    const parts = callbackData.split(':');
    // askq:<permId>:<qIdx>:<optIdx>
    const permId = parts[1] || '';
    const qIdx = parseInt(parts[2], 10);
    const optIdx = parseInt(parts[3], 10);

    let questionText = '';
    let selectedLabel = '';

    try {
      const link = getBridgeContext().store.getPermissionLink(permId);
      const questions = link?.toolInput?.questions as Array<{
        question: string;
        header: string;
        options: Array<{ label: string; description: string }>;
      }> | undefined;

      if (questions?.[qIdx]) {
        questionText = questions[qIdx].question;
        const opt = questions[qIdx].options?.[optIdx];
        if (opt) selectedLabel = opt.label;
      }
    } catch { /* best effort */ }

    const elements: any[] = [];
    if (questionText) {
      elements.push({ tag: 'markdown', content: `**${questionText}**` });
    }
    elements.push({
      tag: 'markdown',
      content: `Selected: **${selectedLabel || 'Option ' + (optIdx + 1)}**`,
    });

    return {
      card: {
        type: 'raw',
        data: {
          config: { wide_screen_mode: true },
          header: {
            template: 'green',
            title: { tag: 'plain_text', content: '✅ Answered' },
          },
          elements,
        },
      },
    };
  }

  /**
   * Build card response when user clicks "Other" — prompt for text input.
   */
  private buildOtherPromptCard(): any {
    return {
      card: {
        type: 'raw',
        data: {
          config: { wide_screen_mode: true },
          header: {
            template: 'blue',
            title: { tag: 'plain_text', content: 'Type your answer' },
          },
          elements: [
            { tag: 'markdown', content: 'Please type your answer as the next message.' },
          ],
        },
      },
    };
  }

  // ── Expired permission card ─────────────────────────────────

  /**
   * Build an inline card response for expired permission requests.
   * Used both for passive expiry (button click after timeout) and active expiry.
   */
  private buildExpiredCard(toolName?: string): any {
    const elements: any[] = [];
    if (toolName) {
      elements.push({ tag: 'markdown', content: `Tool: **${toolName}**` });
    }
    elements.push({ tag: 'markdown', content: '_This permission request has expired._' });

    return {
      card: {
        type: 'raw',
        data: {
          config: { wide_screen_mode: true },
          header: {
            template: 'grey',
            title: { tag: 'plain_text', content: '🔐 Expired' },
          },
          elements,
        },
      },
    };
  }

  /**
   * Update a permission card to show it has expired (proactive timeout).
   * Uses im.message.patch to replace the card with a grey expired card.
   */
  async expirePermissionCard(chatId: string, messageId: string, toolName?: string): Promise<void> {
    if (!this.restClient) return;

    const elements: any[] = [];
    if (toolName) {
      elements.push({ tag: 'markdown', content: `Tool: **${toolName}**` });
    }
    elements.push({ tag: 'markdown', content: '_This permission request has expired._' });

    const cardJson = JSON.stringify({
      config: { wide_screen_mode: true },
      header: {
        template: 'grey',
        title: { tag: 'plain_text', content: '🔐 Expired' },
      },
      elements,
    });

    try {
      await this.restClient.im.message.patch({
        path: { message_id: messageId },
        data: { content: cardJson },
      });
      console.log(`[lark-adapter] Permission card expired: ${messageId}`);
    } catch (err) {
      console.warn('[lark-adapter] Failed to expire permission card:', err instanceof Error ? err.message : err);
    }
  }

  // ── Permission card with real buttons ───────────────────────

  protected override async sendPermissionCard(
    chatId: string,
    text: string,
    inlineButtons: import('../types.js').InlineButton[][],
    permissionMeta?: import('../types.js').PermissionCardMeta,
  ): Promise<SendResult> {
    if (!this.restClient) {
      return { ok: false, error: 'Lark client not initialized' };
    }

    const toolName = permissionMeta?.toolName || '';
    const toolInput = permissionMeta?.toolInput || {};

    // Build card elements
    const elements: any[] = [];

    // Tool name
    if (toolName) {
      elements.push({ tag: 'markdown', content: `Tool: **${toolName}**` });
    }

    // Format tool input as readable key-value pairs
    const entries = Object.entries(toolInput);
    if (entries.length > 0) {
      const lines = entries.map(([k, v]) => {
        const val = typeof v === 'string' ? v : JSON.stringify(v);
        const display = val.length > 200 ? val.slice(0, 200) + '...' : val;
        return `**${k}:** ${display}`;
      });
      elements.push({ tag: 'markdown', content: lines.join('\n') });
    }

    elements.push({ tag: 'hr' });

    // Build action buttons from inline buttons
    const actions = inlineButtons.flat().map((btn) => ({
      tag: 'button' as const,
      text: { tag: 'plain_text' as const, content: btn.text },
      type: btn.callbackData.includes('deny') ? 'danger' as const : 'primary' as const,
      value: { callback_data: btn.callbackData },
    }));
    elements.push({ tag: 'action', actions });

    const cardJson = JSON.stringify({
      config: { wide_screen_mode: true },
      header: {
        template: 'orange',
        title: { tag: 'plain_text', content: '🔐 Permission Required' },
      },
      elements,
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
      console.warn('[lark-adapter] Permission card send failed:', res?.msg);
    } catch (err) {
      console.warn('[lark-adapter] Permission card error:', err instanceof Error ? err.message : err);
    }

    // Fallback: use parent's text-based permission card
    return super.sendPermissionCard(chatId, text, inlineButtons);
  }

  // ── Question card with option buttons (AskUserQuestion) ────

  protected override async sendQuestionCard(
    chatId: string,
    _text: string,
    inlineButtons: import('../types.js').InlineButton[][],
    questionMeta: import('../types.js').QuestionCardMeta,
  ): Promise<SendResult> {
    if (!this.restClient) {
      return { ok: false, error: 'Lark client not initialized' };
    }

    const q = questionMeta.questions[0];
    if (!q) {
      return super.sendQuestionCard(chatId, _text, inlineButtons, questionMeta);
    }

    // Build card elements
    const elements: any[] = [];

    // Question text
    elements.push({ tag: 'markdown', content: `**${q.question}**` });

    // Option descriptions
    const optLines = q.options.map((opt) =>
      `• **${opt.label}** — ${opt.description}`
    );
    elements.push({ tag: 'markdown', content: optLines.join('\n') });

    elements.push({ tag: 'hr' });

    // Build action buttons (Skip/Deny gets danger style)
    const actions = inlineButtons.flat().map((btn) => ({
      tag: 'button' as const,
      text: { tag: 'plain_text' as const, content: btn.text },
      type: (btn.callbackData.includes('deny') ? 'danger' : 'primary') as 'danger' | 'primary',
      value: { callback_data: btn.callbackData },
    }));
    elements.push({ tag: 'action', actions });

    const cardJson = JSON.stringify({
      config: { wide_screen_mode: true },
      header: {
        template: 'blue',
        title: { tag: 'plain_text', content: `❓ ${q.header}` },
      },
      elements,
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
      console.warn('[lark-adapter] Question card send failed:', res?.msg);
    } catch (err) {
      console.warn('[lark-adapter] Question card error:', err instanceof Error ? err.message : err);
    }

    // Fallback: use parent's text-based question card
    return super.sendQuestionCard(chatId, _text, inlineButtons, questionMeta);
  }
}

// Self-register
registerAdapterFactory('lark', () => new LarkAdapter());
