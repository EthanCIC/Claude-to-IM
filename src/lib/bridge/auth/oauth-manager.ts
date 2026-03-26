/**
 * OAuth Manager — handles Claude Code OAuth PKCE flow.
 *
 * Manages: authorize URL generation, code exchange, token refresh,
 * and in-memory pending flow state with auto-expiry.
 */

import crypto from 'node:crypto';

import type { OAuthToken } from '../types.js';

// ── Constants ──

const CLIENT_ID = '9d1c250a-e61b-44d9-88ed-5944d1962f5e';
const AUTHORIZE_URL = 'https://claude.com/cai/oauth/authorize';
const ANTHROPIC_CALLBACK_URL = 'https://platform.claude.com/oauth/code/callback';
const TOKEN_URL = 'https://platform.claude.com/v1/oauth/token';
const SCOPES = 'org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload';
const STATE_TTL_MS = 10 * 60 * 1000; // 10 minutes

// ── PKCE helpers ──

function base64url(buf: Buffer): string {
  return buf.toString('base64url');
}

function generateCodeVerifier(): string {
  return base64url(crypto.randomBytes(32));
}

function generateCodeChallenge(verifier: string): string {
  return base64url(crypto.createHash('sha256').update(verifier).digest());
}

// ── Pending flow state ──

interface PendingFlow {
  codeVerifier: string;
  openId: string;
  createdAt: number;
}

// ── Manager ──

export class OAuthManager {
  private pendingFlows = new Map<string, PendingFlow>();
  private cleanupTimer: ReturnType<typeof setInterval> | null = null;

  constructor() {
    // Auto-cleanup expired flows every 2 minutes
    this.cleanupTimer = setInterval(() => this.cleanupExpired(), 2 * 60 * 1000);
    // Prevent timer from keeping process alive
    if (this.cleanupTimer.unref) this.cleanupTimer.unref();
  }

  /** Stop the cleanup timer (call on shutdown). */
  dispose(): void {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
  }

  /**
   * Generate an OAuth authorize URL with PKCE for a given user.
   * Returns the URL to redirect the user to and the state token
   * (used to correlate the callback).
   */
  generateAuthUrl(openId: string, _redirectUri?: string): { url: string; state: string } {
    const state = base64url(crypto.randomBytes(32));
    const codeVerifier = generateCodeVerifier();
    const codeChallenge = generateCodeChallenge(codeVerifier);

    this.pendingFlows.set(state, {
      codeVerifier,
      openId,
      createdAt: Date.now(),
    });

    // Use Anthropic's own callback URL — third-party redirect_uri
    // is not whitelisted for this client_id.
    // Match CLI parameter order exactly
    const params = new URLSearchParams({
      code: 'true',
      client_id: CLIENT_ID,
      response_type: 'code',
      redirect_uri: ANTHROPIC_CALLBACK_URL,
      scope: SCOPES,
      code_challenge: codeChallenge,
      code_challenge_method: 'S256',
      state,
    });

    return {
      url: `${AUTHORIZE_URL}?${params.toString()}`,
      state,
    };
  }

  /**
   * Exchange an authorization code for tokens.
   * Validates the state parameter against pending flows.
   * Returns the stored token on success, throws on failure.
   */
  /**
   * Parse a pasted auth code string from the Anthropic callback page.
   * Format: `{authorization_code}#{state}`
   * Returns null if the format doesn't match.
   */
  parseAuthCode(input: string): { code: string; state: string } | null {
    const trimmed = input.trim();
    const hashIdx = trimmed.indexOf('#');
    if (hashIdx <= 0 || hashIdx >= trimmed.length - 1) return null;
    const code = trimmed.slice(0, hashIdx);
    const state = trimmed.slice(hashIdx + 1);
    // Validate: state must match a pending flow
    if (!this.pendingFlows.has(state)) return null;
    return { code, state };
  }

  async exchangeCode(
    state: string,
    code: string,
    redirectUri?: string,
  ): Promise<{ token: OAuthToken; openId: string }> {
    const flow = this.pendingFlows.get(state);
    if (!flow) {
      throw new Error('Invalid or expired OAuth state');
    }

    // Check TTL
    if (Date.now() - flow.createdAt > STATE_TTL_MS) {
      this.pendingFlows.delete(state);
      throw new Error('OAuth state expired');
    }

    // Remove used state immediately to prevent replay
    this.pendingFlows.delete(state);

    const body = new URLSearchParams({
      grant_type: 'authorization_code',
      client_id: CLIENT_ID,
      code,
      redirect_uri: redirectUri || ANTHROPIC_CALLBACK_URL,
      code_verifier: flow.codeVerifier,
    });

    const res = await fetch(TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: body.toString(),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Token exchange failed (${res.status}): ${text}`);
    }

    const data = (await res.json()) as {
      access_token: string;
      refresh_token?: string;
      expires_in?: number;
      scope?: string;
    };

    const token: OAuthToken = {
      access_token: data.access_token,
      refresh_token: data.refresh_token,
      expires_at: Math.floor(Date.now() / 1000) + (data.expires_in || 3600),
      scopes: (data.scope || SCOPES).split(' '),
      authorized_at: new Date().toISOString(),
    };

    return { token, openId: flow.openId };
  }

  /**
   * Refresh an expired token using the refresh_token grant.
   * Returns a new OAuthToken on success, throws on failure.
   */
  async refreshToken(token: OAuthToken): Promise<OAuthToken> {
    if (!token.refresh_token) {
      throw new Error('No refresh token available');
    }

    const body = new URLSearchParams({
      grant_type: 'refresh_token',
      client_id: CLIENT_ID,
      refresh_token: token.refresh_token,
    });

    const res = await fetch(TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: body.toString(),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Token refresh failed (${res.status}): ${text}`);
    }

    const data = (await res.json()) as {
      access_token: string;
      refresh_token?: string;
      expires_in?: number;
      scope?: string;
    };

    return {
      access_token: data.access_token,
      refresh_token: data.refresh_token || token.refresh_token,
      expires_at: Math.floor(Date.now() / 1000) + (data.expires_in || 3600),
      scopes: (data.scope || token.scopes.join(' ')).split(' '),
      authorized_at: token.authorized_at,
    };
  }

  /** Check if a token's access_token has not expired. */
  isTokenValid(token: OAuthToken): boolean {
    return token.expires_at > Math.floor(Date.now() / 1000);
  }

  /** Check if a token needs proactive refresh (close to expiry). */
  needsRefresh(token: OAuthToken, bufferSeconds = 300): boolean {
    return token.expires_at - Math.floor(Date.now() / 1000) < bufferSeconds;
  }

  /** Remove expired pending flows. */
  private cleanupExpired(): void {
    const cutoff = Date.now() - STATE_TTL_MS;
    for (const [state, flow] of this.pendingFlows) {
      if (flow.createdAt < cutoff) {
        this.pendingFlows.delete(state);
      }
    }
  }
}
