/**
 * Feishu-specific Markdown processing.
 *
 * Rendering strategy (aligned with Openclaw):
 * - Code blocks / tables → interactive card (schema 2.0 markdown)
 * - Other text → post (msg_type: 'post') with md tag
 *
 * Schema 2.0 cards render code blocks, tables, bold, italic, links properly.
 * Post messages with md tag render bold, italic, inline code, links.
 */

import type { CardDescriptor, CardSection, CardButton, CardHeaderColor } from '../types.js';

/**
 * Detect complex markdown (code blocks / tables).
 * Used by send() to decide between card and post rendering.
 */
export function hasComplexMarkdown(text: string): boolean {
  // Fenced code blocks
  if (/```[\s\S]*?```/.test(text)) return true;
  // Tables: header row followed by separator row with pipes and dashes
  if (/\|.+\|[\r\n]+\|[-:| ]+\|/.test(text)) return true;
  return false;
}

/**
 * Preprocess markdown for Feishu rendering.
 * - Converts headings (# ~ ######) to bold text with spacing (Lark renders
 *   headings without vertical padding, so bold + blank lines looks better).
 * - Ensures code fences have a newline before them.
 * Does NOT touch content inside code blocks.
 */
export function preprocessFeishuMarkdown(text: string): string {
  // Split by code fences to avoid modifying headings inside code blocks
  const parts = text.split(/(```[\s\S]*?```)/g);
  for (let i = 0; i < parts.length; i += 1) {
    // Odd indices are code blocks — skip them
    if (i % 2 === 1) continue;
    // Convert headings to bold with surrounding blank lines
    parts[i] = parts[i]!.replace(/^(#{1,6})\s+(.+)$/gm, (_match, _hashes, title) => {
      return `\n**${title}**\n`;
    });
  }
  let result = parts.join('');
  // Ensure ``` has newline before it (unless at start of text)
  result = result.replace(/([^\n])```/g, '$1\n```');
  // Clean up excessive blank lines (3+ → 2)
  result = result.replace(/\n{3,}/g, '\n\n');
  return result.trim();
}

// ── Card table limit helpers ──────────────────────────────────────────
// Lark interactive cards have a hard limit on the number of markdown tables
// (~10). Exceeding it causes the entire card to be rejected with ErrCode 11310.

/** Maximum tables per card — set below Lark's actual limit for safety margin. */
export const MAX_CARD_TABLES = 8;

/**
 * Count distinct markdown table blocks in text.
 * A table block is a group of consecutive lines starting with `|`.
 * Lines inside fenced code blocks (```) are excluded.
 */
export function countMarkdownTables(text: string): number {
  const lines = text.split('\n');
  let count = 0;
  let inTable = false;
  let inCodeBlock = false;
  for (const line of lines) {
    if (/^\s*```/.test(line)) { inCodeBlock = !inCodeBlock; continue; }
    if (inCodeBlock) continue;
    const isTableLine = /^\s*\|/.test(line);
    if (isTableLine && !inTable) { count++; inTable = true; }
    else if (!isTableLine) { inTable = false; }
  }
  return count;
}

/**
 * Split markdown at a table boundary so the first chunk has at most `maxTables` tables.
 * Returns `{ head, tail }` where head contains the first maxTables tables and
 * tail contains everything after. Splits at the line before the (maxTables+1)-th table.
 */
export function splitAtTableBoundary(text: string, maxTables: number): { head: string; tail: string } {
  const lines = text.split('\n');
  let tableCount = 0;
  let inTable = false;
  let inCodeBlock = false;
  let splitIdx = lines.length;
  for (let i = 0; i < lines.length; i++) {
    if (/^\s*```/.test(lines[i])) { inCodeBlock = !inCodeBlock; continue; }
    if (inCodeBlock) continue;
    const isTableLine = /^\s*\|/.test(lines[i]);
    if (isTableLine && !inTable) {
      tableCount++;
      if (tableCount > maxTables) {
        splitIdx = i;
        break;
      }
      inTable = true;
    } else if (!isTableLine) {
      inTable = false;
    }
  }
  // Walk back past blank lines and section headers that belong to the next table
  while (splitIdx > 0 && lines[splitIdx - 1].trim() === '') splitIdx--;
  // If the line before is a heading (e.g. "**六、ASK Tab**"), include it in tail
  if (splitIdx > 0 && /^\s*[#*]/.test(lines[splitIdx - 1])) splitIdx--;
  while (splitIdx > 0 && lines[splitIdx - 1].trim() === '') splitIdx--;
  return {
    head: lines.slice(0, splitIdx).join('\n'),
    tail: lines.slice(splitIdx).join('\n').trim(),
  };
}

/**
 * Build Feishu interactive card content (schema 2.0 markdown).
 * Renders code blocks, tables, bold, italic, links, inline code properly.
 * Aligned with Openclaw's buildMarkdownCard().
 */
export function buildCardContent(text: string): string {
  return JSON.stringify({
    schema: '2.0',
    config: {
      wide_screen_mode: true,
    },
    body: {
      elements: [
        {
          tag: 'markdown',
          content: text,
        },
      ],
    },
  });
}

/**
 * Build Feishu post message content (msg_type: 'post') with md tag.
 * Used for simple text without code blocks or tables.
 * Aligned with Openclaw's buildFeishuPostMessagePayload().
 */
export function buildPostContent(text: string): string {
  return JSON.stringify({
    zh_cn: {
      content: [[{ tag: 'md', text }]],
    },
  });
}

// ── Interactive card descriptor ──────────────────────────────────────

/** Regex to find ```card:interactive fenced blocks. */
const CARD_BLOCK_RE = /```card:interactive\s*\n([\s\S]*?)```/;

/** Valid header colors for type validation. */
const VALID_COLORS = new Set<string>([
  'blue', 'wathet', 'turquoise', 'green', 'yellow',
  'orange', 'red', 'carmine', 'violet', 'purple',
  'indigo', 'grey', 'default',
]);

/**
 * Extract a card descriptor from LLM output text.
 * Returns the parsed descriptor and the remaining text (outside the fenced block),
 * or null if no valid card block is found.
 */
export function extractCardDescriptor(text: string): { descriptor: CardDescriptor; remainingText: string } | null {
  const match = CARD_BLOCK_RE.exec(text);
  if (!match) return null;

  let raw: unknown;
  try {
    raw = JSON.parse(match[1]);
  } catch {
    return null;
  }

  const descriptor = validateCardDescriptor(raw);
  if (!descriptor) return null;

  const remainingText = text.slice(0, match.index).trimEnd()
    + text.slice(match.index + match[0].length).trimStart();

  return { descriptor, remainingText: remainingText.trim() };
}

/**
 * Validate and coerce a raw JSON value into a CardDescriptor.
 * Returns null if the input is not a valid descriptor.
 */
export function validateCardDescriptor(raw: unknown): CardDescriptor | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;

  const obj = raw as Record<string, unknown>;
  const result: CardDescriptor = {};

  // header
  if (obj.header && typeof obj.header === 'object' && !Array.isArray(obj.header)) {
    const h = obj.header as Record<string, unknown>;
    if (typeof h.title === 'string') {
      const color = typeof h.color === 'string' && VALID_COLORS.has(h.color)
        ? h.color as CardHeaderColor
        : undefined;
      result.header = { title: h.title, color };
    }
  }

  // buttons
  if (Array.isArray(obj.buttons)) {
    const buttons: CardButton[] = [];
    for (const b of obj.buttons) {
      if (b && typeof b === 'object' && typeof (b as any).text === 'string' && typeof (b as any).url === 'string') {
        buttons.push({
          text: (b as any).text,
          url: (b as any).url,
          type: ['primary', 'default', 'danger'].includes((b as any).type) ? (b as any).type : undefined,
        });
      }
    }
    if (buttons.length > 0) result.buttons = buttons;
  }

  // sections
  if (Array.isArray(obj.sections)) {
    const sections: CardSection[] = [];
    for (const s of obj.sections) {
      if (!s || typeof s !== 'object') continue;
      const sec = s as Record<string, unknown>;
      switch (sec.type) {
        case 'markdown':
          if (typeof sec.content === 'string') {
            sections.push({ type: 'markdown', content: sec.content });
          }
          break;
        case 'columns':
          if (Array.isArray(sec.columns) && sec.columns.every((c: unknown) => typeof c === 'string')) {
            const bg = sec.background === 'grey' ? 'grey' : 'default';
            sections.push({ type: 'columns', columns: sec.columns as string[], background: bg });
          }
          break;
        case 'divider':
          sections.push({ type: 'divider' });
          break;
        case 'note':
          if (typeof sec.content === 'string') {
            sections.push({ type: 'note', content: sec.content });
          }
          break;
      }
    }
    if (sections.length > 0) result.sections = sections;
  }

  // Must have at least one meaningful field
  if (!result.header && !result.buttons && !result.sections) return null;

  return result;
}

/**
 * Build a Lark interactive card JSON string from a CardDescriptor.
 * Uses schema 2.0 card format.
 */
export function buildRichCardContent(descriptor: CardDescriptor): string {
  const card: Record<string, unknown> = {
    schema: '2.0',
    config: { wide_screen_mode: true },
  };

  // Header
  if (descriptor.header) {
    card.header = {
      template: descriptor.header.color || 'default',
      title: { tag: 'plain_text', content: descriptor.header.title },
    };
  }

  // Body elements
  const elements: unknown[] = [];

  // Sections
  if (descriptor.sections) {
    for (const section of descriptor.sections) {
      switch (section.type) {
        case 'markdown':
          elements.push({ tag: 'markdown', content: section.content });
          break;
        case 'columns': {
          const columns = section.columns.map((col) => ({
            tag: 'column',
            width: 'weighted',
            weight: 1,
            elements: [{ tag: 'markdown', content: col }],
          }));
          elements.push({
            tag: 'column_set',
            flex_mode: 'none',
            background_style: section.background || 'default',
            columns,
          });
          break;
        }
        case 'divider':
          elements.push({ tag: 'hr' });
          break;
        case 'note':
          elements.push({
            tag: 'note',
            elements: [{ tag: 'plain_text', content: section.content }],
          });
          break;
      }
    }
  }

  // Buttons → action block (after sections)
  if (descriptor.buttons && descriptor.buttons.length > 0) {
    const actions = descriptor.buttons.map((btn) => ({
      tag: 'button',
      text: { tag: 'plain_text', content: btn.text },
      type: btn.type || 'primary',
      url: btn.url,
    }));
    elements.push({ tag: 'action', actions });
  }

  card.body = { elements };

  return JSON.stringify(card);
}

/**
 * Convert simple HTML (from command responses) to markdown for Feishu.
 * Handles common tags: <b>, <i>, <code>, <br>, entities.
 */
export function htmlToFeishuMarkdown(html: string): string {
  return html
    .replace(/<b>(.*?)<\/b>/gi, '**$1**')
    .replace(/<strong>(.*?)<\/strong>/gi, '**$1**')
    .replace(/<i>(.*?)<\/i>/gi, '*$1*')
    .replace(/<em>(.*?)<\/em>/gi, '*$1*')
    .replace(/<code>(.*?)<\/code>/gi, '`$1`')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}
