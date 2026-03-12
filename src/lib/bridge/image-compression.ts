/**
 * Image Compression — auto-resize images that exceed the Claude API's 5MB base64 limit.
 *
 * The Claude Code CLI rejects images whose base64 encoding exceeds 5MB.
 * This utility compresses oversized images before they reach the LLM provider.
 */

import sharp from 'sharp';
import type { FileAttachment } from './host.js';

/** Base64 size limit in bytes. The API limit is 5MB; we target 4MB for safety margin. */
const MAX_BASE64_BYTES = 4 * 1024 * 1024;

/** Raw bytes equivalent of the base64 limit (base64 adds ~33% overhead). */
const MAX_RAW_BYTES = Math.floor(MAX_BASE64_BYTES * 3 / 4);

const IMAGE_TYPES = new Set(['image/png', 'image/jpeg', 'image/jpg', 'image/gif', 'image/webp']);

/**
 * Compress a single image if its base64 representation exceeds the API limit.
 * Returns the original attachment if no compression is needed.
 */
async function compressImage(file: FileAttachment): Promise<FileAttachment> {
  // Only process known image types
  if (!IMAGE_TYPES.has(file.type)) return file;

  const rawBuffer = Buffer.from(file.data, 'base64');

  // Check if compression is needed (base64 size ≈ raw * 4/3)
  const estimatedBase64 = Math.ceil(rawBuffer.length * 4 / 3);
  if (estimatedBase64 <= MAX_BASE64_BYTES) return file;

  console.log(
    `[image-compression] Compressing ${file.name}: ${(estimatedBase64 / 1024 / 1024).toFixed(1)}MB base64 → target <${(MAX_BASE64_BYTES / 1024 / 1024).toFixed(0)}MB`,
  );

  let image = sharp(rawBuffer);
  const metadata = await image.metadata();

  // Calculate scale factor to fit within size limit.
  // Rough heuristic: scale dimensions by sqrt(targetSize / currentSize).
  const scaleFactor = Math.sqrt(MAX_RAW_BYTES / rawBuffer.length);
  const targetWidth = Math.round((metadata.width || 1920) * scaleFactor);
  const targetHeight = Math.round((metadata.height || 1080) * scaleFactor);

  // Cap maximum dimensions at 2048px on the longest side
  const maxDim = 2048;
  const longestSide = Math.max(targetWidth, targetHeight);
  const dimScale = longestSide > maxDim ? maxDim / longestSide : 1;

  const finalWidth = Math.round(targetWidth * dimScale);
  const finalHeight = Math.round(targetHeight * dimScale);

  image = image.resize(finalWidth, finalHeight, { fit: 'inside', withoutEnlargement: true });

  // Convert to JPEG for best compression (unless already JPEG/WebP)
  let outputBuffer: Buffer;
  let outputType = file.type;

  if (file.type === 'image/webp') {
    outputBuffer = await image.webp({ quality: 80 }).toBuffer();
  } else {
    // Convert PNG/GIF/other to JPEG for smaller size
    outputBuffer = await image.jpeg({ quality: 80, mozjpeg: true }).toBuffer();
    outputType = 'image/jpeg';
  }

  // If still too large, progressively reduce quality
  let quality = 70;
  while (Math.ceil(outputBuffer.length * 4 / 3) > MAX_BASE64_BYTES && quality >= 30) {
    if (outputType === 'image/webp') {
      outputBuffer = await sharp(rawBuffer)
        .resize(finalWidth, finalHeight, { fit: 'inside', withoutEnlargement: true })
        .webp({ quality })
        .toBuffer();
    } else {
      outputBuffer = await sharp(rawBuffer)
        .resize(finalWidth, finalHeight, { fit: 'inside', withoutEnlargement: true })
        .jpeg({ quality, mozjpeg: true })
        .toBuffer();
    }
    quality -= 10;
  }

  const newBase64 = outputBuffer.toString('base64');
  console.log(
    `[image-compression] Compressed ${file.name}: ${(estimatedBase64 / 1024 / 1024).toFixed(1)}MB → ${(newBase64.length / 1024 / 1024).toFixed(1)}MB base64`,
  );

  return {
    ...file,
    type: outputType,
    size: outputBuffer.length,
    data: newBase64,
    name: outputType === 'image/jpeg' && !file.name.match(/\.jpe?g$/i)
      ? file.name.replace(/\.[^.]+$/, '.jpg')
      : file.name,
  };
}

/**
 * Compress all oversized images in a list of file attachments.
 * Non-image files are passed through unchanged.
 */
export async function compressImages(files: FileAttachment[]): Promise<FileAttachment[]> {
  return Promise.all(files.map(compressImage));
}
