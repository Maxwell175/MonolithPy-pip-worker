import { AwsClient } from 'aws4fetch';

/**
 * Private PyPI proxy worker.
 *
 * Checks a public S3-compatible bucket for a matching package folder.
 *   - If found  → generates a PEP 503 index.html with direct download links.
 *   - If missing → returns a 302 redirect to the public PyPI index.
 *
 * Aggressive caching keeps S3 list calls to a minimum so you stay in the free tier.
 *
 * Required secrets (set via `npx wrangler secret put <NAME>`):
 *   S3_ENDPOINT        – e.g. https://s3.us-west-002.backblazeb2.com
 *   S3_BUCKET_NAME     – e.g. my-pypi-repo
 *   S3_ACCESS_KEY_ID   – your key id
 *   S3_SECRET_ACCESS_KEY – your app key
 *   S3_REGION          – e.g. us-west-002
 *
 * Optional vars (wrangler.toml [vars]):
 *   CACHE_TTL          – seconds to cache responses (default 86400 = 24 h)
 *   S3_PUBLIC_URL      – public download base URL if different from S3_ENDPOINT
 */

export default {
  async fetch(request, env, ctx) {
    const cache = caches.default;
    const url = new URL(request.url);
    const cacheTtl = parseInt(env.CACHE_TTL || '86400', 10);

    // ── 1. CACHE CHECK ──────────────────────────────────────────────────
    const cacheKey = new Request(url.toString(), request);
    let response = await cache.match(cacheKey);
    if (response) {
      return response;
    }

    // ── DUMMY EMPTY ZIP ─────────────────────────────────────────────────
    // Respond to /dummy/empty/<anything>.zip with a minimal valid zip file.
    // We (ab)use this as a placeholder for pip sources, but the file names need to be distinct.
    if (/^\/dummy\/empty\/.*\.zip$/i.test(url.pathname)) {
      // Minimal empty zip: end-of-central-directory record (22 bytes)
      const emptyZip = new Uint8Array([
        0x50, 0x4b, 0x05, 0x06, // EOCD signature
        0x00, 0x00, 0x00, 0x00, // disk numbers
        0x00, 0x00, 0x00, 0x00, // entry counts
        0x00, 0x00, 0x00, 0x00, // central directory size & offset
        0x00, 0x00,             // comment length
      ]);
      return new Response(emptyZip, {
        headers: {
          'Content-Type': 'application/zip',
        },
      });
    }

    // ── 2. PARSE PATH ─────────────────────────────────────────────────
    // Supports:  /simple/numpy/  or  /main/simple/numpy/
    // The part before /simple/ (if any) becomes a bucket key prefix.
    // e.g. /main/simple/numpy/ → bucketPrefix="main/", pkg="numpy"
    const simpleIdx = url.pathname.indexOf('/simple/');
    if (simpleIdx === -1) {
      return new Response('Private PyPI Repo Active', {
        headers: { 'Content-Type': 'text/plain' },
      });
    }

    const bucketPrefix = url.pathname.slice(1, simpleIdx + 1).replace(/^\/+/, ''); // "main/" or ""
    const pkg = url.pathname
      .slice(simpleIdx + '/simple/'.length)
      .replace(/\/index\.html$/, '')
      .replace(/\/$/, '');

    if (!pkg) {
      return new Response('Private PyPI Repo Active', {
        headers: { 'Content-Type': 'text/plain' },
      });
    }

    // S3 key prefix: e.g. "main/numpy" or just "numpy"
    const s3Prefix = bucketPrefix ? `${bucketPrefix}${pkg}` : pkg;

    // ── 3. BUILD S3 CLIENT ──────────────────────────────────────────────
    const client = new AwsClient({
      accessKeyId: env.S3_ACCESS_KEY_ID,
      secretAccessKey: env.S3_SECRET_ACCESS_KEY,
      service: 's3',
      region: env.S3_REGION,
    });

    // ── 4. LIST OBJECTS IN BUCKET ───────────────────────────────────────
    const listUrl = `${env.S3_ENDPOINT}/${env.S3_BUCKET_NAME}?list-type=2&prefix=${encodeURIComponent(s3Prefix)}/`;
    const s3Response = await client.fetch(listUrl);

    if (s3Response.status === 404 || s3Response.status === 403) {
      // Bucket-level error – fall through to public PyPI
      return Response.redirect(`https://pypi.org/simple/${pkg}/`, 302);
    }

    const xml = await s3Response.text();

    // ── 5. DECIDE: REDIRECT OR GENERATE INDEX ───────────────────────────
    if (!xml.includes('<Key>')) {
      // Package not found in our bucket → cache the redirect so we don't
      // keep asking S3 for packages we don't host.
      response = new Response(null, {
        status: 302,
        headers: {
          Location: `https://pypi.org/simple/${pkg}/`,
        },
      });
      ctx.waitUntil(cache.put(cacheKey, response.clone()));
      return response;
    }

    // ── 6. GENERATE PEP 503 INDEX ───────────────────────────────────────
    // Bucket is public – link directly to the objects, no signing needed.
    const publicBase = (env.S3_PUBLIC_URL || env.S3_ENDPOINT).replace(/\/$/, '');
    const keys = [...xml.matchAll(/<Key>(.*?)<\/Key>/g)].map((m) => m[1]);
    const links = keys
      .map((key) => {
        const filename = key.split('/').pop();
        if (!filename) return '';
        const href = `${publicBase}/${env.S3_BUCKET_NAME}/${key}`;
        return `    <a href="${href}">${filename}</a>`;
      })
      .filter(Boolean);

    const html = [
      '<!DOCTYPE html>',
      '<html><head><title>Links for ' + pkg + '</title></head>',
      '<body>',
      '<h1>Links for ' + pkg + '</h1>',
      ...links,
      '</body></html>',
    ].join('\n');

    response = new Response(html, {
      headers: {
        'Content-Type': 'text/html; charset=utf-8',
      },
    });

    // ── 7. STORE IN CACHE ───────────────────────────────────────────────
    ctx.waitUntil(cache.put(cacheKey, response.clone()));
    return response;
  },
};



