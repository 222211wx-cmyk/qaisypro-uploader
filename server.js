// Qaisypro - Arena Smart Upload
import express from 'express';
import multer from 'multer';
import os from 'os';
import fs from 'fs';
import path from 'path';
import unzipper from 'unzipper';
import { S3Client, PutObjectCommand, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand } from '@aws-sdk/client-s3';

const app = express();
// Disk storage: large uploads never sit in RAM
const upload = multer({ dest: os.tmpdir() });
const Bucket = process.env.R2_BUCKET;
const MB = 1024 * 1024;
const CHUNK = 10 * MB;
const WAR_THRESHOLD = 100 * MB;
const MAX_ZIP_ENTRIES = 2000;
const MAX_ZIP_TOTAL = 5 * 1024 * MB;

const s3 = new S3Client({
  region: 'auto',
  forcePathStyle: true,
  endpoint: process.env.R2_ENDPOINT,
  credentials: { accessKeyId: process.env.R2_ACCESS_KEY_ID || '', secretAccessKey: process.env.R2_SECRET_ACCESS_KEY || '' },
});

const configured = () => ['R2_ENDPOINT', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_BUCKET'].filter((k) => !process.env[k]);

function safeName(p) {
  return path.posix.normalize(p.replace(/\\/g, '/')).split('/').filter((s) => s && s !== '..' && s !== '.').join('/');
}

// Multipart upload from a stream, 10MB parts (R2 needs equal-size parts except the last)
async function warUpload(Key, stream) {
  const { UploadId } = await s3.send(new CreateMultipartUploadCommand({ Bucket, Key }));
  try {
    const parts = [];
    let buf = [], len = 0;
    const flush = async () => {
      const Body = Buffer.concat(buf);
      buf = []; len = 0;
      const PartNumber = parts.length + 1;
      const p = await s3.send(new UploadPartCommand({ Bucket, Key, UploadId, PartNumber, Body }));
      parts.push({ ETag: p.ETag, PartNumber });
    };
    for await (const c of stream) {
      buf.push(c); len += c.length;
      while (len >= CHUNK) {
        const all = Buffer.concat(buf);
        buf = [all.subarray(CHUNK)]; len = all.length - CHUNK;
        const Body = all.subarray(0, CHUNK);
        const PartNumber = parts.length + 1;
        const p = await s3.send(new UploadPartCommand({ Bucket, Key, UploadId, PartNumber, Body }));
        parts.push({ ETag: p.ETag, PartNumber });
      }
    }
    if (len > 0 || parts.length === 0) await flush();
    await s3.send(new CompleteMultipartUploadCommand({ Bucket, Key, UploadId, MultipartUpload: { Parts: parts } }));
  } catch (e) {
    await s3.send(new AbortMultipartUploadCommand({ Bucket, Key, UploadId })).catch(() => {});
    throw e;
  }
}

async function put(Key, size, stream, ContentType) {
  if (size > WAR_THRESHOLD) return warUpload(Key, stream);
  const Body = Buffer.concat(await stream.toArray());
  await s3.send(new PutObjectCommand({ Bucket, Key, Body, ContentType }));
}

app.get('/health', (_req, res) => res.json({ ok: true, missing: configured() }));
app.use(express.static('public'));

app.post('/api/qaisypro', (req, res, next) => {
  const token = process.env.UPLOAD_TOKEN;
  if (token && req.get('authorization') !== `Bearer ${token}`) return res.status(401).json({ success: false, error: 'unauthorized' });
  next();
}, upload.single('file'), async (req, res) => {
  const file = req.file;
  try {
    const missing = configured();
    if (missing.length) return res.status(503).json({ success: false, error: `Missing env vars: ${missing.join(', ')}` });
    if (!file) return res.status(400).json({ success: false, error: 'No file (field name "file")' });
    const name = safeName(file.originalname) || 'file';
    const isZip = name.toLowerCase().endsWith('.zip');
    const WAR = file.size > WAR_THRESHOLD || isZip;
    const stamp = Date.now();
    const uploaded = [];

    if (isZip) {
      const dir = await unzipper.Open.file(file.path);
      let total = 0;
      const entries = dir.files.filter((e) => e.type === 'File');
      if (entries.length > MAX_ZIP_ENTRIES) return res.status(413).json({ success: false, error: 'Too many files in zip' });
      for (const e of entries) {
        total += e.uncompressedSize;
        if (total > MAX_ZIP_TOTAL) return res.status(413).json({ success: false, error: 'Zip too large when extracted' });
      }
      for (const e of entries) {
        const n = safeName(e.path);
        if (!n) continue;
        await put(`qaisypro/${stamp}-${n}`, e.uncompressedSize, e.stream());
        uploaded.push(n);
      }
    } else {
      await put(`qaisypro/${stamp}-${name}`, file.size, fs.createReadStream(file.path), file.mimetype);
      uploaded.push(name);
    }
    res.json({ success: true, system: 'Qaisypro', mode: WAR ? 'WAR' : 'NORMAL', uploaded });
  } catch (e) {
    console.error(e);
    res.status(500).json({ success: false, error: e.message });
  } finally {
    if (file) fs.unlink(file.path, () => {});
  }
});

app.listen(process.env.PORT || 8080, '0.0.0.0', () => console.log('Qaisypro Ready'));
