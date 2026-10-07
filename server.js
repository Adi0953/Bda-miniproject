const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');

const root = __dirname;
const files = new Map([
  ['/', 'index.html'],
  ['/index.html', 'index.html'],
  ['/sample_complaints.csv', 'sample_complaints.csv'],
  ['/rail_complaints_app.csv', 'rail_complaints_app.csv'],
]);
const port = Number(process.env.PORT) || 8000;

const server = http.createServer((req, res) => {
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    res.writeHead(405, { Allow: 'GET, HEAD' }).end();
    return;
  }

  let pathname;
  try {
    pathname = new URL(req.url, 'http://localhost').pathname;
  } catch {
    res.writeHead(400).end('Bad request');
    return;
  }

  const filename = files.get(pathname);
  if (!filename) {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }).end('Not found');
    return;
  }

  const filePath = path.join(root, filename);
  fs.readFile(filePath, (error, content) => {
    if (error) {
      res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' }).end('Could not read app file');
      return;
    }
    const type = filename.endsWith('.csv') ? 'text/csv; charset=utf-8' : 'text/html; charset=utf-8';
    res.writeHead(200, { 'Content-Type': type, 'Cache-Control': 'no-store' });
    res.end(req.method === 'HEAD' ? undefined : content);
  });
});

server.on('error', (error) => {
  if (error.code === 'EADDRINUSE') {
    console.error(`Port ${port} is already in use. The app may already be running at http://localhost:${port}.`);
    process.exitCode = 1;
    return;
  }
  console.error('Could not start the web server:', error.message);
  process.exitCode = 1;
});

server.listen(port, '127.0.0.1', () => {
  console.log(`RailBigData is running at http://localhost:${port}`);
  console.log('Keep this window open while using the dashboard. Press Ctrl+C to stop.');
});
