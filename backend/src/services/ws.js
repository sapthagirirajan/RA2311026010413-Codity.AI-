/** Bonus feature: WebSocket live updates for the dashboard. */
let wss = null;
const clients = new Set();

function init(server) {
  const { WebSocketServer } = require('ws');
  wss = new WebSocketServer({ server, path: '/ws' });
  wss.on('connection', (socket) => {
    clients.add(socket);
    socket.send(JSON.stringify({ event: 'connected', message: 'Live updates connected' }));
    socket.on('close', () => clients.delete(socket));
  });
  console.log('WebSocket live-updates server ready at /ws');
}

function broadcast(payload) {
  const data = JSON.stringify(payload);
  for (const client of clients) {
    if (client.readyState === 1) client.send(data);
  }
}

module.exports = { init, broadcast };
