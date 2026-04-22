/* WebSocket manager — auto-reconnect, event dispatch, connection status */

let _ws = null;
let _wsRetry = 0;
const _wsMaxRetry = 10;
const _wsHandlers = {};
let _wsConnected = false;

function wsConnect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = `${proto}//${location.host}/ws`;

  try { _ws = new WebSocket(url); } catch { return _scheduleReconnect(); }

  _ws.onopen = () => {
    _wsRetry = 0;
    _wsConnected = true;
    _updateStatusDot(true);
  };

  _ws.onclose = () => {
    _wsConnected = false;
    _updateStatusDot(false);
    _scheduleReconnect();
  };

  _ws.onerror = () => { _ws.close(); };

  _ws.onmessage = (evt) => {
    try {
      const msg = JSON.parse(evt.data);
      const type = msg.type || msg.event;
      const data = msg.data ? (typeof msg.data === 'string' ? JSON.parse(msg.data) : msg.data) : msg;
      if (type && _wsHandlers[type]) {
        _wsHandlers[type].forEach(fn => fn(data));
      }
    } catch {}
  };
}

function wsOn(event, handler) {
  if (!_wsHandlers[event]) _wsHandlers[event] = [];
  _wsHandlers[event].push(handler);
}

function wsOff(event, handler) {
  if (!_wsHandlers[event]) return;
  _wsHandlers[event] = _wsHandlers[event].filter(fn => fn !== handler);
}

function wsIsConnected() { return _wsConnected; }

function _scheduleReconnect() {
  if (_wsRetry >= _wsMaxRetry) return;
  _wsRetry++;
  const delay = Math.min(1000 * Math.pow(1.5, _wsRetry), 30000);
  setTimeout(wsConnect, delay);
}

function _updateStatusDot(connected) {
  const dot = document.getElementById('ws-dot');
  if (!dot) return;
  dot.className = connected ? 'dot dot-green' : 'dot dot-red';
  const label = document.getElementById('ws-label');
  if (label) label.textContent = connected ? 'Live' : 'Offline';
}

window.wsConnect = wsConnect;
window.wsOn = wsOn;
window.wsOff = wsOff;
window.wsIsConnected = wsIsConnected;
