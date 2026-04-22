/* Toast notification system — stackable, auto-dismiss, no spinners */

const _toastStack = document.createElement('div');
_toastStack.className = 'toast-stack';
document.body.appendChild(_toastStack);

function toast(message, type = 'info', duration = 2500) {
  const el = document.createElement('div');
  el.className = `toast toast-${type}`;
  el.textContent = message;
  el.onclick = () => _dismiss(el);
  _toastStack.appendChild(el);

  if (_toastStack.children.length > 3) {
    _dismiss(_toastStack.firstChild);
  }

  setTimeout(() => _dismiss(el), duration);
  return el;
}

function _dismiss(el) {
  if (!el || !el.parentNode) return;
  el.classList.add('exiting');
  setTimeout(() => el.remove(), 150);
}

window.toast = toast;
