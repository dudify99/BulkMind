/* Simple hash-based router — no framework dependency */

const _routes = {};
let _currentPage = null;

function registerPage(name, { init, destroy } = {}) {
  _routes[name] = { init, destroy };
}

function navigate(page) {
  if (_currentPage === page) return;

  // Hide all pages
  document.querySelectorAll('[data-page]').forEach(el => {
    el.classList.remove('active');
    el.style.display = 'none';
  });

  // Deactivate all nav items
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));

  // Destroy previous page
  if (_currentPage && _routes[_currentPage]?.destroy) {
    _routes[_currentPage].destroy();
  }

  // Show target page
  const target = document.querySelector(`[data-page="${page}"]`);
  if (target) {
    target.style.display = '';
    target.classList.add('active');
  }

  // Activate nav item
  const navItem = document.querySelector(`.nav-item[data-nav="${page}"]`);
  if (navItem) navItem.classList.add('active');

  // Mobile nav
  const mobileItem = document.querySelector(`.mobile-nav-item[data-nav="${page}"]`);
  document.querySelectorAll('.mobile-nav-item').forEach(el => el.classList.remove('active'));
  if (mobileItem) mobileItem.classList.add('active');

  _currentPage = page;
  window.location.hash = page;

  // Init new page
  if (_routes[page]?.init) {
    _routes[page].init();
  }
}

function initRouter(defaultPage = 'trade') {
  // Hash change listener
  window.addEventListener('hashchange', () => {
    const page = window.location.hash.slice(1) || defaultPage;
    navigate(page);
  });

  // Nav click handlers
  document.querySelectorAll('[data-nav]').forEach(el => {
    el.addEventListener('click', (e) => {
      e.preventDefault();
      navigate(el.dataset.nav);
    });
  });

  // Initial route
  const initial = window.location.hash.slice(1) || defaultPage;
  navigate(initial);
}

window.registerPage = registerPage;
window.navigate = navigate;
window.initRouter = initRouter;
