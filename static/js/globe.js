function initGlobe() {
  if (typeof THREE === 'undefined' || window._noThree) {
    const container = document.getElementById('globe-container');
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--accent);font-size:.85rem;opacity:.6">HyperBulk</div>';
    window.addTradeToGlobe = function() {};
    return;
  }
  const container = document.getElementById('globe-container');
  const canvas = document.getElementById('globe-canvas');
  const w = container.clientWidth, h = 250;

  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(45, w / h, 0.1, 1000);
  camera.position.z = 3.5;

  const renderer = new THREE.WebGLRenderer({ canvas, antialias: true, alpha: true });
  renderer.setSize(w, h);
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
  renderer.setClearColor(0x000000, 0);

  // Wireframe globe
  const geo = new THREE.SphereGeometry(1, 24, 24);
  const mat = new THREE.MeshBasicMaterial({ color: 0x00ff88, wireframe: true, transparent: true, opacity: 0.15 });
  const globe = new THREE.Mesh(geo, mat);
  scene.add(globe);

  // Stars
  const starsGeo = new THREE.BufferGeometry();
  const starVerts = [];
  for (let i = 0; i < 500; i++) {
    starVerts.push((Math.random() - 0.5) * 20, (Math.random() - 0.5) * 20, (Math.random() - 0.5) * 20);
  }
  starsGeo.setAttribute('position', new THREE.Float32BufferAttribute(starVerts, 3));
  const starsMat = new THREE.PointsMaterial({ color: 0xffffff, size: 0.03, transparent: true, opacity: 0.6 });
  scene.add(new THREE.Points(starsGeo, starsMat));

  // Trade dots — driven by WebSocket or simulated fallback
  const dots = [];
  let wsTradeCount = 0;  // Track if real trades are flowing

  function spawnDot(isBuy) {
    const phi = Math.random() * Math.PI;
    const theta = Math.random() * Math.PI * 2;
    const r = 1.02;
    const x = r * Math.sin(phi) * Math.cos(theta);
    const y = r * Math.cos(phi);
    const z = r * Math.sin(phi) * Math.sin(theta);
    const dotGeo = new THREE.SphereGeometry(0.025, 6, 6);
    const dotMat = new THREE.MeshBasicMaterial({ color: isBuy ? 0x00ff88 : 0xff4444, transparent: true, opacity: 1 });
    const dot = new THREE.Mesh(dotGeo, dotMat);
    dot.position.set(x, y, z);
    globe.add(dot);
    dots.push({ mesh: dot, born: Date.now() });
  }

  // Exposed to WebSocket handler
  window.addTradeToGlobe = function(isBuy) {
    wsTradeCount++;
    spawnDot(isBuy);
  };

  // Simulated fallback — only fires if no real trades in last 5s
  setInterval(() => {
    if (wsTradeCount === 0) spawnDot(Math.random() > 0.4);
    wsTradeCount = 0;  // Reset counter each interval
  }, 2000);

  function animate() {
    requestAnimationFrame(animate);
    globe.rotation.y += 0.003;
    globe.rotation.x = 0.15;
    const now = Date.now();
    for (let i = dots.length - 1; i >= 0; i--) {
      const age = (now - dots[i].born) / 1000;
      if (age > 3) { globe.remove(dots[i].mesh); dots[i].mesh.geometry.dispose(); dots[i].mesh.material.dispose(); dots.splice(i, 1); }
      else { dots[i].mesh.material.opacity = 1 - age / 3; }
    }
    renderer.render(scene, camera);
  }
  animate();

  window.addEventListener('resize', () => {
    const nw = container.clientWidth;
    camera.aspect = nw / h;
    camera.updateProjectionMatrix();
    renderer.setSize(nw, h);
  });
}
