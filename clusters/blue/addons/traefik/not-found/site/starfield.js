// starfield.js — animated starfield for a <canvas id="stars">, temporal.io style.
// Parallax drift, twinkle, an occasional shooting star, and a "warp" effect:
// call Starfield.setLoad(0..1) to make stars streak as load rises.
// Starfield.setPaused(true) freezes it; wire that to an optional pause-motion
// control if the page has one. Respects prefers-reduced-motion.
// No dependencies.
//
//   <canvas id="stars" aria-hidden="true"></canvas>
//   <script src="assets/js/starfield.js"></script>
//   <script>Starfield.start(document.getElementById("stars"));</script>
(function () {
  "use strict";
  const reduceMotion = matchMedia("(prefers-reduced-motion: reduce)").matches;
  let load = 0, paused = false;

  function start(canvas) {
    const ctx = canvas.getContext("2d");
    let w = 0, h = 0, field = [], shooting = null, last = performance.now();

    function newStar(anywhere) {
      const depth = Math.random();
      return {
        x: anywhere ? Math.random() * w : w + 2,
        y: Math.random() * h,
        z: depth,
        r: 0.3 + depth * 1.3,
        tw: Math.random() * Math.PI * 2,
        // mostly white, a few lavender/purple
        hue: Math.random() < 0.12 ? (Math.random() < 0.5 ? "203,203,249" : "182,100,255") : "248,250,252",
      };
    }
    function resize() {
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      w = canvas.clientWidth; h = canvas.clientHeight;
      canvas.width = w * dpr; canvas.height = h * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      field = Array.from({ length: Math.round((w * h) / 2600) }, () => newStar(true));
    }
    function frame(now) {
      const dt = Math.min(64, now - last) / 1000; last = now;
      ctx.clearRect(0, 0, w, h);
      const warp = 4 + load * 140;
      const still = reduceMotion || paused;
      for (const s of field) {
        if (!still) s.tw += dt * (1 + s.z * 2);
        if (!still) s.x -= warp * (0.15 + s.z) * dt;
        if (s.x < -10) Object.assign(s, newStar(false));
        const a = 0.35 + 0.65 * (0.5 + 0.5 * Math.sin(s.tw)) * (0.4 + s.z * 0.6);
        if (load > 0.5 && s.z > 0.75 && !still) {
          // subtle streaks at high load; keep them short so text stays readable
          ctx.fillStyle = `rgba(${s.hue},${(a * 0.55).toFixed(3)})`;
          ctx.fillRect(s.x, s.y - s.r / 2, Math.min(14, (load - 0.45) * 26 * s.z), s.r);
        } else {
          ctx.fillStyle = `rgba(${s.hue},${a.toFixed(3)})`;
          ctx.beginPath(); ctx.arc(s.x, s.y, s.r, 0, Math.PI * 2); ctx.fill();
        }
      }
      if (!still) {
        if (!shooting && Math.random() < dt * 0.08) {
          shooting = { x: Math.random() * w, y: Math.random() * h * 0.4, vx: -600 - Math.random() * 400, vy: 220, life: 1 };
        }
        if (shooting) {
          const s = shooting;
          s.x += s.vx * dt; s.y += s.vy * dt; s.life -= dt * 1.2;
          const g = ctx.createLinearGradient(s.x, s.y, s.x - s.vx * 0.12, s.y - s.vy * 0.12);
          g.addColorStop(0, `rgba(248,250,252,${Math.max(0, s.life)})`);
          g.addColorStop(1, "rgba(248,250,252,0)");
          ctx.strokeStyle = g; ctx.lineWidth = 1.5;
          ctx.beginPath(); ctx.moveTo(s.x, s.y); ctx.lineTo(s.x - s.vx * 0.12, s.y - s.vy * 0.12); ctx.stroke();
          if (s.life <= 0) shooting = null;
        }
      }
      requestAnimationFrame(frame);
    }
    addEventListener("resize", resize);
    resize();
    requestAnimationFrame(frame);
  }

  window.Starfield = {
    start,
    setLoad: (v) => { load = Math.max(0, Math.min(1, v)); },
    setPaused: (v) => { paused = !!v; },
  };
})();
