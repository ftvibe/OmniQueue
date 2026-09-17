// Applies the saved theme before first paint (kept separate so the page needs no inline script).
try { const t = localStorage.getItem("omniqueue.theme"); if (t && t !== "auto") document.documentElement.dataset.theme = t; } catch (e) { /* ignore */ }
