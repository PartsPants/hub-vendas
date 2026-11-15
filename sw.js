/* Hub de Vendas · service worker: guarda o "esqueleto" do site para abrir sem internet.
   Os dados NÃO passam por aqui (vão direto ao Supabase). Ao publicar versão nova, mude o número. */
const VERSAO = 'vendas-v1';
const ESQUELETO = ['./', './index.html', './nucleo.js', './config.js', './manifest.webmanifest',
  './icons/icon-192.png', './icons/icon-512.png', './icons/icon-maskable-512.png', './icons/apple-touch-icon.png',
  'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.118.0/dist/umd/supabase.js'];
self.addEventListener('install', e => { e.waitUntil(caches.open(VERSAO).then(c => Promise.all(ESQUELETO.map(u => c.add(u).catch(() => null)))).then(() => self.skipWaiting())); });
self.addEventListener('activate', e => { e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== VERSAO).map(k => caches.delete(k)))).then(() => self.clients.claim())); });
async function redePrimeiro(req, chave) {
  const cache = await caches.open(VERSAO);
  try {
    const resp = await Promise.race([fetch(req), new Promise((_, no) => setTimeout(() => no(new Error('lento')), 4000))]);
    if (resp && resp.ok) cache.put(chave || req, resp.clone());
    return resp;
  } catch (_) {
    const c = await cache.match(chave || req, { ignoreSearch: true }); if (c) return c;
    if (req.mode === 'navigate') { const i = await cache.match('./index.html'); if (i) return i; }
    return Response.error();
  }
}
async function cachePrimeiro(req) {
  const cache = await caches.open(VERSAO), c = await cache.match(req); if (c) return c;
  const resp = await fetch(req); if (resp && (resp.ok || resp.type === 'opaque')) cache.put(req, resp.clone()); return resp;
}
self.addEventListener('fetch', e => {
  const req = e.request; if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (url.origin === self.location.origin) { e.respondWith(redePrimeiro(req, req.mode === 'navigate' ? './index.html' : undefined)); return; }
  if (['cdn.jsdelivr.net', 'fonts.googleapis.com', 'fonts.gstatic.com'].includes(url.hostname)) e.respondWith(cachePrimeiro(req));
});
