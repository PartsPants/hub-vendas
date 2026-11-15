/* =====================================================================
   Hub de Vendas · núcleo do site (pessoal, fechado por e-mail e senha)
   Faz o login, guarda os dados no Supabase com cópia no aparelho e
   entrega ao código do Hub o mesmo "banco" que ele usava no Claude.
   ===================================================================== */
(() => {
  'use strict';
  const CFG = window.HUB_CONFIG || {};
  const DEMO = !CFG.supabaseUrl || !CFG.supabaseKey;
  const COLS = ['kits', 'vendas', 'estoque', 'despesas', 'tarefas', 'diario', 'distribuidores', 'config'];
  const $ = id => document.getElementById(id);
  const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
  const lsGet = k => { try { const v = localStorage.getItem(k); return v ? JSON.parse(v) : null; } catch (_) { return null; } };
  const lsSet = (k, v) => { try { localStorage.setItem(k, JSON.stringify(v)); } catch (_) { } };
  const lsDel = k => { try { localStorage.removeItem(k); } catch (_) { } };

  /* ---------- o código do Hub pede o banco assim: claude.use('db'). Ele espera até o login. ---------- */
  let liberarDb; const dbPronto = new Promise(r => { liberarDb = r; });
  window.claude = { use: async cap => cap === 'db' ? dbPronto : cap === 'user' ? { can: async () => true } : null };

  /* ---------- guardado no aparelho ---------- */
  const kv = (() => {
    let dbp = null;
    const abrir = () => dbp || (dbp = new Promise((ok, no) => {
      try { const r = indexedDB.open('hub-vendas', 1); r.onupgradeneeded = () => r.result.createObjectStore('kv'); r.onsuccess = () => ok(r.result); r.onerror = () => no(r.error); } catch (e) { no(e); }
    }));
    const tx = async (modo, fn) => { const db = await abrir(); return new Promise((ok, no) => { const t = db.transaction('kv', modo), req = fn(t.objectStore('kv')); t.oncomplete = () => ok(req ? req.result : undefined); t.onerror = () => no(t.error); t.onabort = () => no(t.error); }); };
    return {
      get: k => tx('readonly', st => st.get(k)).catch(() => lsGet('kv:' + k)),
      set: (k, v) => tx('readwrite', st => st.put(v, k)).catch(() => lsSet('kv:' + k, v)),
      del: k => tx('readwrite', st => st.delete(k)).catch(() => lsDel('kv:' + k)),
    };
  })();
  const setOffline = v => document.body.classList.toggle('offline', !!v);

  /* ---------- store: coleção/doc/onSnapshot com fila offline ---------- */
  function criarStore(backend, uid) {
    const chave = 'cache:' + uid;
    const docs = new Map(), colL = new Map(), docL = new Map(), pend = new Map(), espera = new Map();
    let fila = [], seq = 1, lastSeen = null, pronto = false, flushing = false, timerFlush = null, timerCache = null, timerFila = null, tent = 0, ativo = true;
    const kk = (c, i) => c + '\u0001' + i;
    const colMap = c => { let m = docs.get(c); if (!m) docs.set(c, m = new Map()); return m; };
    const aplicar = (col, id, data) => { const m = colMap(col); if (data == null) m.delete(id); else m.set(id, data); };
    const snapCol = col => ({ docs: [...colMap(col)].map(([id, data]) => ({ id, exists: true, data: () => data, metadata: { hasPendingWrites: pend.has(kk(col, id)) } })) });
    const snapDoc = (col, id) => { const d = colMap(col).get(id); return { id, exists: d !== undefined, data: () => d }; };
    const sujos = new Set(); let emitP = null;
    function emitir(col) {
      if (!pronto) return;
      sujos.add(col); if (emitP) return;
      emitP = Promise.resolve().then(() => {
        emitP = null; const cs = [...sujos]; sujos.clear();
        for (const c of cs) {
          for (const cb of colL.get(c) || []) { try { cb(snapCol(c)); } catch (e) { console.error(e); } }
          for (const [k, set] of docL) if (k.startsWith(c + '/')) for (const cb of set) { try { cb(snapDoc(c, k.slice(c.length + 1))); } catch (e) { console.error(e); } }
        }
      });
    }
    const emitirTodos = () => new Set([...colL.keys(), ...[...docL.keys()].map(k => k.split('/')[0])]).forEach(emitir);
    async function salvarCache() { const o = {}; for (const [c, m] of docs) { o[c] = {}; for (const [id, d] of m) o[c][id] = d; } await kv.set(chave, { docs: o, lastSeen, t: Date.now() }); }
    const salvarCacheLogo = () => { clearTimeout(timerCache); timerCache = setTimeout(salvarCache, 700); };
    const gravarFila = () => { clearTimeout(timerFila); timerFila = null; return kv.set(chave + ':fila', fila.map(({ col, id, data }) => ({ col, id, data }))); };
    const salvarFila = () => { if (!timerFila) timerFila = setTimeout(gravarFila, 150); };
    function write(col, id, data, op) {
      if (!ativo) return Promise.reject({ code: 'not_granted' });
      let final = op === 'delete' ? null : op === 'update' ? { ...(colMap(col).get(id) || {}), ...data } : data;
      try { final = final == null ? null : JSON.parse(JSON.stringify(final)); } catch (_) { return Promise.reject({ code: 'invalid_argument' }); }
      aplicar(col, id, final);
      const k = kk(col, id); pend.set(k, (pend.get(k) || 0) + 1);
      const s = seq++; fila.push({ s, col, id, data: final });
      salvarFila(); salvarCacheLogo(); emitir(col);
      const p = new Promise((ok, no) => espera.set(s, { ok, no }));
      agendar(0); return p;
    }
    function agendar(ms) { clearTimeout(timerFlush); timerFlush = setTimeout(flush, ms); }
    function resolver(lote, erro) {
      const cols = new Set();
      for (const o of lote) {
        const k = kk(o.col, o.id), n = (pend.get(k) || 1) - 1; if (n <= 0) pend.delete(k); else pend.set(k, n);
        cols.add(o.col);
        const w = espera.get(o.s); if (w) { espera.delete(o.s); erro ? w.no({ code: erro.code }) : w.ok(); }
      }
      cols.forEach(emitir); salvarCacheLogo();
    }
    async function flush() {
      if (flushing || !fila.length || !ativo) return;
      flushing = true;
      try {
        while (fila.length && ativo) {
          const lote = fila.slice(0, 200), ult = new Map();
          for (const o of lote) ult.set(kk(o.col, o.id), o);
          const rows = [...ult.values()].map(o => ({ col: o.col, id: o.id, data: o.data || {}, deleted: o.data == null }));
          let r; try { r = await backend.gravar(uid, rows); } catch (_) { r = { erro: { code: 'unavailable', permanente: false } }; }
          if (r && r.erro) {
            if (!r.erro.permanente) { setOffline(r.erro.code === 'unavailable'); tent++; agendar(Math.min(60000, 1500 * Math.pow(2, Math.min(tent, 6)))); break; }
            fila.splice(0, lote.length); resolver(lote, r.erro); salvarFila(); sincronizar(true); continue;
          }
          fila.splice(0, lote.length); tent = 0; setOffline(false); resolver(lote, null); salvarFila();
        }
      } finally { flushing = false; }
    }
    function aplicarRemoto(rows, completo) {
      const cols = new Set();
      if (completo) {
        const vistos = new Set(rows.filter(r => !r.deleted).map(r => kk(r.col, r.id)));
        for (const [c, m] of docs) for (const id of [...m.keys()]) { const k = kk(c, id); if (!vistos.has(k) && !pend.has(k)) { m.delete(id); cols.add(c); } }
      }
      for (const r of rows) {
        if (r.updated_at && (!lastSeen || r.updated_at > lastSeen)) lastSeen = r.updated_at;
        if (pend.has(kk(r.col, r.id))) continue;
        const novo = r.deleted ? null : r.data;
        if (JSON.stringify(colMap(r.col).get(r.id) ?? null) !== JSON.stringify(novo)) { aplicar(r.col, r.id, novo); cols.add(r.col); }
      }
      cols.forEach(emitir); salvarCacheLogo();
    }
    const menos = (iso, min) => { const t = Date.parse(iso); return Number.isFinite(t) ? new Date(t - min * 60000).toISOString() : null; };
    let sincP = null;
    function sincronizar(completo) {
      if (sincP) return sincP;
      sincP = (async () => {
        const desde = !completo && lastSeen ? menos(lastSeen, 3) : null;
        let r; try { r = desde ? await backend.carregarDesde(uid, desde) : await backend.carregar(uid); } catch (_) { r = { erro: { code: 'unavailable' } }; }
        if (r.erro) { setOffline(r.erro.code === 'unavailable'); if (!pronto) { pronto = true; emitirTodos(); } return false; }
        setOffline(false); aplicarRemoto(r.rows, !desde);
        if (!pronto) { pronto = true; emitirTodos(); }
        agendar(0); return true;
      })().finally(() => { sincP = null; });
      return sincP;
    }
    async function iniciar() {
      const c = await kv.get(chave);
      if (c && c.docs) { for (const [col, obj] of Object.entries(c.docs)) for (const [id, d] of Object.entries(obj)) aplicar(col, id, d); lastSeen = c.lastSeen || null; pronto = true; }
      const f = await kv.get(chave + ':fila');
      if (Array.isArray(f)) for (const o of f) {
        if (!o || typeof o.col !== 'string' || typeof o.id !== 'string') continue;
        aplicar(o.col, o.id, o.data ?? null);
        const k = kk(o.col, o.id); pend.set(k, (pend.get(k) || 0) + 1);
        fila.push({ s: seq++, col: o.col, id: o.id, data: o.data ?? null });
      }
    }
    return {
      collection: col => ({
        doc: id => ({ set: d => write(col, id, d, 'set'), update: d => write(col, id, d, 'update'), delete: () => write(col, id, null, 'delete') }),
        onSnapshot: cb => { if (!colL.has(col)) colL.set(col, new Set()); colL.get(col).add(cb); emitir(col); return () => colL.get(col).delete(cb); },
      }),
      doc: path => {
        const i = path.indexOf('/'), col = path.slice(0, i), id = path.slice(i + 1);
        return { onSnapshot: cb => { const k = col + '/' + id; if (!docL.has(k)) docL.set(k, new Set()); docL.get(k).add(cb); emitir(col); return () => docL.get(k).delete(cb); } };
      },
      iniciar, sincronizar, flush: () => agendar(0),
      aplicarRemotoUm: row => aplicarRemoto([row], false),
      persistir: () => { gravarFila(); clearTimeout(timerCache); salvarCache(); },
      pendentes: () => fila.length,
      parar: () => { ativo = false; clearTimeout(timerFlush); },
      dump: () => { const o = {}; for (const [c, m] of docs) { o[c] = {}; for (const [id, d] of m) o[c][id] = d; } return o; },
      gravarVarios: itens => Promise.allSettled(itens.map(([c, id, d]) => write(c, id, d, 'set'))),
    };
  }

  /* ---------- Supabase ---------- */
  function criarSupabase() {
    if (!window.supabase || !window.supabase.createClient) return null;
    const url = String(CFG.supabaseUrl).replace(/\/+$/, '');
    const sb = window.supabase.createClient(url, CFG.supabaseKey, { auth: { persistSession: true, autoRefreshToken: true, detectSessionInUrl: false, storageKey: 'hub-vendas-auth' } });
    const classificar = (error, status) => {
      const c = String((error && error.code) || ''), msg = String((error && error.message) || '');
      if (c === '42501' || status === 403) return { code: 'not_granted', permanente: true };
      if (['23514', '22P02', '23502', '22023'].includes(c)) return { code: 'invalid_argument', permanente: true };
      if (c === '53100' || c === '54000') return { code: 'quota_exceeded', permanente: true };
      if (status === 401 || c === 'PGRST301' || c === 'PGRST303' || /jwt/i.test(msg)) { sb.auth.refreshSession().catch(() => { }); return { code: 'session_expired', permanente: false }; }
      return { code: 'unavailable', permanente: false };
    };
    async function todas(q) {
      const out = [];
      for (let de = 0; de < 200000; de += 1000) {
        let res; try { res = await q().range(de, de + 999); } catch (_) { return { erro: { code: 'unavailable' } }; }
        if (res.error) return { erro: classificar(res.error, res.status) };
        out.push(...res.data); if (res.data.length < 1000) break;
      }
      return { rows: out };
    }
    const traduz = e => {
      const m = String((e && e.message) || e || '');
      if (/invalid login/i.test(m)) return { message: 'E-mail ou senha errados.' };
      if (/already registered|already exists/i.test(m)) return { message: 'Já existe uma conta com esse e-mail. Use “Entrar”.' };
      if (/at least \d+ char|password should/i.test(m)) return { message: 'A senha precisa ter pelo menos 8 caracteres.' };
      if (/not confirmed/i.test(m)) return { message: 'E-mail ainda não confirmado. Desligue “Confirm email” no Supabase (está no guia).' };
      if (/signups? (not allowed|disabled)/i.test(m)) return { message: 'Este Hub é pessoal: novos cadastros estão fechados.' };
      if (/rate limit|too many/i.test(m)) return { message: 'Muitas tentativas seguidas. Espere um minuto.' };
      if (/failed to fetch|network/i.test(m)) return { message: 'Sem conexão com o servidor. Confira a internet.' };
      return { message: m || 'Não deu certo. Tente de novo.' };
    };
    const C = 'col,id,data,deleted,updated_at';
    return {
      tipo: 'supabase',
      async gravar(uid, rows) {
        let res; try { res = await sb.from('docs').upsert(rows.map(r => ({ user_id: uid, col: r.col, id: r.id, data: r.data, deleted: r.deleted })), { onConflict: 'user_id,col,id' }); }
        catch (_) { return { erro: { code: 'unavailable', permanente: false } }; }
        return res.error ? { erro: classificar(res.error, res.status) } : { ok: true };
      },
      carregar: uid => todas(() => sb.from('docs').select(C).eq('user_id', uid).order('col').order('id')),
      carregarDesde: (uid, iso) => todas(() => sb.from('docs').select(C).eq('user_id', uid).gt('updated_at', iso).order('updated_at').order('col').order('id')),
      assinar(uid, onRow) {
        const ch = sb.channel('docs-' + uid).on('postgres_changes', { event: '*', schema: 'public', table: 'docs', filter: 'user_id=eq.' + uid }, p => { if (p.new && p.new.col) onRow(p.new); }).subscribe();
        return () => { try { sb.removeChannel(ch); } catch (_) { } };
      },
      async sessao() { try { const { data } = await sb.auth.getSession(); const s = data && data.session; return s ? { uid: s.user.id, email: s.user.email } : null; } catch (_) { return null; } },
      async entrar(email, senha) { const { data, error } = await sb.auth.signInWithPassword({ email, password: senha }); if (error) throw traduz(error); return { uid: data.user.id, email: data.user.email }; },
      async cadastrar(email, senha) {
        const { data, error } = await sb.auth.signUp({ email, password: senha });
        if (error) throw traduz(error);
        if (!data.session) throw { message: 'Conta criada, mas o Supabase pediu confirmação por e-mail. Desligue “Confirm email” e entre.' };
        return { uid: data.user.id, email: data.user.email };
      },
      /* confere se esta conta é a dona: tenta ler os dados; quem não é dona recebe lista vazia e não grava */
      async souDono() { const { data, error } = await sb.rpc('eh_dono'); if (error) throw error; return data === true; },
      async sair() { try { await sb.auth.signOut(); } catch (_) { } },
    };
  }

  /* ---------- demonstração (sem Supabase): dados só neste navegador ---------- */
  function criarDemo() {
    let banco = null, timer = null;
    const abrir = async () => banco || (banco = (await kv.get('demo:banco')) || {});
    const salvar = () => { clearTimeout(timer); timer = setTimeout(() => kv.set('demo:banco', banco), 300); };
    const linhas = () => { const out = []; for (const [col, m] of Object.entries(banco)) for (const [id, data] of Object.entries(m)) out.push({ col, id, data, deleted: false }); return out; };
    return {
      tipo: 'demo',
      async gravar(_, rows) { await abrir(); for (const r of rows) { const m = banco[r.col] = banco[r.col] || {}; if (r.deleted) delete m[r.id]; else m[r.id] = r.data; } salvar(); return { ok: true }; },
      async carregar() { await abrir(); return { rows: linhas() }; },
      async carregarDesde() { await abrir(); return { rows: linhas() }; },
      assinar: () => () => { },
      async sessao() { return lsGet('hub-vendas-demo'); },
      async entrar() { const s = { uid: 'demo', email: 'demonstracao@local' }; lsSet('hub-vendas-demo', s); return s; },
      async cadastrar() { return this.entrar(); },
      async souDono() { return true; },
      async sair() { lsDel('hub-vendas-demo'); },
    };
  }

  /* =====================================================================
     TELAS
     ===================================================================== */
  let backend = null, store = null, eu = null, desassinar = null, escondidoEm = 0;
  const box = () => $('auth-body');
  const msg = (t, ok) => t ? `<p class="auth-msg ${ok ? 'ok' : ''}" role="alert">${esc(t)}</p>` : '';
  const ocupar = (f, s) => f.querySelectorAll('button, input').forEach(x => x.disabled = s);

  function telaLogin(aviso, avisoOk) {
    document.body.classList.add('sem-login');
    if (DEMO) {
      box().innerHTML = `<p class="demo-note"><b>Modo demonstração.</b> O config.js ainda está vazio: nada vai para a internet, os dados ficam só neste navegador.</p><button class="btn primary" type="button" id="fa-demo">Entrar na demonstração</button>`;
      $('fa-demo').onclick = async () => depoisDoLogin(await backend.entrar());
      return;
    }
    box().innerHTML = `<form class="form" id="fa-login" autocomplete="on">
      <p class="sub">Área pessoal. Entre com o seu e-mail e senha.</p>${msg(aviso, avisoOk)}
      <label class="field"><span>E-mail</span><input type="email" id="fa-email" autocomplete="username" required></label>
      <label class="field"><span>Senha</span><input type="password" id="fa-senha" autocomplete="current-password" required minlength="6"></label>
      <button class="btn primary" type="submit">Entrar</button>
      <div class="auth-alt"><button type="button" id="fa-novo">Primeiro acesso: criar a conta</button></div>
      <div id="fa-out"></div></form>`;
    $('fa-novo').onclick = telaCadastro;
    $('fa-login').onsubmit = async e => {
      e.preventDefault(); const f = e.currentTarget; ocupar(f, true);
      try { await depoisDoLogin(await backend.entrar($('fa-email').value.trim(), $('fa-senha').value)); }
      catch (err) { ocupar(f, false); $('fa-out').innerHTML = msg(err.message || 'Não deu certo.'); }
    };
    setTimeout(() => $('fa-email') && $('fa-email').focus(), 50);
  }
  function telaCadastro() {
    box().innerHTML = `<form class="form" id="fa-cad" autocomplete="on">
      <p class="sub">Só funciona uma vez: a primeira conta criada vira a dona do Hub, e depois os cadastros são fechados.</p>
      <label class="field"><span>E-mail</span><input type="email" id="fa-email" autocomplete="username" required></label>
      <label class="field"><span>Senha (mínimo 8 caracteres)</span><input type="password" id="fa-senha" autocomplete="new-password" required minlength="8"></label>
      <label class="field"><span>Repita a senha</span><input type="password" id="fa-senha2" autocomplete="new-password" required minlength="8"></label>
      <button class="btn primary" type="submit">Criar a conta</button>
      <div class="auth-alt"><button type="button" id="fa-volta">Já tenho conta</button></div>
      <div id="fa-out"></div></form>`;
    $('fa-volta').onclick = () => telaLogin();
    $('fa-cad').onsubmit = async e => {
      e.preventDefault(); const f = e.currentTarget;
      if ($('fa-senha').value !== $('fa-senha2').value) { $('fa-out').innerHTML = msg('As senhas não são iguais.'); return; }
      ocupar(f, true);
      try { await depoisDoLogin(await backend.cadastrar($('fa-email').value.trim(), $('fa-senha').value)); }
      catch (err) { ocupar(f, false); $('fa-out').innerHTML = msg(err.message || 'Não deu certo.'); }
    };
    setTimeout(() => $('fa-email').focus(), 50);
  }

  async function depoisDoLogin(ses) {
    let dono = null;
    try { dono = await backend.souDono(); }
    catch (_) { const ult = lsGet('hub-vendas:ultimo'); if (ult && ult.uid === ses.uid) dono = true; else { telaLogin('Não consegui falar com o servidor. Confira a internet.'); return; } }
    if (!dono) { await backend.sair(); telaLogin('Esta conta não tem acesso a este Hub. Ele é pessoal.'); return; }
    eu = ses; lsSet('hub-vendas:ultimo', { uid: ses.uid, email: ses.email });
    store = criarStore(backend, ses.uid);
    await store.iniciar();
    document.body.classList.remove('sem-login');
    liberarDb(store);                                         // o código do Hub começa a ler e gravar
    store.sincronizar(true).then(() => { if (!desassinar) desassinar = backend.assinar(ses.uid, row => store.aplicarRemotoUm(row)); });
  }

  /* ---------- janela da conta: backup, importar, instalar, sair ---------- */
  function abrirConta() {
    const est = estadoInstalacao(), pend = store ? store.pendentes() : 0;
    $('conta-body').innerHTML = `
      <p class="sub" style="margin:0">${esc(DEMO ? 'Modo demonstração' : (eu && eu.email) || '')}</p>
      ${pend ? `<p class="note">${pend} registro${pend === 1 ? '' : 's'} esperando a internet para subir.</p>` : ''}
      <div class="btn-row"><button class="btn" type="button" id="ct-backup">Baixar backup</button><button class="btn" type="button" id="ct-importar">Importar backup</button><input type="file" id="ct-arquivo" accept=".json,application/json" hidden></div>
      <div id="ct-out"></div>
      ${est === 'instalado' ? '' : est === 'disponivel' ? '<div class="btn-row"><button class="btn" type="button" id="ct-instalar">Instalar como aplicativo</button></div>'
        : `<p class="note">Instalar no celular: no iPhone, Safari → Compartilhar → Adicionar à Tela de Início. No Android, Chrome → menu ⋮ → Instalar app.</p>`}
      <div class="btn-row" style="justify-content:flex-end"><button class="btn danger" type="button" id="ct-sair">Sair da conta</button></div>`;
    $('conta-modal').hidden = false;
    $('ct-backup').onclick = baixarBackup;
    $('ct-importar').onclick = () => $('ct-arquivo').click();
    $('ct-arquivo').onchange = e => importar(e.target.files && e.target.files[0]);
    if ($('ct-instalar')) $('ct-instalar').onclick = instalar;
    $('ct-sair').onclick = sair;
  }
  function baixarBackup() {
    const d = store.dump(), out = { formato: 'hub-vendas', versao: 1, gerado: new Date().toISOString(), colecoes: {} };
    for (const c of COLS) if (d[c]) out.colecoes[c] = d[c];
    const url = URL.createObjectURL(new Blob([JSON.stringify(out, null, 1)], { type: 'application/json' }));
    const a = document.createElement('a'); a.href = url; a.download = `hub-vendas-backup-${new Date().toISOString().slice(0, 10)}.json`; document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 4000);
    $('ct-out').innerHTML = msg('Backup baixado. Ele tem dados de clientes: guarde num lugar seguro.', true);
  }
  async function importar(file) {
    if (!file) return;
    let d; try { d = JSON.parse(await file.text()); } catch (_) { $('ct-out').innerHTML = msg('Esse arquivo não é um backup válido.'); return; }
    const cols = d && d.colecoes && typeof d.colecoes === 'object' ? d.colecoes : null;
    if (!cols) { $('ct-out').innerHTML = msg('Não reconheci este arquivo. Use um backup do Hub de Vendas.'); return; }
    const itens = [];
    for (const c of COLS) { const m = cols[c]; if (!m || typeof m !== 'object') continue; for (const [id, data] of Object.entries(m)) if (data && typeof data === 'object' && /^[A-Za-z0-9_.~:@+-]{1,120}$/.test(id)) itens.push([c, id, data]); }
    if (!itens.length) { $('ct-out').innerHTML = msg('O arquivo não tem registros.'); return; }
    if (!confirm(`Importar ${itens.length} registros? Registros com o mesmo código serão substituídos pelos do arquivo; o resto continua.`)) return;
    $('ct-out').innerHTML = msg(`Importando ${itens.length} registros…`, true);
    const rs = await store.gravarVarios(itens);
    const falhas = rs.filter(r => r.status === 'rejected').length;
    $('ct-out').innerHTML = msg(falhas ? `${itens.length - falhas} importados; ${falhas} falharam. Tente de novo: os que já entraram não duplicam.` : `Pronto: ${itens.length} registros importados.`, !falhas);
  }
  async function sair() {
    if (store && store.pendentes() > 0 && !confirm('Ainda há registros esperando a internet. Se sair agora, eles se perdem. Sair mesmo assim?')) return;
    if (store) store.parar(); if (desassinar) desassinar();
    if (eu) { await kv.del('cache:' + eu.uid); await kv.del('cache:' + eu.uid + ':fila'); }
    lsDel('hub-vendas:ultimo'); await backend.sair(); location.hash = ''; location.reload();
  }

  /* ---------- instalar (PWA) ---------- */
  let promptInstalar = null;
  const instalado = () => window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone === true;
  const estadoInstalacao = () => instalado() ? 'instalado' : promptInstalar ? 'disponivel' : 'manual';
  async function instalar() { if (!promptInstalar) return; promptInstalar.prompt(); await promptInstalar.userChoice.catch(() => null); promptInstalar = null; $('conta-modal').hidden = true; }
  window.addEventListener('beforeinstallprompt', e => { e.preventDefault(); promptInstalar = e; });
  if ('serviceWorker' in navigator && /^https?:$/.test(location.protocol)) window.addEventListener('load', () => navigator.serviceWorker.register('sw.js').catch(() => { }));

  /* ---------- partida ---------- */
  async function iniciar() {
    $('btn-conta').addEventListener('click', abrirConta);
    $('conta-fechar').addEventListener('click', () => { $('conta-modal').hidden = true; });
    $('conta-modal').addEventListener('click', e => { if (e.target === e.currentTarget) $('conta-modal').hidden = true; });
    window.addEventListener('online', () => { setOffline(false); if (store) { store.flush(); store.sincronizar(false); } });
    window.addEventListener('offline', () => setOffline(true));
    window.addEventListener('pagehide', () => { if (store) store.persistir(); });
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'hidden') { escondidoEm = Date.now(); if (store) { store.persistir(); store.flush(); } }
      else if (store && Date.now() - escondidoEm > 15000) { store.sincronizar(false); store.flush(); }
    });
    setInterval(() => { if (store && document.visibilityState === 'visible') store.flush(); }, 30000);
    backend = DEMO ? criarDemo() : criarSupabase();
    if (!backend) { box().innerHTML = msg('Não consegui carregar a biblioteca do Supabase. Confira a internet e recarregue.'); return; }
    let ses = await backend.sessao();
    if (!ses && !DEMO) { const ult = lsGet('hub-vendas:ultimo'); if (ult && !navigator.onLine) ses = ult; }
    if (!ses) return telaLogin();
    await depoisDoLogin(ses);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', iniciar); else iniciar();
})();
