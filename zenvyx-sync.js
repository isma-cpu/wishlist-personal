/* zenvyx-sync v1.2 — sincronización privada entre dispositivos (Supabase). Copiar tal cual en cada web. */
(function () {
  const SB_URL = 'https://snziqtlsxrbuujjspicy.supabase.co';
  const SB_KEY = 'sb_publishable_GkCahDyMe8b1MPA2keA6Lw_t6CSwBCM';
  const SB_CDN = 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/dist/umd/supabase.min.js';
  const BUCKET = 'media';
  const PULL_EVERY_MS = 30000;

  let sb, opts, app, user = null, status = 'init', flushTimer = null;
  let syncLock = Promise.resolve();
  // Incrementa en cada cambio de sesión: una sincronización en curso de otra sesión se aborta.
  let authGen = 0;
  const k = (s) => `zsync:${app}:${s}`;
  const load = (key, d) => { try { const v = JSON.parse(localStorage.getItem(key)); return v ?? d; } catch { return d; } };
  const save = (key, v) => localStorage.setItem(key, JSON.stringify(v));
  const rk = (c, id) => `${c}\u0000${id}`;

  function stable(v) {
    if (Array.isArray(v)) return `[${v.map(stable).join(',')}]`;
    if (v && typeof v === 'object') return `{${Object.keys(v).sort().map((x) => JSON.stringify(x) + ':' + stable(v[x])).join(',')}}`;
    return JSON.stringify(v ?? null);
  }
  const sameRecord = (a, b) => !!a.deleted === !!b.deleted && stable(a.data) === stable(b.data);

  function loadScript(src) {
    return new Promise((res, rej) => {
      const s = document.createElement('script');
      s.src = src; s.onload = res; s.onerror = () => rej(new Error('No se pudo cargar ' + src));
      document.head.appendChild(s);
    });
  }

  function setStatus(s) {
    status = s;
    renderBadge();
    opts.onStatus && opts.onStatus(s, pendingCount());
  }
  const pendingCount = () => Object.keys(load(k('queue'), {})).length;
  const conflictMap = () => load(k('conflicts'), {});
  const conflictCount = () => Object.keys(conflictMap()).length;

  function enqueue(collection, id, data, deleted) {
    const q = load(k('queue'), {});
    q[rk(collection, id)] = { collection, id: String(id), data: deleted ? null : data, deleted: !!deleted, ts: Date.now() };
    save(k('queue'), q);
    scheduleFlush();
  }

  function scheduleFlush() {
    renderBadge();
    clearTimeout(flushTimer);
    flushTimer = setTimeout(backgroundSync, 800);
  }

  async function emit(changes) {
    if (changes.length && opts.onChange) await opts.onChange(changes);
  }

  function serialize(task) {
    const result = syncLock.then(task);
    syncLock = result.catch(() => {});
    return result;
  }

  function backgroundSync() { syncNow().catch(() => {}); }

  async function scanRecords(chk) {
    const rows = [];
    for (let from = 0; ; from += 500) {
      chk();
      const { data, error } = await sb.from('app_records').select('*')
        .eq('user_id', user.id).eq('app', app)
        .order('collection', { ascending: true }).order('record_id', { ascending: true })
        .range(from, from + 499);
      if (error) throw error;
      rows.push(...data);
      if (data.length < 500) return rows;
    }
  }

  async function fetchRecord(collection, id) {
    const { data, error } = await sb.from('app_records').select('*')
      .eq('user_id', user.id).eq('app', app).eq('collection', collection).eq('record_id', id).single();
    if (error) throw error;
    return data;
  }

  function addConflict(op, srv) {
    const map = conflictMap();
    map[rk(op.collection, op.id)] = {
      collection: op.collection, id: op.id, at: new Date().toISOString(),
      local: { data: op.data, deleted: op.deleted },
      server: { data: srv.data, deleted: srv.deleted, rev: srv.rev, updated_at: srv.updated_at },
    };
    save(k('conflicts'), map);
  }

  // Devuelve la nueva rev, o null si el registro quedó en conflicto (no se sube nada).
  async function pushOne(op, revs) {
    const key = rk(op.collection, op.id);
    const base = revs[key];
    const row = { data: op.data, deleted: op.deleted };
    if (base == null) {
      const { data, error } = await sb.from('app_records')
        .insert({ app, collection: op.collection, record_id: op.id, ...row })
        .select('rev').single();
      if (!error) return data.rev;
      if (error.code !== '23505') throw error;
    } else {
      const { data, error } = await sb.from('app_records').update(row)
        .eq('user_id', user.id).eq('app', app).eq('collection', op.collection).eq('record_id', op.id).eq('rev', base)
        .select('rev');
      if (error) throw error;
      if (data.length) return data[0].rev;
    }
    const srv = await fetchRecord(op.collection, op.id);
    if (sameRecord(op, srv)) return srv.rev;
    addConflict(op, srv);
    return null;
  }

  async function flush(chk) {
    const q = load(k('queue'), {});
    const conflicts = conflictMap();
    for (const key of Object.keys(q)) {
      if (conflicts[key]) continue;
      chk();
      const op = q[key];
      const revs = load(k('revs'), {});
      const rev = await pushOne(op, revs);
      chk();
      if (rev == null) continue;
      revs[key] = rev;
      save(k('revs'), revs);
      const cur = load(k('queue'), {});
      if (cur[key] && cur[key].ts === op.ts) { delete cur[key]; save(k('queue'), cur); }
    }
  }

  async function pull(chk) {
    const revs = load(k('revs'), {});
    const changes = [];
    for (const r of await scanRecords(chk)) {
      const key = rk(r.collection, r.record_id);
      // Releer la cola: puede llegar una edición local mientras la petición está en curso.
      if (load(k('queue'), {})[key] || (revs[key] != null && revs[key] >= r.rev)) continue;
      revs[key] = r.rev;
      changes.push({ collection: r.collection, id: r.record_id, data: r.data, deleted: r.deleted });
    }
    chk();
    await emit(changes);
    chk();
    save(k('revs'), revs);
    save(k('cursor'), new Date().toISOString());
  }

  async function firstSync(chk) {
    if (load(k('init'), null) === user.id) return;
    if (opts.beforeFirstSync) await opts.beforeFirstSync();
    chk();
    const rows = await scanRecords(chk);
    const local = opts.collectLocal ? await opts.collectLocal() : [];
    chk();
    const localMap = new Map(local.map((r) => [rk(r.collection, String(r.id)), r]));
    const revs = {};
    const changes = [];
    const clashes = [];
    for (const r of rows) {
      const key = rk(r.collection, r.record_id);
      const mine = localMap.get(key);
      if (mine && !sameRecord({ data: mine.data, deleted: false }, r)) {
        // Mismo registro distinto en este dispositivo y en la nube: no se pisa ninguno, se pregunta.
        clashes.push({ op: { collection: r.collection, id: r.record_id, data: mine.data, deleted: false, ts: Date.now() }, srv: r });
        continue;
      }
      revs[key] = r.rev;
      changes.push({ collection: r.collection, id: r.record_id, data: r.data, deleted: r.deleted });
    }
    // Si la aplicación falla, no se guarda ningún punto de control ni se marca como inicializado.
    await emit(changes);
    chk();
    const remote = new Set(rows.map((r) => rk(r.collection, r.record_id)));
    for (const { op, srv } of clashes) {
      revs[rk(op.collection, op.id)] = srv.rev;
      addConflict(op, srv);
      const q = load(k('queue'), {});
      q[rk(op.collection, op.id)] = op;
      save(k('queue'), q);
    }
    save(k('revs'), revs);
    save(k('cursor'), new Date().toISOString());
    for (const rec of local) {
      if (!remote.has(rk(rec.collection, String(rec.id)))) enqueue(rec.collection, rec.id, rec.data, false);
    }
    save(k('init'), user.id);
  }

  function ownerOk() {
    const owner = load(k('owner'), null);
    return !owner || owner === user.id;
  }

  function syncNow() {
    return serialize(async () => {
      if (!user) return;
      if (!ownerOk()) { setStatus('account'); return; }
      if (!navigator.onLine) { setStatus('offline'); return; }
      const gen = authGen;
      const who = user.id;
      const chk = () => { if (gen !== authGen || !user || user.id !== who) throw new Error('ACCOUNT_CHANGED'); };
      save(k('owner'), who);
      setStatus('syncing');
      try {
        await firstSync(chk);
        await flush(chk);
        await pull(chk);
        setStatus(conflictCount() ? 'conflict' : pendingCount() ? 'pending' : 'synced');
      } catch (e) {
        if (e && e.message === 'ACCOUNT_CHANGED') return;
        console.error('[zsync] sync', e);
        setStatus(navigator.onLine ? 'error' : 'offline');
        throw e;
      }
    });
  }

  // Resuelve un conflicto: 'local' sube la versión de este dispositivo, 'server' se queda con la de la nube.
  function resolveConflict(collection, id, keep) {
    return serialize(async () => {
      const key = rk(collection, String(id));
      const c = conflictMap()[key];
      if (!c || !user) return;
      if (!ownerOk()) throw new Error('Este dispositivo está vinculado a otra cuenta');
      const gen = authGen;
      const checkSession = () => { if (gen !== authGen || !user || !ownerOk()) throw new Error('ACCOUNT_CHANGED'); };
      const srv = await fetchRecord(collection, String(id));
      checkSession();
      if (keep === 'server') {
        await emit([{ collection, id: String(id), data: srv.data, deleted: srv.deleted }]);
        checkSession();
        const q = load(k('queue'), {}); delete q[key]; save(k('queue'), q);
      } else if (keep !== 'local') {
        throw new Error('keep debe ser "local" o "server"');
      }
      const revs = load(k('revs'), {}); revs[key] = srv.rev; save(k('revs'), revs);
      const map = conflictMap(); delete map[key]; save(k('conflicts'), map);
    }).then(() => syncNow());
  }

  async function resolveAll(keep) {
    for (const c of Object.values(conflictMap())) await resolveConflict(c.collection, c.id, keep);
  }

  /* ---------- interfaz: insignia + modal de acceso ---------- */
  let badge, modal;
  const LABELS = {
    init: '☁️ Conectando…', login: '🔒 Inicia sesión para sincronizar', synced: '☁️ Sincronizado',
    syncing: '🔄 Sincronizando…', pending: '⏳ Pendiente', offline: '📴 Sin conexión (se guarda aquí)',
    error: '⚠️ Error de sincronización', conflict: '⚠️ Hay cambios a revisar',
    account: '⛔ Este dispositivo está vinculado a otra cuenta',
  };
  function renderBadge() {
    if (!badge) return;
    const n = pendingCount();
    const c = conflictCount();
    let t = LABELS[status] || status;
    if (c) t += ` · ${c} por decidir`;
    else if (n && status !== 'login') t += ` · ${n} sin subir`;
    badge.textContent = t;
    badge.style.borderColor = ['error', 'conflict', 'account'].includes(status) ? '#f87171' : status === 'login' ? '#fbbf24' : 'rgba(255,255,255,.18)';
  }

  function buildUI() {
    badge = document.createElement('button');
    badge.type = 'button';
    badge.setAttribute('data-zsync', 'badge');
    Object.assign(badge.style, {
      position: 'fixed', left: '12px', bottom: '12px', zIndex: 2147483000, font: '12px/1.2 system-ui,sans-serif',
      color: '#fff', background: 'rgba(15,15,20,.85)', border: '1px solid rgba(255,255,255,.18)', borderRadius: '999px',
      padding: '6px 12px', cursor: 'pointer', backdropFilter: 'blur(6px)',
    });
    badge.onclick = () => (user ? openMenu() : openLogin());
    document.body.appendChild(badge);
    renderBadge();
  }

  const esc = (s) => String(s).replace(/[&<>"']/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
  function closeModal() { if (modal) { modal.remove(); modal = null; } }
  function shell(inner) {
    closeModal();
    modal = document.createElement('div');
    modal.setAttribute('data-zsync', 'modal');
    Object.assign(modal.style, {
      position: 'fixed', inset: '0', zIndex: 2147483001, background: 'rgba(0,0,0,.6)', display: 'flex',
      alignItems: 'center', justifyContent: 'center', font: '14px/1.4 system-ui,sans-serif',
    });
    modal.innerHTML = `<div style="background:#14141b;color:#fff;border:1px solid rgba(255,255,255,.15);border-radius:14px;padding:22px;width:min(92vw,380px);max-height:85vh;overflow:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">${inner}</div>`;
    modal.addEventListener('click', (e) => { if (e.target === modal) closeModal(); });
    document.body.appendChild(modal);
    return modal;
  }
  const inputCss = 'width:100%;box-sizing:border-box;margin:6px 0 10px;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.2);background:#0b0b10;color:#fff;font:inherit';
  const btnCss = (p) => `width:100%;padding:10px;border-radius:999px;border:${p ? '0' : '1px solid rgba(255,255,255,.3)'};background:${p ? '#fff' : 'transparent'};color:${p ? '#000' : '#fff'};font:inherit;font-weight:600;cursor:pointer;margin-top:6px`;
  const smallBtn = 'padding:6px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.3);background:transparent;color:#fff;font:inherit;font-size:12px;cursor:pointer;margin:4px 4px 0 0';

  function openLogin() {
    const m = shell(`
      <div style="font-weight:700;font-size:16px;margin-bottom:4px">Sincronizar entre dispositivos</div>
      <div style="opacity:.7;font-size:12px;margin-bottom:12px">Usa la misma cuenta en todas tus webs y en todos tus dispositivos.</div>
      <label style="font-size:12px;opacity:.8">Correo</label><input data-f="email" type="email" autocomplete="email" style="${inputCss}">
      <label style="font-size:12px;opacity:.8">Contraseña</label><input data-f="pass" type="password" autocomplete="current-password" style="${inputCss}">
      <div data-f="msg" style="color:#fca5a5;font-size:12px;min-height:16px"></div>
      <button data-f="in" style="${btnCss(true)}">Entrar</button>
      <button data-f="up" style="${btnCss(false)}">Crear cuenta</button>`);
    const $ = (f) => m.querySelector(`[data-f="${f}"]`);
    const go = async (signup) => {
      $('msg').textContent = '';
      const email = $('email').value.trim(), password = $('pass').value;
      if (!email || password.length < 8) { $('msg').textContent = 'Correo y contraseña (mínimo 8 caracteres).'; return; }
      const fn = signup
        ? sb.auth.signUp({ email, password, options: { emailRedirectTo: location.origin + location.pathname } })
        : sb.auth.signInWithPassword({ email, password });
      const { data, error } = await fn;
      if (error) { $('msg').textContent = traducir(error.message); return; }
      if (signup && !data.session) { $('msg').style.color = '#86efac'; $('msg').textContent = 'Cuenta creada. Abre el correo de Supabase y pulsa el enlace de confirmación; si te lleva a una página de error, no pasa nada: vuelve aquí y pulsa Entrar.'; return; }
      closeModal();
    };
    $('in').onclick = () => go(false);
    $('up').onclick = () => go(true);
  }

  function traducir(msg) {
    if (/Invalid login/i.test(msg)) return 'Correo o contraseña incorrectos.';
    if (/Signups not allowed/i.test(msg)) return 'No se permiten cuentas nuevas. Entra con tu cuenta existente.';
    if (/already registered/i.test(msg)) return 'Ese correo ya tiene cuenta: pulsa Entrar.';
    if (/Email not confirmed/i.test(msg)) return 'Confirma tu correo antes de entrar.';
    return msg;
  }

  function openMenu() {
    const n = pendingCount();
    const conflicts = Object.values(conflictMap());
    const accountMsg = status === 'account'
      ? '<div style="color:#fca5a5;font-size:12px;margin-bottom:8px">Este dispositivo ya se sincronizó con otra cuenta. Para no mezclar datos no se sube ni se descarga nada. Cierra sesión y entra con la cuenta original.</div>'
      : '';
    const list = conflicts.map((c, i) => `
      <div style="border:1px solid rgba(255,255,255,.15);border-radius:10px;padding:8px;margin-top:8px;font-size:12px">
        <div style="opacity:.8">${esc(c.collection)} · ${esc(c.id)}</div>
        <button data-c="${i}" data-keep="local" style="${smallBtn}">Usar la de este dispositivo</button>
        <button data-c="${i}" data-keep="server" style="${smallBtn}">Usar la de la nube</button>
      </div>`).join('');
    const m = shell(`
      <div style="font-weight:700;font-size:16px;margin-bottom:4px">${esc(LABELS[status] || status)}</div>
      <div style="opacity:.7;font-size:12px;margin-bottom:12px">${esc(user.email || '')}${n ? ` · ${n} cambios esperando subir` : ''}</div>
      ${accountMsg}
      ${conflicts.length ? `<div style="font-size:12px">El mismo elemento se cambió aquí y en otro dispositivo. Elige cuál conservar (la otra versión no se borra hasta que elijas):</div>${list}
        <button data-f="allLocal" style="${btnCss(false)}">Todas: las de este dispositivo</button>
        <button data-f="allServer" style="${btnCss(false)}">Todas: las de la nube</button>` : ''}
      <button data-f="now" style="${btnCss(true)}">Sincronizar ahora</button>
      <button data-f="out" style="${btnCss(false)}">Cerrar sesión en este dispositivo</button>`);
    const run = (p) => { closeModal(); p.catch((e) => console.error('[zsync]', e)); };
    m.querySelectorAll('[data-c]').forEach((b) => {
      b.onclick = () => { const c = conflicts[+b.dataset.c]; run(resolveConflict(c.collection, c.id, b.dataset.keep)); };
    });
    const all = (f, keep) => { const b = m.querySelector(`[data-f="${f}"]`); if (b) b.onclick = () => run(resolveAll(keep)); };
    all('allLocal', 'local');
    all('allServer', 'server');
    m.querySelector('[data-f="now"]').onclick = () => run(syncNow());
    m.querySelector('[data-f="out"]').onclick = async () => {
      if (pendingCount() && !confirm('Hay cambios sin subir. Si cierras sesión se quedarán solo en este dispositivo. ¿Continuar?')) return;
      await sb.auth.signOut();
      closeModal();
    };
  }

  /* ---------- API pública ---------- */
  async function init(o) {
    opts = o || {};
    app = opts.app;
    if (!app) throw new Error('ZSync.init necesita { app }');
    if (!window.supabase) await loadScript(SB_CDN);
    sb = window.supabase.createClient(SB_URL, SB_KEY, { auth: { persistSession: true, autoRefreshToken: true, storageKey: 'zsync-auth' } });
    if (opts.ui !== false) (document.body ? buildUI() : document.addEventListener('DOMContentLoaded', buildUI));
    let started = false;
    sb.auth.onAuthStateChange((event, session) => {
      const u = session ? session.user : null;
      if (u && (!user || user.id !== u.id)) {
        started = true; authGen++; user = u;
        // setTimeout: supabase-js se bloquea si se llama a la API dentro de este callback.
        setTimeout(backgroundSync, 0);
      } else if (!u && user) { authGen++; user = null; setStatus('login'); }
      else if (!u) setStatus('login');
    });
    const { data } = await sb.auth.getSession();
    if (!data.session && !started) setStatus('login');
    window.addEventListener('online', backgroundSync);
    window.addEventListener('offline', () => setStatus('offline'));
    document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'visible') backgroundSync(); });
    setInterval(() => { if (document.visibilityState === 'visible') backgroundSync(); }, PULL_EVERY_MS);
    return api;
  }

  async function putBlob(path, blob) {
    if (!user) throw new Error('Sin sesión');
    if (!ownerOk()) throw new Error('Este dispositivo está vinculado a otra cuenta');
    const full = `${user.id}/${app}/${path}`;
    const { error } = await sb.storage.from(BUCKET).upload(full, blob, { upsert: true, contentType: blob.type || undefined });
    if (error) throw error;
    return path;
  }
  async function getBlob(path) {
    if (!user) throw new Error('Sin sesión');
    if (!ownerOk()) throw new Error('Este dispositivo está vinculado a otra cuenta');
    const { data, error } = await sb.storage.from(BUCKET).download(`${user.id}/${app}/${path}`);
    if (error) throw error;
    return data;
  }
  async function delBlob(path) {
    if (!user) return;
    if (!ownerOk()) throw new Error('Este dispositivo está vinculado a otra cuenta');
    const { error } = await sb.storage.from(BUCKET).remove([`${user.id}/${app}/${path}`]);
    if (error) throw error;
  }

  const api = {
    init,
    put: (collection, id, data) => enqueue(collection, id, data, false),
    del: (collection, id) => enqueue(collection, id, null, true),
    syncNow,
    resolveConflict, resolveAll,
    putBlob, getBlob, delBlob,
    conflicts: () => Object.values(conflictMap()),
    get user() { return user; },
    get status() { return status; },
    get pending() { return pendingCount(); },
  };
  window.ZSync = api;
})();
