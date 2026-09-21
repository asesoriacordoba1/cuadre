/* Cuadre — capa para trabajar sin internet.
 *
 * Envuelve el cliente de Supabase. Con conexión, cada consulta se reenvía tal cual al
 * cliente real (mismo resultado que antes). Si la red falla, las lecturas se responden
 * con la última copia guardada en este computador (IndexedDB) y las escrituras se aplican
 * a esa copia y quedan en una cola que se sube sola al volver el internet.
 */
(function(global){
"use strict";

var LS_USUARIO = "cuadre_usuario_local";
var LS_NUEVAS = "cuadre_empresas_nuevas";
var GLOBALES = {empresas:1, config:1, reglas_cuentas:1};
var CON_ID = {empresas:1, terceros:1, comprobantes:1};
var OPS_ESCRITURA = ["insert","update","upsert","delete"];

var cfg = {url:"", key:""};
var real = null;
var state = {online: (typeof navigator==="undefined" || navigator.onLine!==false), pendientes:0, sincronizando:false, error:"", ultimaSync:0};
var oyentes = [];
var hooks = {cambioLocal:null, sincronizado:null, sesionVencida:null};
var idb = null, mem = {}, ultimoGuardado = {}, pingTimer = null;

function notificar(){ oyentes.forEach(function(f){ try{ f(state); }catch(e){} }); }
function uuid(){
  if(global.crypto && global.crypto.randomUUID) return global.crypto.randomUUID();
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g,function(c){ var r=Math.random()*16|0; return (c==="x"?r:(r&3|8)).toString(16); });
}
function clonar(o){ return Object.assign({}, o); }
function esErrorDeRed(err){
  var m = String((err && (err.message || err.details)) || err || "");
  return /Failed to fetch|NetworkError|Network request failed|Load failed|fetch failed|ERR_INTERNET|ERR_NETWORK|net::ERR/i.test(m);
}

/* ---------- IndexedDB ---------- */
function abrir(){
  if(idb) return Promise.resolve(idb);
  return new Promise(function(res, rej){
    var r = indexedDB.open("cuadre_offline", 1);
    r.onupgradeneeded = function(){
      var d = r.result;
      d.createObjectStore("t");
      d.createObjectStore("q", {keyPath:"qid", autoIncrement:true});
    };
    r.onsuccess = function(){ idb = r.result; res(idb); };
    r.onerror = function(){ rej(r.error); };
  });
}
function tx(store, modo, fn){
  return abrir().then(function(d){
    return new Promise(function(res, rej){
      var t = d.transaction(store, modo);
      var req = fn(t.objectStore(store));
      t.oncomplete = function(){ res(req && req.result); };
      t.onerror = function(){ rej(t.error); };
      t.onabort = function(){ rej(t.error); };
    });
  });
}
function idbGet(store, key){ return tx(store, "readonly", function(s){ return s.get(key); }); }
function idbPut(store, val, key){ return tx(store, "readwrite", function(s){ return key===undefined ? s.put(val) : s.put(val, key); }); }
function idbDel(store, key){ return tx(store, "readwrite", function(s){ return s.delete(key); }); }
function idbAll(store){ return tx(store, "readonly", function(s){ return s.getAll(); }); }
function idbClear(store){ return tx(store, "readwrite", function(s){ return s.clear(); }); }

/* ---------- copia local de las tablas ---------- */
function clave(tabla, scope){ return tabla + "|" + (scope || "_"); }
function scopeDeTabla(tabla, scope){ return GLOBALES[tabla] ? "_" : (scope || "_"); }

function empresasNuevas(){ try{ return JSON.parse(localStorage.getItem(LS_NUEVAS) || "{}"); }catch(e){ return {}; } }
function marcarEmpresaNueva(id){ var n = empresasNuevas(); n[id] = 1; try{ localStorage.setItem(LS_NUEVAS, JSON.stringify(n)); }catch(e){} }

function cargarTabla(tabla, scope){
  var k = clave(tabla, scopeDeTabla(tabla, scope));
  if(mem[k]) return Promise.resolve(mem[k]);
  return idbGet("t", k).then(function(reg){
    if(!reg){
      // Una empresa creada sin conexión no tiene copia previa: nace vacía.
      if(!GLOBALES[tabla] && scope && empresasNuevas()[scope]){ mem[k] = []; return mem[k]; }
      return null;
    }
    var rows = reg.rows;
    // Se vuelven a aplicar los cambios pendientes por si la copia se guardó antes que ellos.
    return idbAll("q").then(function(cola){
      cola.forEach(function(it){ if(it.table===tabla) aplicarItem(rows, it, scopeDeTabla(tabla, scope)); });
      mem[k] = rows;
      return rows;
    });
  });
}
function guardarTabla(tabla, scope, rows, forzar){
  var k = clave(tabla, scopeDeTabla(tabla, scope));
  var ahora = Date.now();
  mem[k] = (rows || []).map(clonar);
  if(!forzar && ultimoGuardado[k] && ahora - ultimoGuardado[k] < 60000) return Promise.resolve();
  ultimoGuardado[k] = ahora;
  return idbPut("t", {rows: mem[k], ts: ahora}, k).catch(function(e){ console.warn("Cuadre: no se pudo guardar la copia local de "+k, e); });
}
var timersPersistir = {};
function programarPersistencia(tabla, scope){
  // La cola de cambios ya es el respaldo duradero; la copia completa se guarda unos segundos despues.
  var k = clave(tabla, scopeDeTabla(tabla, scope));
  if(timersPersistir[k]) clearTimeout(timersPersistir[k]);
  timersPersistir[k] = setTimeout(function(){ delete timersPersistir[k]; persistir(tabla, scope); }, 3000);
}
function persistir(tabla, scope){
  var k = clave(tabla, scopeDeTabla(tabla, scope));
  if(!mem[k]) return Promise.resolve();
  ultimoGuardado[k] = Date.now();
  return idbPut("t", {rows: mem[k], ts: Date.now()}, k).catch(function(){});
}

/* ---------- interpretación local de una consulta ---------- */
function analizar(rec){
  var q = {op:null, payload:null, opts:null, filtros:[], orden:null, rango:null, limite:null, unico:false, quizaUnico:false, retorna:false, cols:"*", conteo:false};
  rec.calls.forEach(function(c){
    var m = c[0], a = c[1];
    if(m==="select"){
      if(!q.op) { q.op = "select"; q.cols = a[0] || "*"; q.conteo = !!(a[1] && a[1].count); }
      else { q.retorna = true; q.cols = a[0] || "*"; }
    }
    else if(OPS_ESCRITURA.indexOf(m)>=0){ q.op = m; q.payload = a[0]; q.opts = a[1] || {}; }
    else if(m==="eq" || m==="neq") q.filtros.push([m, a[0], a[1]]);
    else if(m==="in") q.filtros.push(["in", a[0], a[1]]);
    else if(m==="order") q.orden = {col:a[0], asc: !(a[1] && a[1].ascending===false)};
    else if(m==="range") q.rango = [a[0], a[1]];
    else if(m==="limit") q.limite = a[0];
    else if(m==="single") q.unico = true;
    else if(m==="maybeSingle") q.quizaUnico = true;
    else q.noSoportado = m;
  });
  return q;
}
function coincide(row, filtros){
  for(var i=0;i<filtros.length;i++){
    var f = filtros[i], v = row[f[1]];
    /* eslint-disable eqeqeq */
    if(f[0]==="eq" && !(v == f[2])) return false;
    if(f[0]==="neq" && (v == f[2])) return false;
    if(f[0]==="in" && !(f[2] || []).some(function(x){ return x == v; })) return false;
  }
  return true;
}
function scopeDeFiltros(tabla, filtros){
  if(GLOBALES[tabla]) return "_";
  var f = filtros.filter(function(x){ return x[0]==="eq" && x[1]==="empresa_id"; })[0];
  return f ? f[2] : "_";
}
function proyectar(row, cols){
  if(!cols || cols==="*") return clonar(row);
  var out = {};
  String(cols).split(",").forEach(function(c){ c = c.trim(); if(c) out[c] = row[c]; });
  return out;
}
function clavesUpsert(tabla, opts){
  if(opts && opts.onConflict) return String(opts.onConflict).split(",").map(function(s){ return s.trim(); });
  return ["id"];
}
function mismoRegistro(a, b, claves){
  return claves.every(function(k){ return a[k] != null && a[k] == b[k]; });
}
// Aplica una escritura a un arreglo de filas. Devuelve las filas afectadas.
function aplicarOp(rows, tabla, op, payload, opts, filtros, soloScope){
  var afectadas = [];
  if(op==="insert" || op==="upsert"){
    var lista = Array.isArray(payload) ? payload : [payload];
    var claves = clavesUpsert(tabla, opts);
    lista.forEach(function(r){
      if(soloScope && soloScope!=="_" && r.empresa_id && r.empresa_id!==soloScope) return;
      var idx = -1;
      for(var i=0;i<rows.length;i++){
        if((r.id && rows[i].id===r.id) || (op==="upsert" && mismoRegistro(rows[i], r, claves))){ idx = i; break; }
      }
      if(idx>=0){
        if(op==="upsert" && opts && opts.ignoreDuplicates){ return; }
        rows[idx] = Object.assign({}, rows[idx], r);
        afectadas.push(rows[idx]);
      } else {
        var nueva = clonar(r);
        rows.push(nueva);
        afectadas.push(nueva);
      }
    });
  } else if(op==="update"){
    rows.forEach(function(row, i){
      if(coincide(row, filtros)){ rows[i] = Object.assign({}, row, payload); afectadas.push(rows[i]); }
    });
  } else if(op==="delete"){
    for(var j=rows.length-1;j>=0;j--){
      if(coincide(rows[j], filtros)){ afectadas.push(rows[j]); rows.splice(j,1); }
    }
  }
  return afectadas;
}
function aplicarItem(rows, item, scope){
  var q = analizar({calls:item.calls});
  if(q.op && q.op!=="select") aplicarOp(rows, item.table, q.op, q.payload, q.opts, q.filtros, scope);
}

function respuesta(q, data, count){
  if(q.unico){
    if(!data.length) return {data:null, error:{message:"No se encontró el registro (sin conexión)", code:"PGRST116"}};
    return {data:data[0], error:null};
  }
  if(q.quizaUnico) return {data: data[0] || null, error:null};
  var r = {data:data, error:null};
  if(count!==undefined) r.count = count;
  return r;
}

function lecturaLocal(rec){
  var q = analizar(rec);
  if(q.noSoportado) return Promise.resolve({data:null, error:{message:"Esta consulta no está disponible sin conexión ("+q.noSoportado+")."}});
  var scope = scopeDeFiltros(rec.table, q.filtros);
  return cargarTabla(rec.table, scope).then(function(rows){
    if(!rows) return {data:null, error:{message:"Sin conexión y sin datos guardados de "+rec.table+". Abre esta empresa una vez con internet para poder usarla sin conexión."}};
    var res = rows.filter(function(r){ return coincide(r, q.filtros); });
    if(q.orden){
      var col = q.orden.col, dir = q.orden.asc ? 1 : -1;
      res = res.slice().sort(function(a,b){
        var x = a[col], y = b[col];
        if(x==null && y==null) return 0; if(x==null) return 1; if(y==null) return -1;
        return (x<y ? -1 : (x>y ? 1 : 0)) * dir;
      });
    }
    var total = res.length;
    if(q.rango) res = res.slice(q.rango[0], q.rango[1]+1);
    if(q.limite!=null) res = res.slice(0, q.limite);
    return respuesta(q, res.map(function(r){ return proyectar(r, q.cols); }), q.conteo ? total : undefined);
  });
}

/* ---------- escrituras sin conexión ---------- */
function refrescarPendientes(){
  return idbAll("q").then(function(c){ state.pendientes = c.length; notificar(); });
}
function escrituraLocal(rec){
  var q = analizar(rec);
  if(q.noSoportado) return Promise.resolve({data:null, error:{message:"Esta operación no está disponible sin conexión ("+q.noSoportado+")."}});
  var tabla = rec.table;
  var calls = rec.calls.map(function(c){ return [c[0], c[1].slice()]; });
  var opIdx = -1;
  calls.forEach(function(c, i){ if(OPS_ESCRITURA.indexOf(c[0])>=0) opIdx = i; });
  var op = calls[opIdx][0];
  var payload = calls[opIdx][1][0];
  var opts = calls[opIdx][1][1] || {};
  var filtros = q.filtros;

  // Identificadores y fechas que normalmente pone el servidor: se fijan aquí para que
  // la copia local y la que se suba después sean idénticas.
  if(op==="insert" || op==="upsert"){
    var lista = (Array.isArray(payload) ? payload : [payload]).map(clonar);
    lista.forEach(function(r){
      if(CON_ID[tabla] && !r.id) r.id = uuid();
      if(tabla==="empresas") marcarEmpresaNueva(r.id);
    });
    payload = Array.isArray(payload) ? lista : lista[0];
    // Un comprobante nuevo (número que no existe aún) debe subir como INSERT y no como
    // UPSERT, para no pisar a otro comprobante si alguien más usó ese número mientras tanto.
    calls[opIdx] = [op, [payload].concat(calls[opIdx][1].slice(1))];
  }

  var scopes = {};
  if(op==="insert" || op==="upsert"){
    (Array.isArray(payload) ? payload : [payload]).forEach(function(r){ scopes[GLOBALES[tabla] ? "_" : (r.empresa_id || "_")] = 1; });
  } else scopes[scopeDeFiltros(tabla, filtros)] = 1;

  var afectadasTotal = [];
  var pasos = Object.keys(scopes).reduce(function(p, sc){
    return p.then(function(){
      return cargarTabla(tabla, sc).then(function(rows){
        if(!rows) return;   // sin copia local de esta empresa: solo se encola
        if(tabla==="comprobantes" && op==="upsert" && String(opts.onConflict||"").indexOf("numero")>=0 && !opts.ignoreDuplicates){
          var lst = Array.isArray(payload) ? payload : [payload];
          var todosNuevos = lst.every(function(r){ return !rows.some(function(x){ return x.numero===r.numero && x.empresa_id===r.empresa_id; }); });
          if(todosNuevos){ calls[opIdx] = ["insert", [payload]]; }
        }
        afectadasTotal = afectadasTotal.concat(aplicarOp(rows, tabla, op, payload, opts, filtros, sc));
        programarPersistencia(tabla, sc);
      });
    });
  }, Promise.resolve());

  return pasos.then(function(){
    return idbPut("q", {table:tabla, calls:calls, ts:Date.now()});
  }).then(function(){
    return refrescarPendientes();
  }).then(function(){
    if(hooks.cambioLocal) try{ hooks.cambioLocal(tabla, Object.keys(scopes)); }catch(e){}
    return respuesta(q, afectadasTotal.map(clonar));
  });
}

/* ---------- ejecutar una consulta ---------- */
function replayReal(rec){
  var b = real.from(rec.table);
  rec.calls.forEach(function(c){ b = b[c[0]].apply(b, c[1]); });
  return b;
}
function ejecutar(rec){
  var esEscritura = rec.calls.some(function(c){ return OPS_ESCRITURA.indexOf(c[0])>=0; });
  var intento = state.online
    ? Promise.resolve().then(function(){ return replayReal(rec); }).then(null, function(e){ return {data:null, error:{message:String(e && e.message || e)}}; })
    : Promise.resolve(null);
  return intento.then(function(r){
    if(r && !(r.error && esErrorDeRed(r.error))) return r;
    if(state.online) marcarConexion(false);
    return esEscritura ? escrituraLocal(rec) : lecturaLocal(rec);
  });
}

function Rec(tabla){ this.table = tabla; this.calls = []; }
["select","insert","update","upsert","delete","eq","neq","gt","gte","lt","lte","like","ilike","is","in","not","or","filter","match","contains","order","range","limit","single","maybeSingle","csv"].forEach(function(m){
  Rec.prototype[m] = function(){ this.calls.push([m, Array.prototype.slice.call(arguments)]); return this; };
});
Rec.prototype.then = function(a, b){ return ejecutar(this).then(a, b); };
Rec.prototype["catch"] = function(f){ return this.then(null, f); };

/* ---------- conexión ---------- */
function ping(){
  var c = (typeof AbortController!=="undefined") ? new AbortController() : null;
  var t = setTimeout(function(){ if(c) c.abort(); }, 5000);
  return fetch(cfg.url + "/auth/v1/health", {headers:{apikey:cfg.key}, cache:"no-store", signal: c ? c.signal : undefined})
    .then(function(r){ clearTimeout(t); return r.status < 500; })
    .then(null, function(){ clearTimeout(t); return false; });
}
function marcarConexion(enLinea){
  if(state.online === enLinea) return;
  state.online = enLinea;
  if(enLinea){
    if(pingTimer){ clearInterval(pingTimer); pingTimer = null; }
    state.error = "";
    notificar();
    alReconectar();
  } else {
    if(!pingTimer) pingTimer = setInterval(function(){ ping().then(function(ok){ if(ok) marcarConexion(true); }); }, 10000);
    notificar();
  }
}
function alReconectar(){
  var p = real ? real.auth.getSession() : Promise.resolve({data:{session:null}});
  p.then(function(res){
    if(!res || !res.data || !res.data.session){
      state.error = "Volvió el internet, pero hay que iniciar sesión otra vez para subir tus cambios.";
      notificar();
      if(hooks.sesionVencida) hooks.sesionVencida();
      return;
    }
    return sincronizar();
  }).then(null, function(){});
}

/* ---------- sincronizar la cola ---------- */
function siguienteNumero(item){
  // Si el número de un comprobante ya lo usó otra persona, se le asigna el siguiente libre.
  var calls = item.calls, filas = [];
  calls.forEach(function(c){ if(OPS_ESCRITURA.indexOf(c[0])>=0){ filas = Array.isArray(c[1][0]) ? c[1][0] : [c[1][0]]; } });
  var porPrefijo = {};
  filas.forEach(function(r){
    var m = /^(.*-)(\d+)$/.exec(r.numero || "");
    if(!m) return;
    var pref = r.empresa_id + "|" + m[1];
    (porPrefijo[pref] = porPrefijo[pref] || []).push({fila:r, pref:m[1], ancho:m[2].length, empresa:r.empresa_id});
  });
  return Object.keys(porPrefijo).reduce(function(p, k){
    return p.then(function(){
      var grupo = porPrefijo[k];
      return real.from("comprobantes").select("numero").eq("empresa_id", grupo[0].empresa).like("numero", grupo[0].pref + "%").order("numero", {ascending:false}).limit(1).then(function(res){
        var max = 0;
        if(res.data && res.data.length){ var mm = /(\d+)$/.exec(res.data[0].numero); if(mm) max = parseInt(mm[1], 10); }
        grupo.forEach(function(g){
          max += 1;
          g.fila.numero = g.pref + String(max).padStart(g.ancho, "0");
        });
      });
    });
  }, Promise.resolve());
}
function ejecutarItem(item){
  return Promise.resolve().then(function(){
    var b = real.from(item.table);
    item.calls.forEach(function(c){ b = b[c[0]].apply(b, c[1]); });
    return b;
  }).then(null, function(e){ return {data:null, error:{message:String(e && e.message || e)}}; });
}
function sincronizar(){
  if(state.sincronizando || !state.online || !real) return Promise.resolve();
  state.sincronizando = true; state.error = ""; notificar();
  var subidos = 0;
  function paso(){
    return idbAll("q").then(function(cola){
      state.pendientes = cola.length; notificar();
      if(!cola.length) return true;
      var item = cola.sort(function(a,b){ return a.qid - b.qid; })[0];
      return ejecutarItem(item).then(function(r){
        if(r.error){
          if(esErrorDeRed(r.error)){ marcarConexion(false); return false; }
          var msg = r.error.message || "";
          if(r.error.code==="PGRST301" || /JWT/i.test(msg)){
            state.error = "La sesión venció. Inicia sesión otra vez para subir tus cambios.";
            if(hooks.sesionVencida) hooks.sesionVencida();
            return false;
          }
          if(item.table==="comprobantes" && r.error.code==="23505" && /numero/.test(msg) && !item.renumerado){
            item.renumerado = true;
            return siguienteNumero(item).then(function(){ return idbPut("q", item); }).then(paso);
          }
          if(item.table==="comprobantes" && r.error.code==="23505" && /cufe/.test(msg)){
            // Ese documento de la DIAN ya está guardado: no hay nada que subir.
            return idbDel("q", item.qid).then(function(){ subidos++; return paso(); });
          }
          state.error = "No se pudo subir un cambio de «"+item.table+"»: "+msg;
          return false;
        }
        return idbDel("q", item.qid).then(function(){ subidos++; return paso(); });
      });
    });
  }
  return paso().then(function(vacia){
    state.sincronizando = false;
    return refrescarPendientes().then(function(){
      if(vacia){ state.ultimaSync = Date.now(); Object.keys(ultimoGuardado).forEach(function(k){ delete ultimoGuardado[k]; }); }
      notificar();
      if(vacia && hooks.sincronizado) try{ hooks.sincronizado(subidos); }catch(e){}
    });
  }, function(e){
    state.sincronizando = false; state.error = String(e && e.message || e); notificar();
  });
}

/* ---------- API pública ---------- */
var API = {
  wrap: function(cliente, opciones){
    real = cliente; cfg.url = opciones.url; cfg.key = opciones.key;
    if(typeof window!=="undefined"){
      window.addEventListener("offline", function(){ marcarConexion(false); });
      window.addEventListener("online", function(){ ping().then(function(ok){ marcarConexion(ok); }); });
    }
    refrescarPendientes().then(null, function(){});
    if(!state.online){ pingTimer = setInterval(function(){ ping().then(function(ok){ if(ok) marcarConexion(true); }); }, 10000); }
    return {
      auth: cliente.auth,
      real: cliente,
      channel: function(){ return cliente.channel.apply(cliente, arguments); },
      removeChannel: function(){ return cliente.removeChannel.apply(cliente, arguments); },
      from: function(t){ return new Rec(t); }
    };
  },
  estado: function(){ return state; },
  enLinea: function(){ return state.online; },
  alCambiar: function(f){ oyentes.push(f); },
  alCambioLocal: function(f){ hooks.cambioLocal = f; },
  alSincronizar: function(f){ hooks.sincronizado = f; },
  alSesionVencida: function(f){ hooks.sesionVencida = f; },
  guardarTabla: function(tabla, scope, rows, forzar){ return guardarTabla(tabla, scope, rows, forzar); },
  marcarSinConexion: function(){ marcarConexion(false); },
  comprobarConexion: function(){ return ping().then(function(ok){ marcarConexion(ok); return ok; }); },
  sincronizar: sincronizar,
  pendientes: function(){ return idbAll("q"); },
  recordarUsuario: function(u){ try{ localStorage.setItem(LS_USUARIO, JSON.stringify({id:u.id, email:u.email})); }catch(e){} },
  usuarioLocal: function(){ try{ return JSON.parse(localStorage.getItem(LS_USUARIO) || "null"); }catch(e){ return null; } },
  // Al cerrar sesión: si no queda nada pendiente se borra la copia de este computador.
  limpiar: function(){
    mem = {}; ultimoGuardado = {};
    try{ localStorage.removeItem(LS_USUARIO); localStorage.removeItem(LS_NUEVAS); }catch(e){}
    return Promise.all([idbClear("t"), idbClear("q")]).then(refrescarPendientes);
  },
  // Solo para pruebas
  _interno: {analizar:analizar, aplicarOp:aplicarOp, coincide:coincide, ejecutar:ejecutar, cargarTabla:cargarTabla, esErrorDeRed:esErrorDeRed}
};
global.CuadreOffline = API;
})(typeof window!=="undefined" ? window : this);
