/* Service worker de Cuadre: deja abrir la aplicación sin internet.
 * Guarda una copia de la página y de las librerías. Los datos (Supabase) NO pasan por aquí:
 * de eso se encarga offline.js con la copia local de la base. */
var VERSION = "cuadre-shell-v1";
var CDN = ["cdn.jsdelivr.net", "fonts.googleapis.com", "fonts.gstatic.com"];
var SHELL = ["./", "./index.html", "./offline.js", "./manifest.webmanifest", "./icon-192.png", "./icon-512.png"];

self.addEventListener("install", function(e){
  e.waitUntil(
    caches.open(VERSION)
      .then(function(c){ return Promise.all(SHELL.map(function(u){ return c.add(new Request(u, {cache:"reload"})).catch(function(){}); })); })
      .then(function(){ return self.skipWaiting(); })
  );
});

self.addEventListener("activate", function(e){
  e.waitUntil(
    caches.keys()
      .then(function(ks){ return Promise.all(ks.filter(function(k){ return k!==VERSION; }).map(function(k){ return caches.delete(k); })); })
      .then(function(){ return self.clients.claim(); })
  );
});

function guardar(req, resp){
  if(resp && (resp.ok || resp.type==="opaque")){
    var copia = resp.clone();
    caches.open(VERSION).then(function(c){ c.put(req, copia); }).catch(function(){});
  }
  return resp;
}

// Páginas propias: primero la red (para tomar siempre la versión nueva), pero si no responde
// en 4 segundos o falla, se usa la última copia guardada.
function redPrimero(req){
  var red = fetch(req).then(function(r){ return guardar(req, r); });
  var limite = new Promise(function(_, rej){ setTimeout(function(){ rej(new Error("tiempo")); }, 4000); });
  return Promise.race([red, limite]).catch(function(){
    return caches.match(req, {ignoreSearch:true}).then(function(hit){
      if(hit){ red.catch(function(){}); return hit; }
      if(req.mode==="navigate") return caches.match("./index.html");
      return red;
    });
  });
}

// Librerías externas (versiones fijas): primero la copia guardada.
function copiaPrimero(req){
  return caches.match(req).then(function(hit){
    if(hit) return hit;
    return fetch(req).then(function(r){ return guardar(req, r); });
  });
}

self.addEventListener("fetch", function(e){
  var req = e.request;
  if(req.method!=="GET") return;
  var url = new URL(req.url);
  if(url.hostname.slice(-12)===".supabase.co") return;   // los datos no se interceptan
  if(url.origin===self.location.origin){ e.respondWith(redPrimero(req)); return; }
  if(CDN.indexOf(url.hostname)>=0){ e.respondWith(copiaPrimero(req)); }
});
