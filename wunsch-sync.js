/*
  Gemeinsamer Speicher für Wunschliste, Admin-Ansicht und Anzeige.

  Ably hebt Nachrichten ohne Einstellung nur 2 Minuten auf. Damit die Liste nicht
  "verschwindet", werden alle Wünsche und Admin-Aktionen aus drei Quellen zusammengeführt:
    1. lokale Kopie in diesem Browser (localStorage)
    2. der letzte "Stand" im Kanal <name>:stand (alle Ereignisse als ein Paket)
    3. der normale Verlauf des Hauptkanals (alle Seiten, nicht nur die erste)
  Hat ein Gerät mehr als der gespeicherte Stand, veröffentlicht es einen neuen Stand.

  Damit der Stand lange erhalten bleibt, im Ably-Dashboard eine Channel Rule für den
  Namespace "<Kanalname>" mit "Persist last message" (und "Persist all messages") anlegen.
*/
(function(){
  const SNAP_SUFFIX = ':stand';

  function keyOf(name, data){
    return name === 'new-track' ? 't:' + data.id : 'a:' + JSON.stringify(data);
  }

  async function encode(events){
    const json = JSON.stringify({ v: 1, events: events });
    if(typeof CompressionStream === 'undefined') return { z: false, s: json };
    const stream = new Blob([json]).stream().pipeThrough(new CompressionStream('gzip'));
    const buf = new Uint8Array(await new Response(stream).arrayBuffer());
    let bin = '';
    for(let i = 0; i < buf.length; i += 0x8000) bin += String.fromCharCode.apply(null, buf.subarray(i, i + 0x8000));
    return { z: true, s: btoa(bin) };
  }

  async function decode(p){
    if(!p || typeof p.s !== 'string') return [];
    let json = p.s;
    if(p.z){
      const bin = atob(p.s);
      const buf = new Uint8Array(bin.length);
      for(let i = 0; i < bin.length; i++) buf[i] = bin.charCodeAt(i);
      const stream = new Blob([buf]).stream().pipeThrough(new DecompressionStream('gzip'));
      json = await new Response(stream).text();
    }
    const o = JSON.parse(json);
    return Array.isArray(o.events) ? o.events : [];
  }

  function loadAllHistory(channel){
    return new Promise((resolve) => {
      const items = [];
      channel.history({ limit: 1000 }, (err, page) => {
        if(err || !page) return resolve(items);
        (function take(p){
          items.push.apply(items, p.items);
          if(p.hasNext()) p.next((e, n) => { if(e || !n) resolve(items); else take(n); });
          else resolve(items);
        })(page);
      });
    });
  }

  function sortForApply(list){
    const t = list.filter(e => e.n === 'new-track');
    const a = list.filter(e => e.n === 'admin-action').sort((x, y) => (x.d.ts || 0) - (y.d.ts || 0));
    return t.concat(a);
  }

  window.WunschSync = function(ably, channelName, opts){
    opts = opts || {};
    const onEvent = opts.onEvent || function(){};
    const onReady = opts.onReady || function(){};
    const cacheKey = 'wunsch_sync_' + channelName;
    const events = new Map();     // key -> {n, d}
    const localKeys = new Set();  // selbst veröffentlicht, schon lokal angewendet
    const channel = ably.channels.get(channelName);
    const snapChannel = ably.channels.get(channelName + SNAP_SUFFIX, { params: { rewind: '1' } });
    let ready = false, snapTimer = null, bestSnapCount = -1;

    function valid(n, d){
      if(!d || typeof d !== 'object') return false;
      if(n === 'new-track') return !!d.id;
      if(n === 'admin-action') return !!d.type;
      return false;
    }
    function add(n, d){
      if(!valid(n, d)) return false;
      const k = keyOf(n, d);
      if(events.has(k)) return false;
      events.set(k, { n: n, d: d });
      return true;
    }
    function saveCache(){
      try{ localStorage.setItem(cacheKey, JSON.stringify(Array.from(events.values()))); }catch(e){}
    }
    function loadCache(){
      try{
        const a = JSON.parse(localStorage.getItem(cacheKey) || '[]');
        return Array.isArray(a) ? a : [];
      }catch(e){ return []; }
    }

    function scheduleSnapshot(fast){
      clearTimeout(snapTimer);
      snapTimer = setTimeout(publishSnapshot, (fast ? 300 : 1500) + Math.random() * 4500);
    }
    async function publishSnapshot(){
      if(events.size <= bestSnapCount) return;
      try{
        const p = await encode(Array.from(events.values()));
        p.count = events.size;
        p.ts = Date.now();
        snapChannel.publish('stand', p);
        bestSnapCount = events.size;
      }catch(e){}
    }

    async function handleSnapshot(data){
      let list = [];
      try{ list = await decode(data); }catch(e){ return; }
      const fresh = list.filter(e => e && add(e.n, e.d));
      bestSnapCount = Math.max(bestSnapCount, list.length);
      if(ready && fresh.length){
        saveCache();
        sortForApply(fresh).forEach(e => onEvent(e.n, e.d));
      }
      // Jemand anders hat bereits alles gesichert, was wir kennen.
      if(list.length >= events.size) clearTimeout(snapTimer);
    }

    // Live: Hauptkanal
    channel.subscribe((msg) => {
      if(!add(msg.name, msg.data)) return;
      saveCache();
      if(ready){
        onEvent(msg.name, msg.data);
        scheduleSnapshot(false);
      }
    });

    // Laden: lokale Kopie, letzter Stand, Verlauf
    loadCache().forEach(e => { if(e) add(e.n, e.d); });

    const snapLoaded = new Promise((resolve) => {
      let done = false;
      const finish = () => { if(!done){ done = true; resolve(); } };
      snapChannel.subscribe('stand', (msg) => { handleSnapshot(msg.data).then(finish); });
      snapChannel.attach((err) => {
        if(err) return finish();
        snapChannel.history({ limit: 1 }, (e, page) => {
          if(!e && page && page.items[0]) handleSnapshot(page.items[0].data).then(finish);
        });
      });
      setTimeout(finish, 3500);
    });

    const historyLoaded = new Promise((resolve) => {
      channel.attach((err) => {
        if(err){ if(opts.onError) opts.onError(err); return resolve([]); }
        loadAllHistory(channel).then(resolve);
      });
    }).then(items => {
      items.forEach(item => add(item.name, item.data));
    });

    Promise.all([snapLoaded, historyLoaded]).then(() => {
      ready = true;
      saveCache();
      sortForApply(Array.from(events.entries())
        .filter(([k]) => !localKeys.has(k))
        .map(([, e]) => e))
        .forEach(e => onEvent(e.n, e.d));
      onReady();
      // Wir kennen mehr als der gespeicherte Stand -> neuen Stand sichern
      if(events.size > bestSnapCount) scheduleSnapshot(true);
    });

    return {
      publish(name, data){
        if(add(name, data)) localKeys.add(keyOf(name, data));
        saveCache();
        channel.publish(name, data);
        scheduleSnapshot(false);
      },
      has(name, data){
        return valid(name, data) && events.has(keyOf(name, data));
      },
      get isReady(){ return ready; }
    };
  };
})();
