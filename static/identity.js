/* DistMod browser identity.
 *
 * Your key lives only on this device. It signs authoring/voting challenges so
 * the network knows an action is really yours — across any machine you carry
 * the key to. We never send the private key anywhere.
 */
(function () {
  const enc = (s) => new TextEncoder().encode(s);
  const hex = (u8) => Array.from(u8).map(b => b.toString(16).padStart(2, "0")).join("");
  const unhex = (h) => new Uint8Array(h.match(/.{1,2}/g).map(x => parseInt(x, 16)));

  function machineFingerprint() {
    const bits = [navigator.userAgent, navigator.language,
                  screen.width + "x" + screen.height,
                  Intl.DateTimeFormat().resolvedOptions().timeZone];
    return hex(nacl.hash(enc(bits.join("|"))).slice(0, 8));
  }

  const KEY_SK = "distmod_sk", KEY_PK = "distmod_pk";

  function loadKeyPair() {
    const sk = localStorage.getItem(KEY_SK);
    if (!sk) return null;
    try { return nacl.sign.keyPair.fromSecretKey(unhex(sk)); }
    catch { return null; }
  }
  function saveKeyPair(kp) {
    localStorage.setItem(KEY_SK, hex(kp.secretKey));
    localStorage.setItem(KEY_PK, hex(kp.publicKey));
  }
  function publicKeyHex() {
    const pk = localStorage.getItem(KEY_PK);
    return pk || null;
  }

  async function ensureIdentity(displayName) {
    let kp = loadKeyPair();
    if (kp) return hex(kp.publicKey);
    kp = nacl.sign.keyPair();
    saveKeyPair(kp);
    const pub = hex(kp.publicKey);
    const sig = hex(nacl.sign.detached(enc(pub), kp.secretKey));
    await fetch("/api/identity/register", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pubkey: pub, sig,
        machine_fingerprint: machineFingerprint(),
        display_name: displayName || "" })
    });
    return pub;
  }

  // Get {pubkey, sig, machine_fingerprint} for a signed-challenge action.
  async function authParams() {
    const pub = await ensureIdentity();
    const kp = loadKeyPair();
    const r = await fetch("/api/identity/challenge", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pubkey: pub })
    });
    const { nonce } = await r.json();
    return {
      pubkey: pub,
      sig: hex(nacl.sign.detached(enc(nonce), kp.secretKey)),
      machine_fingerprint: machineFingerprint()
    };
  }

  let toastTimer;
  function toast(msg, isErr) {
    const t = document.getElementById("toast");
    if (!t) return;
    t.textContent = msg;
    t.classList.toggle("err", !!isErr);
    t.classList.add("show");
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => t.classList.remove("show"), 3200);
  }

  window.distmod = {
    ensureIdentity, authParams, publicKeyHex, loadKeyPair, saveKeyPair,
    machineFingerprint, toast, hex, unhex, enc,
    exportKey() {
      const kp = loadKeyPair();
      return kp ? { public: hex(kp.publicKey), secret: hex(kp.secretKey) } : null;
    },
    importKey(secretHex) {
      const kp = nacl.sign.keyPair.fromSecretKey(unhex(secretHex.trim()));
      saveKeyPair(kp);
      return hex(kp.publicKey);
    }
  };
})();
