const { parentPort } = require("worker_threads");
const { buildRepeaterRank } = require("./server");

(async () => {
  try {
    const data = await buildRepeaterRank();
    parentPort.postMessage({ ok: true, data });
  } catch (err) {
    parentPort.postMessage({
      ok: false,
      error: {
        message: err?.message || String(err),
        stack: err?.stack || null
      }
    });
  }
})();
