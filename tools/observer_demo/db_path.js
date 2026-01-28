"use strict";

const path = require("path");

const projectRoot = path.resolve(__dirname, "..", "..");
const dataDir = path.join(projectRoot, "data");
const defaultDbPath = path.join(dataDir, "meshrank.db");

function getDbPath() {
  const envPath = process.env.MESHRANK_DB_PATH;
  const candidate = envPath && String(envPath).trim() ? envPath : defaultDbPath;
  return path.resolve(candidate);
}

let dbInfoLogged = false;
function logDbInfo(db) {
  if (dbInfoLogged) return;
  try {
    const resolved = getDbPath();
    const dbList = db && typeof db.pragma === "function" ? db.pragma("database_list") : [];
    console.log(`[DB] path=${resolved} cwd=${process.cwd()} database_list=${JSON.stringify(dbList)}`);
  } catch (err) {
    console.log(`[DB] info failed ${err?.message || err}`);
  } finally {
    dbInfoLogged = true;
  }
}

module.exports = { getDbPath, logDbInfo };
