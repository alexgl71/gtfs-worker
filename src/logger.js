const { MongoClient } = require('mongodb');

let logsCollection = null;
let errorsCollection = null;

async function init() {
  const client = new MongoClient(process.env.REMOTE_URI);
  await client.connect();
  const db = client.db('general');
  logsCollection = db.collection('logs');
  errorsCollection = db.collection('errors');
}

async function log(type, data = {}) {
  try {
    if (!logsCollection) await init();
    await logsCollection.insertOne({ ts: new Date(), type, ...data });
  } catch (_) {}
}

async function logError(type, data = {}) {
  try {
    if (!errorsCollection) await init();
    await errorsCollection.insertOne({ ts: new Date(), type, ...data });
  } catch (_) {}
}

module.exports = { log, logError };
