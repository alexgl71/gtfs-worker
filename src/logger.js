const { MongoClient } = require('mongodb');

let logsCollection = null;

async function init() {
  const client = new MongoClient(process.env.REMOTE_URI);
  await client.connect();
  logsCollection = client.db('general').collection('logs');
}

async function log(type, data = {}) {
  try {
    if (!logsCollection) await init();
    await logsCollection.insertOne({ ts: new Date(), type, ...data });
  } catch (_) {}
}

module.exports = { log };
