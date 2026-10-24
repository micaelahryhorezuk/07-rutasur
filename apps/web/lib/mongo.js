import { MongoClient } from "mongodb";

export async function getDatabase() {
  if (!globalThis.rutasurMongo) {
    const client = new MongoClient(process.env.MONGODB_URI ?? "mongodb://localhost:27028/rutasur?replicaSet=rs0&directConnection=true", { serverSelectionTimeoutMS: 5000, maxPoolSize: 30 });
    globalThis.rutasurMongo = client.connect().catch(error => { globalThis.rutasurMongo = undefined; throw error; });
  }
  const client = await globalThis.rutasurMongo;
  return client.db(process.env.MONGODB_DATABASE ?? "rutasur");
}
