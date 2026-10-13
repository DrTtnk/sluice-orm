// ==========================================
// Setup utility for tests
// ==========================================

import { MongoDBContainer, StartedMongoDBContainer } from "@testcontainers/mongodb";
import { Db, MongoClient } from "mongodb";

let container: StartedMongoDBContainer | null = null;
let client: MongoClient | null = null;
let db: Db | null = null;

export const teardown = async () => {
  if (client) await client.close();
  if (container) await container.stop();
  client = null;
  container = null;
  db = null;
};

export const setup = async () => {
  if (db && client && container)
    return {
      db,
      client,
      container,
    };

  // Start MongoDB container.
  // mongo:8.0 and later refuse to start on Linux kernel 6.19+ (SERVER-121912): use 7.0 on such hosts.
  container = await new MongoDBContainer("mongo:7.0").start();
  const uri = container.getConnectionString();

  client = new MongoClient(uri, { directConnection: true });
  await client.connect();
  db = client.db("test_db");

  return {
    db,
    client,
    container,
  };
};

export const getDb = () => {
  if (!db) throw new Error("DB not initialized. Call setup() first.");
  return db;
};
