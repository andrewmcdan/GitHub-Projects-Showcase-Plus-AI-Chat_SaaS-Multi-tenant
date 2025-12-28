import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { migrate } from "drizzle-orm/node-postgres/migrator";
import { db, pool } from "./index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const migrationsFolder = path.join(__dirname, "migrations");

const run = async () => {
  await fs.mkdir(migrationsFolder, { recursive: true });
  await migrate(db, { migrationsFolder });
  await pool.end();
};

run().catch((err) => {
  console.error("Migration failed:", err);
  process.exit(1);
});
