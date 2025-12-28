import { Pool } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { getDatabaseUrl } from "@app/shared";

const connectionString = getDatabaseUrl();

export const pool = new Pool({ connectionString });
export const db = drizzle(pool);
