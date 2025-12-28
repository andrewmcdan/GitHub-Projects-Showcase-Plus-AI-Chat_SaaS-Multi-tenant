export const getDatabaseUrl = () => {
  if (process.env.DATABASE_URL) {
    return process.env.DATABASE_URL;
  }

  const host = process.env.POSTGRES_HOST || "localhost";
  const port = process.env.POSTGRES_PORT || "5432";
  const user = process.env.POSTGRES_USER || "app";
  const password = process.env.POSTGRES_PASSWORD || "app";
  const database = process.env.POSTGRES_DB || "app";

  return `postgres://${encodeURIComponent(user)}:${encodeURIComponent(
    password
  )}@${host}:${port}/${database}`;
};
