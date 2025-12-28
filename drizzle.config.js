module.exports = {
  schema: "./packages/shared/src/db/schema.js",
  out: "./apps/api/src/db/migrations",
  dialect: "postgresql",
  dbCredentials: {
    url: process.env.DATABASE_URL
  }
};
