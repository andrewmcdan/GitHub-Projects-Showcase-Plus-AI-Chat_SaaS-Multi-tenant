export const getRedisConnectionOptions = (
  redisUrl = process.env.REDIS_URL
) => {
  if (!redisUrl) {
    return { host: "localhost", port: 6379 };
  }

  const url = new URL(redisUrl);
  const options = {
    host: url.hostname,
    port: url.port ? Number(url.port) : 6379
  };

  if (url.username) {
    options.username = url.username;
  }

  if (url.password) {
    options.password = url.password;
  }

  if (url.pathname && url.pathname !== "/") {
    const db = Number(url.pathname.replace("/", ""));
    if (!Number.isNaN(db)) {
      options.db = db;
    }
  }

  return options;
};
