import Head from "next/head";

import "../styles/globals.css";

const normalizeBasePath = (value) => {
  if (!value) {
    return "";
  }
  const trimmed = String(value).trim();
  if (!trimmed || trimmed === "/") {
    return "";
  }
  return `/${trimmed.replace(/^\/+|\/+$/g, "")}`;
};

const basePath = normalizeBasePath(process.env.NEXT_PUBLIC_BASE_PATH);

export default function App({ Component, pageProps }) {
  return (
    <>
      <Head>
        <link rel="icon" href={`${basePath}/asstes/favicon.svg`} />
      </Head>
      <Component {...pageProps} />
    </>
  );
}
