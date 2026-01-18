import { useEffect, useMemo, useState } from "react";
import Link from "next/link";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:4011";

const buildApiUrl = (path, params = {}) => {
  const url = new URL(`${API_BASE_URL}${path}`);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, value);
    }
  });
  return url.toString();
};

const normalizeQuery = (value) => {
  if (typeof value !== "string") {
    return "";
  }
  return value.trim().replace(/^@/, "").toLowerCase();
};

const formatShowcaseCount = (value) => {
  const count = Number(value);
  const safeCount = Number.isFinite(count) ? count : 0;
  return `${safeCount} public showcase${safeCount === 1 ? "" : "s"}`;
};

const formatRepoCount = (value) => {
  const count = Number(value);
  const safeCount = Number.isFinite(count) ? count : 0;
  return `${safeCount} repo${safeCount === 1 ? "" : "s"}`;
};

export default function ShowcaseDirectoryPage() {
  const [showcases, setShowcases] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [query, setQuery] = useState("");

  useEffect(() => {
    let isMounted = true;

    const loadShowcases = async () => {
      setLoading(true);
      setError("");
      try {
        const response = await fetch(buildApiUrl("/showcases"), {
          credentials: "include"
        });
        if (!response.ok) {
          throw new Error("Failed to load public showcases.");
        }
        const data = await response.json();
        const entries = Array.isArray(data.showcases) ? data.showcases : [];
        if (isMounted) {
          setShowcases(entries);
        }
      } catch (err) {
        if (isMounted) {
          setError(err.message || "Failed to load public showcases.");
        }
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    };

    loadShowcases();
    return () => {
      isMounted = false;
    };
  }, []);

  const normalizedQuery = useMemo(() => normalizeQuery(query), [query]);
  const filteredShowcases = useMemo(() => {
    if (!normalizedQuery) {
      return showcases;
    }
    return showcases.filter((entry) => {
      const haystack = [
        entry?.name || "",
        entry?.handle || "",
        entry?.bio || ""
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(normalizedQuery);
    });
  }, [showcases, normalizedQuery]);

  const totalCount = showcases.length;
  const filteredCount = filteredShowcases.length;
  const countLabel = loading
    ? "Loading public showcases..."
    : normalizedQuery
      ? `Showing ${filteredCount} of ${formatShowcaseCount(totalCount)}`
      : formatShowcaseCount(totalCount);

  return (
    <main className="directory-page">
      <header className="directory-header">
        <div className="directory-intro">
          <p className="eyebrow">Directory</p>
          <h1>Public showcases</h1>
          <p className="lede">
            Browse builders who have shared their GitHub projects with the AI
            interview experience.
          </p>
        </div>
        <div className="directory-actions">
          <Link className="ghost-button" href="/">
            Back to showcase
          </Link>
        </div>
      </header>

      <section className="panel directory-filters">
        <div className="field">
          <label htmlFor="showcase-search">
            Search by name, handle, or bio
          </label>
          <input
            id="showcase-search"
            type="search"
            placeholder="e.g. @handle, infra, fintech"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
        </div>
        <div className="directory-filter-meta muted">{countLabel}</div>
      </section>

      <section className="directory-grid">
        {loading ? (
          <p className="directory-empty muted">Loading public showcases...</p>
        ) : error ? (
          <p className="directory-empty status error">{error}</p>
        ) : filteredShowcases.length === 0 ? (
          <p className="directory-empty muted">
            {normalizedQuery
              ? "No showcases match that search yet."
              : "No public showcases yet."}
          </p>
        ) : (
          filteredShowcases.map((entry) => {
            const handle = entry?.handle || "";
            const name = entry?.name || `@${handle}`;
            return (
              <article className="directory-card" key={handle}>
                <div className="directory-card-header">
                  <div className="directory-card-main">
                    <h2 className="directory-card-title">{name}</h2>
                    <span className="directory-handle">@{handle}</span>
                  </div>
                  <span className="directory-count">
                    {formatRepoCount(entry?.projectCount)}
                  </span>
                </div>
                <p className="directory-bio">
                  {entry?.bio ? entry.bio : "No bio yet."}
                </p>
                <div className="directory-card-actions">
                  <Link className="ghost-button" href={`/${handle}`}>
                    View showcase
                  </Link>
                </div>
              </article>
            );
          })
        )}
      </section>
    </main>
  );
}
