import { useEffect, useMemo, useState } from "react";
import Link from "next/link";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:4011";

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

const buildApiUrl = (path, params = {}) => {
  const url = new URL(`${API_BASE_URL}${path}`);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, value);
    }
  });
  return url.toString();
};

const buildSharePath = (handle) => {
  if (!handle) {
    return "";
  }
  return `${basePath}/${handle}`.replace(/\/+/g, "/");
};

const formatCount = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "0";
  }
  return numeric.toLocaleString();
};

const formatRepoLabel = (project) => {
  const repo = typeof project?.repo === "string" ? project.repo.trim() : "";
  if (!repo) {
    return "";
  }
  if (repo.startsWith("https://github.com/")) {
    return repo.replace("https://github.com/", "");
  }
  return repo;
};

const formatPeriodLabel = (value) => {
  if (!value) {
    return "Current period";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "Current period";
  }
  const formatted = date.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric"
  });
  return `Since ${formatted}`;
};

const formatLimitInput = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "";
  }
  return String(Math.trunc(numeric));
};

export default function AccountPage() {
  const [authUser, setAuthUser] = useState(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [projects, setProjects] = useState([]);
  const [owner, setOwner] = useState(null);
  const [repoUrl, setRepoUrl] = useState("");
  const [adding, setAdding] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [profile, setProfile] = useState(null);
  const [profileLoading, setProfileLoading] = useState(false);
  const [profileSaving, setProfileSaving] = useState(false);
  const [profileError, setProfileError] = useState("");
  const [profileMessage, setProfileMessage] = useState("");
  const [profileForm, setProfileForm] = useState({
    handle: "",
    bio: "",
    isPublic: true
  });
  const [usage, setUsage] = useState(null);
  const [usageLoading, setUsageLoading] = useState(false);
  const [usageError, setUsageError] = useState("");
  const [limitsLoading, setLimitsLoading] = useState(false);
  const [limitsSaving, setLimitsSaving] = useState(false);
  const [limitsError, setLimitsError] = useState("");
  const [limitsMessage, setLimitsMessage] = useState("");
  const [limitsForm, setLimitsForm] = useState({
    tokenLimit: ""
  });

  const loadAuthUser = async () => {
    setAuthLoading(true);
    try {
      const response = await fetch(`${API_BASE_URL}/auth/me`, {
        credentials: "include"
      });
      if (!response.ok) {
        setAuthUser(null);
        return;
      }
      const data = await response.json().catch(() => ({}));
      setAuthUser(data.user || null);
    } catch {
      setAuthUser(null);
    } finally {
      setAuthLoading(false);
    }
  };

  const loadProjects = async () => {
    setError("");
    try {
      const response = await fetch(buildApiUrl("/projects"), {
        credentials: "include"
      });
      if (!response.ok) {
        if (response.status === 401) {
          setAuthUser(null);
          return;
        }
        throw new Error("Failed to load repos.");
      }
      const data = await response.json();
      setProjects(Array.isArray(data.projects) ? data.projects : []);
      setOwner(data.owner || null);
    } catch (err) {
      setError(err.message || "Failed to load repos.");
      setOwner(null);
    }
  };

  const loadUsage = async () => {
    setUsageLoading(true);
    setUsageError("");
    try {
      const response = await fetch(buildApiUrl("/account/usage"), {
        credentials: "include"
      });
      if (!response.ok) {
        if (response.status === 401) {
          setUsage(null);
          return;
        }
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.error || "Failed to load usage.");
      }
      const data = await response.json();
      setUsage(data.summary || null);
    } catch (err) {
      setUsageError(err.message || "Failed to load usage.");
    } finally {
      setUsageLoading(false);
    }
  };

  const loadProfile = async () => {
    setProfileLoading(true);
    setProfileError("");
    try {
      const response = await fetch(buildApiUrl("/account/profile"), {
        credentials: "include"
      });
      if (!response.ok) {
        if (response.status === 401) {
          setProfile(null);
          return;
        }
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.error || "Failed to load profile.");
      }
      const data = await response.json().catch(() => ({}));
      const nextProfile = data.profile || null;
      setProfile(nextProfile);
      if (nextProfile) {
        setProfileForm({
          handle: nextProfile.handle || "",
          bio: nextProfile.bio || "",
          isPublic: Boolean(nextProfile.isPublic)
        });
      }
    } catch (err) {
      setProfileError(err.message || "Failed to load profile.");
    } finally {
      setProfileLoading(false);
    }
  };

  const loadLimits = async () => {
    setLimitsLoading(true);
    setLimitsError("");
    try {
      const response = await fetch(buildApiUrl("/account/limits"), {
        credentials: "include"
      });
      if (!response.ok) {
        if (response.status === 401) {
          setLimits(null);
          return;
        }
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.error || "Failed to load limits.");
      }
      const data = await response.json().catch(() => ({}));
      const nextLimits = data.limits || null;
      if (nextLimits) {
        setLimitsForm({
          tokenLimit: formatLimitInput(nextLimits.tokenLimit)
        });
      }
    } catch (err) {
      setLimitsError(err.message || "Failed to load limits.");
    } finally {
      setLimitsLoading(false);
    }
  };

  useEffect(() => {
    loadAuthUser();
  }, []);

  useEffect(() => {
    if (!authLoading && authUser) {
      loadProjects();
      loadProfile();
      loadUsage();
      loadLimits();
    }
  }, [authLoading, authUser]);

  const handleAuthStart = () => {
    if (typeof window === "undefined") {
      return;
    }
    const returnTo = window.location.href;
    const url = `${API_BASE_URL}/auth/github/start?returnTo=${encodeURIComponent(
      returnTo
    )}`;
    window.location.href = url;
  };

  const handleAuthLogout = async () => {
    try {
      await fetch(`${API_BASE_URL}/auth/logout`, {
        method: "POST",
        credentials: "include"
      });
    } catch {
      // Ignore logout errors; clear local state regardless.
    } finally {
      setAuthUser(null);
      setProjects([]);
      setOwner(null);
      setProfile(null);
      setUsage(null);
      setProfileForm({
        handle: "",
        bio: "",
        isPublic: true
      });
      setLimitsForm({
        tokenLimit: ""
      });
    }
  };

  const handleAddProject = async (event) => {
    event.preventDefault();
    const trimmedRepo = repoUrl.trim();
    if (!trimmedRepo) {
      return;
    }
    setAdding(true);
    setMessage("");
    setError("");
    try {
      const response = await fetch(buildApiUrl("/projects"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        credentials: "include",
        body: JSON.stringify({ repoUrl: trimmedRepo })
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to add repo.");
      }
      if (payload.status === "exists") {
        setMessage("Repo already listed.");
      } else if (payload.ingestJob) {
        setMessage("Repo added. Indexing queued.");
      } else if (payload.ingestError) {
        setMessage(`Repo added, but indexing failed: ${payload.ingestError}`);
      } else {
        setMessage("Repo added.");
      }
      setRepoUrl("");
      await loadProjects();
      await loadUsage();
    } catch (err) {
      setError(err.message || "Failed to add repo.");
    } finally {
      setAdding(false);
    }
  };

  const handleDeleteProject = async (project) => {
    const projectId = Number(project?.id);
    if (!Number.isFinite(projectId)) {
      setError("Repo id missing.");
      return;
    }
    const label = project?.name || project?.repo || "this repo";
    if (!window.confirm(`Remove "${label}" from your showcase?`)) {
      return;
    }
    setError("");
    setMessage("");
    try {
      const response = await fetch(
        buildApiUrl(`/projects/${encodeURIComponent(projectId)}`),
        {
          method: "DELETE",
          credentials: "include"
        }
      );
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to remove repo.");
      }
      setMessage("Repo removed.");
      await loadProjects();
      await loadUsage();
    } catch (err) {
      setError(err.message || "Failed to remove repo.");
    }
  };

  const handleProfileSubmit = async (event) => {
    event.preventDefault();
    setProfileSaving(true);
    setProfileError("");
    setProfileMessage("");
    try {
      const response = await fetch(buildApiUrl("/account/profile"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        credentials: "include",
        body: JSON.stringify({
          handle: profileForm.handle,
          bio: profileForm.bio,
          isPublic: profileForm.isPublic
        })
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to update profile.");
      }
      const nextProfile = payload.profile || null;
      setProfile(nextProfile);
      if (nextProfile) {
        setProfileForm({
          handle: nextProfile.handle || "",
          bio: nextProfile.bio || "",
          isPublic: Boolean(nextProfile.isPublic)
        });
      }
      setProfileMessage("Profile updated.");
      await loadProjects();
    } catch (err) {
      setProfileError(err.message || "Failed to update profile.");
    } finally {
      setProfileSaving(false);
    }
  };

  const handleLimitsSubmit = async (event) => {
    event.preventDefault();
    setLimitsSaving(true);
    setLimitsError("");
    setLimitsMessage("");
    try {
      const response = await fetch(buildApiUrl("/account/limits"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        credentials: "include",
        body: JSON.stringify({
          tokenLimit: limitsForm.tokenLimit
        })
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to update limits.");
      }
      const nextLimits = payload.limits || null;
      if (nextLimits) {
        setLimitsForm({
          tokenLimit: formatLimitInput(nextLimits.tokenLimit)
        });
      }
      setLimitsMessage("Limits updated.");
    } catch (err) {
      setLimitsError(err.message || "Failed to update limits.");
    } finally {
      setLimitsSaving(false);
    }
  };

  const ownerName =
    profile?.name || owner?.name || authUser?.name || authUser?.githubUsername;
  const profileHandle =
    profile?.handle || profile?.githubUsername || owner?.handle || "";
  const sharePath = buildSharePath(
    profileHandle || authUser?.githubUsername || ""
  );
  const shareUrl = useMemo(() => {
    if (!sharePath) {
      return "";
    }
    if (typeof window === "undefined") {
      return sharePath;
    }
    return `${window.location.origin}${sharePath}`;
  }, [sharePath]);
  const tokenLabel = formatCount(usage?.tokenCount || 0);
  const usagePeriodLabel = formatPeriodLabel(usage?.periodStart);

  return (
    <main className="account-page">
      <header className="account-header">
        <div className="account-intro">
          <p className="eyebrow">Account</p>
          <h1>Manage your showcase</h1>
          <p className="muted">
            {ownerName ? `Signed in as ${ownerName}.` : "Sign in to continue."}
          </p>
        </div>
        <div className="account-actions">
          <Link className="ghost-button" href="/">
            Back to showcase
          </Link>
          {authUser ? (
            <button
              type="button"
              className="ghost-button"
              onClick={handleAuthLogout}
            >
              Sign out
            </button>
          ) : null}
        </div>
      </header>

      {authLoading ? (
        <p className="muted">Loading account...</p>
      ) : !authUser ? (
        <div className="account-signin">
          <p className="muted">
            Sign in with GitHub to manage repos, usage, and billing.
          </p>
          <button
            type="button"
            className="primary-button"
            onClick={handleAuthStart}
          >
            Sign in with GitHub
          </button>
        </div>
      ) : (
        <section className="account-grid">
          <div className="panel account-card">
            <div className="account-card-header">
              <h2>Profile</h2>
              {profile ? (
                <span
                  className={`account-visibility${
                    profile.isPublic ? " is-public" : " is-private"
                  }`}
                >
                  {profile.isPublic ? "Public" : "Private"}
                </span>
              ) : null}
            </div>
            <p className="muted">
              Set the URL and bio that appear on your public showcase.
            </p>
            {shareUrl ? (
              <div className="account-share">
                <span className="muted">Share URL</span>
                {profile?.isPublic === false ? (
                  <span className="muted">
                    Private — enable public access to share.
                  </span>
                ) : (
                  <a href={shareUrl}>{shareUrl}</a>
                )}
              </div>
            ) : null}
            {profileLoading ? (
              <p className="muted">Loading profile...</p>
            ) : (
              <form className="form" onSubmit={handleProfileSubmit}>
                <label className="field">
                  <span>Public handle</span>
                  <input
                    type="text"
                    placeholder="andrew"
                    value={profileForm.handle}
                    onChange={(event) =>
                      setProfileForm((current) => ({
                        ...current,
                        handle: event.target.value
                      }))
                    }
                  />
                  <span className="field-hint">
                    Leave blank to use your GitHub username.
                  </span>
                </label>
                <label className="field">
                  <span>Short bio</span>
                  <textarea
                    rows={4}
                    placeholder="A quick note about what you build."
                    value={profileForm.bio}
                    onChange={(event) =>
                      setProfileForm((current) => ({
                        ...current,
                        bio: event.target.value
                      }))
                    }
                  />
                  <span className="field-hint">Max 500 characters.</span>
                </label>
                <label className="checkbox-field">
                  <input
                    type="checkbox"
                    checked={profileForm.isPublic}
                    onChange={(event) =>
                      setProfileForm((current) => ({
                        ...current,
                        isPublic: event.target.checked
                      }))
                    }
                  />
                  <span>Show my showcase publicly</span>
                </label>
                <button
                  type="submit"
                  className="primary-button"
                  disabled={profileSaving}
                >
                  {profileSaving ? "Saving..." : "Save profile"}
                </button>
              </form>
            )}
            {profileMessage ? <p className="status">{profileMessage}</p> : null}
            {profileError ? (
              <p className="status error">{profileError}</p>
            ) : null}
          </div>

          <div className="panel account-card">
            <div className="account-card-header">
              <h2>Repos</h2>
              <span className="muted">{projects.length} total</span>
            </div>
            <form className="form" onSubmit={handleAddProject}>
              <label className="field">
                <span>GitHub repo URL</span>
                <input
                  type="url"
                  placeholder="https://github.com/owner/repo"
                  value={repoUrl}
                  onChange={(event) => setRepoUrl(event.target.value)}
                  required
                />
              </label>
              <button
                type="submit"
                className="primary-button"
                disabled={adding}
              >
                {adding ? "Adding..." : "Add repo"}
              </button>
            </form>
            {message ? <p className="status">{message}</p> : null}
            {error ? <p className="status error">{error}</p> : null}
            <div className="account-project-list">
              {projects.length === 0 ? (
                <p className="muted">No repos listed yet.</p>
              ) : (
                projects.map((project) => (
                  <div className="account-project" key={project.id || project.repo}>
                    <div className="account-project-main">
                      <div className="account-project-title">
                        {project.name || formatRepoLabel(project) || "Untitled repo"}
                      </div>
                      {project.description ? (
                        <p className="account-project-desc">
                          {project.description}
                        </p>
                      ) : null}
                      {project.repo ? (
                        <a
                          className="account-project-link"
                          href={project.repo}
                          target="_blank"
                          rel="noreferrer"
                        >
                          {formatRepoLabel(project) || "View on GitHub"}
                        </a>
                      ) : null}
                    </div>
                    <button
                      type="button"
                      className="ghost-button"
                      onClick={() => handleDeleteProject(project)}
                    >
                      Remove
                    </button>
                  </div>
                ))
              )}
            </div>
          </div>

          <div className="panel account-card">
            <div className="account-card-header">
              <h2>Usage</h2>
              <span className="muted">{usagePeriodLabel}</span>
            </div>
            {usageLoading ? (
              <p className="muted">Loading usage...</p>
            ) : usageError ? (
              <p className="status error">{usageError}</p>
            ) : (
              <div className="account-metrics">
                <div className="account-metric">
                  <span className="account-metric-label">Repos</span>
                  <span className="account-metric-value">
                    {formatCount(usage?.repoCount || projects.length)}
                  </span>
                </div>
                <div className="account-metric">
                  <span className="account-metric-label">Chat sessions</span>
                  <span className="account-metric-value">
                    {formatCount(usage?.sessionCount)}
                  </span>
                </div>
                <div className="account-metric">
                  <span className="account-metric-label">Messages</span>
                  <span className="account-metric-value">
                    {formatCount(usage?.messageCount)}
                  </span>
                </div>
                <div className="account-metric">
                  <span className="account-metric-label">Chat tokens</span>
                  <span className="account-metric-value">
                    {tokenLabel}
                  </span>
                </div>
              </div>
            )}

            <div className="account-limits">
              <h3>Limits</h3>
              {limitsLoading ? (
                <p className="muted">Loading limits...</p>
              ) : (
                <form className="form" onSubmit={handleLimitsSubmit}>
                  <label className="field">
                    <span>Monthly chat tokens limit</span>
                    <input
                      type="number"
                      min="0"
                      placeholder="Leave blank for no limit"
                      value={limitsForm.tokenLimit}
                      onChange={(event) =>
                        setLimitsForm((current) => ({
                          ...current,
                          tokenLimit: event.target.value
                        }))
                      }
                    />
                  </label>
                  <button
                    type="submit"
                    className="primary-button"
                    disabled={limitsSaving}
                  >
                    {limitsSaving ? "Saving..." : "Save limits"}
                  </button>
                </form>
              )}
              {limitsMessage ? <p className="status">{limitsMessage}</p> : null}
              {limitsError ? (
                <p className="status error">{limitsError}</p>
              ) : null}
            </div>
          </div>

          <div className="panel account-card">
            <div className="account-card-header">
              <h2>Billing</h2>
              <span className="muted">Payments</span>
            </div>
            <p className="muted">
              Billing and payment details will live here when you enable
              subscriptions.
            </p>
            <button type="button" className="ghost-button" disabled>
              Connect payment method (soon)
            </button>
          </div>
        </section>
      )}
    </main>
  );
}
