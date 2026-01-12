import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/router";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:4011";
const GITHUB_APP_INSTALL_URL =
  "https://github.com/apps/projects-homepage-with-ai-chat/installations/new";

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

const formatDateLabel = (value) => {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric"
  });
};

const formatCurrency = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "$0.00";
  }
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(numeric);
};

const TOKEN_UNIT = 250000;
const TOKEN_RATE = 1;

const BILLING_PLANS = [
  {
    id: "starter",
    name: "Starter",
    price: "$1.99 / mo",
    repos: "10 repos",
    tokens: "$1 per 250k tokens"
  },
  {
    id: "pro",
    name: "Pro",
    price: "$3.99 / mo",
    repos: "50 repos",
    tokens: "$1 per 250k tokens"
  },
  {
    id: "unlimited",
    name: "Unlimited",
    price: "$9.99 / mo",
    repos: "Unlimited repos",
    tokens: "Includes tokens + $1 per 250k after"
  }
];

export default function AccountPage() {
  const router = useRouter();
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
  const [billing, setBilling] = useState(null);
  const [billingLoading, setBillingLoading] = useState(false);
  const [billingError, setBillingError] = useState("");
  const [billingMessage, setBillingMessage] = useState("");
  const [billingAction, setBillingAction] = useState("");
  const [categoryProject, setCategoryProject] = useState(null);
  const [categoryValue, setCategoryValue] = useState("");
  const [categorySaving, setCategorySaving] = useState(false);
  const [categoryError, setCategoryError] = useState("");
  const [showInstallPrompt, setShowInstallPrompt] = useState(false);

  const categoryOptions = useMemo(() => {
    const entries = new Map();
    projects.forEach((project) => {
      const category =
        typeof project?.category === "string" ? project.category.trim() : "";
      if (!category) {
        return;
      }
      const key = category.toLowerCase();
      if (!entries.has(key)) {
        entries.set(key, category);
      }
    });
    return Array.from(entries.values()).sort((a, b) => a.localeCompare(b));
  }, [projects]);

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

  const loadBilling = async () => {
    setBillingLoading(true);
    setBillingError("");
    try {
      const response = await fetch(buildApiUrl("/account/billing"), {
        credentials: "include"
      });
      if (!response.ok) {
        if (response.status === 401) {
          setBilling(null);
          return;
        }
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.error || "Failed to load billing.");
      }
      const data = await response.json().catch(() => ({}));
      setBilling(data.billing || null);
    } catch (err) {
      setBillingError(err.message || "Failed to load billing.");
    } finally {
      setBillingLoading(false);
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
      loadBilling();
    }
  }, [authLoading, authUser]);

  useEffect(() => {
    if (!router.isReady) {
      return;
    }
    if (router.query.checkout === "success") {
      setShowInstallPrompt(true);
      const nextQuery = { ...router.query };
      delete nextQuery.checkout;
      router.replace(
        { pathname: router.pathname, query: nextQuery },
        undefined,
        { shallow: true }
      );
    }
  }, [router.isReady, router.query.checkout, router.pathname]);

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
      setBilling(null);
      setBillingError("");
      setBillingMessage("");
      setCategoryProject(null);
      setCategoryValue("");
      setCategoryError("");
      setProfileForm({
        handle: "",
        bio: "",
        isPublic: true
      });
    }
  };

  const handleAddProject = async (event) => {
    event.preventDefault();
    const trimmedRepo = repoUrl.trim();
    if (!trimmedRepo) {
      return;
    }
    if (!billingActive) {
      setError("Select a billing plan to add repos.");
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

  const openCategoryModal = (project) => {
    if (!project) {
      return;
    }
    setCategoryProject(project);
    setCategoryValue(project.category || "");
    setCategoryError("");
  };

  const closeCategoryModal = () => {
    setCategoryProject(null);
    setCategoryValue("");
    setCategoryError("");
  };

  const handleCategorySave = async (event) => {
    event.preventDefault();
    const projectId = Number(categoryProject?.id);
    if (!Number.isFinite(projectId)) {
      setCategoryError("Repo id missing.");
      return;
    }
    setCategorySaving(true);
    setCategoryError("");
    try {
      const response = await fetch(
        buildApiUrl(`/projects/${encodeURIComponent(projectId)}/category`),
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          credentials: "include",
          body: JSON.stringify({ category: categoryValue })
        }
      );
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to update category.");
      }
      setMessage("Category updated.");
      await loadProjects();
      closeCategoryModal();
    } catch (err) {
      setCategoryError(err.message || "Failed to update category.");
    } finally {
      setCategorySaving(false);
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

  const handleCheckout = async (planId) => {
    if (!planId) {
      return;
    }
    setBillingAction(planId);
    setBillingError("");
    setBillingMessage("");
    try {
      const response = await fetch(buildApiUrl("/billing/checkout"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        credentials: "include",
        body: JSON.stringify({ plan: planId })
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to start checkout.");
      }
      if (payload.url && typeof window !== "undefined") {
        window.location.href = payload.url;
      } else {
        setBillingMessage("Checkout session created.");
      }
    } catch (err) {
      setBillingError(err.message || "Failed to start checkout.");
    } finally {
      setBillingAction("");
    }
  };

  const handleOpenPortal = async () => {
    setBillingAction("portal");
    setBillingError("");
    setBillingMessage("");
    try {
      const response = await fetch(buildApiUrl("/billing/portal"), {
        method: "POST",
        credentials: "include"
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to open billing portal.");
      }
      if (payload.url && typeof window !== "undefined") {
        window.location.href = payload.url;
      } else {
        setBillingMessage("Billing portal ready.");
      }
    } catch (err) {
      setBillingError(err.message || "Failed to open billing portal.");
    } finally {
      setBillingAction("");
    }
  };

  const closeInstallPrompt = () => {
    setShowInstallPrompt(false);
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
  const billingStatus = billing?.status || "inactive";
  const billingActive =
    billing && ["active", "trialing", "past_due"].includes(billingStatus);
  const billingPlan = billing?.plan || "";
  const billingPlanLabel = billing?.planLabel || billingPlan || "No plan";
  const billingRenewal = formatDateLabel(billing?.currentPeriodEnd);
  const tokensUsed = Number(usage?.tokenCount || 0);
  const repoLimitLabel =
    billing?.repoLimit === null || billing?.repoLimit === undefined
      ? "Unlimited repos"
      : `${billing.repoLimit} repos`;
  const unlimitedTokenCap =
    billing?.unlimitedTokenLimit || billing?.tokenLimit || null;
  const unlimitedTokenLabel = unlimitedTokenCap
    ? `Includes ${formatCount(unlimitedTokenCap)} tokens + $1 per 250k after`
    : "Includes tokens + $1 per 250k after";
  const tokenLimitLabel =
    billingPlan === "unlimited"
      ? unlimitedTokenLabel
      : billing?.tokenLimit === null || billing?.tokenLimit === undefined
      ? billing?.tokenUsage
        ? "Metered tokens"
        : "Unlimited tokens"
      : `${formatCount(billing.tokenLimit)} token cap`;
  const categoryValueTrimmed = categoryValue.trim();
  const categoryValueKey = categoryValueTrimmed.toLowerCase();
  const categoryProjectLabel = categoryProject
    ? categoryProject.name ||
      formatRepoLabel(categoryProject) ||
      "this repo"
    : "";
  const includedTokens =
    billingPlan === "unlimited" && Number.isFinite(Number(unlimitedTokenCap))
      ? Number(unlimitedTokenCap)
      : 0;
  const billableTokens = billing?.tokenUsage
    ? Math.max(0, tokensUsed - includedTokens)
    : 0;
  const usageCost = billing?.tokenUsage
    ? Math.ceil((billableTokens / TOKEN_UNIT) * TOKEN_RATE)
    : 0;
  const usageCostLabel = billing?.tokenUsage
    ? formatCurrency(usageCost)
    : "";
  const usageDetailLabel = billing?.tokenUsage
    ? `${formatCount(billableTokens)} billable tokens`
    : "";

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
                  disabled={!billingActive}
                />
              </label>
              <button
                type="submit"
                className="primary-button"
                disabled={adding || !billingActive}
              >
                {adding ? "Adding..." : "Add repo"}
              </button>
            </form>
            {!billingActive ? (
              <p className="muted">
                Choose a plan to start adding repos to your showcase.
              </p>
            ) : null}
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
                      {project.category ? (
                        <div className="project-meta">
                          Category: {project.category}
                        </div>
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
                    <div className="account-project-actions">
                      <button
                        type="button"
                        className="ghost-button"
                        onClick={() => openCategoryModal(project)}
                      >
                        Category
                      </button>
                      <button
                        type="button"
                        className="ghost-button"
                        onClick={() => handleDeleteProject(project)}
                      >
                        Remove
                      </button>
                    </div>
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
              <h3>Plan limits</h3>
              {billingLoading ? (
                <p className="muted">Loading plan limits...</p>
              ) : (
                <div className="account-limit-list">
                  <div className="account-limit">
                    <span className="account-limit-label">Repo access</span>
                    <span className="account-limit-value">{repoLimitLabel}</span>
                  </div>
                  <div className="account-limit">
                    <span className="account-limit-label">Token usage</span>
                    <span className="account-limit-value">{tokenLimitLabel}</span>
                  </div>
                  {billing?.tokenUsage ? (
                    <p className="muted">
                      Tokens are metered at $1 per 250k after any included
                      amount.
                    </p>
                  ) : null}
                </div>
              )}
            </div>
          </div>

          <div className="panel account-card">
            <div className="account-card-header">
              <h2>Billing</h2>
              <span className="muted">
                {billingActive ? "Active" : "Not active"}
              </span>
            </div>
            {billingLoading ? (
              <p className="muted">Loading billing...</p>
            ) : (
              <>
                <div className="billing-summary">
                  <div>
                    <span className="muted">Plan</span>
                    <p className="billing-value">{billingPlanLabel}</p>
                  </div>
                  <div>
                    <span className="muted">Status</span>
                    <p className="billing-value">{billingStatus}</p>
                  </div>
                  {billingRenewal ? (
                    <div>
                      <span className="muted">Renews</span>
                      <p className="billing-value">{billingRenewal}</p>
                    </div>
                  ) : null}
                </div>
                <div className="billing-actions">
                  {billing?.hasCustomer ? (
                    <button
                      type="button"
                      className="ghost-button"
                      onClick={handleOpenPortal}
                      disabled={billingAction === "portal"}
                    >
                      {billingAction === "portal"
                        ? "Opening portal..."
                        : "Manage / cancel plan"}
                    </button>
                  ) : null}
                </div>
                {billing?.tokenUsage ? (
                  <div className="billing-usage">
                    <span className="muted">Estimated usage charges</span>
                    <p className="billing-value">
                      {usageCostLabel}
                      {usageDetailLabel ? ` · ${usageDetailLabel}` : ""}
                    </p>
                    {billingPlan === "unlimited" && includedTokens ? (
                      <p className="muted">
                        Includes {formatCount(includedTokens)} tokens before
                        usage charges apply.
                      </p>
                    ) : null}
                  </div>
                ) : null}
                <div className="billing-plans">
                  {BILLING_PLANS.map((plan) => {
                    const isActivePlan =
                      billingActive && plan.id === billingPlan;
                    const isPreviousPlan =
                      !billingActive && plan.id === billingPlan;
                    const tokenLabel =
                      plan.id === "unlimited"
                        ? unlimitedTokenLabel
                        : plan.tokens;
                    return (
                      <div
                        key={plan.id}
                        className={`billing-plan${
                          isActivePlan ? " is-active" : ""
                        }`}
                      >
                        <div className="billing-plan-header">
                          <h3>{plan.name}</h3>
                          {isActivePlan ? (
                            <span className="muted">Current</span>
                          ) : isPreviousPlan ? (
                            <span className="muted">Last</span>
                          ) : null}
                        </div>
                        <p className="billing-plan-price">{plan.price}</p>
                        <p className="billing-plan-meta">{plan.repos}</p>
                        <p className="billing-plan-meta">{tokenLabel}</p>
                        <button
                          type="button"
                          className="primary-button"
                          onClick={() => handleCheckout(plan.id)}
                          disabled={isActivePlan || billingAction === plan.id}
                        >
                          {isActivePlan
                            ? "Selected"
                            : billingAction === plan.id
                            ? "Starting..."
                            : isPreviousPlan
                            ? "Restart plan"
                            : "Choose plan"}
                        </button>
                      </div>
                    );
                  })}
                </div>
              </>
            )}
            {billingMessage ? <p className="status">{billingMessage}</p> : null}
            {billingError ? (
              <p className="status error">{billingError}</p>
            ) : null}
          </div>
        </section>
      )}

      {categoryProject ? (
        <div
          className="category-overlay"
          role="dialog"
          aria-modal="true"
          aria-labelledby="category-title"
        >
          <div className="category-card">
            <div className="category-header">
              <h2 id="category-title">Set category</h2>
              <button
                type="button"
                className="ghost-button"
                onClick={closeCategoryModal}
              >
                Close
              </button>
            </div>
            <div className="category-body">
              <p className="muted">
                Choose a category for {categoryProjectLabel}.
              </p>
              <div className="category-options" role="listbox">
                <button
                  type="button"
                  className={`category-option${
                    categoryValueTrimmed ? "" : " is-selected"
                  }`}
                  onClick={() => setCategoryValue("")}
                >
                  No category
                </button>
                {categoryOptions.map((category) => {
                  const isSelected =
                    categoryValueTrimmed &&
                    category.toLowerCase() === categoryValueKey;
                  return (
                    <button
                      key={category}
                      type="button"
                      className={`category-option${
                        isSelected ? " is-selected" : ""
                      }`}
                      onClick={() => setCategoryValue(category)}
                    >
                      {category}
                    </button>
                  );
                })}
                {categoryOptions.length === 0 ? (
                  <span className="muted">No categories yet.</span>
                ) : null}
              </div>
              <form className="form" onSubmit={handleCategorySave}>
                <label className="field">
                  <span>Category name</span>
                  <input
                    type="text"
                    placeholder="e.g., Infrastructure"
                    value={categoryValue}
                    onChange={(event) => setCategoryValue(event.target.value)}
                  />
                </label>
                {categoryError ? (
                  <p className="status error">{categoryError}</p>
                ) : null}
                <div className="category-actions">
                  <button
                    type="button"
                    className="ghost-button"
                    onClick={closeCategoryModal}
                    disabled={categorySaving}
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    className="primary-button"
                    disabled={categorySaving}
                  >
                    {categorySaving ? "Saving..." : "Save category"}
                  </button>
                </div>
              </form>
            </div>
          </div>
          <button
            type="button"
            className="category-backdrop"
            aria-label="Close category"
            onClick={closeCategoryModal}
          />
        </div>
      ) : null}

      {showInstallPrompt ? (
        <div
          className="install-overlay"
          role="dialog"
          aria-modal="true"
          aria-labelledby="install-title"
        >
          <div className="install-card">
            <div className="install-header">
              <h2 id="install-title">Install the GitHub App</h2>
              <button
                type="button"
                className="ghost-button"
                onClick={closeInstallPrompt}
              >
                Close
              </button>
            </div>
            <div className="install-body">
              <p className="muted">
                Next step: install the projects-homepage-with-ai-chat GitHub
                App so we can access your repos and keep your showcase up to
                date.
              </p>
              <div className="install-actions">
                <a
                  className="primary-button"
                  href={GITHUB_APP_INSTALL_URL}
                  target="_blank"
                  rel="noreferrer"
                >
                  Install GitHub App
                </a>
                <button
                  type="button"
                  className="ghost-button"
                  onClick={closeInstallPrompt}
                >
                  Later
                </button>
              </div>
            </div>
          </div>
          <button
            type="button"
            className="install-backdrop"
            aria-label="Close install prompt"
            onClick={closeInstallPrompt}
          />
        </div>
      ) : null}
    </main>
  );
}
