import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:3001";
const CHAT_HISTORY_LIMIT = 8;
const VISITOR_ID_KEY = "gh-projects-visitor-id";
const ADMIN_KEY_STORAGE = "gh-projects-admin-key";
const ABOUT_SEEN_KEY = "gh-projects-about-seen";

const extractRepoFromUrl = (value) => {
  if (typeof value !== "string") {
    return null;
  }

  const match = value.match(
    /https?:\/\/(?:www\.)?github\.com\/([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+)(?:[/?#]|$)/i
  );
  if (!match) {
    return null;
  }

  return `${match[1]}/${match[2]}`;
};

const normalizeLoose = (value) =>
  value.toLowerCase().replace(/[^a-z0-9]+/g, "");
const tokenize = (value) =>
  value.toLowerCase().split(/[^a-z0-9]+/g).filter(Boolean);

const repoIntentTokens = new Set([
  "project",
  "repo",
  "repository",
  "codebase",
  "app",
  "service",
  "library"
]);

const hasProjectIntent = (question) => {
  if (typeof question !== "string") {
    return false;
  }
  const normalized = question.trim().toLowerCase();
  if (!normalized) {
    return false;
  }
  if (
    normalized.startsWith("what is ") ||
    normalized.startsWith("tell me about ") ||
    normalized.startsWith("describe ") ||
    normalized.startsWith("explain ") ||
    normalized.startsWith("summarize ") ||
    normalized.startsWith("overview of ")
  ) {
    return true;
  }
  const tokens = tokenize(question);
  return tokens.some((token) => repoIntentTokens.has(token));
};

const hasTokenSequence = (tokens, phraseTokens, minTokens) => {
  if (!Array.isArray(tokens) || !Array.isArray(phraseTokens)) {
    return false;
  }
  if (tokens.length === 0 || phraseTokens.length === 0) {
    return false;
  }
  const minLen = Math.min(
    phraseTokens.length,
    Math.max(minTokens || phraseTokens.length, 1)
  );
  const haystack = ` ${tokens.join(" ")} `;
  for (let len = phraseTokens.length; len >= minLen; len -= 1) {
    for (let start = 0; start <= phraseTokens.length - len; start += 1) {
      const needle = ` ${phraseTokens.slice(start, start + len).join(" ")} `;
      if (haystack.includes(needle)) {
        return true;
      }
    }
  }
  return false;
};

const isExplicitTokenMatch = (questionTokens, phraseTokens, hasIntent) => {
  if (!Array.isArray(phraseTokens) || phraseTokens.length === 0) {
    return false;
  }
  const minTokens = phraseTokens.length >= 2 ? 2 : 1;
  if (!hasTokenSequence(questionTokens, phraseTokens, minTokens)) {
    return false;
  }
  if (phraseTokens.length === 1) {
    const token = phraseTokens[0];
    if (token.length < 5 && !hasIntent) {
      return false;
    }
  }
  return true;
};

const getProjectRepo = (project) => {
  if (!project || typeof project !== "object") {
    return null;
  }

  const repoValue =
    typeof project.repo === "string" ? project.repo.trim() : "";
  if (repoValue) {
    const repoFromUrl = extractRepoFromUrl(repoValue);
    if (repoFromUrl) {
      return repoFromUrl;
    }
    if (repoValue.includes("/") && !repoValue.includes("http")) {
      return repoValue;
    }
  }

  const nameValue =
    typeof project.name === "string" ? project.name.trim() : "";
  if (nameValue && nameValue.includes("/") && !nameValue.includes(" ")) {
    return nameValue;
  }

  return null;
};

const normalizeRepoId = (value) => {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const repoFromUrl = extractRepoFromUrl(trimmed);
  if (repoFromUrl) {
    return repoFromUrl;
  }
  if (trimmed.includes("/") && !trimmed.includes("http")) {
    return trimmed;
  }
  return null;
};

const normalizeCategory = (value) => {
  if (typeof value !== "string") {
    return "";
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : "";
};

const getProjectCategory = (project) => {
  if (!project || typeof project !== "object") {
    return "";
  }

  const category = normalizeCategory(project.category);
  if (category) {
    return category;
  }

  if (Array.isArray(project.categories)) {
    for (const value of project.categories) {
      const fallback = normalizeCategory(value);
      if (fallback) {
        return fallback;
      }
    }
  }

  return "";
};

const groupProjectsByCategory = (projects) => {
  const groups = new Map();
  const uncategorized = [];

  for (const project of projects || []) {
    const category = getProjectCategory(project);
    if (!category) {
      uncategorized.push(project);
      continue;
    }
    const bucket = groups.get(category);
    if (bucket) {
      bucket.push(project);
    } else {
      groups.set(category, [project]);
    }
  }

  if (uncategorized.length > 0) {
    groups.set("Uncategorized", uncategorized);
  }

  return Array.from(groups.entries());
};

const sendTelemetry = (payload) => {
  if (!payload || !payload.visitorId) {
    return;
  }
  const url = `${API_BASE_URL}/telemetry`;
  const body = JSON.stringify(payload);
  if (typeof navigator !== "undefined" && navigator.sendBeacon) {
    const blob = new Blob([body], { type: "application/json" });
    navigator.sendBeacon(url, blob);
    return;
  }
  fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
    keepalive: true
  }).catch(() => {});
};

const resolveRepoFromQuestion = (question, projects) => {
  if (typeof question !== "string") {
    return null;
  }

  const urlRepo = extractRepoFromUrl(question);
  if (urlRepo) {
    return { repo: urlRepo, explicit: true };
  }

  const normalizedQuestion = question.toLowerCase();
  const looseQuestion = normalizeLoose(question);
  const questionTokens = tokenize(question);
  const questionTokenSet = new Set(questionTokens);
  const questionHasIntent = hasProjectIntent(question);
  const candidates = [];

  for (const project of projects || []) {
    const repoId = getProjectRepo(project);
    if (!repoId) {
      continue;
    }

    const repoLower = repoId.toLowerCase();
    if (normalizedQuestion.includes(repoLower)) {
      candidates.push({ repo: repoId, score: 3, explicit: true });
      continue;
    }

    const nameValue =
      typeof project.name === "string" ? project.name.trim() : "";
    const nameLoose = normalizeLoose(nameValue);
    const nameTokens = tokenize(nameValue);
    const explicitNameMatch = isExplicitTokenMatch(
      questionTokens,
      nameTokens,
      questionHasIntent
    );
    if (nameLoose && looseQuestion.includes(nameLoose)) {
      candidates.push({ repo: repoId, score: 2, explicit: explicitNameMatch });
      continue;
    }

    const repoName = repoId.split("/")[1] || "";
    const repoLoose = normalizeLoose(repoName);
    const repoTokens = tokenize(repoName);
    const explicitRepoMatch = isExplicitTokenMatch(
      questionTokens,
      repoTokens,
      questionHasIntent
    );
    if (repoLoose && looseQuestion.includes(repoLoose)) {
      candidates.push({ repo: repoId, score: 1, explicit: explicitRepoMatch });
      continue;
    }

    const tokenMatches = [
      ...new Set([...tokenize(nameValue), ...tokenize(repoId)])
    ].filter((token) => token.length >= 4 && questionTokenSet.has(token));
    if (tokenMatches.length > 0) {
      candidates.push({ repo: repoId, score: 0, explicit: false });
    }
  }

  if (candidates.length === 0) {
    return null;
  }

  candidates.sort((a, b) => b.score - a.score);
  const topScore = candidates[0].score;
  const topRepos = [
    ...new Map(
      candidates
        .filter((item) => item.score === topScore)
        .map((item) => [item.repo, item])
    ).values()
  ];

  return topRepos.length === 1 ? topRepos[0] : null;
};

const buildHistoryPayload = (messages) => {
  if (!Array.isArray(messages)) {
    return [];
  }

  const trimmed = messages
    .filter((message) => message && typeof message.content === "string")
    .map((message) => ({
      role: message.role,
      content: message.content.trim(),
      citations:
        message.role === "assistant" && Array.isArray(message.citations)
          ? message.citations
          : undefined
    }))
    .filter(
      (message) =>
        (message.role === "user" || message.role === "assistant") &&
        message.content
    );

  if (trimmed.length <= CHAT_HISTORY_LIMIT) {
    return trimmed;
  }

  return trimmed.slice(-CHAT_HISTORY_LIMIT);
};

const inferRepoFromCitations = (citations) => {
  if (!Array.isArray(citations) || citations.length === 0) {
    return null;
  }

  const counts = new Map();
  for (const citation of citations) {
    const repo =
      citation && typeof citation.repo === "string" ? citation.repo.trim() : "";
    if (!repo) {
      continue;
    }
    counts.set(repo, (counts.get(repo) || 0) + 1);
  }

  if (counts.size === 0) {
    return null;
  }

  return [...counts.entries()].sort((a, b) => b[1] - a[1])[0][0];
};

const formatSessionLabel = (session) => {
  const raw =
    (session && typeof session.lastMessage === "string"
      ? session.lastMessage
      : "") || "New chat";
  const trimmed = raw.replace(/\s+/g, " ").trim();
  if (trimmed.length <= 60) {
    return trimmed;
  }
  return `${trimmed.slice(0, 57)}...`;
};

const formatSessionTime = (value) => {
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
  });
};

const formatTimestamp = (value) => {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
};

const formatJobStatus = (status) => {
  if (!status) {
    return "unknown";
  }
  return String(status).replace(/_/g, " ");
};

const formatJobProgress = (job) => {
  if (!job || typeof job !== "object") {
    return "";
  }
  const total = Number.isFinite(job.totalFiles) ? job.totalFiles : 0;
  const processed = Number.isFinite(job.filesProcessed)
    ? job.filesProcessed
    : 0;
  const chunks = Number.isFinite(job.chunksStored) ? job.chunksStored : 0;
  if (total > 0) {
    return `${processed}/${total} files · ${chunks} chunks`;
  }
  if (processed > 0 || chunks > 0) {
    return `${processed} files · ${chunks} chunks`;
  }
  return "";
};

const buildLastIndexedMap = (jobs) => {
  const map = new Map();
  for (const job of jobs || []) {
    if (!job || job.status !== "completed") {
      continue;
    }
    const repoId = normalizeRepoId(job.projectRepo);
    if (!repoId) {
      continue;
    }
    const finishedAt = job.finishedAt || job.updatedAt || job.createdAt;
    const timestamp = new Date(finishedAt || "").getTime();
    if (!Number.isFinite(timestamp)) {
      continue;
    }
    const existing = map.get(repoId);
    if (!existing || timestamp > existing.timestamp) {
      map.set(repoId, { timestamp, value: finishedAt });
    }
  }
  return map;
};

export default function Home() {
  const [projects, setProjects] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [repoUrl, setRepoUrl] = useState("");
  const [adding, setAdding] = useState(false);
  const [message, setMessage] = useState("");
  const [expandedProjectId, setExpandedProjectId] = useState(null);
  const [chatInput, setChatInput] = useState("");
  const [chatMessages, setChatMessages] = useState([]);
  const [chatError, setChatError] = useState("");
  const [isStreaming, setIsStreaming] = useState(false);
  const [activeRepo, setActiveRepo] = useState(null);
  const [visitorId, setVisitorId] = useState("");
  const [chatSessions, setChatSessions] = useState([]);
  const [sessionsLoading, setSessionsLoading] = useState(false);
  const [sessionsError, setSessionsError] = useState("");
  const [activeSessionId, setActiveSessionId] = useState(null);
  const [messagesLoading, setMessagesLoading] = useState(false);
  const [isAboutOpen, setIsAboutOpen] = useState(false);
  const [adminKey, setAdminKey] = useState("");
  const [ingestJobs, setIngestJobs] = useState([]);
  const [ingestLoading, setIngestLoading] = useState(false);
  const [ingestError, setIngestError] = useState("");
  const [contextMenu, setContextMenu] = useState(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const abortRef = useRef(null);
  const chatStreamRef = useRef(null);
  const latestMessageRef = useRef(null);
  const contextMenuRef = useRef(null);
  const autoScrollRef = useRef(true);
  const isAutoScrollingRef = useRef(false);
  const telemetryStartRef = useRef(null);
  const telemetryPageViewRef = useRef(false);
  const lastIndexedByRepo = useMemo(
    () => buildLastIndexedMap(ingestJobs),
    [ingestJobs]
  );
  const projectGroups = useMemo(
    () => groupProjectsByCategory(projects),
    [projects]
  );

  const loadProjects = async () => {
    setLoading(true);
    setError("");
    try {
      const response = await fetch(`${API_BASE_URL}/projects`);
      if (!response.ok) {
        throw new Error("Failed to load showcase entries.");
      }
      const data = await response.json();
      setProjects(Array.isArray(data.projects) ? data.projects : []);
    } catch (err) {
      setError(err.message || "Failed to load showcase entries.");
    } finally {
      setLoading(false);
    }
  };

  const loadSessions = async () => {
    if (!visitorId) {
      return;
    }
    setSessionsLoading(true);
    setSessionsError("");
    try {
      const response = await fetch(
        `${API_BASE_URL}/chat/sessions?visitorId=${encodeURIComponent(
          visitorId
        )}`
      );
      if (!response.ok) {
        throw new Error("Failed to load sessions");
      }
      const data = await response.json();
      setChatSessions(Array.isArray(data.sessions) ? data.sessions : []);
    } catch (err) {
      setSessionsError(err.message || "Failed to load sessions");
    } finally {
      setSessionsLoading(false);
    }
  };

  const loadIngestJobs = async () => {
    if (!adminKey) {
      return;
    }
    setIngestLoading(true);
    setIngestError("");
    try {
      const response = await fetch(`${API_BASE_URL}/admin/jobs?limit=25`, {
        headers: { "x-admin-key": adminKey }
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.error || "Failed to load ingest jobs");
      }
      const data = await response.json();
      setIngestJobs(Array.isArray(data.jobs) ? data.jobs : []);
    } catch (err) {
      setIngestError(err.message || "Failed to load ingest jobs");
    } finally {
      setIngestLoading(false);
    }
  };

  useEffect(() => {
    loadProjects();
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const stored = window.localStorage.getItem(VISITOR_ID_KEY);
    const id = stored || crypto.randomUUID();
    if (!stored) {
      window.localStorage.setItem(VISITOR_ID_KEY, id);
    }
    setVisitorId(id);
  }, []);

  useEffect(() => {
    if (!visitorId || typeof window === "undefined") {
      return;
    }
    const path = window.location.pathname;
    if (!telemetryPageViewRef.current) {
      sendTelemetry({
        visitorId,
        eventType: "page_view",
        metadata: { path }
      });
      telemetryPageViewRef.current = true;
    }

    const startTimer = () => {
      if (!telemetryStartRef.current) {
        telemetryStartRef.current = Date.now();
      }
    };

    const recordDuration = () => {
      const start = telemetryStartRef.current;
      if (!start) {
        return;
      }
      telemetryStartRef.current = null;
      const duration = Date.now() - start;
      if (duration < 1000) {
        return;
      }
      sendTelemetry({
        visitorId,
        eventType: "time_on_page",
        value: Math.round(duration),
        metadata: { path }
      });
    };

    startTimer();

    const handleVisibility = () => {
      if (document.visibilityState === "hidden") {
        recordDuration();
      } else {
        startTimer();
      }
    };

    const handlePageHide = () => {
      recordDuration();
    };

    document.addEventListener("visibilitychange", handleVisibility);
    window.addEventListener("pagehide", handlePageHide);
    return () => {
      recordDuration();
      document.removeEventListener("visibilitychange", handleVisibility);
      window.removeEventListener("pagehide", handlePageHide);
    };
  }, [visitorId]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const stored = window.localStorage.getItem(ADMIN_KEY_STORAGE);
    if (stored) {
      setAdminKey(stored);
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const seen = window.localStorage.getItem(ABOUT_SEEN_KEY);
    if (!seen) {
      setIsAboutOpen(true);
      window.localStorage.setItem(ABOUT_SEEN_KEY, "true");
    }
  }, []);

  useEffect(() => {
    autoScrollRef.current = autoScroll;
  }, [autoScroll]);

  useEffect(() => {
    if (!adminKey) {
      setIngestJobs([]);
      setIngestError("");
      return;
    }
    loadIngestJobs();
    const intervalId = window.setInterval(() => {
      loadIngestJobs();
    }, 20000);
    return () => window.clearInterval(intervalId);
  }, [adminKey]);

  useEffect(() => {
    if (visitorId) {
      loadSessions();
    }
  }, [visitorId]);

  useEffect(() => {
    const container = chatStreamRef.current;
    if (!container) {
      return;
    }
    const handleScroll = () => {
      if (isAutoScrollingRef.current) {
        return;
      }
      const target = latestMessageRef.current;
      if (!target) {
        return;
      }
      const containerRect = container.getBoundingClientRect();
      const targetRect = target.getBoundingClientRect();
      const offset = targetRect.top - containerRect.top;
      const nearTarget = Math.abs(offset) < 16;
      if (autoScrollRef.current !== nearTarget) {
        setAutoScroll(nearTarget);
      }
    };
    container.addEventListener("scroll", handleScroll, { passive: true });
    return () => {
      container.removeEventListener("scroll", handleScroll);
    };
  }, []);

  useEffect(() => {
    const container = chatStreamRef.current;
    const target = latestMessageRef.current;
    if (!container || !target || !autoScroll) {
      return;
    }

    isAutoScrollingRef.current = true;
    const containerRect = container.getBoundingClientRect();
    const targetRect = target.getBoundingClientRect();
    const offset = targetRect.top - containerRect.top;
    if (Math.abs(offset) > 1) {
      container.scrollTop += offset;
    }
    window.requestAnimationFrame(() => {
      isAutoScrollingRef.current = false;
    });
  }, [chatMessages, autoScroll]);

  useEffect(() => {
    if (!contextMenu) {
      return;
    }
    const handleDismiss = (event) => {
      if (
        contextMenuRef.current &&
        contextMenuRef.current.contains(event.target)
      ) {
        return;
      }
      setContextMenu(null);
    };
    const handleKeyDown = (event) => {
      if (event.key === "Escape") {
        setContextMenu(null);
      }
    };
    window.addEventListener("click", handleDismiss);
    window.addEventListener("contextmenu", handleDismiss);
    window.addEventListener("resize", handleDismiss);
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("click", handleDismiss);
      window.removeEventListener("contextmenu", handleDismiss);
      window.removeEventListener("resize", handleDismiss);
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [contextMenu]);

  const handleSubmit = async (event) => {
    event.preventDefault();
    if (!adminKey) {
      setMessage("Admin key required to add to the showcase.");
      return;
    }
    setAdding(true);
    setMessage("");
    try {
      const response = await fetch(`${API_BASE_URL}/projects`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-admin-key": adminKey
        },
        body: JSON.stringify({ repoUrl })
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to add repo to showcase.");
      }
      if (payload.status === "exists") {
        setMessage("Repo already listed.");
      } else if (payload.ingestJob) {
        setMessage("Repo added. Indexing queued.");
      } else if (payload.ingestError) {
        setMessage(
          `Repo added, but indexing failed: ${payload.ingestError}`
        );
      } else {
        setMessage("Repo added.");
      }
      setRepoUrl("");
      await loadProjects();
    } catch (err) {
      setMessage(err.message || "Failed to add repo to showcase.");
    } finally {
      setAdding(false);
    }
  };

  const toggleProject = (projectId) => {
    setExpandedProjectId((current) =>
      current === projectId ? null : projectId
    );
  };

  const stopStreaming = () => {
    if (abortRef.current) {
      abortRef.current.abort();
    }
  };

  const openAbout = () => {
    setIsAboutOpen(true);
  };

  const closeAbout = () => {
    setIsAboutOpen(false);
  };

  const promptAdminKey = () => {
    if (typeof window === "undefined") {
      return;
    }
    const next = window.prompt(
      adminKey
        ? "Update admin key (leave blank to clear)"
        : "Enter admin key"
    );
    if (next === null) {
      return;
    }
    const trimmed = next.trim();
    if (!trimmed) {
      window.localStorage.removeItem(ADMIN_KEY_STORAGE);
      setAdminKey("");
      return;
    }
    window.localStorage.setItem(ADMIN_KEY_STORAGE, trimmed);
    setAdminKey(trimmed);
  };

  const openContextMenu = (event, menu) => {
    if (!adminKey) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    const menuWidth = 200;
    const menuHeight =
      menu.type === "project" || menu.type === "job" ? 120 : 72;
    const x = Math.min(
      event.clientX,
      window.innerWidth - menuWidth - 12
    );
    const y = Math.min(
      event.clientY,
      window.innerHeight - menuHeight - 12
    );
    setContextMenu({ ...menu, x, y });
  };

  const handleDeleteProject = async (project) => {
    if (!adminKey) {
      setMessage("Admin key required to remove repos.");
      return;
    }
    const projectId =
      typeof project?.id === "string" ? project.id.trim() : "";
    if (!projectId) {
      setMessage("Repo id missing.");
      return;
    }
    const label = project?.name || project?.repo || projectId;
    if (!window.confirm(`Remove "${label}" from the showcase?`)) {
      return;
    }
    setMessage("");
    setContextMenu(null);
    try {
      const response = await fetch(
        `${API_BASE_URL}/projects/${encodeURIComponent(projectId)}`,
        {
          method: "DELETE",
          headers: {
            "Content-Type": "application/json",
            "x-admin-key": adminKey
          },
          body: project?.repo ? JSON.stringify({ repo: project.repo }) : undefined
        }
      );
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to remove repo.");
      }
      setMessage("Repo removed from showcase.");
      await loadProjects();
    } catch (err) {
      setMessage(err.message || "Failed to remove repo.");
    }
  };

  const handleReindexProject = async (project) => {
    if (!adminKey) {
      setMessage("Admin key required to reindex repos.");
      return;
    }
    const projectId =
      typeof project?.id === "string" ? project.id.trim() : "";
    if (!projectId) {
      setMessage("Repo id missing.");
      return;
    }
    const label = project?.name || project?.repo || projectId;
    if (!window.confirm(`Reindex repo "${label}" now?`)) {
      return;
    }
    setMessage("");
    setContextMenu(null);
    try {
      const response = await fetch(
        `${API_BASE_URL}/admin/projects/${encodeURIComponent(
          projectId
        )}/reindex`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-admin-key": adminKey
          },
          body: project?.repo ? JSON.stringify({ repo: project.repo }) : undefined
        }
      );
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to reindex repo.");
      }
      setMessage("Reindex queued.");
      loadIngestJobs();
    } catch (err) {
      setMessage(err.message || "Failed to reindex repo.");
    }
  };

  const handleDeleteSession = async (session) => {
    if (!adminKey) {
      setChatError("Admin key required to delete chats.");
      return;
    }
    const sessionId = session?.id;
    if (!sessionId) {
      setChatError("Chat session id missing.");
      return;
    }
    if (!window.confirm("Delete this chat session?")) {
      return;
    }
    setChatError("");
    setContextMenu(null);
    try {
      const response = await fetch(
        `${API_BASE_URL}/chat/sessions/${sessionId}`,
        {
          method: "DELETE",
          headers: {
            "x-admin-key": adminKey
          }
        }
      );
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to delete chat session");
      }
      setChatSessions((prev) => prev.filter((item) => item.id !== sessionId));
      if (activeSessionId === sessionId) {
        startNewChat();
      }
    } catch (err) {
      setChatError(err.message || "Failed to delete chat session");
    }
  };

  const handleRetryJob = async (job) => {
    if (!adminKey) {
      setIngestError("Admin key required to retry jobs.");
      return;
    }
    const jobId = job?.id;
    if (!jobId) {
      setIngestError("Job id missing.");
      return;
    }
    if (!window.confirm("Retry this ingest job?")) {
      return;
    }
    setIngestError("");
    setContextMenu(null);
    try {
      const response = await fetch(
        `${API_BASE_URL}/admin/jobs/${jobId}/retry`,
        {
          method: "POST",
          headers: {
            "x-admin-key": adminKey
          }
        }
      );
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to retry job");
      }
      loadIngestJobs();
    } catch (err) {
      setIngestError(err.message || "Failed to retry job");
    }
  };

  const handleCancelJob = async (job) => {
    if (!adminKey) {
      setIngestError("Admin key required to cancel jobs.");
      return;
    }
    const jobId = job?.id;
    if (!jobId) {
      setIngestError("Job id missing.");
      return;
    }
    if (!window.confirm("Cancel this ingest job?")) {
      return;
    }
    setIngestError("");
    setContextMenu(null);
    try {
      const response = await fetch(
        `${API_BASE_URL}/admin/jobs/${jobId}/cancel`,
        {
          method: "POST",
          headers: {
            "x-admin-key": adminKey
          }
        }
      );
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to cancel job");
      }
      loadIngestJobs();
    } catch (err) {
      setIngestError(err.message || "Failed to cancel job");
    }
  };

  const loadSessionMessages = async (sessionId) => {
    if (!visitorId || !sessionId) {
      return;
    }
    setMessagesLoading(true);
    setChatError("");
    try {
      const response = await fetch(
        `${API_BASE_URL}/chat/sessions/${sessionId}/messages?visitorId=${encodeURIComponent(
          visitorId
        )}&limit=200`
      );
      if (!response.ok) {
        throw new Error("Failed to load messages");
      }
      const data = await response.json();
      const messages = Array.isArray(data.messages) ? data.messages : [];
      const mapped = messages.map((msg) => ({
        id: `session-${msg.id}`,
        role: msg.role,
        content: msg.content,
        citations: Array.isArray(msg.citations) ? msg.citations : []
      }));
      setChatMessages(mapped);
      const lastAssistant = [...mapped]
        .reverse()
        .find(
          (msg) =>
            msg.role === "assistant" &&
            Array.isArray(msg.citations) &&
            msg.citations.length > 0
        );
      if (lastAssistant) {
        setActiveRepo(inferRepoFromCitations(lastAssistant.citations));
      } else {
        setActiveRepo(null);
      }
    } catch (err) {
      setChatError(err.message || "Failed to load messages");
    } finally {
      setMessagesLoading(false);
    }
  };

  const startNewChat = () => {
    stopStreaming();
    setActiveSessionId(null);
    setChatMessages([]);
    setChatError("");
    setActiveRepo(null);
  };

  const handleSelectSession = async (sessionId) => {
    if (!sessionId || sessionId === activeSessionId) {
      return;
    }
    stopStreaming();
    setActiveSessionId(sessionId);
    setChatMessages([]);
    await loadSessionMessages(sessionId);
  };

  const submitChat = async () => {
    if (!chatInput.trim() || isStreaming) {
      return;
    }

    const question = chatInput.trim();
    const history = buildHistoryPayload(chatMessages);
    const repoMatch = resolveRepoFromQuestion(question, projects);
    const explicitRepo = repoMatch?.explicit ? repoMatch.repo : null;
    const repo = explicitRepo || activeRepo || repoMatch?.repo || null;
    const shouldResetHistory = explicitRepo && explicitRepo !== activeRepo;
    const historyPayload = shouldResetHistory ? [] : history;
    setChatInput("");
    setChatError("");
    setAutoScroll(true);
    if (explicitRepo) {
      setActiveRepo(explicitRepo);
    } else if (!activeRepo && repoMatch?.repo) {
      setActiveRepo(repoMatch.repo);
    }

    const userMessage = {
      id: crypto.randomUUID(),
      role: "user",
      content: question
    };
    const assistantId = crypto.randomUUID();
    const assistantMessage = {
      id: assistantId,
      role: "assistant",
      content: "",
      citations: []
    };

    setChatMessages((prev) => [...prev, userMessage, assistantMessage]);
    setIsStreaming(true);

    const controller = new AbortController();
    abortRef.current = controller;

    const updateAssistant = (updateFn) => {
      setChatMessages((prev) =>
        prev.map((msg) => (msg.id === assistantId ? updateFn(msg) : msg))
      );
    };

    try {
      const payload = { question, stream: true, history: historyPayload };
      if (visitorId) {
        payload.visitorId = visitorId;
      }
      if (repo) {
        payload.repo = repo;
      }
      if (activeSessionId) {
        payload.sessionId = activeSessionId;
      }

      const response = await fetch(`${API_BASE_URL}/chat`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "text/event-stream"
        },
        body: JSON.stringify(payload),
        signal: controller.signal
      });

      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.error || "Chat failed");
      }

      if (!response.body) {
        throw new Error("No response body");
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }

        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split("\n\n");
        buffer = parts.pop() || "";

        for (const part of parts) {
          const lines = part.split("\n").filter(Boolean);
          if (lines.length === 0) {
            continue;
          }
          let eventType = "message";
          let data = "";
          for (const line of lines) {
            if (line.startsWith("event:")) {
              eventType = line.slice(6).trim();
            } else if (line.startsWith("data:")) {
              data += line.slice(5).trim();
            }
          }
          if (!data) {
            continue;
          }

          let payload;
          try {
            payload = JSON.parse(data);
          } catch {
            continue;
          }

          if (eventType === "meta") {
            updateAssistant((msg) => ({
              ...msg,
              citations: payload.citations || []
            }));
            if (payload.sessionId) {
              setActiveSessionId(payload.sessionId);
            }
            const inferredRepo = inferRepoFromCitations(payload.citations);
            if (inferredRepo) {
              setActiveRepo(inferredRepo);
            }
          } else if (eventType === "delta") {
            updateAssistant((msg) => ({
              ...msg,
              content: msg.content + (payload.delta || "")
            }));
          } else if (eventType === "done") {
            loadSessions();
          } else if (eventType === "error") {
            throw new Error(payload.error || "Chat failed");
          }
        }
      }
    } catch (err) {
      if (err.name !== "AbortError") {
        setChatError(err.message || "Chat failed");
      }
    } finally {
      setIsStreaming(false);
    }
  };

  const handleChatSubmit = (event) => {
    event.preventDefault();
    submitChat();
  };

  const handleChatKeyDown = (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      submitChat();
    }
  };

  return (
    <main className="layout">
      <aside className="panel sidebar">
        <div className="sidebar-header">
          <p className="eyebrow">Showcase</p>
          <h2>Showcase catalog</h2>
        </div>

        <div className="project-list">
          {loading ? (
            <p className="muted">Loading showcase entries...</p>
          ) : error ? (
            <p className="status error">{error}</p>
          ) : projects.length === 0 ? (
            <p className="muted">
              No showcase entries yet. Add your first repo below.
            </p>
          ) : (
            projectGroups.map(([category, categoryProjects]) => (
              <div className="project-group" key={category}>
                <div className="project-group-header">
                  <span className="project-group-title">{category}</span>
                  <span className="project-group-count">
                    {categoryProjects.length}
                  </span>
                </div>
                <div className="project-group-list">
                  {categoryProjects.map((project) => {
                    const projectId = project.id || project.repo;
                    const isExpanded = projectId === expandedProjectId;
                    const fallbackName =
                      typeof project.repo === "string" &&
                      project.repo.startsWith("https://github.com/")
                        ? project.repo.replace("https://github.com/", "")
                        : projectId;
                    const displayName =
                      typeof project.name === "string" &&
                      project.name.includes("/")
                        ? project.name
                        : fallbackName;
                    const projectRepoId = getProjectRepo(project);
                    const lastIndexed =
                      projectRepoId && adminKey
                        ? lastIndexedByRepo.get(projectRepoId)
                        : null;
                    return (
                      <div
                        className="project-item"
                        key={projectId}
                        onContextMenu={(event) =>
                          openContextMenu(event, {
                            type: "project",
                            project
                          })
                        }
                      >
                        <button
                          type="button"
                          className="project-button"
                          onClick={() => toggleProject(projectId)}
                          aria-expanded={isExpanded}
                        >
                          <span className="project-name">{displayName}</span>
                        </button>
                        {isExpanded ? (
                          <div className="project-details">
                            <p>{project.description || "No description yet."}</p>
                            {adminKey ? (
                              <p className="project-meta">
                                {lastIndexed
                                  ? `Last indexed: ${formatTimestamp(
                                      lastIndexed.value
                                    )}`
                                  : "Last indexed: not yet"}
                              </p>
                            ) : null}
                            {project.repo ? (
                              <a
                                href={project.repo}
                                target="_blank"
                                rel="noreferrer"
                              >
                                View on GitHub
                              </a>
                            ) : null}
                          </div>
                        ) : null}
                      </div>
                    );
                  })}
                </div>
              </div>
            ))
          )}
        </div>

        <form className="form add-project" onSubmit={handleSubmit}>
          <label className="field">
            <span>GitHub repo URL</span>
            <input
              type="url"
              placeholder="https://github.com/owner/repo"
              value={repoUrl}
              onChange={(event) => setRepoUrl(event.target.value)}
              disabled={!adminKey}
              required
            />
          </label>
          <button
            type="submit"
            className="primary-button"
            disabled={!adminKey || adding}
          >
            {adding ? "Adding..." : "Add to showcase"}
          </button>
          {!adminKey ? (
            <p className="status error">
              Admin key required to add to the showcase.
            </p>
          ) : null}
          {message ? <p className="status">{message}</p> : null}
        </form>
      </aside>

      <section className="main">
        <header className="hero">
          <p className="eyebrow">GitHub Projects Showcase</p>
          <div className="hero-title">
            <h1>Showcase + AI Chat</h1>
            <button
              type="button"
              className="ghost-button about-button"
              onClick={openAbout}
            >
              About
            </button>
          </div>
          <p className="lede">
            A curated showcase with an AI assistant that answers using GitHub as
            the source of truth.
          </p>
        </header>

        <section className="chat-window">
          <div className="chat-header">
            <h2>Ask about these repos</h2>
            {isStreaming ? (
              <button
                type="button"
                className="ghost-button"
                onClick={stopStreaming}
              >
                Stop
              </button>
            ) : null}
          </div>
          <div className="chat-stream" ref={chatStreamRef}>
            {messagesLoading ? (
              <p className="muted">Loading messages...</p>
            ) : chatMessages.length === 0 ? (
              <p className="muted">
                Ask a question about the indexed repos to get started.
              </p>
            ) : (
              chatMessages.map((msg, index) => (
                <div
                  key={msg.id}
                  className={`chat-message ${msg.role}`}
                  ref={
                    index === chatMessages.length - 1
                      ? latestMessageRef
                      : null
                  }
                >
                  <div className="chat-bubble">
                    {msg.role === "assistant" ? (
                      <ReactMarkdown remarkPlugins={[remarkGfm]}>
                        {msg.content}
                      </ReactMarkdown>
                    ) : (
                      msg.content
                    )}
                  </div>
                  {msg.role === "assistant" &&
                  Array.isArray(msg.citations) &&
                  msg.citations.length > 0 ? (
                    <details className="citation-disclosure">
                      <summary>Sources ({msg.citations.length})</summary>
                      <div className="citation-list">
                        {msg.citations.map((citation) => {
                          const label = `${citation.repo || "source"}${
                            citation.path ? `/${citation.path}` : ""
                          }`;
                          const href = citation.url || null;
                          return href ? (
                            <a
                              className="citation-item"
                              key={`${msg.id}-${citation.index}`}
                              href={href}
                              target="_blank"
                              rel="noreferrer"
                            >
                              [{citation.index}] {label}
                            </a>
                          ) : (
                            <span
                              className="citation-item"
                              key={`${msg.id}-${citation.index}`}
                            >
                              [{citation.index}] {label}
                            </span>
                          );
                        })}
                      </div>
                    </details>
                  ) : null}
                </div>
              ))
            )}
          </div>
          <form className="chat-input" onSubmit={handleChatSubmit}>
            <textarea
              rows={3}
              placeholder="Ask about architecture, code, or design decisions..."
              value={chatInput}
              onChange={(event) => setChatInput(event.target.value)}
              onKeyDown={handleChatKeyDown}
              required
            />
            <div className="chat-actions">
              {chatError ? <span className="status error">{chatError}</span> : null}
              <button type="submit" className="primary-button" disabled={isStreaming}>
                {isStreaming ? "Streaming..." : "Send"}
              </button>
            </div>
          </form>
        </section>
      </section>

      <aside className="panel sidebar">
        <div className="sidebar-header">
          <p className="eyebrow">Chats</p>
          <div className="sidebar-row">
            <h2>Recent chats</h2>
            <button
              type="button"
              className="ghost-button new-chat-button"
              onClick={startNewChat}
            >
              New
            </button>
          </div>
        </div>
        <div className="chat-list">
          {sessionsLoading ? (
            <p className="muted">Loading chats...</p>
          ) : sessionsError ? (
            <p className="status error">{sessionsError}</p>
          ) : chatSessions.length === 0 ? (
            <p className="muted">No chats yet.</p>
          ) : (
            chatSessions.map((session) => {
              const label = formatSessionLabel(session);
              const timestamp = formatSessionTime(
                session.lastMessageAt || session.createdAt
              );
              const isActive = session.id === activeSessionId;
              return (
                <button
                  key={session.id}
                  type="button"
                  className={`chat-session${isActive ? " active" : ""}`}
                  onClick={() => handleSelectSession(session.id)}
                  onContextMenu={(event) =>
                    openContextMenu(event, { type: "session", session })
                  }
                >
                  <span className="chat-session-title">{label}</span>
                  {timestamp ? (
                    <span className="chat-session-meta">{timestamp}</span>
                  ) : null}
                </button>
              );
            })
          )}
        </div>
        {adminKey ? (
          <div className="ingest-panel">
            <div className="sidebar-row">
              <h3>Ingest jobs</h3>
              <div className="sidebar-actions">
                <Link className="ghost-button" href="/telemetry">
                  Telemetry
                </Link>
                <button
                  type="button"
                  className="ghost-button"
                  onClick={loadIngestJobs}
                  disabled={ingestLoading}
                >
                  Refresh
                </button>
              </div>
            </div>
            <div className="ingest-list">
              {(() => {
                const visibleJobs = ingestJobs.filter(
                  (job) => job && !["completed", "canceled"].includes(job.status)
                );
                if (ingestLoading) {
                  return <p className="muted">Loading jobs...</p>;
                }
                if (ingestError) {
                  return <p className="status error">{ingestError}</p>;
                }
                if (visibleJobs.length === 0) {
                  return <p className="muted">No active ingest jobs.</p>;
                }
                return visibleJobs.map((job) => {
                  const statusLabel = formatJobStatus(job.status);
                  const progress = formatJobProgress(job);
                  const timestamp = formatTimestamp(
                    job.updatedAt || job.createdAt
                  );
                  return (
                    <div
                      key={job.id}
                      className="ingest-item"
                      onContextMenu={(event) =>
                        openContextMenu(event, { type: "job", job })
                      }
                    >
                      <div className="ingest-title">
                        {job.projectName || job.projectRepo}
                      </div>
                      <div className="ingest-meta">
                        <span className={`ingest-status ${job.status || ""}`}>
                          {statusLabel}
                        </span>
                        {progress ? <span>{progress}</span> : null}
                      </div>
                      {job.lastMessage ? (
                        <div className="ingest-detail">{job.lastMessage}</div>
                      ) : null}
                      {timestamp ? (
                        <div className="ingest-time">{timestamp}</div>
                      ) : null}
                    </div>
                  );
                });
              })()}
            </div>
          </div>
        ) : null}
      </aside>

      {contextMenu ? (
        <div
          className="context-menu"
          style={{ top: contextMenu.y, left: contextMenu.x }}
          ref={contextMenuRef}
          role="menu"
        >
          {contextMenu.type === "project" ? (
            <>
              <button
                type="button"
                className="context-menu-item"
                onClick={() => handleReindexProject(contextMenu.project)}
              >
                Reindex repo
              </button>
              <button
                type="button"
                className="context-menu-item danger"
                onClick={() => handleDeleteProject(contextMenu.project)}
              >
                Remove from showcase
              </button>
            </>
          ) : null}
          {contextMenu.type === "session" ? (
            <button
              type="button"
              className="context-menu-item danger"
              onClick={() => handleDeleteSession(contextMenu.session)}
            >
              Delete chat
            </button>
          ) : null}
          {contextMenu.type === "job" ? (
            <>
              <button
                type="button"
                className="context-menu-item"
                onClick={() => handleRetryJob(contextMenu.job)}
              >
                Retry job
              </button>
              <button
                type="button"
                className="context-menu-item danger"
                onClick={() => handleCancelJob(contextMenu.job)}
              >
                Cancel job
              </button>
            </>
          ) : null}
        </div>
      ) : null}

      <button
        type="button"
        className={`admin-key-button${adminKey ? " active" : ""}`}
        onClick={promptAdminKey}
        title={adminKey ? "Admin key set" : "Set admin key"}
      >
        Admin
      </button>

      {isAboutOpen ? (
        <div
          className="about-overlay"
          role="dialog"
          aria-modal="true"
          aria-labelledby="about-title"
        >
          <div className="about-card">
            <div className="about-header">
              <h2 id="about-title">About</h2>
              <button
                type="button"
                className="ghost-button"
                onClick={closeAbout}
              >
                Close
              </button>
            </div>
            <div className="about-body">
              <p>
                My name is Andrew, and I built this GitHub projects showcase so
                recruiters (and anyone who wants to run it themselves) can
                explore my work in a conversational way. Ask questions about a
                project to get the what and how without digging through dozens
                of source files, and jump to the GitHub links when you want the
                raw code.
              </p>
              <p>
                The stack is Next.js + Fastify + Postgres/pgvector + Redis +
                MinIO, and the app is proxied behind Nginx and deployed with
                Docker on AWS Lightsail. This site was built with AI (OpenAI's
                Codex in VS Code).
              </p>
            </div>
          </div>
          <button
            type="button"
            className="about-backdrop"
            aria-label="Close About"
            onClick={closeAbout}
          />
        </div>
      ) : null}
    </main>
  );
}
