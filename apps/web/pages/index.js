import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/router";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:3001";
const CHAT_HISTORY_LIMIT = 8;
const VISITOR_ID_KEY = "gh-projects-visitor-id";
const ABOUT_SEEN_KEY = "gh-projects-about-seen";

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

const normalizeHandle = (value) => {
  if (typeof value !== "string") {
    return "";
  }
  return value.trim().replace(/^@/, "").toLowerCase();
};

const buildSharePath = (handle) => {
  if (!handle) {
    return "";
  }
  return `${basePath}/${handle}`.replace(/\/+/g, "/");
};
const SLASH_COMMANDS = [
  {
    id: "overview",
    label: "/overview",
    hint: "Quick summary",
    preview: "Give me an overview of ",
    prompt: (project) => `Give me an overview of ${project}.`
  },
  {
    id: "architecture",
    label: "/architecture",
    hint: "System structure",
    preview: "Explain the architecture of ",
    prompt: (project) => `Explain the architecture of ${project}.`
  },
  {
    id: "stack",
    label: "/stack",
    hint: "Tech choices",
    preview: "What is the tech stack for ",
    prompt: (project) => `What is the tech stack for ${project}?`
  },
  {
    id: "setup",
    label: "/setup",
    hint: "Local setup",
    preview: "How do I set up ",
    prompt: (project) => `How do I set up ${project} locally?`
  }
];

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

const getProjectLabel = (project) => {
  if (!project || typeof project !== "object") {
    return "this project";
  }
  const repoId = getProjectRepo(project);
  if (repoId) {
    return repoId;
  }
  const nameValue =
    typeof project.name === "string" ? project.name.trim() : "";
  if (nameValue) {
    return nameValue;
  }
  const repoValue =
    typeof project.repo === "string" ? project.repo.trim() : "";
  if (repoValue) {
    return repoValue;
  }
  const idValue = typeof project.id === "string" ? project.id.trim() : "";
  return idValue || "this project";
};

const getProjectDisplayName = (project) => {
  if (!project || typeof project !== "object") {
    return "Unknown project";
  }
  const nameValue =
    typeof project.name === "string" ? project.name.trim() : "";
  const repoId = getProjectRepo(project);
  if (nameValue && repoId && nameValue !== repoId) {
    return `${nameValue} (${repoId})`;
  }
  if (nameValue) {
    return nameValue;
  }
  if (repoId) {
    return repoId;
  }
  const repoValue =
    typeof project.repo === "string" ? project.repo.trim() : "";
  if (repoValue) {
    return repoValue;
  }
  const idValue = typeof project.id === "string" ? project.id.trim() : "";
  return idValue || "Unknown project";
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

export default function Home() {
  const router = useRouter();
  const routeHandle = normalizeHandle(
    Array.isArray(router.query.handle)
      ? router.query.handle[0]
      : router.query.handle
  );
  const [projects, setProjects] = useState([]);
  const [owner, setOwner] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [expandedProjectId, setExpandedProjectId] = useState(null);
  const [chatInput, setChatInput] = useState("");
  const [slashStep, setSlashStep] = useState(null);
  const [slashIndex, setSlashIndex] = useState(0);
  const [slashCommand, setSlashCommand] = useState(null);
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
  const [authUser, setAuthUser] = useState(null);
  const [authLoading, setAuthLoading] = useState(true);
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
  const projectOptions = useMemo(() => {
    const options = (projects || []).map((project, index) => {
      const label = getProjectDisplayName(project);
      const value = getProjectLabel(project);
      const key =
        (typeof project?.id === "string" && project.id.trim()) ||
        value ||
        label ||
        `project-${index}`;
      return { key, label, value, project };
    });
    options.sort((a, b) => a.label.localeCompare(b.label));
    return options;
  }, [projects]);
  const activeProjectLabel = useMemo(() => {
    if (!activeRepo) {
      return "";
    }
    const normalizedActive = normalizeRepoId(activeRepo) || activeRepo;
    const match = (projects || []).find((project) => {
      const repoId = getProjectRepo(project);
      if (repoId && normalizedActive && repoId === normalizedActive) {
        return true;
      }
      const projectId =
        typeof project?.id === "string" ? project.id.trim() : "";
      if (projectId && projectId === activeRepo) {
        return true;
      }
      const repoValue =
        typeof project?.repo === "string" ? project.repo.trim() : "";
      if (repoValue && normalizedActive && repoValue.includes(normalizedActive)) {
        return true;
      }
      return false;
    });
    if (match) {
      return getProjectDisplayName(match);
    }
    return activeRepo;
  }, [activeRepo, projects]);
  const projectGroups = useMemo(
    () => groupProjectsByCategory(projects),
    [projects]
  );

  const loadProjects = async () => {
    setLoading(true);
    setError("");
    try {
      const response = await fetch(
        buildApiUrl("/projects", routeHandle ? { handle: routeHandle } : {}),
        { credentials: "include" }
      );
      if (response.status === 401 && !routeHandle) {
        setProjects([]);
        setOwner(null);
        return;
      }
      if (!response.ok) {
        if (response.status === 404) {
          throw new Error("Showcase not found.");
        }
        throw new Error("Failed to load showcase entries.");
      }
      const data = await response.json();
      setProjects(Array.isArray(data.projects) ? data.projects : []);
      setOwner(data.owner || null);
    } catch (err) {
      setError(err.message || "Failed to load showcase entries.");
      setOwner(null);
    } finally {
      setLoading(false);
    }
  };

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

  const loadSessions = async () => {
    if (!visitorId) {
      return;
    }
    if (!routeHandle && !authUser) {
      return;
    }
    setSessionsLoading(true);
    setSessionsError("");
    try {
      const response = await fetch(
        buildApiUrl("/chat/sessions", {
          visitorId,
          handle: routeHandle || undefined
        }),
        { credentials: "include" }
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


  useEffect(() => {
    if (!routeHandle && authLoading) {
      return;
    }
    loadProjects();
  }, [routeHandle, authLoading]);

  useEffect(() => {
    loadAuthUser();
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
    if (!slashStep) {
      return;
    }
    const options = slashStep === "commands" ? SLASH_COMMANDS : projectOptions;
    if (options.length === 0) {
      if (slashIndex !== 0) {
        setSlashIndex(0);
      }
      return;
    }
    if (slashIndex > options.length - 1) {
      setSlashIndex(options.length - 1);
    }
  }, [slashStep, slashIndex, projectOptions]);

  useEffect(() => {
    if (visitorId) {
      loadSessions();
    }
  }, [visitorId, routeHandle, authUser]);

  useEffect(() => {
    setChatSessions([]);
    setChatMessages([]);
    setActiveSessionId(null);
    setActiveRepo(null);
  }, [routeHandle]);

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
      if (!routeHandle) {
        loadProjects();
      }
    }
  };

  const openContextMenu = (event, menu) => {
    event.preventDefault();
    event.stopPropagation();
    const menuWidth = 200;
    const menuHeight = 72;
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

  const handleDeleteSession = async (session) => {
    const sessionId = session?.id;
    if (!sessionId) {
      setChatError("Chat session id missing.");
      return;
    }
    if (!visitorId) {
      setChatError("Visitor id missing.");
      return;
    }
    if (!window.confirm("Delete this chat session?")) {
      return;
    }
    setChatError("");
    setContextMenu(null);
    try {
      const response = await fetch(
        buildApiUrl(`/chat/sessions/${sessionId}`, {
          visitorId: visitorId || undefined,
          handle: routeHandle || undefined
        }),
        {
          method: "DELETE",
          credentials: "include"
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

  const loadSessionMessages = async (sessionId) => {
    if (!visitorId || !sessionId) {
      return;
    }
    setMessagesLoading(true);
    setChatError("");
    try {
      const response = await fetch(
        buildApiUrl(`/chat/sessions/${sessionId}/messages`, {
          visitorId,
          handle: routeHandle || undefined,
          limit: 200
        }),
        { credentials: "include" }
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

  const closeSlashMenu = () => {
    setSlashStep(null);
    setSlashIndex(0);
    setSlashCommand(null);
  };

  const openSlashCommands = (nextIndex = 0) => {
    setSlashStep("commands");
    setSlashIndex(nextIndex);
    setSlashCommand(null);
  };

  const openSlashProjects = (command) => {
    setSlashStep("projects");
    setSlashIndex(0);
    setSlashCommand(command || null);
  };

  const submitChat = async (questionOverride) => {
    const rawQuestion =
      typeof questionOverride === "string" ? questionOverride : chatInput;
    const question = rawQuestion.trim();
    if (!question || isStreaming) {
      return;
    }
    if (!routeHandle && !authUser) {
      setChatError("Sign in or open a public showcase to chat.");
      return;
    }

    closeSlashMenu();
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
      const handle =
        routeHandle || owner?.handle || authUser?.githubUsername || "";
      if (handle) {
        payload.handle = handle;
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
        credentials: "include",
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

  const handleSlashCommandSelect = (command) => {
    if (!command) {
      return;
    }
    setChatInput(command.preview);
    openSlashProjects(command);
  };

  const handleSlashProjectSelect = (project) => {
    if (!slashCommand) {
      return;
    }
    const projectLabel = getProjectLabel(project);
    const question = slashCommand.prompt(projectLabel);
    closeSlashMenu();
    submitChat(question);
  };

  const handleSlashKeyDown = (event) => {
    if (!slashStep) {
      return false;
    }
    const options =
      slashStep === "commands" ? SLASH_COMMANDS : projectOptions;
    if (event.key === "Escape") {
      event.preventDefault();
      closeSlashMenu();
      return true;
    }
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      if (options.length === 0) {
        return true;
      }
      const delta = event.key === "ArrowDown" ? 1 : -1;
      setSlashIndex((current) => {
        const next = current + delta;
        if (next < 0) {
          return options.length - 1;
        }
        if (next >= options.length) {
          return 0;
        }
        return next;
      });
      return true;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      const selected = options[slashIndex];
      if (!selected) {
        return true;
      }
      if (slashStep === "commands") {
        handleSlashCommandSelect(selected);
      } else {
        handleSlashProjectSelect(selected.project);
      }
      return true;
    }
    return false;
  };

  const handleChatInputChange = (event) => {
    const nextValue = event.target.value;
    setChatInput(nextValue);
    if (slashStep === "projects") {
      return;
    }
    const trimmed = nextValue.trimStart();
    if (!trimmed.startsWith("/")) {
      if (slashStep) {
        closeSlashMenu();
      }
      return;
    }
    const query = trimmed.slice(1).toLowerCase();
    const matchIndex = SLASH_COMMANDS.findIndex((command) =>
      command.label.slice(1).startsWith(query)
    );
    openSlashCommands(matchIndex >= 0 ? matchIndex : 0);
  };

  const handleChatSubmit = (event) => {
    event.preventDefault();
    submitChat();
  };

  const handleChatKeyDown = (event) => {
    if (handleSlashKeyDown(event)) {
      return;
    }
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      submitChat();
    }
  };

  const authDisplayName = authUser
    ? authUser.name || authUser.githubUsername || authUser.email
    : "";
  const ownerName = owner?.name || owner?.handle || "";
  const ownerHandle = owner?.handle ? `@${owner.handle}` : "";
  const shareHandle = owner?.handle || authUser?.githubUsername || "";
  const sharePath = buildSharePath(shareHandle);
  const ownerVisibility =
    owner && owner.isPublic === false ? "Private showcase" : "Public showcase";
  const shareUrl = useMemo(() => {
    if (!sharePath) {
      return "";
    }
    if (typeof window === "undefined") {
      return sharePath;
    }
    return `${window.location.origin}${sharePath}`;
  }, [sharePath]);
  const emptyChatMessage =
    !routeHandle && !authUser
      ? "Sign in or open a public showcase to start chatting."
      : "Ask a question about the indexed repos to get started.";
  const chatPlaceholder = activeProjectLabel
    ? `Context: ${activeProjectLabel}. Ask about architecture, code, or design decisions...`
    : "Type / for quick prompts, or ask about architecture, code, or design decisions...";
  const chatHint = activeProjectLabel
    ? `Context: ${activeProjectLabel} | Type / for quick prompts.`
    : "Type / for quick prompts.";

  return (
    <main className="layout">
      <aside className="panel sidebar">
        <div className="sidebar-header">
          <p className="eyebrow">Showcase</p>
          <h2>{ownerName ? `Showcase for ${ownerName}` : "Showcase catalog"}</h2>
          {ownerHandle ? <span className="muted">{ownerHandle}</span> : null}
        </div>

        <div className="project-list">
          {loading ? (
            <p className="muted">Loading showcase entries...</p>
          ) : error ? (
            <p className="status error">{error}</p>
          ) : projects.length === 0 ? (
            <p className="muted">
              No showcase entries yet.
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
                    return (
                      <div className="project-item" key={projectId}>
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

        <div className="sidebar-card">
          {authUser ? (
            <>
              <p className="muted">
                Manage your repos, usage, and billing from your account.
              </p>
              <Link className="primary-button" href="/account">
                Go to account
              </Link>
            </>
          ) : (
            <>
              <p className="muted">
                Sign in to create your own GitHub projects showcase.
              </p>
              <button
                type="button"
                className="primary-button"
                onClick={handleAuthStart}
              >
                Sign in with GitHub
              </button>
            </>
          )}
        </div>
      </aside>

      <section className="main">
        <header className="hero">
          <p className="eyebrow">GitHub Projects Showcase</p>
          <div className="hero-title">
            <h1>Showcase + AI Chat</h1>
            <div className="hero-actions">
              {authDisplayName ? (
                <span className="hero-user">Signed in as {authDisplayName}</span>
              ) : null}
              <button
                type="button"
                className="ghost-button about-button"
                onClick={openAbout}
              >
                About
              </button>
              {authUser ? (
                <Link className="ghost-button" href="/account">
                  Account
                </Link>
              ) : null}
              {!authLoading ? (
                authUser ? (
                  <button
                    type="button"
                    className="ghost-button"
                    onClick={handleAuthLogout}
                  >
                    Sign out
                  </button>
                ) : (
                  <button
                    type="button"
                    className="ghost-button"
                    onClick={handleAuthStart}
                  >
                    Sign in with GitHub
                  </button>
                )
              ) : null}
            </div>
          </div>
          <p className="lede">
            A curated showcase with an AI assistant that answers using GitHub as
            the source of truth.
          </p>
          {owner?.bio ? <p className="hero-bio">{owner.bio}</p> : null}
          {shareUrl ? (
            <div className="hero-share">
              <span className="muted">
                {owner?.isPublic === false ? "Private URL" : "Public URL"}
              </span>
              <a href={shareUrl}>{shareUrl}</a>
            </div>
          ) : null}
          {ownerVisibility ? (
            <span
              className={`visibility-pill${
                owner?.isPublic === false ? " is-private" : " is-public"
              }`}
            >
              {ownerVisibility}
            </span>
          ) : null}
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
              <p className="muted">{emptyChatMessage}</p>
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
            <div className="chat-input-field">
              <textarea
                rows={3}
                placeholder={chatPlaceholder}
                value={chatInput}
                onChange={handleChatInputChange}
                onKeyDown={handleChatKeyDown}
                required
              />
              {slashStep ? (
                <div
                  className="slash-menu"
                  role="listbox"
                  aria-label={
                    slashStep === "commands"
                      ? "Quick prompts"
                      : "Select a project"
                  }
                >
                  <div className="slash-menu-header">
                    {slashStep === "commands"
                      ? "Quick prompts"
                      : "Select a project"}
                  </div>
                  {slashStep === "commands" ? (
                    <div className="slash-menu-list">
                      {SLASH_COMMANDS.map((command, index) => (
                        <button
                          key={command.id}
                          type="button"
                          className={`slash-menu-item${
                            index === slashIndex ? " is-active" : ""
                          }`}
                          onClick={() => handleSlashCommandSelect(command)}
                          role="option"
                          aria-selected={index === slashIndex}
                        >
                          <span className="slash-menu-item-label">
                            {command.label}
                          </span>
                          <span className="slash-menu-item-hint">
                            {command.hint}
                          </span>
                        </button>
                      ))}
                    </div>
                  ) : projectOptions.length === 0 ? (
                    <div className="slash-menu-empty">
                      {loading
                        ? "Loading projects..."
                        : "No projects available yet."}
                    </div>
                  ) : (
                    <div className="slash-menu-list">
                      {projectOptions.map((option, index) => (
                        <button
                          key={option.key}
                          type="button"
                          className={`slash-menu-item${
                            index === slashIndex ? " is-active" : ""
                          }`}
                          onClick={() => handleSlashProjectSelect(option.project)}
                          role="option"
                          aria-selected={index === slashIndex}
                        >
                          <span className="slash-menu-item-label">
                            {option.label}
                          </span>
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              ) : null}
            </div>
            <p className="chat-hint">{chatHint}</p>
            <div className="chat-actions">
              {chatError ? (
                <span className="status error">{chatError}</span>
              ) : null}
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
      </aside>

      {contextMenu ? (
        <div
          className="context-menu"
          style={{ top: contextMenu.y, left: contextMenu.x }}
          ref={contextMenuRef}
          role="menu"
        >
          {contextMenu.type === "session" ? (
            <button
              type="button"
              className="context-menu-item danger"
              onClick={() => handleDeleteSession(contextMenu.session)}
            >
              Delete chat
            </button>
          ) : null}
        </div>
      ) : null}

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
