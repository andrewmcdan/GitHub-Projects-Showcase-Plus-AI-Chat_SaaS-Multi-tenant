import { useEffect, useRef, useState } from "react";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:3001";

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
  const abortRef = useRef(null);

  const loadProjects = async () => {
    setLoading(true);
    setError("");
    try {
      const response = await fetch(`${API_BASE_URL}/projects`);
      if (!response.ok) {
        throw new Error("Failed to load projects");
      }
      const data = await response.json();
      setProjects(Array.isArray(data.projects) ? data.projects : []);
    } catch (err) {
      setError(err.message || "Failed to load projects");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadProjects();
  }, []);

  const handleSubmit = async (event) => {
    event.preventDefault();
    setAdding(true);
    setMessage("");
    try {
      const response = await fetch(`${API_BASE_URL}/projects`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ repoUrl })
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Failed to add project");
      }
      setMessage(
        payload.status === "exists"
          ? "Project already listed."
          : "Project added."
      );
      setRepoUrl("");
      await loadProjects();
    } catch (err) {
      setMessage(err.message || "Failed to add project");
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

  const handleChatSubmit = async (event) => {
    event.preventDefault();
    if (!chatInput.trim() || isStreaming) {
      return;
    }

    const question = chatInput.trim();
    setChatInput("");
    setChatError("");

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
      const response = await fetch(`${API_BASE_URL}/chat`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "text/event-stream"
        },
        body: JSON.stringify({ question, stream: true }),
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
          } else if (eventType === "delta") {
            updateAssistant((msg) => ({
              ...msg,
              content: msg.content + (payload.delta || "")
            }));
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

  return (
    <main className="layout">
      <aside className="panel sidebar">
        <div className="sidebar-header">
          <p className="eyebrow">Projects</p>
          <h2>Project catalog</h2>
          <p className="muted">
            Paste a GitHub repo URL to add it. Details are pulled from GitHub.
          </p>
        </div>

        <div className="project-list">
          {loading ? (
            <p className="muted">Loading projects...</p>
          ) : error ? (
            <p className="status error">{error}</p>
          ) : projects.length === 0 ? (
            <p className="muted">No projects yet. Add your first repo below.</p>
          ) : (
            projects.map((project) => {
              const projectId = project.id || project.repo;
              const isExpanded = projectId === expandedProjectId;
              const fallbackName =
                typeof project.repo === "string" &&
                project.repo.startsWith("https://github.com/")
                  ? project.repo.replace("https://github.com/", "")
                  : projectId;
              const displayName =
                typeof project.name === "string" && project.name.includes("/")
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
                      <p>
                        {project.description || "No description yet."}
                      </p>
                      {project.repo ? (
                        <a href={project.repo} target="_blank" rel="noreferrer">
                          View on GitHub
                        </a>
                      ) : null}
                    </div>
                  ) : null}
                </div>
              );
            })
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
              required
            />
          </label>
          <button type="submit" className="primary-button" disabled={adding}>
            {adding ? "Adding..." : "Add project"}
          </button>
          {message ? <p className="status">{message}</p> : null}
        </form>
      </aside>

      <section className="main">
        <header className="hero">
          <p className="eyebrow">GitHub Projects</p>
          <h1>Projects + AI Chat</h1>
          <p className="lede">
            A curated homepage with an AI assistant that answers using GitHub as
            the source of truth.
          </p>
        </header>

        <section className="chat-window">
          <div className="chat-header">
            <h2>Ask about these projects</h2>
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
          <div className="chat-stream">
            {chatMessages.length === 0 ? (
              <p className="muted">
                Ask a question about the indexed repos to get started.
              </p>
            ) : (
              chatMessages.map((msg) => (
                <div key={msg.id} className={`chat-message ${msg.role}`}>
                  <div className="chat-bubble">{msg.content}</div>
                  {msg.role === "assistant" &&
                  Array.isArray(msg.citations) &&
                  msg.citations.length > 0 ? (
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
          <h2>Recent chats</h2>
        </div>
        <div className="chat-list">
          <p className="muted">No chats yet.</p>
        </div>
      </aside>
    </main>
  );
}
