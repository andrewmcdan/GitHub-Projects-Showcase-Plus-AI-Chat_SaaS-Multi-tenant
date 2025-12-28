import { useEffect, useState } from "react";

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
          <h2>Ask about these projects</h2>
          <div className="chat-box">
            <p className="muted">Chat UI coming next.</p>
          </div>
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
