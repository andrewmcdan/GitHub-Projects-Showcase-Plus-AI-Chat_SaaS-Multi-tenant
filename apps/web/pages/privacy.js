import Link from "next/link";

export default function PrivacyPage() {
  return (
    <main className="legal-page">
      <header className="legal-header">
        <div className="legal-intro">
          <p className="eyebrow">Privacy</p>
          <h1>Privacy policy</h1>
          <p className="lede">
            We keep this policy short and focused on transparency.
          </p>
        </div>
        <div className="legal-actions">
          <Link className="ghost-button" href="/">
            Back to showcase
          </Link>
        </div>
      </header>

      <div className="panel legal-card">
        <section className="legal-section">
          <h2>Anonymous usage information</h2>
          <p>
            We may collect anonymous usage information for quality control and
            product improvement.
          </p>
        </section>

        <section className="legal-section">
          <h2>Data sharing</h2>
          <p>We will never share any data.</p>
        </section>
      </div>
    </main>
  );
}
