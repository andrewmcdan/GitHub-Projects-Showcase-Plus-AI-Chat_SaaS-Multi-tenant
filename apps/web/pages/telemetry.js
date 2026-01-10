import Link from "next/link";

export default function TelemetryPage() {
  return (
    <main className="telemetry-page">
      <header className="telemetry-header">
        <div className="telemetry-intro">
          <p className="eyebrow">Telemetry</p>
          <h1>Telemetry moved</h1>
          <p className="muted">
            Usage insights now live in the account dashboard.
          </p>
        </div>
        <div className="telemetry-actions">
          <Link className="ghost-button" href="/account">
            Account
          </Link>
          <Link className="ghost-button" href="/">
            Back
          </Link>
        </div>
      </header>
      <p className="muted">
        We consolidated analytics into the account page so each owner can track
        their own showcase usage.
      </p>
    </main>
  );
}
