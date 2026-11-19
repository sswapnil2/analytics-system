import React from "react";
import { AlertTriangle } from "lucide-react";
import { btn } from "./ui";
import { errorMessage } from "../lib/format";

interface Props {
  /** Changing this key resets a previously caught error (e.g. active tab or project). */
  resetKey?: string;
  /** Full-page layout for the root boundary, or inline panel for a workspace boundary. */
  fullPage?: boolean;
  children: React.ReactNode;
}

interface State {
  error: Error | null;
}

export class ErrorBoundary extends React.Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidUpdate(prevProps: Props): void {
    if (this.state.error && prevProps.resetKey !== this.props.resetKey) {
      this.setState({ error: null });
    }
  }

  private handleReset = () => {
    this.setState({ error: null });
  };

  render(): React.ReactNode {
    const { error } = this.state;
    if (!error) return this.props.children;

    const card = (
      <div
        role="alert"
        className="max-w-lg space-y-3 rounded-xl border border-danger/30 bg-panel p-6 text-ink shadow-sm"
      >
        <div className="flex items-start gap-3">
          <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-danger" aria-hidden="true" />
          <div className="min-w-0 space-y-1">
            <h2 className="text-base font-semibold">Something went wrong</h2>
            <p className="text-sm text-muted">
              This view hit an unexpected error. Your data is safe; try reloading this section or the page.
            </p>
            <p className="break-words rounded-md bg-sunken px-2.5 py-1.5 font-mono text-xs text-ink-2">
              {errorMessage(error)}
            </p>
          </div>
        </div>
        <div className="flex flex-wrap gap-2 pt-1">
          <button type="button" onClick={this.handleReset} className={btn.primary}>
            Try again
          </button>
          <button type="button" onClick={() => window.location.reload()} className={btn.secondary}>
            Reload page
          </button>
        </div>
      </div>
    );

    if (this.props.fullPage) {
      return <div className="flex min-h-screen items-center justify-center bg-canvas p-6">{card}</div>;
    }
    return <div className="py-8">{card}</div>;
  }
}
