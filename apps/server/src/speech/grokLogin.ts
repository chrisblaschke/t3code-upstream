// @effect-diagnostics nodeBuiltinImport:off - isolated CLI auth-file reader and login renewal
/** Only the CLI writes the login. Never log tokens or include them in errors. */
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

// First-party xAI logins; other issuers in the file are not for this API.
const XAI_LOGIN_PREFIX = "https://auth.x.ai::";
// The same margin the Grok CLI provider uses: never start a request on a token about to lapse.
const MIN_TOKEN_LIFETIME_MS = 5 * 60_000;
const REFRESH_TIMEOUT_MS = 30_000;

export const grokAuthPath = (home: string) => NodePath.join(home, ".grok", "auth.json");
const grokBinaryPath = (home: string) => NodePath.join(home, ".grok", "bin", "grok");

interface GrokLogin {
  readonly token: string;
  readonly expiresAtMs: number;
}

/** First-party xAI OIDC logins in auth.json, freshest first. */
function grokLogins(auth: unknown): GrokLogin[] {
  if (typeof auth !== "object" || auth === null) return [];
  const logins: GrokLogin[] = [];
  for (const [issuer, entry] of Object.entries(auth)) {
    if (!issuer.startsWith(XAI_LOGIN_PREFIX)) continue;
    const { auth_mode, key, expires_at } = (entry ?? {}) as Record<string, unknown>;
    // Header-safe characters only, without the API-key setting's length cap.
    if (auth_mode !== "oidc" || typeof key !== "string" || key === "" || /[^!-~]/.test(key)) {
      continue;
    }
    // Fractional seconds can run past milliseconds; drop them as the CLI provider does.
    const expiresAtMs =
      typeof expires_at === "string" ? Date.parse(expires_at.replace(/(:\d{2})\.\d+/, "$1")) : NaN;
    logins.push({ token: key, expiresAtMs: Number.isNaN(expiresAtMs) ? 0 : expiresAtMs });
  }
  return logins.toSorted((a, b) => b.expiresAtMs - a.expiresAtMs);
}

/** Whether the Grok CLI is logged in at all, fresh or not; a stale token can be renewed. */
export const hasGrokLogin = (auth: unknown): boolean => grokLogins(auth).length > 0;

/** The freshest login's token, unless it lapses within five minutes. */
export function pickGrokToken(auth: unknown, nowMs: number): string | undefined {
  const freshest = grokLogins(auth)[0];
  return freshest && freshest.expiresAtMs - nowMs > MIN_TOKEN_LIFETIME_MS
    ? freshest.token
    : undefined;
}

/**
 * A usable token, renewing a stale login once through the Grok CLI. Undefined
 * when there is no login, or the CLI could not renew it.
 */
export async function resolveGrokToken(sources: {
  readonly readAuth: () => Promise<unknown>;
  readonly refresh: () => Promise<void>;
  readonly now: () => number;
}): Promise<string | undefined> {
  const auth = await sources.readAuth();
  const token = pickGrokToken(auth, sources.now());
  if (token !== undefined || !hasGrokLogin(auth)) return token;
  await sources.refresh().catch(() => undefined);
  return pickGrokToken(await sources.readAuth(), sources.now());
}

/** auth.json parsed, or undefined when it is missing, unreadable, or malformed. */
export const readGrokAuth = (home: string): Promise<unknown> =>
  NodeFSP.readFile(grokAuthPath(home), "utf8")
    .then((text) => JSON.parse(text) as unknown)
    .catch(() => undefined);

let pendingRefresh: Promise<void> | undefined;

/** `grok models` renews the login as a side effect; concurrent callers share one run. */
export function refreshGrokLogin(home: string): Promise<void> {
  pendingRefresh ??= new Promise<void>((resolve) => {
    NodeChildProcess.execFile(
      grokBinaryPath(home),
      ["--no-auto-update", "models"],
      { timeout: REFRESH_TIMEOUT_MS },
      // Output is ignored either way; the next auth.json read tells whether it worked.
      () => resolve(),
    );
  }).finally(() => {
    pendingRefresh = undefined;
  });
  return pendingRefresh;
}
