import { describe, expect, it } from "vite-plus/test";
import { hasGrokLogin, pickGrokToken, resolveGrokToken } from "./grokLogin.ts";

const NOW = Date.parse("2026-10-14T18:00:00Z");
// @effect-diagnostics-next-line globalDate:off - fixed ISO expiry fixture for the CLI auth format
const at = (minutesFromNow: number) => new Date(NOW + minutesFromNow * 60_000).toISOString();
const login = (key: string, expiresAt: string, authMode = "oidc") => ({
  key,
  expires_at: expiresAt,
  auth_mode: authMode,
});

describe("pickGrokToken", () => {
  it("takes the freshest first-party OIDC login and skips other issuers and modes", () => {
    const auth = {
      "https://auth.x.ai::old": login("old", at(30)),
      "https://auth.x.ai::new": login("new", at(240)),
      "https://auth.x.ai::api-key": login("api", at(999), "api_key"),
      "https://other.example::later": login("other", at(999)),
    };
    expect(pickGrokToken(auth, NOW)).toBe("new");
  });

  it("refuses a token that lapses within five minutes", () => {
    expect(pickGrokToken({ "https://auth.x.ai::a": login("a", at(6)) }, NOW)).toBe("a");
    expect(pickGrokToken({ "https://auth.x.ai::a": login("a", at(4)) }, NOW)).toBeUndefined();
  });

  it("reads expiry times with sub-millisecond digits, as the CLI writes them", () => {
    const auth = { "https://auth.x.ai::a": login("a", "2026-10-14T19:00:00.123456789Z") };
    expect(pickGrokToken(auth, NOW)).toBe("a");
  });

  it("survives a missing or malformed file", () => {
    expect(pickGrokToken(undefined, NOW)).toBeUndefined();
    expect(pickGrokToken({ "https://auth.x.ai::a": null }, NOW)).toBeUndefined();
    for (const token of ["", "a\nb", "a\n", "a\rb", "a\tb", "a b", "a\u0000b", "a\u007fb", "aéb"]) {
      const auth = { "https://auth.x.ai::a": login(token, at(60)) };
      expect(pickGrokToken(auth, NOW)).toBeUndefined();
      expect(hasGrokLogin(auth)).toBe(false);
    }
  });
});

describe("resolveGrokToken", () => {
  const sources = (files: unknown[], refreshes: string[]) => ({
    readAuth: async () => (files.length > 1 ? files.shift() : files[0]),
    refresh: async () => {
      refreshes.push("refresh");
    },
    now: () => NOW,
  });

  it("uses a fresh token without running the CLI", async () => {
    const refreshes: string[] = [];
    const token = await resolveGrokToken(
      sources([{ "https://auth.x.ai::a": login("a", at(60)) }], refreshes),
    );
    expect(token).toBe("a");
    expect(refreshes).toEqual([]);
  });

  it("has the CLI renew a stale login, then rereads the file", async () => {
    const refreshes: string[] = [];
    const token = await resolveGrokToken(
      sources(
        [
          { "https://auth.x.ai::a": login("stale", at(2)) },
          { "https://auth.x.ai::a": login("renewed", at(360)) },
        ],
        refreshes,
      ),
    );
    expect(token).toBe("renewed");
    expect(refreshes).toEqual(["refresh"]);
  });

  it("gives up when the renewal didn't help, and never runs the CLI without a login", async () => {
    const stale: string[] = [];
    expect(
      await resolveGrokToken(sources([{ "https://auth.x.ai::a": login("a", at(-10)) }], stale)),
    ).toBeUndefined();
    expect(stale).toEqual(["refresh"]);

    const none: string[] = [];
    expect(await resolveGrokToken(sources([undefined], none))).toBeUndefined();
    expect(none).toEqual([]);
  });

  it("treats a failed renewal like an unrenewed login", async () => {
    const token = await resolveGrokToken({
      readAuth: async () => ({ "https://auth.x.ai::a": login("a", at(1)) }),
      refresh: () => Promise.reject(new Error("grok exited 1")),
      now: () => NOW,
    });
    expect(token).toBeUndefined();
  });
});
