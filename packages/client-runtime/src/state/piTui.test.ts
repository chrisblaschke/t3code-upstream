import { describe, expect, it } from "vite-plus/test";
import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";

import { resolvePiTuiLaunch } from "./piTui.ts";

const projection = (driver: string, nativeId: string | null) =>
  ({
    thread: { activeProviderThreadId: "pt-1" },
    providerThreads: [
      {
        id: "pt-1",
        providerInstanceId: "pi-custom",
        driver,
        nativeThreadRef: nativeId === null ? null : { driver, nativeId, strength: "strong" },
      },
    ],
  }) as unknown as Parameters<typeof resolvePiTuiLaunch>[0];

describe("resolvePiTuiLaunch", () => {
  it("opens the active Pi session file, quoted for a shell", () => {
    const launch = resolvePiTuiLaunch(projection("pi", "/Users/c/it's here/s.jsonl"));
    expect(launch?.argv).toEqual(["pi", "--session", "/Users/c/it's here/s.jsonl"]);
    expect(launch?.shellCommand).toBe(`'pi' '--session' '/Users/c/it'\\''s here/s.jsonl'`);
  });

  it("uses the active instance executable and shell-quotes its path", () => {
    const launch = resolvePiTuiLaunch(projection("pi", "/tmp/s.jsonl"), {
      [ProviderInstanceId.make("pi-custom")]: {
        driver: ProviderDriverKind.make("pi"),
        config: { binaryPath: "/opt/custom pi/bin/pi" },
      },
    });
    expect(launch?.argv).toEqual(["/opt/custom pi/bin/pi", "--session", "/tmp/s.jsonl"]);
    expect(launch?.shellCommand).toBe("'/opt/custom pi/bin/pi' '--session' '/tmp/s.jsonl'");
  });

  it("offers nothing for other providers or a Pi thread without a session yet", () => {
    expect(resolvePiTuiLaunch(projection("codex", "/tmp/s.jsonl"))).toBeNull();
    expect(resolvePiTuiLaunch(projection("pi", null))).toBeNull();
  });
});
