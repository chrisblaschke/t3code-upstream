import { it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { makeLocalFileTracer, makeTraceSink } from "@t3tools/shared/observability";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { DEFAULT_SPEECH_TRANSCRIPTION_OPTIONS, SecretProviderId } from "@t3tools/contracts";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { expect, vi } from "vite-plus/test";
import * as ServerSettings from "../serverSettings.ts";
import * as CloudSpeech from "./CloudSpeech.ts";
import * as GrokLogin from "./grokLogin.ts";

it.effect("rejects malformed Grok login tokens without putting them in traces", () =>
  Effect.gen(function* () {
    const token = "SYNTHETIC_GROK_TOKEN\nINVALID";
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const home = yield* fs.makeTempDirectoryScoped();
    yield* fs.makeDirectory(path.join(home, ".grok"));
    yield* fs.writeFileString(
      path.join(home, ".grok", "auth.json"),
      JSON.stringify({
        "https://auth.x.ai::fixture": {
          auth_mode: "oidc",
          key: token,
          expires_at: "2099-01-01T00:00:00Z",
        },
      }),
    );
    const tracePath = path.join(home, "trace.ndjson");
    const options = {
      filePath: tracePath,
      maxBytes: 1024 * 1024,
      maxFiles: 2,
      batchWindowMs: 10000,
    };
    const sink = yield* makeTraceSink(options);
    const tracer = yield* makeLocalFileTracer({ ...options, sink });
    const service = yield* CloudSpeech.CloudSpeech.pipe(
      Effect.provide(
        CloudSpeech.layer.pipe(
          Layer.provide(FetchHttpClient.layer),
          Layer.provide(ServerSettings.ServerSettingsService.layerTest()),
          Layer.provide(Layer.mock(SecretResolver.SecretResolver)({})),
        ),
      ),
      Effect.provideService(HostProcess.HomeDirectory, home),
    );
    // Real header validation, without any network I/O.
    const validateOnly: typeof fetch = async (input, init) => {
      throw new Error(
        `Unexpected valid ${new Request(input, init).method} request; network disabled`,
      );
    };
    const failure = yield* service
      .transcribe("xai", new Uint8Array(4), DEFAULT_SPEECH_TRANSCRIPTION_OPTIONS)
      .pipe(
        Effect.provideService(FetchHttpClient.Fetch, validateOnly),
        Effect.withTracer(tracer),
        Effect.flip,
      );
    yield* sink.close();
    expect(yield* fs.readFileString(tracePath)).not.toContain("SYNTHETIC_GROK_TOKEN");
    expect(JSON.stringify(failure)).not.toContain("SYNTHETIC_GROK_TOKEN");
    expect(failure).toMatchObject({ reason: "missing-key" });
    expect((yield* service.configured).xai).toBe(false);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each([undefined, "saved-key"])(
  "uses a Grok CLI login unless a stored key wins (%s)",
  (storedKey) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped();
      yield* fs.makeDirectory(path.join(home, ".grok"));
      // OIDC token size is not bounded by the API-key setting's 4096-character limit.
      const token = `eyJhbGciOiJSUzI1NiJ9.${"a".repeat(5000)}.signature`;
      yield* fs.writeFileString(
        path.join(home, ".grok", "auth.json"),
        JSON.stringify({
          "https://auth.x.ai::fixture": {
            auth_mode: "oidc",
            key: token,
            expires_at: "2099-01-01T00:00:00Z",
          },
        }),
      );
      const service = yield* CloudSpeech.CloudSpeech.pipe(
        Effect.provide(
          CloudSpeech.layer.pipe(
            Layer.provide(FetchHttpClient.layer),
            Layer.provide(ServerSettings.ServerSettingsService.layerTest()),
            Layer.provide(Layer.mock(SecretResolver.SecretResolver)({})),
          ),
        ),
        Effect.provideService(HostProcess.HomeDirectory, home),
      );
      if (storedKey) yield* service.setKey("xai", Redacted.make(storedKey));
      const upstream = vi.fn<typeof fetch>(async () => Response.json({ text: "from Grok" }));
      const transcribe = () =>
        service
          .transcribe("xai", new Uint8Array(4), DEFAULT_SPEECH_TRANSCRIPTION_OPTIONS)
          .pipe(Effect.provideService(FetchHttpClient.Fetch, upstream));
      expect(yield* transcribe()).toBe("from Grok");
      expect(yield* service.configured).toEqual({ openai: false, xai: true, openrouter: false });
      expect(String(upstream.mock.calls[0]?.[0])).toBe("https://api.x.ai/v1/stt");
      expect(new Headers(upstream.mock.calls[0]?.[1]?.headers).get("authorization")).toBe(
        `Bearer ${storedKey ?? token}`,
      );
      yield* service.setKey("xai", null);
      yield* fs.writeFileString(path.join(home, ".grok", "auth.json"), "{partial");
      expect((yield* service.configured).xai).toBe(false);
      expect(yield* Effect.flip(transcribe())).toMatchObject({
        provider: "xai",
        reason: "missing-key",
      });
      expect(upstream).toHaveBeenCalledTimes(1);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("does not upload with a Grok login if a stored key arrives during resolution", () =>
  Effect.gen(function* () {
    const started = Promise.withResolvers<void>();
    const auth = Promise.withResolvers<unknown>();
    using _read = vi.spyOn(GrokLogin, "readGrokAuth").mockImplementation(() => {
      started.resolve();
      return auth.promise;
    });
    const service = yield* CloudSpeech.CloudSpeech.pipe(
      Effect.provide(
        CloudSpeech.layer.pipe(
          Layer.provide(FetchHttpClient.layer),
          Layer.provide(ServerSettings.ServerSettingsService.layerTest()),
          Layer.provide(Layer.mock(SecretResolver.SecretResolver)({})),
        ),
      ),
    );
    const upstream = vi.fn<typeof fetch>(async () => Response.json({ text: "unexpected" }));
    const job = yield* service
      .transcribe("xai", new Uint8Array(4), DEFAULT_SPEECH_TRANSCRIPTION_OPTIONS)
      .pipe(Effect.provideService(FetchHttpClient.Fetch, upstream), Effect.flip, Effect.forkChild);
    expect(
      yield* Effect.raceFirst(
        Effect.promise(() => started.promise).pipe(Effect.as("started")),
        Fiber.join(job).pipe(Effect.as("finished")),
      ),
    ).toBe("started");
    yield* service.setKey("xai", Redacted.make("new-key"));
    auth.resolve({
      "https://auth.x.ai::fixture": {
        auth_mode: "oidc",
        key: "login-token",
        expires_at: "2099-01-01T00:00:00Z",
      },
    });
    expect(yield* Fiber.join(job)).toMatchObject({ reason: "key-changed" });
    expect(upstream).not.toHaveBeenCalled();
  }),
);

it.effect.each(["openai", "xai"] as const)("sends only to %s using its own saved key", (provider) =>
  Effect.gen(function* () {
    const reference = {
      provider: SecretProviderId.make("work"),
      reference: "op://Work/Voice/key",
    };
    let resolved: string | undefined;
    let cached: string | undefined;
    const read = vi.fn(() => Effect.sync(() => [(cached ??= resolved)]));
    const upstream = vi.fn<typeof fetch>(async () => Response.json({ text: "hello" }));
    const service = yield* CloudSpeech.CloudSpeech.pipe(
      Effect.provide(
        CloudSpeech.layer.pipe(
          Layer.provide(FetchHttpClient.layer),
          Layer.provide(ServerSettings.ServerSettingsService.layerTest()),
          Layer.provide(
            Layer.mock(SecretResolver.SecretResolver)({
              read,
              label: () => Effect.succeed("Work"),
              invalidate: () =>
                Effect.sync(() => {
                  cached = undefined;
                }),
            }),
          ),
        ),
      ),
    );
    const transcribe = (pcm = new Uint8Array(new Float32Array([0, 0.5]).buffer)) =>
      service
        .transcribe(provider, pcm, {
          ...DEFAULT_SPEECH_TRANSCRIPTION_OPTIONS,
          speechLanguage: "en",
          speechCustomWords: [{ term: "T3 Code", aliases: [] }],
        })
        .pipe(Effect.provideService(FetchHttpClient.Fetch, upstream));
    yield* service.setKey(provider === "openai" ? "xai" : "openai", Redacted.make("other"));
    expect((yield* Effect.flip(transcribe()))._tag).toBe("CloudSpeechError");
    yield* service.setKey(provider, Redacted.make("chosen"));
    expect(yield* service.configured).toEqual({ openai: true, xai: true, openrouter: false });
    yield* Effect.flip(transcribe(new Uint8Array(3)));
    expect(upstream).not.toHaveBeenCalled();
    expect(yield* transcribe()).toBe("hello");
    const [url, init] = upstream.mock.calls[0]!;
    expect(String(url)).toBe(
      {
        openai: "https://api.openai.com/v1/audio/transcriptions",
        xai: "https://api.x.ai/v1/stt",
      }[provider],
    );
    expect(init?.redirect).toBe("error");
    expect(new Headers(init?.headers).get("authorization")).toBe("Bearer chosen");
    const form = init?.body as FormData;
    expect(form.get("model")).toBe(
      provider === "openai" ? "gpt-transcribe" : "grok-voice-transcribe-2.0",
    );
    expect(form.getAll(provider === "openai" ? "keywords[]" : "keyterm")).toEqual(["T3 Code"]);
    expect(form.get(provider === "openai" ? "languages[]" : "language")).toBe("en");
    expect([...form.keys()].at(-1)).toBe("file");
    const wav = new DataView(yield* Effect.promise(() => (form.get("file") as Blob).arrayBuffer()));
    expect(wav.getUint32(24, true)).toBe(16_000);
    expect(wav.getInt16(46, true)).toBe(16384);
    upstream.mockResolvedValueOnce(new Response("sensitive upstream diagnostic", { status: 401 }));
    expect(yield* Effect.flip(transcribe())).toMatchObject({ provider, reason: "rejected-key" });
    yield* service.setKey(provider, null);
    expect((yield* service.configured)[provider]).toBe(false);
    yield* Effect.flip(transcribe());
    expect(upstream).toHaveBeenCalledTimes(2);
    yield* service.setKey(provider, reference);
    expect((yield* service.configured)[provider]).toBe(true);
    expect(read).not.toHaveBeenCalled();
    resolved = "resolved-fixture";
    expect(yield* transcribe()).toBe("hello");
    expect(read).toHaveBeenCalledWith([reference]);
    expect(new Headers(upstream.mock.calls[2]?.[1]?.headers).get("authorization")).toBe(
      "Bearer resolved-fixture",
    );
    resolved = undefined;
    expect(yield* Effect.flip(transcribe())).toMatchObject({
      provider,
      reason: "unreadable-key",
    });
    expect(upstream).toHaveBeenCalledTimes(3);
  }).pipe(Effect.provideService(HostProcess.HomeDirectory, "/nonexistent-cloud-speech-test-home")),
);

it.effect("uploads to OpenRouter only for a listed exact model, as zero-retention JSON WAV", () =>
  Effect.gen(function* () {
    const upstream = vi.fn<typeof fetch>(async (input) =>
      new URL(String(input)).pathname.endsWith("/models")
        ? Response.json({ data: [{ id: "acme/transcribe-1" }, { id: "acme/transcribe-2" }] })
        : Response.json({ text: "hello" }),
    );
    const service = yield* CloudSpeech.CloudSpeech.pipe(
      Effect.provide(
        CloudSpeech.layer.pipe(
          Layer.provide(FetchHttpClient.layer),
          Layer.provide(ServerSettings.ServerSettingsService.layerTest()),
          Layer.provide(Layer.mock(SecretResolver.SecretResolver)({})),
        ),
      ),
    );
    const uploads = () =>
      upstream.mock.calls.filter(([url]) => String(url).endsWith("/audio/transcriptions"));
    const transcribe = (model: string | undefined) =>
      service
        .transcribe(
          "openrouter",
          new Uint8Array(new Float32Array([0, 0.5]).buffer),
          { ...DEFAULT_SPEECH_TRANSCRIPTION_OPTIONS, speechLanguage: "en" },
          model,
        )
        .pipe(Effect.provideService(FetchHttpClient.Fetch, upstream));
    yield* service.setKey("openrouter", Redacted.make("router-key"));
    expect(yield* Effect.flip(transcribe(undefined))).toMatchObject({ provider: "openrouter" });
    upstream.mockResolvedValueOnce(new Response(null, { status: 503 }));
    expect(yield* Effect.flip(transcribe("acme/transcribe-2"))).toMatchObject({
      reason: "unavailable",
    });
    expect(yield* Effect.flip(transcribe("acme/delisted"))).toMatchObject({
      provider: "openrouter",
    });
    expect(uploads()).toEqual([]);
    expect(yield* transcribe("acme/transcribe-2")).toBe("hello");
    const [[url, init]] = uploads() as [[URL, RequestInit]];
    expect(new URL(String(upstream.mock.calls[0]![0])).searchParams.get("zdr")).toBe("true");
    expect(String(url)).toBe("https://openrouter.ai/api/v1/audio/transcriptions");
    expect(init.redirect).toBe("error");
    const headers = new Headers(init.headers);
    expect(headers.get("authorization")).toBe("Bearer router-key");
    expect(headers.get("content-type")).toContain("application/json");
    const body = JSON.parse(yield* Effect.promise(() => new Response(init.body).text())) as {
      model: string;
      models?: unknown;
      provider: { zdr: boolean };
      input_audio: { format: string; data: string };
    };
    expect(body).toMatchObject({
      model: "acme/transcribe-2",
      provider: { zdr: true },
      input_audio: { format: "wav" },
    });
    expect(body.models).toBeUndefined();
    const wav = Buffer.from(body.input_audio.data, "base64");
    expect(wav.toString("ascii", 0, 4)).toBe("RIFF");
    expect(wav.readInt16LE(46)).toBe(16384);
  }),
);

it.effect("removing a key while a catalog read is pending prevents the upload", () =>
  Effect.gen(function* () {
    const service = yield* CloudSpeech.CloudSpeech.pipe(
      Effect.provide(
        CloudSpeech.layer.pipe(
          Layer.provide(FetchHttpClient.layer),
          Layer.provide(ServerSettings.ServerSettingsService.layerTest()),
          Layer.provide(Layer.mock(SecretResolver.SecretResolver)({})),
        ),
      ),
    );
    const catalogStarted = Promise.withResolvers<void>();
    const catalog = Promise.withResolvers<Response>();
    const uploads: string[] = [];
    const upstream: typeof fetch = async (input) => {
      if (new URL(String(input)).pathname.endsWith("/models")) {
        catalogStarted.resolve();
        return catalog.promise;
      }
      uploads.push(String(input));
      return Response.json({ text: "unexpected" });
    };
    yield* service.setKey("openrouter", Redacted.make("old-key"));
    const job = yield* service
      .transcribe(
        "openrouter",
        new Uint8Array(4),
        DEFAULT_SPEECH_TRANSCRIPTION_OPTIONS,
        "acme/voice",
      )
      .pipe(Effect.provideService(FetchHttpClient.Fetch, upstream), Effect.forkChild);
    yield* Effect.promise(() => catalogStarted.promise);
    yield* service.setKey("openrouter", null);
    catalog.resolve(Response.json({ data: [{ id: "acme/voice" }] }));
    const exit = yield* Fiber.await(job);
    expect(uploads).toEqual([]);
    expect(exit._tag).toBe("Failure");
  }),
);
