import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ModelSelection,
  type ServerProvider,
} from "@t3tools/contracts";
import { DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts/settings";
import { DEFAULT_RESOLVED_KEYBINDINGS } from "@t3tools/shared/keybindings";
import { useRef } from "react";
import "../../index.css";
import { expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
import { useComposerDraftStore } from "../../composerDraftStore";
import { getAppModelOptionsForInstance } from "../../modelSelection";
import { deriveProviderInstanceEntries } from "../../providerInstances";
import { ModelPickerContent } from "./ModelPickerContent";
import { useComposerSelection } from "./useComposerSelection";

const fast = "openai-codex/gpt-6.1-sol";
const ultrafast = "openai-codex/gpt-6.1-sol-ultrafast";
const helper = ProviderInstanceId.make("piHelpers");
const stock = ProviderInstanceId.make("pi");

// Keep the server module real: useSettings now imports its environment atoms.
vi.mock(import("../../hooks/useSettings"), async (importOriginal) => ({
  ...(await importOriginal()),
  useEnvironmentModelPreferences: () => ({
    providerModelPreferences: {},
    favorites: [
      { provider: stock, model: fast },
      { provider: stock, model: ultrafast },
    ],
  }),
  useToggleEnvironmentModelFavorite: () => async () => {},
}));

function provider(instanceId: ProviderInstanceId): ServerProvider {
  return {
    instanceId,
    driver: ProviderDriverKind.make("pi"),
    displayName: instanceId === helper ? "Helper profile" : "Stock Pi",
    enabled: true,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-01-01T00:00:00.000Z",
    models: [fast, ultrafast].map((slug) => ({
      slug,
      name: slug === fast ? "Sol Fast" : "Sol Ultrafast",
      isCustom: false,
      capabilities: {},
    })),
    slashCommands: [],
    skills: [],
  };
}

function Picker({
  selection,
  target,
  started,
}: {
  selection: ModelSelection;
  target: ReturnType<typeof scopeThreadRef>;
  started: boolean;
}) {
  const providers = [provider(stock), provider(helper)];
  const entries = deriveProviderInstanceEntries(providers);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const { onProviderModelSelectStable } = useComposerSelection({
    environmentId: target.environmentId,
    composerDraftTarget: target,
    draftThread: null,
    // Other shell fields are unused by the selection handler.
    activeThread: {
      environmentId: target.environmentId,
      id: target.threadId,
      modelSelection: selection,
    } as EnvironmentThreadShell,
    activeRuntime: started
      ? {
          status: "idle",
          activeRunId: null,
          providerInstanceId: selection.instanceId,
          providerName: "pi",
          lastError: null,
        }
      : null,
    activeProjectSettings: { settings: { newWorktreesStartFromOrigin: false } },
    settings: DEFAULT_UNIFIED_SETTINGS,
    providerStatuses: providers,
    lockedProvider: null,
    supportsProviderSwitchingViaHandoff: true,
    multipleModelSelections: null,
    runtimeMode: "full-access",
    isLocalDraftThread: !started,
    canOverrideServerThreadEnvMode: false,
    setPendingServerThreadEnvModeEntry: () => {},
    setPendingServerThreadStartFromOriginByThreadId: () => {},
    scheduleComposerFocus: () => {},
  });
  return (
    // Like PopoverPopup, shrink-wrap the content rather than stretching it to
    // the full test viewport. The real app CSS gives LegendList a bounded height.
    <div style={{ width: "max-content" }}>
      <ModelPickerContent
        environmentId={target.environmentId}
        activeInstanceId={selection.instanceId}
        model={selection.model}
        lockedProvider={null}
        instanceEntries={entries}
        keybindings={DEFAULT_RESOLVED_KEYBINDINGS}
        modelOptionsByInstance={
          new Map(
            entries.map((entry) => [
              entry.instanceId,
              getAppModelOptionsForInstance(DEFAULT_UNIFIED_SETTINGS, entry),
            ]),
          )
        }
        terminalOpen={false}
        searchInputRef={searchInputRef}
        onInstanceModelChange={onProviderModelSelectStable}
      />
    </div>
  );
}

it.each([
  [false, helper, fast, helper, ultrafast],
  [false, helper, ultrafast, helper, fast],
  [false, helper, fast, stock, fast],
  [false, helper, ultrafast, stock, fast],
  [false, stock, fast, helper, fast],
  [true, helper, fast, helper, ultrafast],
  [true, helper, ultrafast, helper, fast],
  [true, helper, fast, stock, fast],
  [true, helper, ultrafast, stock, fast],
  [true, stock, fast, helper, fast],
] as const)(
  "started=%s: %s / %s → %s / %s",
  async (started, fromInstance, fromModel, toInstance, toModel) => {
    const target = scopeThreadRef(
      EnvironmentId.make("picker-test"),
      ThreadId.make(`${started}-${fromInstance}-${fromModel}-${toInstance}-${toModel}`),
    );
    const store = useComposerDraftStore.getState();
    const selection = { instanceId: fromInstance, model: fromModel };
    store.setModelSelection(target, selection, { explicit: true, replaceOptions: true });
    await using screen = await render(
      <Picker selection={selection} target={target} started={started} />,
    );
    await expect
      .element(
        screen.getByRole("button", {
          name: fromInstance === helper ? "Helper profile" : "Stock Pi",
          exact: true,
        }),
      )
      .toHaveAttribute("aria-pressed", "true");
    // Each option includes its nested favorite button in its accessible name.
    const optionName = (instance: ProviderInstanceId, model: string) =>
      new RegExp(
        `^${model === fast ? "Sol Fast" : "Sol Ultrafast"} , ${instance === stock ? "Stock Pi" : "Helper profile"} (Add to|Remove from) favorites$`,
      );
    await expect
      .element(
        screen.getByRole("option", { name: optionName(fromInstance, fromModel), exact: true }),
      )
      .toHaveAttribute("aria-selected", "true");
    if (toInstance !== fromInstance) {
      await screen
        .getByRole("button", {
          name: toInstance === helper ? "Helper profile" : "Stock Pi",
          exact: true,
        })
        .click();
    }
    await screen
      .getByRole("option", { name: optionName(toInstance, toModel), exact: true })
      .click();
    const draft = useComposerDraftStore.getState().getComposerDraft(target);
    expect(draft?.activeProvider).toBe(toInstance);
    expect(draft?.modelSelectionByProvider[toInstance]?.model).toBe(toModel);
  },
);
