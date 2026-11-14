import { act, createElement } from "react";
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vite-plus/test";
import type { AppBarAction, AppBarMenu } from "../../components/ui/AppBar.types";

// Keep the real header and terminal menu; replace only native presentation and hooks.
vi.mock("../../components/ui/AppBar", () => ({
  AppBar: (props: { actions: readonly AppBarAction[]; menus: readonly AppBarMenu[] }) =>
    createElement("div", null, [
      ...props.actions.map((action) =>
        createElement(
          "button",
          {
            key: action.accessibilityLabel,
            disabled: action.disabled,
            onClick: action.onPress,
          },
          action.accessibilityLabel,
        ),
      ),
      ...props.menus.flatMap((menu) =>
        menu.items.map((item) =>
          createElement(
            "button",
            {
              key: item.id,
              disabled: "disabled" in item ? item.disabled : undefined,
              onClick: "onPress" in item ? item.onPress : undefined,
            },
            item.title,
          ),
        ),
      ),
    ]),
}));
vi.mock("../layout/AdaptiveWorkspaceLayout", () => ({
  useAdaptiveWorkspaceLayout: () => ({ toggleAuxiliaryPane: vi.fn(), startNewTask: vi.fn() }),
  useWorkspacePaneState: () => ({ panes: { auxiliaryPaneVisible: false } }),
}));
vi.mock("../layout/workspace/useAndroidWorkspaceNavigation", () => ({
  useWorkspaceBackNavigation: () => vi.fn(),
}));
vi.mock("../../components/ui/Dialog", () => ({ alert: vi.fn(), confirm: vi.fn() }));
vi.mock("../../native/useOpenPullRequest", () => ({ useOpenPullRequest: () => vi.fn() }));
vi.mock("@react-navigation/native", () => ({ useNavigation: () => ({ navigate: vi.fn() }) }));
vi.mock("../../state/session", () => ({ useEnvironmentScope: () => false }));
vi.mock("../pull-requests/pullRequestGlyphs", () => ({ PullRequestGlyph: { merged: "merged" } }));

import { ThreadHeader } from "./ThreadHeader";

it("disables Pi launch after terminal operate access is revoked while preserving read access", () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  const rootElement = document.createElement("div");
  const root = createRoot(rootElement);
  const onOpenTerminal = vi.fn();
  const onOpenPiTerminal = vi.fn();
  const render = (canOperateTerminal: boolean) => {
    const props = {
      title: "Thread",
      subtitle: "",
      hasThreadCwd: false,
      inspectorMode: null,
      onOpenFilesInspector: vi.fn(),
      gitControls: {
        environmentId: "test",
        threadId: "test",
        currentBranch: null,
        gitStatus: null,
        gitOperationLabel: null,
        canOpenTerminal: true,
        canOperateTerminal,
        canOpenFiles: false,
        projectScripts: [],
        terminalSessions: [
          {
            terminalId: "shell",
            displayLabel: "Existing shell",
            cwd: "/workspace",
            status: "ready",
            hasRunningSubprocess: false,
          },
        ],
        onOpenTerminal,
        onOpenNewTerminal: vi.fn(),
        onOpenPiTerminal,
      },
    } as unknown as Parameters<typeof ThreadHeader>[0];
    act(() => root.render(createElement(ThreadHeader, props)));
  };
  const button = (label: string) =>
    [...rootElement.querySelectorAll("button")].find((element) => element.textContent === label)!;
  try {
    render(true);
    expect(button("Open in Pi terminal").disabled).toBe(false);
    act(() => button("Open in Pi terminal").click());
    expect(onOpenPiTerminal).toHaveBeenCalledOnce();
    render(false);
    expect(button("Open in Pi terminal").disabled).toBe(true);
    act(() => button("Open in Pi terminal").click());
    expect(onOpenPiTerminal).toHaveBeenCalledOnce();
    expect(button("Existing shell").disabled).toBe(false);
    act(() => button("Existing shell").click());
    expect(onOpenTerminal).toHaveBeenCalledWith("shell");
  } finally {
    act(() => root.unmount());
    vi.unstubAllGlobals();
  }
});
