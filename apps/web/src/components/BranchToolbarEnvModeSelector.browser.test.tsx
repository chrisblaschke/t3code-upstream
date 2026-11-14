import { describe, expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

import { render } from "../../test/render";
import { BranchToolbarEnvModeSelector } from "./BranchToolbarEnvModeSelector";

vi.mock(import("@effect/atom-react"), async (original) => ({
  ...(await original()),
  useAtomValue: (() => []) as never,
}));

describe("workspace picker", () => {
  it.each([false, true])(
    "offers only usable checkouts when multi-model send is %s",
    async (forceNewWorktree) => {
      const onEnvModeChange = vi.fn();
      await using screen = await render(
        <BranchToolbarEnvModeSelector
          forceNewWorktree={forceNewWorktree}
          riftUsable
          envLocked={false}
          effectiveEnvMode="worktree"
          activeWorktreePath={null}
          onEnvModeChange={onEnvModeChange}
        />,
      );
      await screen.getByRole("combobox", { name: "Workspace" }).click();
      const current = page.getByRole("option", { name: "Current checkout" });
      if (forceNewWorktree) {
        await expect.element(current).not.toBeInTheDocument();
      } else {
        await expect.element(current).toBeVisible();
      }
      await page.getByRole("option", { name: "Copy (rift)" }).click();
      expect(onEnvModeChange).toHaveBeenCalledWith("rift");
    },
  );
});
