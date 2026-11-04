import { expect, it } from "vite-plus/test";

import { toggleThreadFollowUp } from "./threadStatusPresentation.ts";

it.each(["waiting-on-others", "circle-back"] as const)(
  "sets, replaces, and explicitly dismisses %s without retaining the previous mark",
  (selected) => {
    expect(toggleThreadFollowUp(undefined, selected)).toBe(selected);
    expect(toggleThreadFollowUp(null, selected)).toBe(selected);
    const other = selected === "circle-back" ? "waiting-on-others" : "circle-back";
    const replaced = toggleThreadFollowUp(selected, other);
    expect(replaced).toBe(other);
    expect(toggleThreadFollowUp(replaced, other)).toBeNull();
    expect(toggleThreadFollowUp(selected, selected)).toBeNull();
  },
);
