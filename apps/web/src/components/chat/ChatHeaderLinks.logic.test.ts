import { describe, expect, it } from "vite-plus/test";
import { newestBrewTestUrl } from "./ChatHeaderLinks.logic";

const assistant = (text: string) => ({ item: { type: "assistant_message", text } });

describe("newestBrewTestUrl", () => {
  it("uses the last matching link in the newest assistant post containing one", () => {
    expect(
      newestBrewTestUrl([
        assistant("http://localhost:3398/eng-100-test"),
        assistant(
          "First [checklist](http://127.0.0.1:3398/eng-200-test). Then <http://localhost:3398/eng-201-test/?step=2#checks>.",
        ),
        { item: { type: "user_message", text: "http://localhost:3398/user-test" } },
        { item: { type: "reasoning", text: "http://localhost:3398/reasoning-test" } },
        { item: { type: "command_execution", text: "http://localhost:3398/tool-test" } },
        assistant("Done, no newer checklist."),
      ]),
    ).toBe("http://localhost:3398/eng-201-test/?step=2#checks");
  });

  it.each([
    "http://localhost:3398/eng-123-test",
    "http://127.0.0.1:3398/eng-123-test",
    "http://[::1]:3398/eng-123-test",
    "http://localhost/eng-123-test",
  ])("accepts a literal loopback HTTP checklist: %s", (url) => {
    expect(newestBrewTestUrl([assistant(`Checklist: [Test](${url}).`)])).toBe(url);
  });

  it.each([
    "https://localhost:3398/eng-123-test",
    "http://localhost.evil.test:3398/eng-123-test",
    "http://localhost:3398@evil.test/eng-123-test",
    "http://evil.test/http://localhost:3398/eng-123-test",
    "http://user:password@localhost:3398/eng-123-test",
    "http://127.1:3398/eng-123-test",
    "http://2130706433:3398/eng-123-test",
    "http://0x7f000001:3398/eng-123-test",
    "http://0.0.0.0:3398/eng-123-test",
    "http://localhost:99999/eng-123-test",
    "http://localhost:3398\\@evil.test/eng-123-test",
    "http://localhost:3398/eng-123-testing",
    "http://localhost:3398/eng-123-test/other",
    "http://localhost:3398/nested/eng-123-test",
    "http://localhost:3398/eng-123",
    "http://localhost:3398/",
    "http://localhost:3398/eng-123-test.html",
    "http://localhost:3398/eng-123-tes",
  ])("ignores non-checklist and unsafe URLs: %s", (url) => {
    expect(newestBrewTestUrl([assistant(url)])).toBeNull();
  });

  it("does not clear an earlier checklist when the newest URL is incomplete or unrelated", () => {
    expect(
      newestBrewTestUrl([
        assistant("http://localhost:3398/eng-123-test"),
        assistant("http://localhost:3398/eng-124-tes"),
      ]),
    ).toBe("http://localhost:3398/eng-123-test");
    expect(newestBrewTestUrl([])).toBeNull();
  });
});
