export function newestBrewTestUrl(
  rows: ReadonlyArray<{
    readonly item: { readonly type: string; readonly text?: string };
  }>,
): string | null {
  for (let index = rows.length - 1; index >= 0; index--) {
    const item = rows[index]!.item;
    if (item.type !== "assistant_message" || !item.text) continue;
    let newest: string | null = null;
    for (const match of item.text.matchAll(/(?:^|[\s([<"'`])(http:\/\/[^\s<>"'`]+)/giu)) {
      const candidate = match[1]!.replace(/[)\]},.;:!]+$/u, "");
      // Validate the literal authority before URL normalizes alternate IP spellings or backslashes.
      if (
        !/^http:\/\/(?:localhost|127\.0\.0\.1|\[::1\])(?::\d+)?\//iu.test(candidate) ||
        candidate.includes("\\")
      ) {
        continue;
      }
      try {
        const url = new URL(candidate);
        if (/^\/[a-z0-9][a-z0-9_-]*-test\/?$/iu.test(url.pathname)) newest = url.href;
      } catch {
        // Incomplete streamed URLs and invalid ports are not links yet.
      }
    }
    if (newest !== null) return newest;
  }
  return null;
}
