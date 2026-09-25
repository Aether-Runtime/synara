import { describe, expect, it } from "vitest";

import { repositorySlug } from "./aetherTaskBase.ts";

describe("repositorySlug", () => {
  it.each([
    ["https://github.com/Acme/Widgets.git", "acme/widgets"],
    ["https://github.com/acme/widgets", "acme/widgets"],
    ["https://token@github.com/acme/widgets/", "acme/widgets"],
    ["git@github.com:acme/widgets.git", "acme/widgets"],
    ["ssh://git@github.com:22/acme/widgets.git", "acme/widgets"],
  ])("reads %s as %s", (url, slug) => {
    expect(repositorySlug(url)).toBe(slug);
  });

  it.each(["", "/srv/git/widgets.git", "https://gitlab.com/group/sub/widgets.git"])(
    "refuses %j",
    (url) => {
      expect(repositorySlug(url)).toBeUndefined();
    },
  );
});
