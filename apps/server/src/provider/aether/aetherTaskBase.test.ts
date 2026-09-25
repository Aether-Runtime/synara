import { describe, expect, it } from "vitest";

import { repositorySlug } from "./aetherTaskBase.ts";

describe("repositorySlug", () => {
  it.each([
    ["https://github.com/Acme/Widgets.git", "github.com/acme/widgets"],
    ["https://github.com/acme/widgets", "github.com/acme/widgets"],
    ["https://token@github.com/acme/widgets/", "github.com/acme/widgets"],
    ["git@github.com:acme/widgets.git", "github.com/acme/widgets"],
    ["ssh://git@github.com:22/acme/widgets.git", "github.com/acme/widgets"],
    ["https://gitlab.com/acme/widgets.git", "gitlab.com/acme/widgets"],
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
