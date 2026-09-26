// FILE: nonInteractiveGitEnv.ts
// Purpose: The environment for a Git command that talks to a remote on the server's behalf and
// must never wait on a person: it fails instead of prompting.
// Layer: Server git boundary.

/**
 * No terminal prompt (`GIT_TERMINAL_PROMPT`), no Git Credential Manager sign-in window
 * (`GCM_INTERACTIVE`), and no askpass program for Git or ssh (`GIT_ASKPASS`, `SSH_ASKPASS`,
 * `SSH_ASKPASS_REQUIRE`): a missing or expired credential fails the command.
 */
export const NON_INTERACTIVE_GIT_ENV = {
  GCM_INTERACTIVE: "never",
  GIT_ASKPASS: "",
  GIT_TERMINAL_PROMPT: "0",
  SSH_ASKPASS: "",
  SSH_ASKPASS_REQUIRE: "never",
} as const;
