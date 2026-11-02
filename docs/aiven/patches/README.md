# Patch dossiers {#patch-dossiers}

One file per Aiven patch identity: `<NNN>-<slug>.md`. The slug is the stable
identity; the file name and its frontmatter slug carry no version, so the same
file moves from line to line.

- Template: [`../skills/dossier-template.md`](../skills/dossier-template.md)
- A dossier is staged in the same commit as its patch's code and tests, and
  every later `patch-fix` updates it in that same commit
  ([commit shape](../AGENTS.md#commit-shape)).
- On a new line, a dossier arrives with its patch's first commit there — taken
  from the prior `-dev` line with `git checkout <prior-dev> -- <file>` and made
  true for the code as ported. It is never imported in bulk.
- A dropped patch still gets its dossier, in a `patch-drop` commit, so the
  reason survives into the next uplift.

This directory is empty until the first patch lands on the line.
