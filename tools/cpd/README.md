# Duplicate code check

`aspect check-cpd` runs jscpd 5.4.1 with a zero-duplicate gate over Rust sources.
The `cpd` CI job remains required by the merge gate. The minimum clone size is
60 tokens and 5 lines, in weak mode (whitespace and comments are ignored).

The AXL task copies Git's tracked and non-ignored untracked Rust inventory to a
temporary directory, including tests, benchmarks and hidden paths. Deleted
sources are omitted; missing sources, sparse checkouts and symlinks fail.
The isolated scan uses an explicit empty config, so local jscpd settings and
Git ignore files cannot weaken the check. No baseline or file exclusions apply.

Git, Node.js 18+ and npm are required. `npm ci --ignore-scripts` installs the
exact detector version and checks artifact integrity using `package-lock.json`.
The npm package selects the pinned native binary for the host platform.

Reports and `source-inventory.json` are saved under `.cpd/` locally, or in the
`cpd` CI artifact. AXL tests run with `aspect axl-tests --suite check-cpd`.
