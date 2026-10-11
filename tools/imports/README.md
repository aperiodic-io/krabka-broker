# Imported Rust paths

Rust code should import an item where it is used instead of referring to it
through `crate::...`. In code, use the imported item name or an imported
`module::item` path. Use declarations, documentation, attributes, and macro
bodies are outside this rule because their path resolution has different
semantics.

Run the AXL check locally with:

```sh
aspect check-imported-paths
```

CI runs the same check in the `jscpd` job.
