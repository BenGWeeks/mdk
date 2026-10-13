# Changelog

## Unreleased

- Allow an admin device to leave when another canonical leaf of its account remains. Proposal admission, auto-commit
  and commit validation preserve the account's admin rights; the last admin-account leaf still gets
  `AdminCannotSelfRemove`.
- `GroupAuthorityFacts::self_has_sibling_leaf` supplies the canonical sibling fact used by leave capability hints.
