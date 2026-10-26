# auth_wal

An authenticated, bounded, in-memory set of recent writes that sits between the
EVM validators (`Avax-pEVM`, `libevm`) and an authenticated storage engine
(`qmdb-ffi` or `firewood`), exposing the same Go `ffi` interface so validators
need no code changes.

- `docs/00-project-description.md`: the requirements as stated.
- `docs/01-design.md`: the design (write set, eviction policy, vector
  commitment, proposal tree, speculative eviction, benchmarks).
- `docs/TODO.md`: deferred work, including the firewood-side changes that
  would remove the per-link commit and rehashing costs.

Status: skeleton. Design v9 (design §14) reworks speculative eviction around
firewood and adds the `Prefetch` hint API after the project owner's
clarifications (requirements R13, R14), reviewed in rounds 7–9; v1–v6 went
through five review rounds plus a spot-check. Module structure, traits and
generic types are in place and every check below passes; all bodies are
`todo!()`. Next step is milestone 1 (design §13).

```bash
cargo check --workspace --all-features            # lib + benchmark
cargo check -p auth_wal --no-default-features      # feature checks must use -p: the benchmark
cargo check -p auth_wal --features merkle,kzg      # crate re-enables lthash/merkle for the workspace
cargo clippy --workspace --all-features --all-targets
cargo doc -p auth_wal --no-deps --all-features
cargo fmt --all -- --check                        # rustfmt.toml: max_width = 120
cargo build --release                             # target/release/libauth_wal.a for the Go shim
```
