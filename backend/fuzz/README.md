# Adapter fuzzing

The fuzz workspace is intentionally separate from `backend/Cargo.toml`; it is
not built by normal workspace commands and requires nightly Rust through
`cargo-fuzz`.

Install the tool once with `cargo install cargo-fuzz`, then run from this
directory:

```sh
cargo +nightly fuzz run binance_adapter -- -max_len=4096
cargo +nightly fuzz run aster_adapter -- -max_len=4096
cargo +nightly fuzz run lighter_adapter -- -max_len=4096
```

The targets start from small deterministic protocol fixtures, then feed
arbitrary UTF-8-decoded text through each active adapter's public text entry
point. The committed corpus uses protocol-shaped fixtures from the adapter test
data; these are not claimed to be verified live-captured frames. Any panic or invalid published Order Book snapshot fails the fuzz run.
Promote every discovered failure to a deterministic unit regression test
before fixing it. Corpus files are intentionally small protocol frames.
