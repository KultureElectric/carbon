# KultureElectric/carbon Fork

This branch carries Polaris-specific behavior on top of
[`sevenlabs-hq/carbon`](https://github.com/sevenlabs-hq/carbon) v2.0.0
(`e901103c`). Carbon v2 supplies the Solana/Agave 4 transaction model,
including transaction V1 protobuf conversion in `carbon-core`.

## Maintained v2 changes

### Account ordering metadata

- Propagate `write_version: Option<u64>` through `AccountUpdate`,
  `AccountDeletion`, `AccountMetadata`, and the account pipeline.
- Populate it from Yellowstone and Helius Laserstream, which expose the Geyser
  write version. Other account datasources use `None`.

### Yellowstone continuity and observability

- Apply bounded, lossless backpressure to authoritative account and
  transaction output instead of dropping updates when a channel is full.
- Treat a closed output channel during cancellation as expected shutdown;
  surface an unexpected closure and reconnect otherwise.
- Reject malformed protobuf transactions without ending the subscription, and
  count rejections with bounded transaction-version and reason labels.
- Track ingress bytes, messages, connection state, per-update processing time,
  inter-arrival timing, source-control progress, and output failures using
  bounded service/region/source/subscription labels.
- Send the configured `x-subscription-id` metadata on subscriptions and reuse a
  fixed slot filter to observe source progress without a second stream.

### Decoder and generator behavior

- Preserve lossless `u64` instruction discriminators in the TypeScript CLI and
  Rust renderer.
- Support strict instruction generation while leaving account collections with
  the looser traits required by downstream Carbon processors.
- Keep the Jupiter swap decoder additions needed by Polaris historical swap
  ingestion, with a reproducible regeneration script.

### Jito ShredStream on Solana 4

- Keep the datasource in the v2 workspace on Agave 4.2.2 and deserialize entries
  with the Agave wincode format.
- Reject trailing bytes and skip malformed transactions with no signatures.

## Retained legacy-provider source

Upstream v2 excludes Jetstreamer while that provider remains on its Solana v3
stack. Its Polaris source changes remain in this branch for forward-porting or
maintenance on a compatible 1.x line:

- Jetstreamer buffers transactions until its block callback supplies historical
  block time and block hash, then flushes unmatched transactions with a metric.
  The excluded Jetstreamer package is not part of the Carbon v2 build or release.

## Upstream-owned behavior

The fork no longer carries separate patches for Solana/Agave 4, transaction V1
conversion, Yellowstone protobuf compatibility, or Jetstreamer's transaction
slot index. Those behaviors are in upstream v2. The former
`yellowstone-grpc-convert` crate has been replaced by
`carbon_core::transformers::yellowstone`.

## Rebasing

Start from the intended upstream release and replay only commits whose behavior
is still absent upstream. Resolve against the upstream v2 public API, then run
the core, Yellowstone, generator, and downstream Polaris checks before moving a
production dependency pin.

```bash
git fetch upstream
git switch -c kulture/v2-polaris upstream/v2.0.0
git cherry-pick <retained-commits>
```
