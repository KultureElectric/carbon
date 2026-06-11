# KultureElectric/carbon Fork

Changes maintained on top of [sevenlabs-hq/carbon](https://github.com/sevenlabs-hq/carbon) `main`.

## Changes

### `write_version` on AccountUpdate / AccountMetadata
- Added `write_version: Option<u64>` to `AccountUpdate` (datasource.rs) and `AccountMetadata` (account.rs)
- Propagated through the pipeline (pipeline.rs)
- Set from yellowstone-grpc and helius-laserstream datasources (which expose it from geyser)
- Set to `None` for datasources that don't provide it (rpc-gpa, rpc-program-subscribe, helius-atlas-ws, helius-gpa-v2, validator-snapshot)
- Used downstream for ordering account updates within a slot

### ShredStream empty-signature guard
- `jito-shredstream-grpc-datasource`: skip transactions with empty signatures array
- `get_signature()` panics on malformed transactions; this guard prevents crashes

### Yellowstone inter-arrival timing metrics
- Intra-slot span histograms (`yellowstone_grpc_slot_span_us`)
- Per-slot update count (`yellowstone_grpc_slot_update_count`)
- Intra-slot inter-arrival deltas (`yellowstone_grpc_intra_slot_interarrival_us`)
- Global account inter-arrival deltas (`yellowstone_grpc_account_interarrival_us`)
- Debug logging every 5000 account updates, final stats on cancellation

### Yellowstone ingress byte metrics
- Decoded protobuf payload byte counter (`yellowstone_grpc_ingress_bytes_total`)
- Message counter (`yellowstone_grpc_ingress_messages_total`)
- Message size histogram (`yellowstone_grpc_ingress_message_bytes`)
- Subscription connection gauge (`yellowstone_grpc_subscription_connected`)
- Labels: `service`, `region`, `source`, `subscription`, and `update_type` for payload metrics

### Already upstream in v1
- Jetstreamer now uses `transaction.transaction_slot_index` for the transaction index field

## Rebasing onto upstream

```bash
git fetch upstream
git switch -c kulture/upstream-v1-polaris upstream/main
# Re-apply the fork commits that still differ from upstream.
git cherry-pick <write_version> <shredstream-empty-signature> <yellowstone-timing> <fork-docs> <yellowstone-ingress-bytes>
git push origin kulture/upstream-v1-polaris
```
