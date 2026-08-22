# Carbon Yellowstone gRPC Datasource

## Ingress Metrics

The datasource records decoded protobuf payload size for every `SubscribeUpdate`
received from Yellowstone gRPC:

- `yellowstone_grpc_ingress_bytes_total`
- `yellowstone_grpc_ingress_messages_total`
- `yellowstone_grpc_ingress_message_bytes`
- `yellowstone_grpc_subscription_connected`
- `yellowstone_grpc_last_control_progress_timestamp_seconds`
- `yellowstone_grpc_last_ping_timestamp_seconds`
- `yellowstone_grpc_last_slot`
- `yellowstone_grpc_last_slot_progress_timestamp_seconds`
- `yellowstone_grpc_transaction_rejections_total`

Metrics are labelled with `service`, `region`, `source`, and `subscription`.
Payload metrics also include `update_type`. Defaults are read from
`SERVICE_NAME`, `REGION`, `YELLOWSTONE_SOURCE_NAME`, and
`YELLOWSTONE_SUBSCRIPTION_NAME`; services can override them with
`YellowstoneGrpcGeyserClient::with_metrics_labels`.

The datasource adds one fixed slot filter to its existing subscription for
source-control progress; it does not open a second subscription or emit slot
updates into the Carbon pipeline. Control progress follows the datasource's
any-message timeout semantics. Operators can calculate ages with PromQL such
as `time() - yellowstone_grpc_last_control_progress_timestamp_seconds` while
keeping account and transaction activity as separate workload signals.

Transaction rejection metrics add only the closed labels
`transaction_version` (`legacy`, `v0`, `v1`, `unknown`) and `reason`. They never
use signatures, arbitrary error text, or exact sizes as labels.
