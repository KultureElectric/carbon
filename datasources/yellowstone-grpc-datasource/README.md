# Carbon Yellowstone gRPC Datasource

## Ingress Metrics

The datasource records decoded protobuf payload size for every `SubscribeUpdate`
received from Yellowstone gRPC:

- `yellowstone_grpc_ingress_bytes_total`
- `yellowstone_grpc_ingress_messages_total`
- `yellowstone_grpc_ingress_message_bytes`
- `yellowstone_grpc_subscription_connected`

Metrics are labelled with `service`, `region`, `source`, and `subscription`.
Payload metrics also include `update_type`. Defaults are read from
`SERVICE_NAME`, `REGION`, `YELLOWSTONE_SOURCE_NAME`, and
`YELLOWSTONE_SUBSCRIPTION_NAME`; services can override them with
`YellowstoneGrpcGeyserClient::with_metrics_labels`.
