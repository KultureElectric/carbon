# Carbon Jetstreamer Datasource

Historical Solana transaction and block ingestion from Jetstreamer/Old
Faithful. The Carbon 2 datasource pins the first reviewed Jetstreamer revision
with Agave 4 and Transaction V1 decoding because those changes have not yet
been published as a crate release.

Transactions are held briefly until their block callback supplies historical
block time and block hash. This buffer is bounded and fails the datasource
closed if the configured limit is exhausted. Downstream sends are awaited, so
Carbon pipeline backpressure remains authoritative.

`JetstreamerDatasource::with_sequential_mode` selects Jetstreamer's ordered
range downloader. The datasource forwards Carbon cancellation to the firehose
shutdown signal and only returns success after the requested half-open slot
range completes.
