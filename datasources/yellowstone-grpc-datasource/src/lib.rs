use {
    async_trait::async_trait,
    carbon_core::{
        datasource::{
            AccountUpdate, Datasource, DatasourceDisconnection, DatasourceId, TransactionUpdate,
            Update, UpdateType,
        },
        error::CarbonResult,
        metrics::{Counter, Histogram, MetricsRegistry},
        transformers::yellowstone::{create_tx_meta, create_tx_versioned},
    },
    chrono::{DateTime, Utc},
    futures::{sink::SinkExt, StreamExt},
    solana_account::Account,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::HashMap,
        convert::TryFrom,
        env,
        sync::LazyLock,
        time::Duration,
    },
    tokio::sync::{mpsc, mpsc::Sender},
    tokio_util::sync::CancellationToken,
    yellowstone_grpc_client::{
        GeyserGrpcBuilder, GeyserGrpcBuilderResult, GeyserGrpcClient, ReconnectConfig,
    },
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks,
            SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdateAccountInfo,
            SubscribeUpdateTransactionInfo,
        },
        prost::Message,
        tonic::{codec::CompressionEncoding, transport::ClientTlsConfig},
    },
};

static ACCOUNT_PROCESS_TIME_NANOS: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_account_process_time_nanoseconds",
        "Time taken to process account updates in nanoseconds",
        vec![
            1_000.0,
            10_000.0,
            100_000.0,
            1_000_000.0,
            10_000_000.0,
            100_000_000.0,
            1_000_000_000.0,
        ],
    )
});
static ACCOUNT_UPDATES_RECEIVED: Counter = Counter::new(
    "yellowstone_grpc_account_updates_received_total",
    "Total account updates received from Yellowstone gRPC",
);
static ACCOUNT_DELETION_PROCESS_TIME_NANOS: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_account_deletion_process_time_nanoseconds",
        "Time taken to process account deletions in nanoseconds",
        vec![
            1_000.0,
            10_000.0,
            100_000.0,
            1_000_000.0,
            10_000_000.0,
            100_000_000.0,
            1_000_000_000.0,
        ],
    )
});
static ACCOUNT_DELETIONS_RECEIVED: Counter = Counter::new(
    "yellowstone_grpc_account_deletions_received_total",
    "Total account deletions received from Yellowstone gRPC",
);
static TRANSACTION_PROCESS_TIME_NANOS: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_transaction_process_time_nanoseconds",
        "Time taken to process transaction updates in nanoseconds",
        vec![
            1_000.0,
            10_000.0,
            100_000.0,
            1_000_000.0,
            10_000_000.0,
            100_000_000.0,
            1_000_000_000.0,
        ],
    )
});
static TRANSACTION_UPDATES_RECEIVED: Counter = Counter::new(
    "yellowstone_grpc_transaction_updates_received_total",
    "Total transaction updates received from Yellowstone gRPC",
);
static ACCOUNT_INTERARRIVAL_US: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_account_interarrival_us",
        "Inter-arrival time between account updates in microseconds",
        vec![10.0, 50.0, 100.0, 500.0, 1_000.0, 5_000.0, 10_000.0],
    )
});
static INTRA_SLOT_INTERARRIVAL_US: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_intra_slot_interarrival_us",
        "Inter-arrival time between account updates in the same slot in microseconds",
        vec![10.0, 50.0, 100.0, 500.0, 1_000.0, 5_000.0, 10_000.0],
    )
});
static SLOT_SPAN_US: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_slot_span_us",
        "Time from first to last account update in a slot in microseconds",
        vec![
            100.0, 500.0, 1_000.0, 5_000.0, 10_000.0, 50_000.0, 100_000.0,
        ],
    )
});
static SLOT_UPDATE_COUNT: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_slot_update_count",
        "Account update count observed per slot",
        vec![1.0, 10.0, 100.0, 1_000.0, 5_000.0, 10_000.0, 50_000.0],
    )
});

fn register_yellowstone_metrics() {
    let registry = MetricsRegistry::global();
    registry.register_counter(&ACCOUNT_UPDATES_RECEIVED);
    registry.register_counter(&ACCOUNT_DELETIONS_RECEIVED);
    registry.register_counter(&TRANSACTION_UPDATES_RECEIVED);
    registry.register_histogram(&ACCOUNT_PROCESS_TIME_NANOS);
    registry.register_histogram(&ACCOUNT_DELETION_PROCESS_TIME_NANOS);
    registry.register_histogram(&TRANSACTION_PROCESS_TIME_NANOS);
    registry.register_histogram(&ACCOUNT_INTERARRIVAL_US);
    registry.register_histogram(&INTRA_SLOT_INTERARRIVAL_US);
    registry.register_histogram(&SLOT_SPAN_US);
    registry.register_histogram(&SLOT_UPDATE_COUNT);
}

/// Default timeout for detecting stale connections (30 seconds)
pub const DEFAULT_STREAM_TIMEOUT_SECS: u64 = 30;
const DEFAULT_METRICS_SERVICE: &str = "unknown";
const DEFAULT_METRICS_REGION: &str = "unknown";
const DEFAULT_METRICS_SOURCE: &str = "yellowstone-grpc";
const DEFAULT_METRICS_SUBSCRIPTION: &str = "default";
const MAX_METRICS_LABEL_LEN: usize = 96;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct YellowstoneGrpcSubscriptionMetricsLabels {
    pub service: String,
    pub region: String,
    pub source: String,
    pub subscription: String,
}

impl YellowstoneGrpcSubscriptionMetricsLabels {
    pub fn new(
        service: impl Into<String>,
        region: impl Into<String>,
        source: impl Into<String>,
        subscription: impl Into<String>,
    ) -> Self {
        Self {
            service: sanitize_metric_label(service, DEFAULT_METRICS_SERVICE),
            region: sanitize_metric_label(region, DEFAULT_METRICS_REGION),
            source: sanitize_metric_label(source, DEFAULT_METRICS_SOURCE),
            subscription: sanitize_metric_label(subscription, DEFAULT_METRICS_SUBSCRIPTION),
        }
    }

    fn sanitized(self) -> Self {
        Self::new(self.service, self.region, self.source, self.subscription)
    }
}

impl Default for YellowstoneGrpcSubscriptionMetricsLabels {
    fn default() -> Self {
        Self::new(
            first_env_label(&["SERVICE_NAME", "OTEL_SERVICE_NAME", "K_SERVICE"]),
            first_env_label(&["REGION", "GCP_REGION", "AWS_REGION"]),
            first_env_label(&["YELLOWSTONE_SOURCE_NAME", "GEYSER_SOURCE_NAME"]),
            first_env_label(&["YELLOWSTONE_SUBSCRIPTION_NAME", "GEYSER_SUBSCRIPTION_NAME"]),
        )
    }
}

fn first_env_label(names: &[&str]) -> String {
    names
        .iter()
        .find_map(|name| env::var(name).ok().filter(|value| !value.trim().is_empty()))
        .unwrap_or_default()
}

fn sanitize_metric_label(value: impl Into<String>, default: &str) -> String {
    let mut sanitized = String::with_capacity(MAX_METRICS_LABEL_LEN);
    for ch in value.into().trim().chars().take(MAX_METRICS_LABEL_LEN) {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':') {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }

    if sanitized.is_empty() {
        default.to_string()
    } else {
        sanitized
    }
}

/// Initial delay before retrying a failed subscription attempt
const RECONNECT_INITIAL_DELAY_MS: u64 = 100;

/// Upper bound on the retry delay
const RECONNECT_MAX_DELAY_MS: u64 = 3_000;

#[derive(Debug)]
pub struct YellowstoneGrpcGeyserClient {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub commitment: Option<CommitmentLevel>,
    pub account_filters: HashMap<String, SubscribeRequestFilterAccounts>,
    pub transaction_filters: HashMap<String, SubscribeRequestFilterTransactions>,
    pub block_filters: BlockFilters,
    pub geyser_config: YellowstoneGrpcClientConfig,
    pub disconnect_notifier: Option<mpsc::Sender<DatasourceDisconnection>>,
    /// Timeout for detecting hung/stale connections. Default: 30 seconds.
    pub stream_timeout: Duration,
    pub metrics_labels: YellowstoneGrpcSubscriptionMetricsLabels,
}

#[derive(Debug, Clone)]
pub struct YellowstoneGrpcClientConfig {
    pub compression: Option<CompressionEncoding>,
    pub connect_timeout: Option<Duration>,
    pub timeout: Option<Duration>,
    pub max_decoding_message_size: Option<usize>,
    pub tls_config: Option<ClientTlsConfig>,
    pub tcp_nodelay: Option<bool>,
    /// When set, the client reconnects and replays inside the stream instead of
    /// surfacing the disconnect. Off by default.
    pub reconnect: Option<ReconnectConfig>,
}

impl Default for YellowstoneGrpcClientConfig {
    fn default() -> Self {
        Self {
            compression: None,
            connect_timeout: Some(Duration::from_secs(15)),
            timeout: Some(Duration::from_secs(15)),
            max_decoding_message_size: None,
            tls_config: None,
            tcp_nodelay: None,
            reconnect: None,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct BlockFilters {
    pub filters: HashMap<String, SubscribeRequestFilterBlocks>,
    pub failed_transactions: Option<bool>,
}

impl YellowstoneGrpcGeyserClient {
    /// Creates a new YellowstoneGrpcGeyserClient with optional stream timeout.
    /// If `stream_timeout` is None, defaults to 30 seconds.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: String,
        x_token: Option<String>,
        commitment: Option<CommitmentLevel>,
        account_filters: HashMap<String, SubscribeRequestFilterAccounts>,
        transaction_filters: HashMap<String, SubscribeRequestFilterTransactions>,
        block_filters: BlockFilters,
        geyser_config: YellowstoneGrpcClientConfig,
        disconnect_notifier: Option<mpsc::Sender<DatasourceDisconnection>>,
        stream_timeout: Option<Duration>,
    ) -> Self {
        YellowstoneGrpcGeyserClient {
            endpoint,
            x_token,
            commitment,
            account_filters,
            transaction_filters,
            block_filters,
            geyser_config,
            disconnect_notifier,
            stream_timeout: stream_timeout
                .unwrap_or(Duration::from_secs(DEFAULT_STREAM_TIMEOUT_SECS)),
            metrics_labels: YellowstoneGrpcSubscriptionMetricsLabels::default(),
        }
    }

    pub fn with_metrics_labels(mut self, labels: YellowstoneGrpcSubscriptionMetricsLabels) -> Self {
        self.metrics_labels = labels.sanitized();
        self
    }
}

impl YellowstoneGrpcClientConfig {
    pub const fn new(
        compression: Option<CompressionEncoding>,
        connect_timeout: Option<Duration>,
        timeout: Option<Duration>,
        max_decoding_message_size: Option<usize>,
        tls_config: Option<ClientTlsConfig>,
        tcp_nodelay: Option<bool>,
    ) -> Self {
        YellowstoneGrpcClientConfig {
            compression,
            connect_timeout,
            timeout,
            max_decoding_message_size,
            tls_config,
            tcp_nodelay,
            reconnect: None,
        }
    }

    pub fn with_reconnect(self, reconnect: ReconnectConfig) -> Self {
        YellowstoneGrpcClientConfig {
            reconnect: Some(reconnect),
            ..self
        }
    }

    pub fn geyser_config_builder(
        &self,
        mut builder: GeyserGrpcBuilder,
    ) -> GeyserGrpcBuilderResult<GeyserGrpcBuilder> {
        builder = builder.connect_timeout(self.connect_timeout.unwrap_or(Duration::from_secs(15)));

        builder = builder.timeout(self.timeout.unwrap_or(Duration::from_secs(15)));
        let tls = self
            .tls_config
            .clone()
            .unwrap_or_else(|| ClientTlsConfig::new().with_enabled_roots());
        builder = builder.tls_config(tls)?;

        if let Some(compression) = self.compression {
            builder = builder
                .send_compressed(compression)
                .accept_compressed(compression);
        }
        if let Some(val) = self.max_decoding_message_size {
            builder = builder.max_decoding_message_size(val);
        }

        if let Some(val) = self.tcp_nodelay {
            builder = builder.tcp_nodelay(val);
        }

        if let Some(reconnect) = self.reconnect.clone() {
            builder = builder.set_reconnect_config(reconnect);
        }
        Ok(builder)
    }
}

#[derive(Debug, Clone, Copy)]
enum YellowstoneGrpcUpdateType {
    Account,
    Slot,
    Transaction,
    TransactionStatus,
    Block,
    Ping,
    Pong,
    BlockMeta,
    Entry,
    Other,
}

impl YellowstoneGrpcUpdateType {
    fn from_update(update: &Option<UpdateOneof>) -> Self {
        match update {
            Some(UpdateOneof::Account(_)) => Self::Account,
            Some(UpdateOneof::Slot(_)) => Self::Slot,
            Some(UpdateOneof::Transaction(_)) => Self::Transaction,
            Some(UpdateOneof::TransactionStatus(_)) => Self::TransactionStatus,
            Some(UpdateOneof::Block(_)) => Self::Block,
            Some(UpdateOneof::Ping(_)) => Self::Ping,
            Some(UpdateOneof::Pong(_)) => Self::Pong,
            Some(UpdateOneof::BlockMeta(_)) => Self::BlockMeta,
            Some(UpdateOneof::Entry(_)) => Self::Entry,
            None => Self::Other,
        }
    }

    fn as_label(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Slot => "slot",
            Self::Transaction => "transaction",
            Self::TransactionStatus => "transaction_status",
            Self::Block => "block",
            Self::Ping => "ping",
            Self::Pong => "pong",
            Self::BlockMeta => "block_meta",
            Self::Entry => "entry",
            Self::Other => "other",
        }
    }
}

#[derive(Debug, Clone)]
struct YellowstoneGrpcUpdateMetricHandles {
    bytes: metrics::Counter,
    messages: metrics::Counter,
    message_bytes: metrics::Histogram,
}

impl YellowstoneGrpcUpdateMetricHandles {
    fn new(
        labels: &YellowstoneGrpcSubscriptionMetricsLabels,
        update_type: YellowstoneGrpcUpdateType,
    ) -> Self {
        let metric_labels = ingress_update_metric_labels(labels, update_type.as_label());

        Self {
            bytes: metrics::counter!(
                "yellowstone_grpc_ingress_bytes_total",
                metric_labels.clone()
            ),
            messages: metrics::counter!(
                "yellowstone_grpc_ingress_messages_total",
                metric_labels.clone()
            ),
            message_bytes: metrics::histogram!(
                "yellowstone_grpc_ingress_message_bytes",
                metric_labels
            ),
        }
    }

    fn record(&self, encoded_len: usize) {
        self.bytes.increment(encoded_len as u64);
        self.messages.increment(1);
        self.message_bytes.record(encoded_len as f64);
    }
}

#[derive(Debug, Clone)]
struct YellowstoneGrpcIngressMetricHandles {
    connected: metrics::Gauge,
    account: YellowstoneGrpcUpdateMetricHandles,
    slot: YellowstoneGrpcUpdateMetricHandles,
    transaction: YellowstoneGrpcUpdateMetricHandles,
    transaction_status: YellowstoneGrpcUpdateMetricHandles,
    block: YellowstoneGrpcUpdateMetricHandles,
    ping: YellowstoneGrpcUpdateMetricHandles,
    pong: YellowstoneGrpcUpdateMetricHandles,
    block_meta: YellowstoneGrpcUpdateMetricHandles,
    entry: YellowstoneGrpcUpdateMetricHandles,
    other: YellowstoneGrpcUpdateMetricHandles,
}

impl YellowstoneGrpcIngressMetricHandles {
    fn new(labels: YellowstoneGrpcSubscriptionMetricsLabels) -> Self {
        let connection_labels = ingress_connection_metric_labels(&labels);

        Self {
            connected: metrics::gauge!(
                "yellowstone_grpc_subscription_connected",
                connection_labels
            ),
            account: YellowstoneGrpcUpdateMetricHandles::new(
                &labels,
                YellowstoneGrpcUpdateType::Account,
            ),
            slot: YellowstoneGrpcUpdateMetricHandles::new(&labels, YellowstoneGrpcUpdateType::Slot),
            transaction: YellowstoneGrpcUpdateMetricHandles::new(
                &labels,
                YellowstoneGrpcUpdateType::Transaction,
            ),
            transaction_status: YellowstoneGrpcUpdateMetricHandles::new(
                &labels,
                YellowstoneGrpcUpdateType::TransactionStatus,
            ),
            block: YellowstoneGrpcUpdateMetricHandles::new(
                &labels,
                YellowstoneGrpcUpdateType::Block,
            ),
            ping: YellowstoneGrpcUpdateMetricHandles::new(&labels, YellowstoneGrpcUpdateType::Ping),
            pong: YellowstoneGrpcUpdateMetricHandles::new(&labels, YellowstoneGrpcUpdateType::Pong),
            block_meta: YellowstoneGrpcUpdateMetricHandles::new(
                &labels,
                YellowstoneGrpcUpdateType::BlockMeta,
            ),
            entry: YellowstoneGrpcUpdateMetricHandles::new(
                &labels,
                YellowstoneGrpcUpdateType::Entry,
            ),
            other: YellowstoneGrpcUpdateMetricHandles::new(
                &labels,
                YellowstoneGrpcUpdateType::Other,
            ),
        }
    }

    fn set_connected(&self, connected: bool) {
        self.connected.set(if connected { 1.0 } else { 0.0 });
    }

    fn record_message(&self, update: &Option<UpdateOneof>, encoded_len: usize) {
        let handles = match YellowstoneGrpcUpdateType::from_update(update) {
            YellowstoneGrpcUpdateType::Account => &self.account,
            YellowstoneGrpcUpdateType::Slot => &self.slot,
            YellowstoneGrpcUpdateType::Transaction => &self.transaction,
            YellowstoneGrpcUpdateType::TransactionStatus => &self.transaction_status,
            YellowstoneGrpcUpdateType::Block => &self.block,
            YellowstoneGrpcUpdateType::Ping => &self.ping,
            YellowstoneGrpcUpdateType::Pong => &self.pong,
            YellowstoneGrpcUpdateType::BlockMeta => &self.block_meta,
            YellowstoneGrpcUpdateType::Entry => &self.entry,
            YellowstoneGrpcUpdateType::Other => &self.other,
        };

        handles.record(encoded_len);
    }
}

fn ingress_connection_metric_labels(
    labels: &YellowstoneGrpcSubscriptionMetricsLabels,
) -> Vec<metrics::Label> {
    vec![
        metrics::Label::new("service", labels.service.clone()),
        metrics::Label::new("region", labels.region.clone()),
        metrics::Label::new("source", labels.source.clone()),
        metrics::Label::new("subscription", labels.subscription.clone()),
    ]
}

fn ingress_update_metric_labels(
    labels: &YellowstoneGrpcSubscriptionMetricsLabels,
    update_type: &'static str,
) -> Vec<metrics::Label> {
    let mut metric_labels = ingress_connection_metric_labels(labels);
    metric_labels.push(metrics::Label::new("update_type", update_type));
    metric_labels
}

#[async_trait]
impl Datasource for YellowstoneGrpcGeyserClient {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: Sender<(Update, DatasourceId)>,
        cancellation_token: CancellationToken,
    ) -> CarbonResult<()> {
        register_yellowstone_metrics();
        let endpoint = self.endpoint.clone();
        let x_token = self.x_token.clone();
        let commitment = self.commitment;
        let account_filters = self.account_filters.clone();
        let transaction_filters = self.transaction_filters.clone();
        let BlockFilters {
            filters,
            failed_transactions: block_failed_transactions,
        } = self.block_filters.clone();
        let retain_block_failed_transactions = block_failed_transactions.unwrap_or(true);

        let builder = GeyserGrpcClient::build_from_shared(endpoint)
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?
            .x_token(x_token)
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?;

        let mut geyser_client = self
            .geyser_config
            .geyser_config_builder(builder)
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?
            .connect()
            .await
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?;

        let disconnect_tx_clone = self.disconnect_notifier.clone();
        let stream_timeout = self.stream_timeout;
        let ingress_metrics = YellowstoneGrpcIngressMetricHandles::new(self.metrics_labels.clone());

        tokio::spawn(async move {
            let subscribe_request = SubscribeRequest {
                slots: HashMap::new(),
                accounts: account_filters,
                transactions: transaction_filters,
                transactions_status: HashMap::new(),
                entry: HashMap::new(),
                blocks: filters,
                blocks_meta: HashMap::new(),
                commitment: commitment.map(|x| x as i32),
                accounts_data_slice: vec![],
                ping: None,
                from_slot: None,
            };

            let id_for_loop = id.clone();

            let mut last_disconnect_time: Option<DateTime<Utc>> = None;
            let mut last_slot_before_disconnect: Option<u64> = None;
            let mut last_processed_slot: u64 = 0;
            let mut reconnect_delay = Duration::from_millis(RECONNECT_INITIAL_DELAY_MS);

            // Inter-arrival timing tracking for account updates (global)
            let mut last_account_arrival: Option<std::time::Instant> = None;
            let mut arrival_count: u64 = 0;
            let mut total_delta_us: u64 = 0;
            let mut min_delta_us: u64 = u64::MAX;
            let mut max_delta_us: u64 = 0;

            // Intra-slot timing tracking (per-slot metrics)
            let mut current_slot: Option<u64> = None;
            let mut slot_first_arrival: Option<std::time::Instant> = None;
            let mut slot_last_arrival: Option<std::time::Instant> = None;
            let mut slot_update_count: u64 = 0;

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        log::info!("Cancelling Yellowstone gRPC subscription.");
                        ingress_metrics.set_connected(false);
                        // Log final arrival stats
                        if arrival_count > 1 {
                            let avg_delta_us = total_delta_us / (arrival_count - 1);
                            log::info!(
                                "Account arrival stats: count={}, avg_delta={}us, min={}us, max={}us",
                                arrival_count, avg_delta_us, min_delta_us, max_delta_us
                            );
                        }
                        break;
                    }
                    result = geyser_client.subscribe_with_request(Some(subscribe_request.clone())) => {
                        match result {
                            Ok((mut subscribe_tx, mut stream)) => {
                                reconnect_delay = Duration::from_millis(RECONNECT_INITIAL_DELAY_MS);
                                ingress_metrics.set_connected(true);
                                let mut first_message_after_reconnect = last_disconnect_time.is_some();

                                loop {
                                    if cancellation_token.is_cancelled() {
                                        break;
                                    }

                                    let message_result = tokio::time::timeout(
                                        stream_timeout,
                                        stream.next()
                                    ).await;

                                    let message = match message_result {
                                        Ok(Some(msg)) => msg,
                                        Ok(None) => {
                                            log::warn!("Stream closed");
                                            if last_disconnect_time.is_none() {
                                                last_disconnect_time = Some(Utc::now());
                                                last_slot_before_disconnect = Some(last_processed_slot);
                                                log::warn!("Disconnected at slot {last_processed_slot}");
                                            }
                                            break;
                                        }
                                        Err(_) => {
                                            log::warn!("Stream timeout - no messages for {stream_timeout:?}");
                                            if last_disconnect_time.is_none() {
                                                last_disconnect_time = Some(Utc::now());
                                                last_slot_before_disconnect = Some(last_processed_slot);
                                                log::warn!("Disconnected at slot {last_processed_slot} (timeout)");
                                            }
                                            break;
                                        }
                                    };

                                    match message {
                                        Ok(msg) => {
                                            ingress_metrics.record_message(
                                                &msg.update_oneof,
                                                msg.encoded_len(),
                                            );

                                            if first_message_after_reconnect {
                                                let current_slot = match &msg.update_oneof {
                                                    Some(UpdateOneof::Account(ref update)) => Some(update.slot),
                                                    Some(UpdateOneof::Transaction(ref update)) => Some(update.slot),
                                                    Some(UpdateOneof::Block(ref update)) => Some(update.slot),
                                                    _ => None,
                                                };

                                                if let Some(slot) = current_slot {
                                                    first_message_after_reconnect = false;

                                                    if let (Some(disconnect_time), Some(last_slot)) =
                                                        (last_disconnect_time.take(), last_slot_before_disconnect.take())
                                                    {
                                                        let missed = slot.saturating_sub(last_slot);

                                                        let disconnection = DatasourceDisconnection {
                                                            source: "yellowstone-grpc".to_string(),
                                                            disconnect_time,
                                                            last_slot_before_disconnect: last_slot,
                                                            first_slot_after_reconnect: slot,
                                                            missed_slots: missed,
                                                        };

                                                        if let Some(tx) = &disconnect_tx_clone {
                                                            let _ = tx.try_send(disconnection);
                                                        }

                                                        log::info!("Reconnected. Slots: {last_slot} -> {slot} (missed: {missed})");
                                                    }
                                                }
                                            }

                                            match msg.update_oneof {
                                            Some(UpdateOneof::Account(account_update)) => {
                                                let arrival_time = std::time::Instant::now();
                                                let update_slot = account_update.slot;
                                                last_processed_slot = update_slot;

                                                // Check if slot changed - emit metrics for previous slot
                                                if let Some(prev_slot) = current_slot {
                                                    if update_slot != prev_slot {
                                                        if let (Some(first), Some(last)) = (slot_first_arrival, slot_last_arrival) {
                                                            let span_us = last.duration_since(first).as_micros() as u64;
                                                            SLOT_SPAN_US.record(span_us as f64);
                                                        }
                                                        SLOT_UPDATE_COUNT.record(slot_update_count as f64);

                                                        // Reset for new slot
                                                        slot_first_arrival = Some(arrival_time);
                                                        slot_last_arrival = Some(arrival_time);
                                                        slot_update_count = 1;
                                                        current_slot = Some(update_slot);
                                                    } else {
                                                        // Same slot - track intra-slot inter-arrival
                                                        if let Some(last_slot_arrival) = slot_last_arrival {
                                                            let intra_delta_us = arrival_time.duration_since(last_slot_arrival).as_micros() as u64;
                                                            INTRA_SLOT_INTERARRIVAL_US.record(intra_delta_us as f64);
                                                        }
                                                        slot_last_arrival = Some(arrival_time);
                                                        slot_update_count += 1;
                                                    }
                                                } else {
                                                    current_slot = Some(update_slot);
                                                    slot_first_arrival = Some(arrival_time);
                                                    slot_last_arrival = Some(arrival_time);
                                                    slot_update_count = 1;
                                                }

                                                // Track global inter-arrival timing
                                                if let Some(last_arrival) = last_account_arrival {
                                                    let delta_us = arrival_time.duration_since(last_arrival).as_micros() as u64;
                                                    total_delta_us += delta_us;
                                                    min_delta_us = min_delta_us.min(delta_us);
                                                    max_delta_us = max_delta_us.max(delta_us);
                                                    ACCOUNT_INTERARRIVAL_US.record(delta_us as f64);
                                                }
                                                last_account_arrival = Some(arrival_time);
                                                arrival_count += 1;

                                                if arrival_count > 1 && arrival_count % 5000 == 0 {
                                                    let avg_delta_us = total_delta_us / (arrival_count - 1);
                                                    log::info!(
                                                        "Account arrival stats (slot {}): count={}, avg_delta={}us, min={}us, max={}us",
                                                        update_slot, arrival_count, avg_delta_us, min_delta_us, max_delta_us
                                                    );
                                                }

                                                send_subscribe_account_update_info(
                                                    account_update.account,
                                                    &sender,
                                                    id_for_loop.clone(),
                                                    update_slot,
                                                )
                                                .await
                                            }

                                            Some(UpdateOneof::Transaction(transaction_update)) => {
                                                last_processed_slot = transaction_update.slot;
                                                send_subscribe_update_transaction_info(
                                                    transaction_update.transaction,
                                                    &sender,
                                                    id_for_loop.clone(),
                                                    transaction_update.slot,
                                                    None,
                                                )
                                                .await
                                            }
                                            Some(UpdateOneof::Block(block_update)) => {
                                                last_processed_slot = block_update.slot;
                                                let block_time = block_update.block_time.map(|ts| ts.timestamp);

                                                for transaction_update in block_update.transactions {
                                                    if retain_block_failed_transactions || transaction_update.meta.as_ref().map(|meta| meta.err.is_none()).unwrap_or(false) {
                                                        send_subscribe_update_transaction_info(Some(transaction_update), &sender, id_for_loop.clone(), block_update.slot, block_time).await
                                                    }
                                                }

                                                for account_info in block_update.accounts {
                                                    send_subscribe_account_update_info(
                                                        Some(account_info),
                                                        &sender,
                                                        id_for_loop.clone(),
                                                        block_update.slot,
                                                    )
                                                    .await;
                                                }
                                            }

                                            Some(UpdateOneof::Ping(_)) => {
                                                // Sink replays the last request on reconnect.
                                                match subscribe_tx
                                                    .send(SubscribeRequest {
                                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                                        ..subscribe_request.clone()
                                                    })
                                                    .await {
                                                        Ok(()) => (),
                                                        Err(error) => {
                                                            log::error!("Failed to send ping error: {error:?}");
                                                            break;
                                                        },
                                                    }
                                            }

                                            _ => {}
                                        }
                                        }
                                        Err(error) => {
                                            log::error!("Geyser stream error: {error:?}");

                                            if last_disconnect_time.is_none() {
                                                last_disconnect_time = Some(Utc::now());
                                                last_slot_before_disconnect = Some(last_processed_slot);
                                                log::error!("Disconnected at slot {last_processed_slot}");
                                            }

                                            break;
                                        }
                                    }
                                }
                                ingress_metrics.set_connected(false);
                            }
                            Err(e) => {
                                log::error!("Failed to subscribe: {e:?}");
                                ingress_metrics.set_connected(false);

                                if last_disconnect_time.is_none() {
                                    last_disconnect_time = Some(Utc::now());
                                    last_slot_before_disconnect = Some(last_processed_slot);
                                }

                                tokio::select! {
                                    _ = cancellation_token.cancelled() => {
                                        log::info!("Cancelling Yellowstone gRPC subscription.");
                                        break;
                                    }
                                    _ = tokio::time::sleep(reconnect_delay) => {}
                                }

                                reconnect_delay = (reconnect_delay * 2)
                                    .min(Duration::from_millis(RECONNECT_MAX_DELAY_MS));
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    fn update_types(&self) -> Vec<UpdateType> {
        vec![
            UpdateType::AccountUpdate,
            UpdateType::Transaction,
            UpdateType::AccountDeletion,
        ]
    }
}

async fn send_subscribe_account_update_info(
    account_update_info: Option<SubscribeUpdateAccountInfo>,
    sender: &Sender<(Update, DatasourceId)>,
    id: DatasourceId,
    slot: u64,
) {
    let start_time = std::time::Instant::now();

    if let Some(account_info) = account_update_info {
        let Ok(account_pubkey) = Pubkey::try_from(account_info.pubkey) else {
            return;
        };

        let Ok(account_owner_pubkey) = Pubkey::try_from(account_info.owner) else {
            return;
        };

        let account = Account {
            lamports: account_info.lamports,
            data: account_info.data,
            owner: account_owner_pubkey,
            executable: account_info.executable,
            rent_epoch: account_info.rent_epoch,
        };

        let update = AccountUpdate {
            pubkey: account_pubkey,
            account,
            slot,
            transaction_signature: account_info
                .txn_signature
                .and_then(|sig| Signature::try_from(sig).ok()),
            write_version: Some(account_info.write_version),
        }
        .into_update();
        let is_deletion = matches!(&update, Update::AccountDeletion(_));

        if let Err(e) = sender.try_send((update, id)) {
            log::error!(
                "Failed to send account event for pubkey {account_pubkey:?} at slot {slot}: {e:?}"
            );
        }

        if is_deletion {
            ACCOUNT_DELETION_PROCESS_TIME_NANOS.record(start_time.elapsed().as_nanos() as f64);
            ACCOUNT_DELETIONS_RECEIVED.inc();
        } else {
            ACCOUNT_PROCESS_TIME_NANOS.record(start_time.elapsed().as_nanos() as f64);
            ACCOUNT_UPDATES_RECEIVED.inc();
        }
    } else {
        log::error!("No account info in UpdateOneof::Account at slot {slot}");
    }
}

async fn send_subscribe_update_transaction_info(
    transaction_info: Option<SubscribeUpdateTransactionInfo>,
    sender: &Sender<(Update, DatasourceId)>,
    id: DatasourceId,
    slot: u64,
    block_time: Option<i64>,
) {
    let start_time = std::time::Instant::now();

    if let Some(transaction_info) = transaction_info {
        let Ok(signature) = Signature::try_from(transaction_info.signature) else {
            return;
        };
        let Some(yellowstone_transaction) = transaction_info.transaction else {
            return;
        };
        let Some(yellowstone_tx_meta) = transaction_info.meta else {
            return;
        };
        let Ok(versioned_transaction) = create_tx_versioned(yellowstone_transaction) else {
            return;
        };
        let meta_original = match create_tx_meta(yellowstone_tx_meta) {
            Ok(meta) => meta,
            Err(err) => {
                log::error!("Failed to create transaction meta: {err:?}");
                return;
            }
        };
        let update = Update::Transaction(Box::new(TransactionUpdate {
            signature,
            transaction: versioned_transaction,
            meta: meta_original,
            is_vote: transaction_info.is_vote,
            slot,
            index: Some(transaction_info.index),
            block_time,
            block_hash: None,
        }));
        if let Err(e) = sender.try_send((update, id)) {
            log::error!(
                "Failed to send transaction update with signature {signature:?} at slot {slot}: {e:?}"
            );
            return;
        }

        TRANSACTION_PROCESS_TIME_NANOS.record(start_time.elapsed().as_nanos() as f64);
        TRANSACTION_UPDATES_RECEIVED.inc();
    } else {
        log::error!("No transaction info in `UpdateOneof::Transaction` at slot {slot}");
    }
}
