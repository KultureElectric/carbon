use {
    crate::{
        filter::{JetstreamerFilter, TransactionFilter},
        range::JetstreamerRange,
    },
    async_trait::async_trait,
    carbon_core::{
        datasource::{
            BlockDetails, Datasource, DatasourceId, TransactionUpdate, Update, UpdateType,
        },
        error::CarbonResult,
        metrics::{Counter, Gauge, MetricsRegistry},
    },
    futures_util::FutureExt,
    jetstreamer_firehose::firehose::{
        BlockData, EntryData, HandlerFn, OnErrorFn, RewardsData, Stats, StatsTracking,
        TransactionData,
    },
    solana_transaction_status_client_types::Reward,
    std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    },
    tokio::sync::{broadcast, Mutex},
    tokio_util::sync::CancellationToken,
};

static BLOCKS_SENT: Counter = Counter::new(
    "jetstreamer_blocks_sent_total",
    "Block details sent by Jetstreamer datasource",
);
static TRANSACTIONS_SENT: Counter = Counter::new(
    "jetstreamer_transactions_sent_total",
    "Transactions sent by Jetstreamer datasource",
);
static TRANSACTIONS_FILTERED_OUT: Counter = Counter::new(
    "jetstreamer_transactions_filtered_out_total",
    "Transactions filtered out by Jetstreamer datasource (did not match filters)",
);
static TRANSACTIONS_FILTERED_IN: Counter = Counter::new(
    "jetstreamer_transactions_filtered_in_total",
    "Transactions that passed filters (before send) in Jetstreamer datasource",
);
static INTERNAL_SLOTS_PROCESSED: Gauge = Gauge::new(
    "jetstreamer_internal_slots_processed",
    "Internal firehose slots processed (from Stats)",
);
static INTERNAL_BLOCKS_PROCESSED: Gauge = Gauge::new(
    "jetstreamer_internal_blocks_processed",
    "Internal firehose blocks processed (from Stats)",
);
static INTERNAL_TRANSACTIONS_PROCESSED: Gauge = Gauge::new(
    "jetstreamer_internal_transactions_processed",
    "Internal firehose transactions processed (from Stats)",
);
static INTERNAL_SLOT_HIGH_WATERMARK: Gauge = Gauge::new(
    "jetstreamer_internal_slot_high_watermark",
    "Highest slot observed by the internal firehose stats callback",
);
static TRANSACTIONS_BUFFERED_FOR_BLOCK_TIME: Counter = Counter::new(
    "jetstreamer_transactions_buffered_for_block_time_total",
    "Transactions buffered until block metadata is available",
);
static TRANSACTIONS_SENT_WITH_BLOCK_TIME: Counter = Counter::new(
    "jetstreamer_transactions_sent_with_block_time_total",
    "Transactions sent with historical block_time populated",
);
static TRANSACTIONS_SENT_WITHOUT_BLOCK_TIME: Counter = Counter::new(
    "jetstreamer_transactions_sent_without_block_time_total",
    "Transactions sent without block_time after the firehose finished",
);

type PendingTransactionKey = (usize, u64);
type PendingTransactions = Arc<Mutex<PendingTransactionBuffer>>;

struct PendingTransactionBuffer {
    by_slot: HashMap<PendingTransactionKey, Vec<TransactionUpdate>>,
    len: usize,
    max_len: usize,
}

impl PendingTransactionBuffer {
    fn new(max_len: usize) -> Self {
        Self {
            by_slot: HashMap::new(),
            len: 0,
            max_len,
        }
    }

    fn push(
        &mut self,
        key: PendingTransactionKey,
        transaction: TransactionUpdate,
    ) -> Result<(), std::io::Error> {
        if self.len >= self.max_len {
            return Err(std::io::Error::other(format!(
                "Jetstreamer metadata buffer reached its {} transaction limit",
                self.max_len
            )));
        }
        self.by_slot.entry(key).or_default().push(transaction);
        self.len += 1;
        Ok(())
    }

    fn remove(&mut self, key: &PendingTransactionKey) -> Vec<TransactionUpdate> {
        let transactions = self.by_slot.remove(key).unwrap_or_default();
        self.len = self.len.saturating_sub(transactions.len());
        transactions
    }
}

fn register_jetstreamer_metrics() {
    let registry = MetricsRegistry::global();
    registry.register_counter(&BLOCKS_SENT);
    registry.register_counter(&TRANSACTIONS_SENT);
    registry.register_counter(&TRANSACTIONS_FILTERED_OUT);
    registry.register_counter(&TRANSACTIONS_FILTERED_IN);
    registry.register_counter(&TRANSACTIONS_BUFFERED_FOR_BLOCK_TIME);
    registry.register_counter(&TRANSACTIONS_SENT_WITH_BLOCK_TIME);
    registry.register_counter(&TRANSACTIONS_SENT_WITHOUT_BLOCK_TIME);
    registry.register_gauge(&INTERNAL_SLOTS_PROCESSED);
    registry.register_gauge(&INTERNAL_BLOCKS_PROCESSED);
    registry.register_gauge(&INTERNAL_TRANSACTIONS_PROCESSED);
    registry.register_gauge(&INTERNAL_SLOT_HIGH_WATERMARK);
}

fn reset_jetstreamer_internal_stats() {
    INTERNAL_SLOTS_PROCESSED.set(0.0);
    INTERNAL_BLOCKS_PROCESSED.set(0.0);
    INTERNAL_TRANSACTIONS_PROCESSED.set(0.0);
    INTERNAL_SLOT_HIGH_WATERMARK.set(0.0);
}

#[derive(Debug, Clone, Copy)]
pub struct JetstreamerStatsSnapshot {
    pub slots_processed: u64,
    pub blocks_processed: u64,
    pub transactions_processed: u64,
    pub slot_high_watermark: u64,
}

pub fn jetstreamer_internal_stats_snapshot() -> JetstreamerStatsSnapshot {
    JetstreamerStatsSnapshot {
        slots_processed: INTERNAL_SLOTS_PROCESSED.get().max(0.0) as u64,
        blocks_processed: INTERNAL_BLOCKS_PROCESSED.get().max(0.0) as u64,
        transactions_processed: INTERNAL_TRANSACTIONS_PROCESSED.get().max(0.0) as u64,
        slot_high_watermark: INTERNAL_SLOT_HIGH_WATERMARK.get().max(0.0) as u64,
    }
}

fn should_request_blocks(include_transactions: bool, include_blocks: bool) -> bool {
    include_transactions || include_blocks
}

pub mod filter;
pub mod range;

pub struct JetstreamerDatasource {
    pub range: JetstreamerRange,
    pub filter: JetstreamerFilter,
    pub threads: u64,
    pub tracking_interval_slots: Option<u64>,
    pub archive_url: Option<String>,
    pub network: Option<String>,
    /// Stream epochs in order with one firehose worker and parallel ranged downloads.
    pub sequential: bool,
    /// Process epochs newest-first. Jetstreamer implicitly enables sequential mode.
    pub reverse: bool,
    /// Maximum hot/cold download window used in sequential mode.
    pub buffer_window_bytes: Option<u64>,
    /// Maximum transactions waiting for their block-time and block-hash metadata.
    pub pending_transaction_limit: usize,
}

impl JetstreamerDatasource {
    pub fn new(
        range: JetstreamerRange,
        filter: JetstreamerFilter,
        threads: u64,
        tracking_interval_slots: Option<u64>,
        archive_url: Option<String>,
        network: Option<String>,
    ) -> Self {
        Self {
            range,
            filter,
            threads,
            tracking_interval_slots,
            archive_url,
            network,
            sequential: false,
            reverse: false,
            buffer_window_bytes: None,
            pending_transaction_limit: 100_000,
        }
    }

    pub fn new_with_old_faithful_mainnet(
        range: JetstreamerRange,
        filter: JetstreamerFilter,
        threads: u64,
        tracking_interval_slots: Option<u64>,
    ) -> Self {
        Self::new(range, filter, threads, tracking_interval_slots, None, None)
    }

    pub fn with_sequential_mode(mut self, reverse: bool, buffer_window_bytes: Option<u64>) -> Self {
        self.sequential = true;
        self.reverse = reverse;
        self.buffer_window_bytes = buffer_window_bytes;
        self
    }

    pub fn with_pending_transaction_limit(mut self, limit: usize) -> Self {
        self.pending_transaction_limit = limit;
        self
    }
}

#[async_trait]
impl Datasource for JetstreamerDatasource {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: tokio::sync::mpsc::Sender<(Update, DatasourceId)>,
        cancellation_token: CancellationToken,
    ) -> CarbonResult<()> {
        register_jetstreamer_metrics();
        reset_jetstreamer_internal_stats();

        if self.pending_transaction_limit == 0 {
            return Err(carbon_core::error::Error::FailedToConsumeDatasource(
                "Jetstreamer pending transaction limit must be positive".to_owned(),
            ));
        }

        let (start_slot, end_slot) = self.range.into_slots();
        let (include_transactions, include_blocks) =
            (self.filter.include_transactions, self.filter.include_blocks);
        let pending_transactions: PendingTransactions = Arc::new(Mutex::new(
            PendingTransactionBuffer::new(self.pending_transaction_limit),
        ));

        if let Some(archive_url) = &self.archive_url {
            unsafe { std::env::set_var("JETSTREAMER_COMPACT_INDEX_BASE_URL", archive_url) }
        }

        if let Some(network) = &self.network {
            unsafe { std::env::set_var("JETSTREAMER_NETWORK", network) }
        }

        let sender_for_block = sender.clone();
        let id_for_block = id.clone();
        let pending_transactions_for_block = pending_transactions.clone();
        let on_block_fn = move |thread_id: usize, block: BlockData| {
            let sender = sender_for_block.clone();
            let id = id_for_block.clone();
            let pending_transactions = pending_transactions_for_block.clone();
            async move {
                JetstreamerDatasource::on_block(
                    thread_id,
                    block,
                    id,
                    sender,
                    pending_transactions,
                    include_blocks,
                )
                .await
            }
            .boxed()
        };

        let filter_for_transaction = self.filter.transaction_filters.clone();
        let pending_transactions_for_transaction = pending_transactions.clone();
        let on_transaction_fn = move |thread_id: usize, transaction: TransactionData| {
            let transaction_filters = filter_for_transaction.clone();
            let pending_transactions = pending_transactions_for_transaction.clone();
            async move {
                JetstreamerDatasource::on_transaction(
                    thread_id,
                    transaction,
                    transaction_filters,
                    pending_transactions,
                )
                .await
            }
            .boxed()
        };

        let on_stats_fn = move |_thread_id: usize, stats: Stats| {
            async move { JetstreamerDatasource::on_stats(stats).await }.boxed()
        };

        let stats_tracking = self
            .tracking_interval_slots
            .map(|interval_slots| StatsTracking {
                on_stats: on_stats_fn,
                tracking_interval_slots: interval_slots,
            });

        let (shutdown_sender, shutdown_receiver) = broadcast::channel(1);
        let cancellation_task = tokio::spawn(async move {
            cancellation_token.cancelled().await;
            let _ = shutdown_sender.send(());
        });

        let result = jetstreamer_firehose::firehose::firehose(
            self.threads,
            self.sequential,
            self.reverse,
            self.buffer_window_bytes,
            start_slot..end_slot,
            if should_request_blocks(include_transactions, include_blocks) {
                Some(on_block_fn)
            } else {
                None
            },
            if include_transactions {
                Some(on_transaction_fn)
            } else {
                None
            },
            None::<HandlerFn<EntryData>>,
            None::<HandlerFn<RewardsData>>,
            None::<OnErrorFn>,
            stats_tracking,
            Some(shutdown_receiver),
        )
        .await;
        cancellation_task.abort();

        match result {
            Ok(()) => {
                JetstreamerDatasource::flush_all_pending_transactions_without_block_time(
                    id,
                    sender,
                    pending_transactions,
                )
                .await
                .map_err(|error| {
                    carbon_core::error::Error::FailedToConsumeDatasource(error.to_string())
                })?;
            }
            Err((error, _)) => {
                return Err(carbon_core::error::Error::FailedToConsumeDatasource(
                    error.to_string(),
                ));
            }
        }

        Ok(())
    }

    fn update_types(&self) -> Vec<carbon_core::datasource::UpdateType> {
        let mut update_types = Vec::new();
        if self.filter.include_transactions {
            update_types.push(UpdateType::Transaction);
        }
        if self.filter.include_blocks {
            update_types.push(UpdateType::BlockDetails);
        }
        update_types
    }
}

impl JetstreamerDatasource {
    async fn on_block(
        thread_id: usize,
        block: BlockData,
        id: DatasourceId,
        sender: tokio::sync::mpsc::Sender<(Update, DatasourceId)>,
        pending_transactions: PendingTransactions,
        emit_block_details: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let BlockData::Block {
            parent_blockhash,
            slot,
            blockhash,
            rewards,
            block_time,
            block_height,
            ..
        } = block
        else {
            let key = (thread_id, block.slot());
            Self::flush_pending_transactions(pending_transactions, key, id, sender, false, |_| {})
                .await?;
            return Ok(());
        };

        let key = (thread_id, slot);
        let transaction_blockhash = blockhash;
        Self::flush_pending_transactions(
            pending_transactions,
            key,
            id.clone(),
            sender.clone(),
            block_time.is_some(),
            move |transaction| {
                transaction.block_time = block_time;
                transaction.block_hash = Some(transaction_blockhash);
            },
        )
        .await?;

        if emit_block_details {
            sender
                .send((
                    Update::BlockDetails(BlockDetails {
                        slot,
                        block_hash: Some(blockhash),
                        previous_block_hash: Some(parent_blockhash),
                        rewards: Some(
                            rewards
                                .keyed_rewards
                                .iter()
                                .map(|(pubkey, reward)| Reward {
                                    pubkey: pubkey.to_string(),
                                    lamports: reward.lamports,
                                    post_balance: reward.post_balance,
                                    reward_type: Some(reward.reward_type),
                                    commission: reward
                                        .commission_bps
                                        .filter(|basis_points| basis_points % 100 == 0)
                                        .and_then(|basis_points| {
                                            u8::try_from(basis_points / 100).ok()
                                        }),
                                    commission_bps: reward.commission_bps,
                                })
                                .collect::<Vec<_>>(),
                        ),
                        num_reward_partitions: rewards.num_partitions,
                        block_time,
                        block_height,
                    }),
                    id,
                ))
                .await?;

            BLOCKS_SENT.inc();
        }

        Ok(())
    }

    async fn on_transaction(
        thread_id: usize,
        transaction: TransactionData,
        transaction_filters: Vec<TransactionFilter>,
        pending_transactions: PendingTransactions,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !transaction_filters.is_empty() {
            let mut accounts = HashSet::new();
            accounts.extend(transaction.transaction.message.static_account_keys());
            accounts.extend(
                transaction
                    .transaction_status_meta
                    .loaded_addresses
                    .readonly
                    .iter(),
            );
            accounts.extend(
                transaction
                    .transaction_status_meta
                    .loaded_addresses
                    .writable
                    .iter(),
            );

            if transaction_filters.iter().all(|filter| {
                !filter.matches(
                    &accounts,
                    transaction.is_vote,
                    transaction.transaction_status_meta.status.is_err(),
                )
            }) {
                TRANSACTIONS_FILTERED_OUT.inc();
                return Ok(());
            }
        }

        TRANSACTIONS_FILTERED_IN.inc();

        let update = TransactionUpdate {
            signature: transaction.signature,
            transaction: transaction.transaction,
            meta: transaction.transaction_status_meta,
            is_vote: transaction.is_vote,
            slot: transaction.slot,
            index: Some(transaction.transaction_slot_index as u64),
            block_time: None,
            block_hash: None,
        };

        let slot = update.slot;
        pending_transactions
            .lock()
            .await
            .push((thread_id, slot), update)?;

        TRANSACTIONS_BUFFERED_FOR_BLOCK_TIME.inc();
        Ok(())
    }

    async fn flush_pending_transactions<F>(
        pending_transactions: PendingTransactions,
        key: PendingTransactionKey,
        id: DatasourceId,
        sender: tokio::sync::mpsc::Sender<(Update, DatasourceId)>,
        has_block_time: bool,
        mut apply_metadata: F,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnMut(&mut TransactionUpdate),
    {
        let mut transactions = {
            let mut pending_transactions = pending_transactions.lock().await;
            pending_transactions.remove(&key)
        };
        let count = transactions.len();

        if count == 0 {
            return Ok(0);
        }

        for transaction in &mut transactions {
            apply_metadata(transaction);
        }

        for transaction in transactions {
            sender
                .send((Update::Transaction(Box::new(transaction)), id.clone()))
                .await?;
        }

        TRANSACTIONS_SENT.inc_by(count as u64);
        if has_block_time {
            TRANSACTIONS_SENT_WITH_BLOCK_TIME.inc_by(count as u64);
        } else {
            TRANSACTIONS_SENT_WITHOUT_BLOCK_TIME.inc_by(count as u64);
        }

        Ok(count)
    }

    async fn flush_all_pending_transactions_without_block_time(
        id: DatasourceId,
        sender: tokio::sync::mpsc::Sender<(Update, DatasourceId)>,
        pending_transactions: PendingTransactions,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let keys: Vec<PendingTransactionKey> = {
            let pending_transactions = pending_transactions.lock().await;
            pending_transactions.by_slot.keys().copied().collect()
        };
        let mut flushed = 0;

        for key in keys {
            flushed += Self::flush_pending_transactions(
                pending_transactions.clone(),
                key,
                id.clone(),
                sender.clone(),
                false,
                |_| {},
            )
            .await?;
        }

        Ok(flushed)
    }

    pub async fn on_stats(stats: Stats) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        INTERNAL_SLOTS_PROCESSED.set(stats.slots_processed as f64);
        INTERNAL_BLOCKS_PROCESSED.set(stats.blocks_processed as f64);
        INTERNAL_TRANSACTIONS_PROCESSED.set(stats.transactions_processed as f64);
        let previous_high_watermark = INTERNAL_SLOT_HIGH_WATERMARK.get().max(0.0) as u64;
        INTERNAL_SLOT_HIGH_WATERMARK
            .set(previous_high_watermark.max(stats.thread_stats.current_slot) as f64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::JetstreamerFilter;
    use crate::range::JetstreamerRange;
    use solana_message::VersionedMessage;

    #[test]
    fn transaction_stream_requests_block_callbacks_for_metadata() {
        assert!(should_request_blocks(true, false));
        assert!(should_request_blocks(false, true));
        assert!(should_request_blocks(true, true));
        assert!(!should_request_blocks(false, false));
    }

    #[test]
    fn update_types_only_advertise_publicly_emitted_updates() {
        let transaction_only = JetstreamerDatasource::new_with_old_faithful_mainnet(
            JetstreamerRange::Slot(1, 2),
            JetstreamerFilter {
                include_transactions: true,
                include_blocks: false,
                transaction_filters: Vec::new(),
            },
            1,
            None,
        );
        assert_eq!(
            transaction_only.update_types(),
            vec![UpdateType::Transaction]
        );

        let transactions_and_blocks = JetstreamerDatasource::new_with_old_faithful_mainnet(
            JetstreamerRange::Slot(1, 2),
            JetstreamerFilter {
                include_transactions: true,
                include_blocks: true,
                transaction_filters: Vec::new(),
            },
            1,
            None,
        );
        assert_eq!(
            transactions_and_blocks.update_types(),
            vec![UpdateType::Transaction, UpdateType::BlockDetails]
        );
    }

    fn v1_transaction(slot: u64, transaction_slot_index: usize) -> TransactionData {
        TransactionData {
            slot,
            transaction_slot_index,
            signature: solana_signature::Signature::default(),
            message_hash: solana_hash::Hash::default(),
            is_vote: false,
            transaction_status_meta: solana_transaction_status::TransactionStatusMeta::default(),
            transaction: solana_transaction::versioned::VersionedTransaction {
                signatures: Vec::new(),
                message: VersionedMessage::V1(solana_message::v1::Message::default()),
            },
        }
    }

    #[tokio::test]
    async fn v1_transaction_is_buffered_with_global_index() {
        let pending = Arc::new(Mutex::new(PendingTransactionBuffer::new(2)));
        JetstreamerDatasource::on_transaction(
            3,
            v1_transaction(42, 7),
            Vec::new(),
            pending.clone(),
        )
        .await
        .unwrap();

        let pending = pending.lock().await;
        let update = &pending.by_slot[&(3, 42)][0];
        assert_eq!(update.index, Some(7));
        assert!(matches!(
            update.transaction.message,
            VersionedMessage::V1(_)
        ));
    }

    #[tokio::test]
    async fn metadata_buffer_fails_closed_at_configured_limit() {
        let pending = Arc::new(Mutex::new(PendingTransactionBuffer::new(1)));
        JetstreamerDatasource::on_transaction(
            0,
            v1_transaction(42, 0),
            Vec::new(),
            pending.clone(),
        )
        .await
        .unwrap();
        let error =
            JetstreamerDatasource::on_transaction(0, v1_transaction(43, 1), Vec::new(), pending)
                .await
                .unwrap_err();
        assert!(error.to_string().contains("1 transaction limit"));
    }
}
