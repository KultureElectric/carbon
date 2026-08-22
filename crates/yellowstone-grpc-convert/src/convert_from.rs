use {
    crate::ConversionError,
    solana_account::Account,
    solana_account_decoder::parse_token::UiTokenAmount,
    solana_hash::{Hash, HASH_BYTES},
    solana_message::{
        compiled_instruction::CompiledInstruction,
        v0::{LoadedAddresses, Message as MessageV0, MessageAddressTableLookup},
        v1::{Message as MessageV1, TransactionConfig, MAX_TRANSACTION_SIZE, SIGNATURE_SIZE},
        Message, MessageHeader, VersionedMessage,
    },
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_context::transaction::TransactionReturnData,
    solana_transaction_error::TransactionError,
    solana_transaction_status::{
        ConfirmedBlock, InnerInstruction, InnerInstructions, Reward, RewardType,
        RewardsAndNumPartitions, TransactionStatusMeta, TransactionTokenBalance,
        TransactionWithStatusMeta, VersionedTransactionWithStatusMeta,
    },
    yellowstone_grpc_proto::prelude as proto,
};

type CreateResult<T> = Result<T, ConversionError>;

pub fn create_block(block: proto::SubscribeUpdateBlock) -> CreateResult<ConfirmedBlock> {
    let mut transactions = vec![];
    for tx in block.transactions {
        transactions.push(create_tx_with_meta(tx)?);
    }

    let block_rewards = block.rewards.ok_or("failed to get rewards")?;
    let mut rewards = vec![];
    for reward in block_rewards.rewards {
        rewards.push(create_reward(reward)?);
    }

    Ok(ConfirmedBlock {
        previous_blockhash: block.parent_blockhash,
        blockhash: block.blockhash,
        parent_slot: block.parent_slot,
        transactions,
        rewards,
        num_partitions: block_rewards.num_partitions.map(|msg| msg.num_partitions),
        block_time: Some(
            block
                .block_time
                .map(|wrapper| wrapper.timestamp)
                .ok_or("failed to get block_time")?,
        ),
        block_height: Some(
            block
                .block_height
                .map(|wrapper| wrapper.block_height)
                .ok_or("failed to get block_height")?,
        ),
    })
}

pub fn create_tx_with_meta(
    tx: proto::SubscribeUpdateTransactionInfo,
) -> CreateResult<TransactionWithStatusMeta> {
    let meta = tx.meta.ok_or("failed to get transaction meta")?;
    let tx = tx
        .transaction
        .ok_or("failed to get transaction transaction")?;

    Ok(TransactionWithStatusMeta::Complete(
        VersionedTransactionWithStatusMeta {
            transaction: create_tx_versioned(tx)?,
            meta: create_tx_meta(meta)?,
        },
    ))
}

pub fn create_tx_versioned(tx: proto::Transaction) -> CreateResult<VersionedTransaction> {
    let mut signatures = Vec::with_capacity(tx.signatures.len());
    for signature in tx.signatures {
        signatures.push(match Signature::try_from(signature.as_slice()) {
            Ok(signature) => signature,
            Err(_error) => return Err("failed to parse Signature".into()),
        });
    }

    let message = create_message(tx.message.ok_or("failed to get message")?)?;
    if matches!(message, VersionedMessage::V1(_))
        && signatures.len() != usize::from(message.header().num_required_signatures)
    {
        return Err(ConversionError::InvalidV1SignatureCount);
    }

    Ok(VersionedTransaction {
        signatures,
        message,
    })
}

pub fn create_message(message: proto::Message) -> CreateResult<VersionedMessage> {
    let header = message.header.ok_or("failed to get MessageHeader")?;
    let header = MessageHeader {
        num_required_signatures: header
            .num_required_signatures
            .try_into()
            .map_err(|_| "failed to parse num_required_signatures")?,
        num_readonly_signed_accounts: header
            .num_readonly_signed_accounts
            .try_into()
            .map_err(|_| "failed to parse num_readonly_signed_accounts")?,
        num_readonly_unsigned_accounts: header
            .num_readonly_unsigned_accounts
            .try_into()
            .map_err(|_| "failed to parse num_readonly_unsigned_accounts")?,
    };

    if message.recent_blockhash.len() != HASH_BYTES {
        return Err("failed to parse hash".into());
    }

    let account_keys = create_pubkey_vec(message.account_keys)?;
    let recent_blockhash = Hash::new_from_array(
        message
            .recent_blockhash
            .as_slice()
            .try_into()
            .map_err(|_| "failed to parse hash")?,
    );
    let instructions = create_message_instructions(message.instructions)?;

    Ok(if let Some(config) = message.config {
        if !message.address_table_lookups.is_empty() {
            return Err(ConversionError::V1AddressTableLookups);
        }

        let message = MessageV1::new(
            header,
            TransactionConfig {
                priority_fee: config.priority_fee,
                compute_unit_limit: config.compute_unit_limit,
                loaded_accounts_data_size_limit: config.loaded_accounts_data_size_limit,
                heap_size: config.heap_size,
            },
            recent_blockhash,
            account_keys,
            instructions,
        );
        message.validate()?;

        let transaction_size = 1usize.saturating_add(message.size()).saturating_add(
            usize::from(message.header.num_required_signatures).saturating_mul(SIGNATURE_SIZE),
        );
        if transaction_size > MAX_TRANSACTION_SIZE {
            return Err(ConversionError::V1TransactionTooLarge);
        }

        VersionedMessage::V1(message)
    } else if message.versioned {
        let mut address_table_lookups = Vec::with_capacity(message.address_table_lookups.len());
        for table in message.address_table_lookups {
            address_table_lookups.push(MessageAddressTableLookup {
                account_key: Pubkey::try_from(table.account_key.as_slice())
                    .map_err(|_| "failed to parse Pubkey")?,
                writable_indexes: table.writable_indexes,
                readonly_indexes: table.readonly_indexes,
            });
        }

        VersionedMessage::V0(MessageV0 {
            header,
            account_keys,
            recent_blockhash,
            instructions,
            address_table_lookups,
        })
    } else {
        VersionedMessage::Legacy(Message {
            header,
            account_keys,
            recent_blockhash,
            instructions,
        })
    })
}

pub fn create_message_instructions(
    ixs: Vec<proto::CompiledInstruction>,
) -> CreateResult<Vec<CompiledInstruction>> {
    ixs.into_iter().map(create_message_instruction).collect()
}

pub fn create_message_instruction(
    ix: proto::CompiledInstruction,
) -> CreateResult<CompiledInstruction> {
    Ok(CompiledInstruction {
        program_id_index: ix
            .program_id_index
            .try_into()
            .map_err(|_| "failed to decode CompiledInstruction.program_id_index)")?,
        accounts: ix.accounts,
        data: ix.data,
    })
}

pub fn create_tx_meta(meta: proto::TransactionStatusMeta) -> CreateResult<TransactionStatusMeta> {
    let meta_status = match create_tx_error(meta.err.as_ref())? {
        Some(err) => Err(err),
        None => Ok(()),
    };
    let meta_rewards = meta
        .rewards
        .into_iter()
        .map(create_reward)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TransactionStatusMeta {
        status: meta_status,
        fee: meta.fee,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        inner_instructions: Some(create_meta_inner_instructions(meta.inner_instructions)?),
        log_messages: Some(meta.log_messages),
        pre_token_balances: Some(create_token_balances(meta.pre_token_balances)?),
        post_token_balances: Some(create_token_balances(meta.post_token_balances)?),
        rewards: Some(meta_rewards),
        loaded_addresses: create_loaded_addresses(
            meta.loaded_writable_addresses,
            meta.loaded_readonly_addresses,
        )?,
        return_data: if meta.return_data_none {
            None
        } else {
            let data = meta.return_data.ok_or("failed to get return_data")?;
            Some(TransactionReturnData {
                program_id: Pubkey::try_from(data.program_id.as_slice())
                    .map_err(|_| "failed to parse program_id")?,
                data: data.data,
            })
        },
        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
    })
}

pub fn create_tx_error(
    err: Option<&proto::TransactionError>,
) -> CreateResult<Option<TransactionError>> {
    Ok(err
        .map(|err| bincode::deserialize::<TransactionError>(&err.err))
        .transpose()
        .map_err(|_| "failed to decode TransactionError")?)
}

pub fn create_meta_inner_instructions(
    ixs: Vec<proto::InnerInstructions>,
) -> CreateResult<Vec<InnerInstructions>> {
    ixs.into_iter().map(create_meta_inner_instruction).collect()
}

pub fn create_meta_inner_instruction(
    ix: proto::InnerInstructions,
) -> CreateResult<InnerInstructions> {
    let mut instructions = vec![];
    for ix in ix.instructions {
        instructions.push(InnerInstruction {
            instruction: CompiledInstruction {
                program_id_index: ix
                    .program_id_index
                    .try_into()
                    .map_err(|_| "failed to decode CompiledInstruction.program_id_index)")?,
                accounts: ix.accounts,
                data: ix.data,
            },
            stack_height: ix.stack_height,
        });
    }
    Ok(InnerInstructions {
        index: ix
            .index
            .try_into()
            .map_err(|_| "failed to decode InnerInstructions.index")?,
        instructions,
    })
}

pub fn create_rewards_obj(rewards: proto::Rewards) -> CreateResult<RewardsAndNumPartitions> {
    Ok(RewardsAndNumPartitions {
        rewards: rewards
            .rewards
            .into_iter()
            .map(create_reward)
            .collect::<Result<_, _>>()?,
        num_partitions: rewards.num_partitions.map(|wrapper| wrapper.num_partitions),
    })
}

pub fn create_reward(reward: proto::Reward) -> CreateResult<Reward> {
    Ok(Reward {
        pubkey: reward.pubkey,
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        commission_bps: if reward.commission_bps.is_empty() {
            None
        } else {
            Some(
                reward
                    .commission_bps
                    .parse()
                    .map_err(|_| "failed to parse reward commission bps")?,
            )
        },
        reward_type: match proto::RewardType::try_from(reward.reward_type)
            .map_err(|_| "failed to parse reward_type")?
        {
            proto::RewardType::Unspecified => None,
            proto::RewardType::Fee => Some(RewardType::Fee),
            proto::RewardType::Rent => Some(RewardType::Rent),
            proto::RewardType::Staking => Some(RewardType::Staking),
            proto::RewardType::Voting => Some(RewardType::Voting),
            proto::RewardType::DeactivatedStake => Some(RewardType::DeactivatedStake),
        },
        commission: if reward.commission.is_empty() {
            None
        } else {
            Some(
                reward
                    .commission
                    .parse()
                    .map_err(|_| "failed to parse reward commission")?,
            )
        },
    })
}

pub fn create_token_balances(
    balances: Vec<proto::TokenBalance>,
) -> CreateResult<Vec<TransactionTokenBalance>> {
    let mut vec = Vec::with_capacity(balances.len());
    for balance in balances {
        let ui_amount = balance
            .ui_token_amount
            .ok_or("failed to get ui_token_amount")?;
        vec.push(TransactionTokenBalance {
            account_index: balance
                .account_index
                .try_into()
                .map_err(|_| "failed to parse account_index")?,
            mint: balance.mint,
            ui_token_amount: UiTokenAmount {
                ui_amount: Some(ui_amount.ui_amount),
                decimals: ui_amount
                    .decimals
                    .try_into()
                    .map_err(|_| "failed to parse decimals")?,
                amount: ui_amount.amount,
                ui_amount_string: ui_amount.ui_amount_string,
            },
            owner: balance.owner,
            program_id: balance.program_id,
        });
    }
    Ok(vec)
}

pub fn create_loaded_addresses(
    writable: Vec<Vec<u8>>,
    readonly: Vec<Vec<u8>>,
) -> CreateResult<LoadedAddresses> {
    Ok(LoadedAddresses {
        writable: create_pubkey_vec(writable)?,
        readonly: create_pubkey_vec(readonly)?,
    })
}

pub fn create_pubkey_vec(pubkeys: Vec<Vec<u8>>) -> CreateResult<Vec<Pubkey>> {
    pubkeys
        .iter()
        .map(|pubkey| create_pubkey(pubkey.as_slice()))
        .collect()
}

pub fn create_pubkey(pubkey: &[u8]) -> CreateResult<Pubkey> {
    Ok(Pubkey::try_from(pubkey).map_err(|_| "failed to parse Pubkey")?)
}

#[cfg(feature = "account-data-as-bytes")]
fn take_account_data(account: &mut proto::SubscribeUpdateAccountInfo) -> Vec<u8> {
    // By taking the data, we make sure the reference count goes quicker to 1.
    // If only one reference remains (say this instance), during `into()`, it won't
    // copy the vector Compared to `Bytes:to_vec`, it should not allocate new
    // memory.
    std::mem::take(&mut account.data)
}

#[cfg(not(feature = "account-data-as-bytes"))]
fn take_account_data(account: &mut proto::SubscribeUpdateAccountInfo) -> Vec<u8> {
    std::mem::take(&mut account.data)
}

pub fn create_account(
    mut account: proto::SubscribeUpdateAccountInfo,
) -> CreateResult<(Pubkey, Account)> {
    let pubkey = create_pubkey(&account.pubkey)?;
    let account_data = take_account_data(&mut account);
    let account = Account {
        lamports: account.lamports,
        data: account_data,
        owner: create_pubkey(&account.owner)?,
        executable: account.executable,
        rent_epoch: account.rent_epoch,
    };
    Ok((pubkey, account))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_message::v1::{Message as MessageV1, TransactionConfig},
    };

    fn proto_v1_message(config: proto::TransactionConfig) -> proto::Message {
        proto::Message {
            header: Some(proto::MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            }),
            account_keys: vec![
                <Pubkey as AsRef<[u8]>>::as_ref(&Pubkey::new_unique()).to_vec(),
                <Pubkey as AsRef<[u8]>>::as_ref(&Pubkey::new_unique()).to_vec(),
            ],
            recent_blockhash: [17; HASH_BYTES].to_vec(),
            instructions: vec![proto::CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![4, 5, 6],
            }],
            versioned: true,
            address_table_lookups: vec![],
            config: Some(config),
        }
    }

    #[test]
    fn protobuf_v1_with_all_config_fields_decodes_exactly() {
        let original = proto_v1_message(proto::TransactionConfig {
            priority_fee: Some(9_001),
            compute_unit_limit: Some(700_000),
            loaded_accounts_data_size_limit: Some(64 * 1024),
            heap_size: Some(128 * 1024),
        });

        let decoded = create_message(original.clone()).unwrap();
        let VersionedMessage::V1(MessageV1 {
            header,
            config,
            lifetime_specifier,
            account_keys,
            instructions,
        }) = decoded
        else {
            panic!("protobuf field 7 must select VersionedMessage::V1");
        };

        assert_eq!(
            header,
            MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            }
        );
        assert_eq!(
            account_keys,
            create_pubkey_vec(original.account_keys).unwrap()
        );
        assert_eq!(lifetime_specifier, Hash::new_from_array([17; HASH_BYTES]));
        assert_eq!(
            instructions,
            create_message_instructions(original.instructions).unwrap()
        );
        assert_eq!(
            config,
            TransactionConfig {
                priority_fee: Some(9_001),
                compute_unit_limit: Some(700_000),
                loaded_accounts_data_size_limit: Some(64 * 1024),
                heap_size: Some(128 * 1024),
            }
        );
    }

    #[test]
    fn protobuf_v1_accepts_4096_bytes_and_rejects_4097() {
        let mut at_limit = proto_v1_message(proto::TransactionConfig::default());
        at_limit.instructions[0].data = vec![0; 3_921];
        assert!(matches!(
            create_message(at_limit),
            Ok(VersionedMessage::V1(_))
        ));

        let mut over_limit = proto_v1_message(proto::TransactionConfig::default());
        over_limit.instructions[0].data = vec![0; 3_922];
        assert_eq!(
            create_message(over_limit),
            Err(ConversionError::V1TransactionTooLarge)
        );
    }

    #[test]
    fn protobuf_v1_rejects_invalid_config_and_v0_lookups() {
        let invalid_config = proto_v1_message(proto::TransactionConfig {
            heap_size: Some(32 * 1024 + 1),
            ..proto::TransactionConfig::default()
        });
        assert_eq!(
            create_message(invalid_config),
            Err(ConversionError::InvalidV1Message(
                solana_message::v1::MessageError::InvalidHeapSize
            ))
        );

        let mut with_lookup = proto_v1_message(proto::TransactionConfig::default());
        with_lookup
            .address_table_lookups
            .push(proto::MessageAddressTableLookup::default());
        assert_eq!(
            create_message(with_lookup),
            Err(ConversionError::V1AddressTableLookups)
        );
    }

    #[test]
    fn protobuf_v1_rejects_signature_count_mismatch() {
        let transaction = proto::Transaction {
            signatures: vec![],
            message: Some(proto_v1_message(proto::TransactionConfig::default())),
        };

        assert!(create_tx_versioned(transaction).is_err());
    }
}
