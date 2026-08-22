use {
    crate::ConversionError,
    solana_clock::UnixTimestamp,
    solana_message::{
        compiled_instruction::CompiledInstruction,
        v0::MessageAddressTableLookup,
        v1::{MAX_TRANSACTION_SIZE, SIGNATURE_SIZE},
        MessageHeader, VersionedMessage,
    },
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_context::transaction::TransactionReturnData,
    solana_transaction_error::TransactionError,
    solana_transaction_status::{
        InnerInstruction, InnerInstructions, Reward, RewardType, TransactionStatusMeta,
        TransactionTokenBalance,
    },
    yellowstone_grpc_proto::prelude as proto,
};

pub fn create_transaction(
    tx: &VersionedTransaction,
) -> Result<proto::Transaction, ConversionError> {
    if matches!(tx.message, VersionedMessage::V1(_))
        && tx.signatures.len() != usize::from(tx.message.header().num_required_signatures)
    {
        return Err(ConversionError::InvalidV1SignatureCount);
    }

    Ok(proto::Transaction {
        signatures: tx
            .signatures
            .iter()
            .map(|signature| <Signature as AsRef<[u8]>>::as_ref(signature).into())
            .collect(),
        message: Some(create_message(&tx.message)?),
    })
}

pub fn create_message(message: &VersionedMessage) -> Result<proto::Message, ConversionError> {
    Ok(match message {
        VersionedMessage::Legacy(message) => proto::Message {
            header: Some(create_header(&message.header)),
            account_keys: create_pubkeys(&message.account_keys),
            recent_blockhash: message.recent_blockhash.to_bytes().into(),
            instructions: create_instructions(&message.instructions),
            versioned: false,
            address_table_lookups: vec![],
            config: None,
        },
        VersionedMessage::V0(message) => proto::Message {
            header: Some(create_header(&message.header)),
            account_keys: create_pubkeys(&message.account_keys),
            recent_blockhash: message.recent_blockhash.to_bytes().into(),
            instructions: create_instructions(&message.instructions),
            versioned: true,
            address_table_lookups: create_lookups(&message.address_table_lookups),
            config: None,
        },
        VersionedMessage::V1(message) => {
            message.validate()?;
            let transaction_size = 1usize.saturating_add(message.size()).saturating_add(
                usize::from(message.header.num_required_signatures).saturating_mul(SIGNATURE_SIZE),
            );
            if transaction_size > MAX_TRANSACTION_SIZE {
                return Err(ConversionError::V1TransactionTooLarge);
            }

            proto::Message {
                header: Some(create_header(&message.header)),
                account_keys: create_pubkeys(&message.account_keys),
                recent_blockhash: message.lifetime_specifier.to_bytes().into(),
                instructions: create_instructions(&message.instructions),
                versioned: true,
                address_table_lookups: vec![],
                config: Some(proto::TransactionConfig {
                    priority_fee: message.config.priority_fee,
                    compute_unit_limit: message.config.compute_unit_limit,
                    loaded_accounts_data_size_limit: message.config.loaded_accounts_data_size_limit,
                    heap_size: message.config.heap_size,
                }),
            }
        }
    })
}

pub const fn create_header(header: &MessageHeader) -> proto::MessageHeader {
    proto::MessageHeader {
        num_required_signatures: header.num_required_signatures as u32,
        num_readonly_signed_accounts: header.num_readonly_signed_accounts as u32,
        num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u32,
    }
}

pub fn create_pubkeys(pubkeys: &[Pubkey]) -> Vec<Vec<u8>> {
    pubkeys
        .iter()
        .map(|key| <Pubkey as AsRef<[u8]>>::as_ref(key).into())
        .collect()
}

pub fn create_instructions(ixs: &[CompiledInstruction]) -> Vec<proto::CompiledInstruction> {
    ixs.iter().map(create_instruction).collect()
}

pub fn create_instruction(ix: &CompiledInstruction) -> proto::CompiledInstruction {
    proto::CompiledInstruction {
        program_id_index: ix.program_id_index as u32,
        accounts: ix.accounts.clone(),
        data: ix.data.clone(),
    }
}

pub fn create_lookups(
    lookups: &[MessageAddressTableLookup],
) -> Vec<proto::MessageAddressTableLookup> {
    lookups.iter().map(create_lookup).collect()
}

pub fn create_lookup(lookup: &MessageAddressTableLookup) -> proto::MessageAddressTableLookup {
    proto::MessageAddressTableLookup {
        account_key: <Pubkey as AsRef<[u8]>>::as_ref(&lookup.account_key).into(),
        writable_indexes: lookup.writable_indexes.clone(),
        readonly_indexes: lookup.readonly_indexes.clone(),
    }
}

pub fn create_transaction_meta(meta: &TransactionStatusMeta) -> proto::TransactionStatusMeta {
    let TransactionStatusMeta {
        status,
        fee,
        pre_balances,
        post_balances,
        inner_instructions,
        log_messages,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_addresses,
        return_data,
        compute_units_consumed,
        cost_units,
    } = meta;
    let err = create_transaction_error(status);
    let inner_instructions_none = inner_instructions.is_none();
    let inner_instructions = inner_instructions
        .as_deref()
        .map(create_inner_instructions_vec)
        .unwrap_or_default();
    let log_messages_none = log_messages.is_none();
    let log_messages = log_messages.clone().unwrap_or_default();
    let pre_token_balances = pre_token_balances
        .as_deref()
        .map(create_token_balances)
        .unwrap_or_default();
    let post_token_balances = post_token_balances
        .as_deref()
        .map(create_token_balances)
        .unwrap_or_default();
    let rewards = rewards.as_deref().map(create_rewards).unwrap_or_default();
    let loaded_writable_addresses = create_pubkeys(&loaded_addresses.writable);
    let loaded_readonly_addresses = create_pubkeys(&loaded_addresses.readonly);

    proto::TransactionStatusMeta {
        err,
        fee: *fee,
        pre_balances: pre_balances.clone(),
        post_balances: post_balances.clone(),
        inner_instructions,
        inner_instructions_none,
        log_messages,
        log_messages_none,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_writable_addresses,
        loaded_readonly_addresses,
        return_data: return_data.as_ref().map(create_return_data),
        return_data_none: return_data.is_none(),
        compute_units_consumed: *compute_units_consumed,
        cost_units: *cost_units,
    }
}

pub fn create_transaction_error(
    status: &Result<(), TransactionError>,
) -> Option<proto::TransactionError> {
    match status {
        Ok(()) => None,
        Err(err) => Some(proto::TransactionError {
            err: bincode::serialize(&err).expect("transaction error to serialize to bytes"),
        }),
    }
}

pub fn create_inner_instructions_vec(ixs: &[InnerInstructions]) -> Vec<proto::InnerInstructions> {
    ixs.iter().map(create_inner_instructions).collect()
}

pub fn create_inner_instructions(instructions: &InnerInstructions) -> proto::InnerInstructions {
    proto::InnerInstructions {
        index: instructions.index as u32,
        instructions: create_inner_instruction_vec(&instructions.instructions),
    }
}

pub fn create_inner_instruction_vec(ixs: &[InnerInstruction]) -> Vec<proto::InnerInstruction> {
    ixs.iter().map(create_inner_instruction).collect()
}

pub fn create_inner_instruction(instruction: &InnerInstruction) -> proto::InnerInstruction {
    proto::InnerInstruction {
        program_id_index: instruction.instruction.program_id_index as u32,
        accounts: instruction.instruction.accounts.clone(),
        data: instruction.instruction.data.clone(),
        stack_height: instruction.stack_height,
    }
}

pub fn create_token_balances(balances: &[TransactionTokenBalance]) -> Vec<proto::TokenBalance> {
    balances.iter().map(create_token_balance).collect()
}

pub fn create_token_balance(balance: &TransactionTokenBalance) -> proto::TokenBalance {
    proto::TokenBalance {
        account_index: balance.account_index as u32,
        mint: balance.mint.clone(),
        ui_token_amount: Some(proto::UiTokenAmount {
            ui_amount: balance.ui_token_amount.ui_amount.unwrap_or_default(),
            decimals: balance.ui_token_amount.decimals as u32,
            amount: balance.ui_token_amount.amount.clone(),
            ui_amount_string: balance.ui_token_amount.ui_amount_string.clone(),
        }),
        owner: balance.owner.clone(),
        program_id: balance.program_id.clone(),
    }
}

pub fn create_rewards_obj(rewards: &[Reward], num_partitions: Option<u64>) -> proto::Rewards {
    proto::Rewards {
        rewards: create_rewards(rewards),
        num_partitions: num_partitions.map(create_num_partitions),
    }
}

pub fn create_rewards(rewards: &[Reward]) -> Vec<proto::Reward> {
    rewards.iter().map(create_reward).collect()
}

pub fn create_reward(reward: &Reward) -> proto::Reward {
    proto::Reward {
        pubkey: reward.pubkey.clone(),
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: create_reward_type(reward.reward_type) as i32,
        commission: reward.commission.map(|c| c.to_string()).unwrap_or_default(),
        commission_bps: reward
            .commission_bps
            .map(|c| c.to_string())
            .unwrap_or_default(),
    }
}

pub const fn create_reward_type(reward_type: Option<RewardType>) -> proto::RewardType {
    match reward_type {
        None => proto::RewardType::Unspecified,
        Some(RewardType::Fee) => proto::RewardType::Fee,
        Some(RewardType::Rent) => proto::RewardType::Rent,
        Some(RewardType::Staking) => proto::RewardType::Staking,
        Some(RewardType::Voting) => proto::RewardType::Voting,
        Some(RewardType::DeactivatedStake) => proto::RewardType::DeactivatedStake,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::convert_from,
        solana_hash::Hash,
        solana_message::{
            v0::{Message as MessageV0, MessageAddressTableLookup},
            v1::{Message as MessageV1, TransactionConfig},
            Message as LegacyMessage,
        },
    };

    fn header() -> MessageHeader {
        MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 1,
        }
    }

    fn instruction() -> CompiledInstruction {
        CompiledInstruction {
            program_id_index: 1,
            accounts: vec![0],
            data: vec![1, 2, 3],
        }
    }

    #[test]
    fn legacy_message_round_trips_without_version_metadata() {
        let message = VersionedMessage::Legacy(LegacyMessage {
            header: header(),
            account_keys: vec![Pubkey::new_unique(), Pubkey::new_unique()],
            recent_blockhash: Hash::new_from_array([7; 32]),
            instructions: vec![instruction()],
        });

        let encoded = create_message(&message).unwrap();
        assert!(!encoded.versioned);
        assert!(encoded.address_table_lookups.is_empty());
        assert!(encoded.config.is_none());
        assert_eq!(convert_from::create_message(encoded).unwrap(), message);
    }

    #[test]
    fn v0_message_with_lookup_table_round_trips_without_v1_config() {
        let message = VersionedMessage::V0(MessageV0 {
            header: header(),
            account_keys: vec![Pubkey::new_unique(), Pubkey::new_unique()],
            recent_blockhash: Hash::new_from_array([9; 32]),
            instructions: vec![instruction()],
            address_table_lookups: vec![MessageAddressTableLookup {
                account_key: Pubkey::new_unique(),
                writable_indexes: vec![0, 2],
                readonly_indexes: vec![1],
            }],
        });

        let encoded = create_message(&message).unwrap();
        assert!(encoded.versioned);
        assert_eq!(encoded.address_table_lookups.len(), 1);
        assert!(encoded.config.is_none());
        assert_eq!(convert_from::create_message(encoded).unwrap(), message);
    }

    #[test]
    fn deactivated_stake_reward_type_round_trips_without_downgrade() {
        let encoded = create_reward_type(Some(RewardType::DeactivatedStake));
        assert_eq!(encoded, proto::RewardType::DeactivatedStake);
    }

    #[test]
    fn v1_transaction_encodes_all_config_fields_and_signatures() {
        let signature = Signature::from([23; 64]);
        let transaction = VersionedTransaction {
            signatures: vec![signature],
            message: VersionedMessage::V1(MessageV1::new(
                header(),
                TransactionConfig {
                    priority_fee: Some(42),
                    compute_unit_limit: Some(900_000),
                    loaded_accounts_data_size_limit: Some(128 * 1024),
                    heap_size: Some(64 * 1024),
                },
                Hash::new_from_array([31; 32]),
                vec![Pubkey::new_unique(), Pubkey::new_unique()],
                vec![instruction()],
            )),
        };

        let encoded = create_transaction(&transaction).unwrap();
        assert_eq!(encoded.signatures, vec![signature.as_ref().to_vec()]);
        let message = encoded.message.as_ref().unwrap();
        assert!(message.versioned);
        assert!(message.address_table_lookups.is_empty());
        assert_eq!(
            message.header,
            Some(create_header(transaction.message.header()))
        );
        assert_eq!(
            message.account_keys,
            create_pubkeys(transaction.message.static_account_keys())
        );
        assert_eq!(message.recent_blockhash, vec![31; 32]);
        assert_eq!(
            message.instructions,
            create_instructions(transaction.message.instructions())
        );
        assert_eq!(
            message.config,
            Some(proto::TransactionConfig {
                priority_fee: Some(42),
                compute_unit_limit: Some(900_000),
                loaded_accounts_data_size_limit: Some(128 * 1024),
                heap_size: Some(64 * 1024),
            })
        );
        assert_eq!(
            convert_from::create_tx_versioned(encoded).unwrap(),
            transaction
        );
    }
}

pub const fn create_num_partitions(num_partitions: u64) -> proto::NumPartitions {
    proto::NumPartitions { num_partitions }
}

pub fn create_return_data(return_data: &TransactionReturnData) -> proto::ReturnData {
    proto::ReturnData {
        program_id: return_data.program_id.to_bytes().into(),
        data: return_data.data.clone(),
    }
}

pub const fn create_block_height(block_height: u64) -> proto::BlockHeight {
    proto::BlockHeight { block_height }
}

pub const fn create_timestamp(timestamp: UnixTimestamp) -> proto::UnixTimestamp {
    proto::UnixTimestamp { timestamp }
}
