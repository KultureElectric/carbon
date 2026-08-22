pub mod convert_from;
pub mod convert_to;

use solana_message::v1::MessageError;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConversionError {
    #[error("{0}")]
    Legacy(&'static str),
    #[error("invalid V1 message: {0}")]
    InvalidV1Message(#[from] MessageError),
    #[error("V1 messages cannot contain address table lookups")]
    V1AddressTableLookups,
    #[error("V1 transaction exceeds the 4096-byte wire limit")]
    V1TransactionTooLarge,
    #[error("V1 signature count does not match the message header")]
    InvalidV1SignatureCount,
}

impl From<&'static str> for ConversionError {
    fn from(message: &'static str) -> Self {
        Self::Legacy(message)
    }
}
