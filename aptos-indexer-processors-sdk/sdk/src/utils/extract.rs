// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! Helpers for extracting data from transactions.

use super::convert::{deserialize_from_string, standardize_address, truncate_str};
use aptos_protos::transaction::v1::{
    multisig_transaction_payload::Payload as MultisigPayloadType,
    transaction_payload::Payload as PayloadType, write_set::WriteSet as WriteSetType,
    EntryFunctionId, EntryFunctionPayload, MoveScriptBytecode, MoveType, ScriptPayload,
    TransactionPayload, UserTransactionRequest, WriteSet,
};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, warn};

/// Max length of entry function id string to ensure that db doesn't explode
pub const MAX_ENTRY_FUNCTION_LENGTH: usize = 1000;

////////////
// Supporting structs to get clean payload without escaped strings.
////////////

#[derive(Debug, Deserialize, Serialize)]
pub struct EntryFunctionPayloadClean {
    pub function: Option<EntryFunctionId>,
    pub type_arguments: Vec<MoveType>,
    pub arguments: Vec<Value>,
    pub entry_function_id_str: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ScriptPayloadClean {
    pub code: Option<MoveScriptBytecode>,
    pub type_arguments: Vec<MoveType>,
    pub arguments: Vec<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ScriptWriteSetClean {
    pub execute_as: String,
    pub script: ScriptPayloadClean,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MultisigPayloadClean {
    pub multisig_address: String,
    pub transaction_payload: Option<Value>,
}

////////////
// Common structs for data extraction.
////////////

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Aggregator {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub value: BigDecimal,
    #[serde(deserialize_with = "deserialize_from_string")]
    pub max_value: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AggregatorSnapshot {
    #[serde(deserialize_with = "deserialize_from_string")]
    pub value: BigDecimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DerivedStringSnapshot {
    pub value: String,
}

pub fn split_entry_function_id_str(user_request: &UserTransactionRequest) -> Option<String> {
    get_clean_entry_function_payload_from_user_request(user_request, 0)
        .map(|payload| payload.entry_function_id_str)
}

pub fn get_entry_function_from_user_request(
    user_request: &UserTransactionRequest,
) -> Option<String> {
    let entry_function_id_str: Option<String> = split_entry_function_id_str(user_request);

    entry_function_id_str.map(|s| truncate_str(&s, MAX_ENTRY_FUNCTION_LENGTH))
}

pub fn get_entry_function_contract_address_from_user_request(
    user_request: &UserTransactionRequest,
) -> Option<String> {
    let contract_address = split_entry_function_id_str(user_request).and_then(|s| {
        s.split("::").next().map(String::from) // Get the first element (contract address)
    });
    contract_address.map(|s| standardize_address(&s))
}

pub fn get_entry_function_module_name_from_user_request(
    user_request: &UserTransactionRequest,
) -> Option<String> {
    split_entry_function_id_str(user_request).and_then(|s| {
        s.split("::")
            .nth(1) // Get the second element (module name)
            .map(String::from)
    })
}

pub fn get_entry_function_function_name_from_user_request(
    user_request: &UserTransactionRequest,
) -> Option<String> {
    split_entry_function_id_str(user_request).and_then(|s| {
        s.split("::")
            .nth(2) // Get the third element (function name)
            .map(String::from)
    })
}

pub fn get_payload_type(payload: &TransactionPayload) -> String {
    payload.r#type().as_str_name().to_string()
}

/// Part of the json comes escaped from the protobuf so we need to unescape in a safe way
/// This function converts the string into json recursively and lets the diesel ORM handles
/// the escaping.
pub fn get_clean_payload(payload: &TransactionPayload, version: i64) -> Option<Value> {
    if payload.payload.as_ref().is_none() {
        warn!(
            transaction_version = version,
            "Transaction payload doesn't exist",
        );
        return None;
    }
    match payload.payload.as_ref().unwrap() {
        PayloadType::EntryFunctionPayload(inner) => {
            let clean = get_clean_entry_function_payload(inner, version);
            Some(serde_json::to_value(clean).unwrap_or_else(|_| {
                error!(version = version, "Unable to serialize payload into value");
                panic!()
            }))
        },
        PayloadType::ScriptPayload(inner) => {
            let clean = get_clean_script_payload(inner, version);
            Some(serde_json::to_value(clean).unwrap_or_else(|_| {
                error!(version = version, "Unable to serialize payload into value");
                panic!()
            }))
        },
        PayloadType::WriteSetPayload(inner) => {
            if let Some(writeset) = inner.write_set.as_ref() {
                get_clean_writeset(writeset, version)
            } else {
                None
            }
        },
        PayloadType::MultisigPayload(inner) => {
            let clean = if let Some(payload) = inner.transaction_payload.as_ref() {
                let payload_clean = match payload.payload.as_ref().unwrap() {
                    MultisigPayloadType::EntryFunctionPayload(payload) => {
                        let clean = get_clean_entry_function_payload(payload, version);
                        Some(serde_json::to_value(clean).unwrap_or_else(|_| {
                            error!(version = version, "Unable to serialize payload into value");
                            panic!()
                        }))
                    },
                };
                MultisigPayloadClean {
                    multisig_address: inner.multisig_address.clone(),
                    transaction_payload: payload_clean,
                }
            } else {
                MultisigPayloadClean {
                    multisig_address: inner.multisig_address.clone(),
                    transaction_payload: None,
                }
            };
            Some(serde_json::to_value(clean).unwrap_or_else(|_| {
                error!(version = version, "Unable to serialize payload into value");
                panic!()
            }))
        },
    }
}

/// Part of the json comes escaped from the protobuf so we need to unescape in a safe way
/// Note that DirectWriteSet is just events + writeset which is already represented separately
pub fn get_clean_writeset(writeset: &WriteSet, version: i64) -> Option<Value> {
    match writeset.write_set.as_ref().unwrap() {
        WriteSetType::ScriptWriteSet(inner) => {
            let payload = inner.script.as_ref().unwrap();
            Some(
                serde_json::to_value(get_clean_script_payload(payload, version)).unwrap_or_else(
                    |_| {
                        error!(version = version, "Unable to serialize payload into value");
                        panic!()
                    },
                ),
            )
        },
        WriteSetType::DirectWriteSet(_) => None,
    }
}

/// Part of the json comes escaped from the protobuf so we need to unescape in a safe way
pub fn get_clean_entry_function_payload(
    payload: &EntryFunctionPayload,
    version: i64,
) -> EntryFunctionPayloadClean {
    EntryFunctionPayloadClean {
        function: payload.function.clone(),
        type_arguments: payload.type_arguments.clone(),
        arguments: payload
            .arguments
            .iter()
            .map(|arg| {
                serde_json::from_str(arg).unwrap_or_else(|_| {
                    error!(version = version, "Unable to serialize payload into value");
                    panic!()
                })
            })
            .collect(),
        entry_function_id_str: payload.entry_function_id_str.clone(),
    }
}

pub fn get_clean_entry_function_payload_from_user_request(
    user_request: &UserTransactionRequest,
    version: i64,
) -> Option<EntryFunctionPayloadClean> {
    let clean_payload: Option<EntryFunctionPayloadClean> = match &user_request.payload {
        Some(txn_payload) => match &txn_payload.payload {
            Some(PayloadType::EntryFunctionPayload(payload)) => {
                Some(get_clean_entry_function_payload(payload, version))
            },
            Some(PayloadType::MultisigPayload(payload)) => {
                if let Some(payload) = payload.transaction_payload.as_ref() {
                    match payload.payload.as_ref().unwrap() {
                        MultisigPayloadType::EntryFunctionPayload(payload) => {
                            Some(get_clean_entry_function_payload(payload, version))
                        },
                    }
                } else {
                    None
                }
            },
            _ => return None,
        },
        None => return None,
    };
    clean_payload
}

/// Part of the json comes escaped from the protobuf so we need to unescape in a safe way
fn get_clean_script_payload(payload: &ScriptPayload, version: i64) -> ScriptPayloadClean {
    ScriptPayloadClean {
        code: payload.code.clone(),
        type_arguments: payload.type_arguments.clone(),
        arguments: payload
            .arguments
            .iter()
            .map(|arg| {
                serde_json::from_str(arg).unwrap_or_else(|_| {
                    error!(version = version, "Unable to serialize payload into value");
                    panic!()
                })
            })
            .collect(),
    }
}

/// Get name from unwrapped move type
/// E.g. 0x1::domain::Name will return Name
pub fn get_name_from_unnested_move_type(move_type: &str) -> &str {
    let t: Vec<&str> = move_type.split("::").collect();
    t.last().unwrap()
}
