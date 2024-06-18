// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_protos::{
    transaction::v1::{
        multisig_transaction_payload::Payload as MultisigPayloadType,
        transaction_payload::Payload as PayloadType, write_set::WriteSet as WriteSetType,
        EntryFunctionId, EntryFunctionPayload, MoveScriptBytecode, MoveType, ScriptPayload,
        TransactionPayload, UserTransactionRequest, WriteSet,
    },
    util::timestamp::Timestamp,
};
use bigdecimal::{BigDecimal, Signed, ToPrimitive, Zero};
use lazy_static::lazy_static;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::Digest;
use std::str::FromStr;
use tiny_keccak::{Hasher, Sha3};

// 9999-12-31 23:59:59, this is the max supported by Google BigQuery
pub const MAX_TIMESTAMP_SECS: i64 = 253_402_300_799;
// Max length of entry function id string to ensure that db doesn't explode
pub const MAX_ENTRY_FUNCTION_LENGTH: usize = 1000;

pub const APTOS_COIN_TYPE_STR: &str = "0x1::aptos_coin::AptosCoin";

lazy_static! {
    pub static ref APT_METADATA_ADDRESS_RAW: [u8; 32] = {
        let mut addr = [0u8; 32];
        addr[31] = 10u8;
        addr
    };
    pub static ref APT_METADATA_ADDRESS_HEX: String =
        format!("0x{}", hex::encode(*APT_METADATA_ADDRESS_RAW));
}
// Supporting structs to get clean payload without escaped strings
#[derive(Debug, Deserialize, Serialize)]
pub struct EntryFunctionPayloadClean {
    pub function: Option<EntryFunctionId>,
    pub type_arguments: Vec<MoveType>,
    pub arguments: Vec<Value>,
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

/// Standardizes all addresses and table handles to be length 66 (0x-64 length hash)
pub fn standardize_address(handle: &str) -> String {
    if let Some(handle) = handle.strip_prefix("0x") {
        format!("0x{:0>64}", handle)
    } else {
        format!("0x{:0>64}", handle)
    }
}

pub fn hash_str(val: &str) -> String {
    hex::encode(sha2::Sha256::digest(val.as_bytes()))
}

pub fn sha3_256(buffer: &[u8]) -> [u8; 32] {
    let mut output = [0; 32];
    let mut sha3 = Sha3::v256();
    sha3.update(buffer);
    sha3.finalize(&mut output);
    output
}

pub fn truncate_str(val: &str, max_chars: usize) -> String {
    let mut trunc = val.to_string();
    trunc.truncate(max_chars);
    trunc
}

pub fn u64_to_bigdecimal(val: u64) -> BigDecimal {
    BigDecimal::from(val)
}

pub fn bigdecimal_to_u64(val: &BigDecimal) -> u64 {
    val.to_u64().expect("Unable to convert big decimal to u64")
}

pub fn ensure_not_negative(val: BigDecimal) -> BigDecimal {
    if val.is_negative() {
        return BigDecimal::zero();
    }
    val
}

pub fn get_entry_function_from_user_request(
    user_request: &UserTransactionRequest,
) -> Option<String> {
    let entry_function_id_str: String = match &user_request.payload.as_ref().unwrap().payload {
        Some(PayloadType::EntryFunctionPayload(payload)) => payload.entry_function_id_str.clone(),
        Some(PayloadType::MultisigPayload(payload)) => {
            if let Some(payload) = payload.transaction_payload.as_ref() {
                match payload.payload.as_ref().unwrap() {
                    MultisigPayloadType::EntryFunctionPayload(payload) => {
                        Some(payload.entry_function_id_str.clone())
                    },
                };
            }
            return None;
        },
        _ => return None,
    };
    Some(truncate_str(
        &entry_function_id_str,
        MAX_ENTRY_FUNCTION_LENGTH,
    ))
}

pub fn get_payload_type(payload: &TransactionPayload) -> String {
    payload.r#type().as_str_name().to_string()
}

/// Part of the json comes escaped from the protobuf so we need to unescape in a safe way
/// This function converts the string into json recursively and lets the diesel ORM handles
/// the escaping.
pub fn get_clean_payload(payload: &TransactionPayload, version: i64) -> Option<Value> {
    if payload.payload.as_ref().is_none() {
        // PROCESSOR_UNKNOWN_TYPE_COUNT
        //     .with_label_values(&["TransactionPayload"])
        //     .inc();
        tracing::warn!(
            transaction_version = version,
            "Transaction payload doesn't exist",
        );
        return None;
    }
    match payload.payload.as_ref().unwrap() {
        PayloadType::EntryFunctionPayload(inner) => {
            let clean = get_clean_entry_function_payload(inner, version);
            Some(serde_json::to_value(clean).unwrap_or_else(|_| {
                tracing::error!(version = version, "Unable to serialize payload into value");
                panic!()
            }))
        },
        PayloadType::ScriptPayload(inner) => {
            let clean = get_clean_script_payload(inner, version);
            Some(serde_json::to_value(clean).unwrap_or_else(|_| {
                tracing::error!(version = version, "Unable to serialize payload into value");
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
                            tracing::error!(
                                version = version,
                                "Unable to serialize payload into value"
                            );
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
                tracing::error!(version = version, "Unable to serialize payload into value");
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
                        tracing::error!(
                            version = version,
                            "Unable to serialize payload into value"
                        );
                        panic!()
                    },
                ),
            )
        },
        WriteSetType::DirectWriteSet(_) => None,
    }
}

/// Part of the json comes escaped from the protobuf so we need to unescape in a safe way
fn get_clean_entry_function_payload(
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
                    tracing::error!(version = version, "Unable to serialize payload into value");
                    panic!()
                })
            })
            .collect(),
    }
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
                    tracing::error!(version = version, "Unable to serialize payload into value");
                    panic!()
                })
            })
            .collect(),
    }
}

pub fn parse_timestamp(ts: &Timestamp, version: i64) -> chrono::NaiveDateTime {
    let final_ts = if ts.seconds >= MAX_TIMESTAMP_SECS {
        Timestamp {
            seconds: MAX_TIMESTAMP_SECS,
            nanos: 0,
        }
    } else {
        ts.clone()
    };
    chrono::NaiveDateTime::from_timestamp_opt(final_ts.seconds, final_ts.nanos as u32)
        .unwrap_or_else(|| panic!("Could not parse timestamp {:?} for version {}", ts, version))
}

pub fn parse_timestamp_secs(ts: u64, version: i64) -> chrono::NaiveDateTime {
    chrono::NaiveDateTime::from_timestamp_opt(
        std::cmp::min(ts, MAX_TIMESTAMP_SECS as u64) as i64,
        0,
    )
    .unwrap_or_else(|| panic!("Could not parse timestamp {:?} for version {}", ts, version))
}

pub fn remove_null_bytes<T: serde::Serialize + for<'de> serde::Deserialize<'de>>(input: &T) -> T {
    let mut txn_json = serde_json::to_value(input).unwrap();
    recurse_remove_null_bytes_from_json(&mut txn_json);
    serde_json::from_value::<T>(txn_json).unwrap()
}

fn recurse_remove_null_bytes_from_json(sub_json: &mut Value) {
    match sub_json {
        Value::Array(array) => {
            for item in array {
                recurse_remove_null_bytes_from_json(item);
            }
        },
        Value::Object(object) => {
            for (_key, value) in object {
                recurse_remove_null_bytes_from_json(value);
            }
        },
        Value::String(str) => {
            if !str.is_empty() {
                let replacement = string_null_byte_replacement(str);
                *str = replacement;
            }
        },
        _ => {},
    }
}

fn string_null_byte_replacement(value: &str) -> String {
    value.replace('\u{0000}', "").replace("\\u0000", "")
}

// /// convert the bcs encoded inner value of property_map to its original value in string format
// pub fn deserialize_property_map_from_bcs_hexstring<'de, D>(
//     deserializer: D,
// ) -> core::result::Result<Value, D::Error>
// where
//     D: Deserializer<'de>,
// {
//     let s = serde_json::Value::deserialize(deserializer)?;
//     // iterate the json string to convert key-value pair
//     // assume the format of {“map”: {“data”: [{“key”: “Yuri”, “value”: {“type”: “String”, “value”: “0x42656e”}}, {“key”: “Tarded”, “value”: {“type”: “String”, “value”: “0x446f766572"}}]}}
//     // if successfully parsing we return the decoded property_map string otherwise return the original string
//     Ok(convert_bcs_propertymap(s.clone()).unwrap_or(s))
// }

// /// convert the bcs encoded inner value of property_map to its original value in string format
// pub fn deserialize_token_object_property_map_from_bcs_hexstring<'de, D>(
//     deserializer: D,
// ) -> core::result::Result<Value, D::Error>
// where
//     D: Deserializer<'de>,
// {
//     let s = serde_json::Value::deserialize(deserializer)?;
//     // iterate the json string to convert key-value pair
//     Ok(convert_bcs_token_object_propertymap(s.clone()).unwrap_or(s))
// }

pub fn deserialize_string_from_hexstring<'de, D>(
    deserializer: D,
) -> core::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = <String>::deserialize(deserializer)?;
    Ok(String::from_utf8(hex_to_raw_bytes(&s).unwrap()).unwrap_or(s))
}

/// Convert the bcs serialized vector<u8> to its original string format
pub fn convert_bcs_hex(typ: String, value: String) -> Option<String> {
    let decoded = hex::decode(value.strip_prefix("0x").unwrap_or(&*value)).ok()?;

    match typ.as_str() {
        "0x1::string::String" => bcs::from_bytes::<String>(decoded.as_slice()),
        "u8" => bcs::from_bytes::<u8>(decoded.as_slice()).map(|e| e.to_string()),
        "u64" => bcs::from_bytes::<u64>(decoded.as_slice()).map(|e| e.to_string()),
        "u128" => bcs::from_bytes::<u128>(decoded.as_slice()).map(|e| e.to_string()),
        "bool" => bcs::from_bytes::<bool>(decoded.as_slice()).map(|e| e.to_string()),
        "address" => bcs::from_bytes::<String>(decoded.as_slice()).map(|e| format!("0x{}", e)),
        _ => Ok(value),
    }
    .ok()
}

/// Convert the bcs serialized vector<u8> to its original string format for token v2 property map.
pub fn convert_bcs_hex_new(typ: u8, value: String) -> Option<String> {
    let decoded = hex::decode(value.strip_prefix("0x").unwrap_or(&*value)).ok()?;

    match typ {
        0 /* bool */ => bcs::from_bytes::<bool>(decoded.as_slice()).map(|e| e.to_string()),
        1 /* u8 */ => bcs::from_bytes::<u8>(decoded.as_slice()).map(|e| e.to_string()),
        2 /* u16 */ => bcs::from_bytes::<u16>(decoded.as_slice()).map(|e| e.to_string()),
        3 /* u32 */ => bcs::from_bytes::<u32>(decoded.as_slice()).map(|e| e.to_string()),
        4 /* u64 */ => bcs::from_bytes::<u64>(decoded.as_slice()).map(|e| e.to_string()),
        5 /* u128 */ => bcs::from_bytes::<u128>(decoded.as_slice()).map(|e| e.to_string()),
        6 /* u256 */ => bcs::from_bytes::<BigDecimal>(decoded.as_slice()).map(|e| e.to_string()),
        7 /* address */ => bcs::from_bytes::<String>(decoded.as_slice()).map(|e| format!("0x{}", e)),
        8 /* byte_vector */ => bcs::from_bytes::<Vec<u8>>(decoded.as_slice()).map(|e| format!("0x{}", hex::encode(e))),
        9 /* string */ => bcs::from_bytes::<String>(decoded.as_slice()),
        _ => Ok(value),
    }
        .ok()
}

// /// Convert the json serialized PropertyMap's inner BCS fields to their original value in string format
// pub fn convert_bcs_propertymap(s: Value) -> Option<Value> {
//     match PropertyMap::from_bcs_encode_str(s) {
//         Some(e) => match serde_json::to_value(&e) {
//             Ok(val) => Some(val),
//             Err(_) => None,
//         },
//         None => None,
//     }
// }

// pub fn convert_bcs_token_object_propertymap(s: Value) -> Option<Value> {
//     match TokenObjectPropertyMap::from_bcs_encode_str(s) {
//         Some(e) => match serde_json::to_value(&e) {
//             Ok(val) => Some(val),
//             Err(_) => None,
//         },
//         None => None,
//     }
// }

/// Convert from hex string to raw byte string
pub fn hex_to_raw_bytes(val: &str) -> anyhow::Result<Vec<u8>> {
    Ok(hex::decode(val.strip_prefix("0x").unwrap_or(val))?)
}

/// Deserialize from string to type T
pub fn deserialize_from_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    use serde::de::Error;

    let s = <String>::deserialize(deserializer)?;
    s.parse::<T>().map_err(D::Error::custom)
}

/// Convert the protobuf Timestamp to epcoh time in seconds.
pub fn time_diff_since_pb_timestamp_in_secs(timestamp: &Timestamp) -> f64 {
    let current_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("SystemTime before UNIX EPOCH!")
        .as_secs_f64();
    let transaction_time = timestamp.seconds as f64 + timestamp.nanos as f64 * 1e-9;
    current_timestamp - transaction_time
}

/// Convert the protobuf timestamp to ISO format
pub fn timestamp_to_iso(timestamp: &Timestamp) -> String {
    let dt = parse_timestamp(timestamp, 0);
    dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()
}

/// Convert the protobuf timestamp to unixtime
pub fn timestamp_to_unixtime(timestamp: &Timestamp) -> f64 {
    timestamp.seconds as f64 + timestamp.nanos as f64 * 1e-9
}

/// Get name from unwrapped move type
/// E.g. 0x1::domain::Name will return Name
pub fn get_name_from_unnested_move_type(move_type: &str) -> &str {
    let t: Vec<&str> = move_type.split("::").collect();
    t.last().unwrap()
}

/* COMMON STRUCTS */
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