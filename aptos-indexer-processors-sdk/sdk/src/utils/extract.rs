// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! Helpers for extracting data from transactions.

use super::{
    convert::{deserialize_from_string, standardize_address, truncate_str},
    property_map::{PropertyMap, TokenObjectPropertyMap},
};
use aptos_protos::transaction::v1::{
    multisig_transaction_payload::Payload as MultisigPayloadType,
    transaction_payload::Payload as PayloadType, write_set::WriteSet as WriteSetType,
    EntryFunctionId, EntryFunctionPayload, MoveScriptBytecode, MoveType, ScriptPayload,
    TransactionPayload, UserTransactionRequest, WriteSet,
};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::Digest;
use tracing::{error, warn};

/// Max length of entry function id string to ensure that db doesn't explode
pub const MAX_ENTRY_FUNCTION_LENGTH: usize = 1000;

pub fn hash_str(val: &str) -> String {
    hex::encode(sha2::Sha256::digest(val.as_bytes()))
}

/// convert the bcs encoded inner value of property_map to its original value in string format
pub fn deserialize_property_map_from_bcs_hexstring<'de, D>(
    deserializer: D,
) -> core::result::Result<Value, D::Error>
where
    D: Deserializer<'de>,
{
    let s = serde_json::Value::deserialize(deserializer)?;
    // iterate the json string to convert key-value pair
    // assume the format of {“map”: {“data”: [{“key”: “Yuri”, “value”: {“type”: “String”, “value”: “0x42656e”}}, {“key”: “Tarded”, “value”: {“type”: “String”, “value”: “0x446f766572"}}]}}
    // if successfully parsing we return the decoded property_map string otherwise return the original string
    Ok(convert_bcs_propertymap(s.clone()).unwrap_or(s))
}

/// convert the bcs encoded inner value of property_map to its original value in string format
pub fn deserialize_token_object_property_map_from_bcs_hexstring<'de, D>(
    deserializer: D,
) -> core::result::Result<Value, D::Error>
where
    D: Deserializer<'de>,
{
    let s = serde_json::Value::deserialize(deserializer)?;
    // iterate the json string to convert key-value pair
    Ok(convert_bcs_token_object_propertymap(s.clone()).unwrap_or(s))
}

/// Convert the json serialized PropertyMap's inner BCS fields to their original value in string format
pub fn convert_bcs_propertymap(s: Value) -> Option<Value> {
    PropertyMap::from_bcs_encode_str(s).and_then(|e| serde_json::to_value(&e).ok())
}

pub fn convert_bcs_token_object_propertymap(s: Value) -> Option<Value> {
    TokenObjectPropertyMap::from_bcs_encode_str(s).and_then(|e| serde_json::to_value(&e).ok())
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::convert::deserialize_string_from_hexstring;
    use serde::Serialize;

    #[derive(Serialize, Deserialize, Debug)]
    struct TypeInfoMock {
        #[serde(deserialize_with = "deserialize_string_from_hexstring")]
        pub module_name: String,
        #[serde(deserialize_with = "deserialize_string_from_hexstring")]
        pub struct_name: String,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct TokenDataMock {
        #[serde(deserialize_with = "deserialize_property_map_from_bcs_hexstring")]
        pub default_properties: serde_json::Value,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct TokenObjectDataMock {
        #[serde(deserialize_with = "deserialize_token_object_property_map_from_bcs_hexstring")]
        pub default_properties: serde_json::Value,
    }

    #[test]
    fn test_deserialize_string_from_bcs() {
        let test_struct = TypeInfoMock {
            module_name: String::from("0x6170746f735f636f696e"),
            struct_name: String::from("0x4170746f73436f696e"),
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TypeInfoMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.module_name.as_str(), "aptos_coin");
        assert_eq!(d.struct_name.as_str(), "AptosCoin");
    }

    #[test]
    fn test_deserialize_property_map() {
        let test_property_json = r#"
        {
            "map":{
               "data":[
                  {
                     "key":"type",
                     "value":{
                        "type":"0x1::string::String",
                        "value":"0x06646f6d61696e"
                     }
                  },
                  {
                     "key":"creation_time_sec",
                     "value":{
                        "type":"u64",
                        "value":"0x140f4f6300000000"
                     }
                  },
                  {
                     "key":"expiration_time_sec",
                     "value":{
                        "type":"u64",
                        "value":"0x9442306500000000"
                     }
                  }
               ]
            }
        }"#;
        let test_property_json: serde_json::Value =
            serde_json::from_str(test_property_json).unwrap();
        let test_struct = TokenDataMock {
            default_properties: test_property_json,
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TokenDataMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.default_properties["type"], "domain");
        assert_eq!(d.default_properties["creation_time_sec"], "1666125588");
        assert_eq!(d.default_properties["expiration_time_sec"], "1697661588");
    }

    #[test]
    fn test_empty_property_map() {
        let test_property_json = r#"{"map": {"data": []}}"#;
        let test_property_json: serde_json::Value =
            serde_json::from_str(test_property_json).unwrap();
        let test_struct = TokenDataMock {
            default_properties: test_property_json,
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TokenDataMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.default_properties, Value::Object(serde_json::Map::new()));
    }

    #[test]
    fn test_deserialize_token_object_property_map() {
        let test_property_json = r#"
        {
            "data": [{
                    "key": "Rank",
                    "value": {
                        "type": 9,
                        "value": "0x0642726f6e7a65"
                    }
                },
                {
                    "key": "address_property",
                    "value": {
                        "type": 7,
                        "value": "0x2b4d540735a4e128fda896f988415910a45cab41c9ddd802b32dd16e8f9ca3cd"
                    }
                },
                {
                    "key": "bytes_property",
                    "value": {
                        "type": 8,
                        "value": "0x0401020304"
                    }
                },
                {
                    "key": "u64_property",
                    "value": {
                        "type": 4,
                        "value": "0x0000000000000001"
                    }
                }
            ]
        }
        "#;
        let test_property_json: serde_json::Value =
            serde_json::from_str(test_property_json).unwrap();
        let test_struct = TokenObjectDataMock {
            default_properties: test_property_json,
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TokenObjectDataMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.default_properties["Rank"], "Bronze");
        assert_eq!(
            d.default_properties["address_property"],
            "0x2b4d540735a4e128fda896f988415910a45cab41c9ddd802b32dd16e8f9ca3cd"
        );
        assert_eq!(d.default_properties["bytes_property"], "0x01020304");
        assert_eq!(d.default_properties["u64_property"], "72057594037927936");
    }

    #[test]
    fn test_empty_token_object_property_map() {
        let test_property_json = r#"{"data": []}"#;
        let test_property_json: serde_json::Value =
            serde_json::from_str(test_property_json).unwrap();
        let test_struct = TokenObjectDataMock {
            default_properties: test_property_json,
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TokenObjectDataMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.default_properties, Value::Object(serde_json::Map::new()));
    }
}
