// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! Helpers related to basic conversion like string manipulation, converting between
//! number types, BCS, and hashing.

use bigdecimal::{BigDecimal, Signed, ToPrimitive, Zero};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::str::FromStr;
use tiny_keccak::{Hasher, Sha3};

#[allow(clippy::too_long_first_doc_paragraph)]
/// Standardizes an address / table handle to be a string with length 66 (0x+64 length hex string).
pub fn standardize_address(handle: &str) -> String {
    if let Some(handle) = handle.strip_prefix("0x") {
        format!("0x{handle:0>64}")
    } else {
        format!("0x{handle:0>64}")
    }
}

#[allow(clippy::too_long_first_doc_paragraph)]
/// Standardizes an address / table handle to be a string with length 66 (0x+64 length hex string).
pub fn standardize_address_from_bytes(bytes: &[u8]) -> String {
    let encoded_bytes = hex::encode(bytes);
    standardize_address(&encoded_bytes)
}

/// Convert a hex string into a raw byte string. Any leading 0x will be stripped.
pub fn hex_to_raw_bytes(val: &str) -> anyhow::Result<Vec<u8>> {
    Ok(hex::decode(val.strip_prefix("0x").unwrap_or(val))?)
}

/// Truncate a string to a maximum number of characters.
pub fn truncate_str(val: &str, max_chars: usize) -> String {
    let mut trunc = val.to_string();
    trunc.truncate(max_chars);
    trunc
}

pub fn sha3_256(buffer: &[u8]) -> [u8; 32] {
    let mut output = [0; 32];
    let mut sha3 = Sha3::v256();
    sha3.update(buffer);
    sha3.finalize(&mut output);
    output
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

/// Remove null bytes from a JSON object.
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
        Value::String(str) if !str.is_empty() => {
            let replacement = string_null_byte_replacement(str);
            *str = replacement;
        },
        _ => {},
    }
}

fn string_null_byte_replacement(value: &str) -> String {
    value.replace('\u{0000}', "").replace("\\u0000", "")
}

pub fn deserialize_string_from_hexstring<'de, D>(
    deserializer: D,
) -> core::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = <String>::deserialize(deserializer)?;
    Ok(String::from_utf8(hex_to_raw_bytes(&s).unwrap()).unwrap_or(s))
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

/// Convert the bcs serialized vector<u8> to its original string format
pub fn convert_bcs_hex(typ: String, value: String) -> Option<String> {
    let decoded = hex::decode(value.strip_prefix("0x").unwrap_or(&*value)).ok()?;

    match typ.as_str() {
        "0x1::string::String" => bcs::from_bytes::<String>(decoded.as_slice()),
        "u8" => bcs::from_bytes::<u8>(decoded.as_slice()).map(|e| e.to_string()),
        "u16" => bcs::from_bytes::<u16>(decoded.as_slice()).map(|e| e.to_string()),
        "u32" => bcs::from_bytes::<u32>(decoded.as_slice()).map(|e| e.to_string()),
        "u64" => bcs::from_bytes::<u64>(decoded.as_slice()).map(|e| e.to_string()),
        "u128" => bcs::from_bytes::<u128>(decoded.as_slice()).map(|e| e.to_string()),
        "u256" => bcs::from_bytes::<[u8; 32]>(decoded.as_slice()).map(|e| u256_to_string(&e)),
        "i8" => bcs::from_bytes::<i8>(decoded.as_slice()).map(|e| e.to_string()),
        "i16" => bcs::from_bytes::<i16>(decoded.as_slice()).map(|e| e.to_string()),
        "i32" => bcs::from_bytes::<i32>(decoded.as_slice()).map(|e| e.to_string()),
        "i64" => bcs::from_bytes::<i64>(decoded.as_slice()).map(|e| e.to_string()),
        "i128" => bcs::from_bytes::<i128>(decoded.as_slice()).map(|e| e.to_string()),
        "i256" => bcs::from_bytes::<[u8; 32]>(decoded.as_slice()).map(|e| i256_to_string(&e)),
        "bool" => bcs::from_bytes::<bool>(decoded.as_slice()).map(|e| e.to_string()),
        "address" => bcs::from_bytes::<String>(decoded.as_slice()).map(|e| format!("0x{e}")),
        _ => Ok(value),
    }
    .ok()
}

/// Converts 32 little-endian bytes of an unsigned 256-bit integer to its decimal string.
fn u256_to_string(bytes: &[u8; 32]) -> String {
    let lo = u128::from_le_bytes(bytes[..16].try_into().expect("16 bytes"));
    let hi = u128::from_le_bytes(bytes[16..].try_into().expect("16 bytes"));
    // 2^128 does not fit in u128, so build it as (2^64)^2 with exact BigDecimal arithmetic.
    const TWO_POW_64: u128 = 2u128.pow(64);
    let value = BigDecimal::from(hi) * BigDecimal::from(TWO_POW_64) * BigDecimal::from(TWO_POW_64)
        + BigDecimal::from(lo);
    value.to_string()
}

/// Converts 32 little-endian bytes of a 256-bit two's complement integer to its decimal string.
fn i256_to_string(bytes: &[u8; 32]) -> String {
    let negative = bytes[31] & 0x80 != 0;
    if !negative {
        return u256_to_string(bytes);
    }

    // Two's complement negation: invert all bytes, then add 1.
    let mut inverted = *bytes;
    for byte in inverted.iter_mut() {
        *byte = !*byte;
    }
    let mut carry = 1u16;
    for byte in inverted.iter_mut() {
        let sum = *byte as u16 + carry;
        *byte = sum as u8;
        carry = sum >> 8;
    }
    format!("-{}", u256_to_string(&inverted))
}

/// Convert the bcs serialized vector<u8> to its original string format for token v2 property map.
pub fn convert_bcs_hex_new(typ: u8, value: String) -> Option<String> {
    let decoded = hex::decode(value.strip_prefix("0x").unwrap_or(&*value)).ok()?;

    // Signed integers are not supported in token v2 property maps right now:
    // https://github.com/aptos-labs/aptos-core/blob/5f5d138562dd0732e14c3e4265d3aa1218144145/aptos-move/framework/aptos-token-objects/sources/property_map.move#L37
    match typ {
        0 /* bool */ => bcs::from_bytes::<bool>(decoded.as_slice()).map(|e| e.to_string()),
        1 /* u8 */ => bcs::from_bytes::<u8>(decoded.as_slice()).map(|e| e.to_string()),
        2 /* u16 */ => bcs::from_bytes::<u16>(decoded.as_slice()).map(|e| e.to_string()),
        3 /* u32 */ => bcs::from_bytes::<u32>(decoded.as_slice()).map(|e| e.to_string()),
        4 /* u64 */ => bcs::from_bytes::<u64>(decoded.as_slice()).map(|e| e.to_string()),
        5 /* u128 */ => bcs::from_bytes::<u128>(decoded.as_slice()).map(|e| e.to_string()),
        6 /* u256 */ => bcs::from_bytes::<[u8; 32]>(decoded.as_slice()).map(|e| u256_to_string(&e)),
        7 /* address */ => bcs::from_bytes::<String>(decoded.as_slice()).map(|e| format!("0x{e}")),
        8 /* byte_vector */ => bcs::from_bytes::<Vec<u8>>(decoded.as_slice()).map(|e| format!("0x{}", hex::encode(e))),
        9 /* string */ => bcs::from_bytes::<String>(decoded.as_slice()),
        _ => Ok(value),
    }
        .ok()
}

#[cfg(test)]
mod u256_decode_tests {
    use super::*;

    fn hex_of(bytes: &[u8]) -> String {
        format!("0x{}", hex::encode(bytes))
    }

    #[test]
    fn test_u256_decode_small_value() {
        let mut bytes = [0u8; 32];
        bytes[0] = 1;
        assert_eq!(
            convert_bcs_hex("u256".to_string(), hex_of(&bytes)),
            Some("1".to_string())
        );
    }

    #[test]
    fn test_u256_decode_upper_half() {
        // 2^128: high 128 bits = 1, low = 0.
        let mut bytes = [0u8; 32];
        bytes[16] = 1;
        assert_eq!(
            convert_bcs_hex("u256".to_string(), hex_of(&bytes)),
            Some("340282366920938463463374607431768211456".to_string())
        );
    }

    #[test]
    fn test_u256_decode_max_value() {
        let bytes = [0xff; 32];
        assert_eq!(
            convert_bcs_hex("u256".to_string(), hex_of(&bytes)),
            Some(
                "115792089237316195423570985008687907853269984665640564039457584007913129639935"
                    .to_string()
            )
        );
    }

    #[test]
    fn test_u256_decode_rejects_wrong_length() {
        // 31 bytes is not a valid u256 BCS encoding.
        let bytes = [0u8; 31];
        assert_eq!(convert_bcs_hex("u256".to_string(), hex_of(&bytes)), None);
    }

    #[test]
    fn test_i256_decode_negative_one() {
        let bytes = [0xff; 32];
        assert_eq!(
            convert_bcs_hex("i256".to_string(), hex_of(&bytes)),
            Some("-1".to_string())
        );
    }

    #[test]
    fn test_i256_decode_positive() {
        let mut bytes = [0u8; 32];
        bytes[0] = 42;
        assert_eq!(
            convert_bcs_hex("i256".to_string(), hex_of(&bytes)),
            Some("42".to_string())
        );
    }

    #[test]
    fn test_i256_decode_min_value() {
        // Two's complement minimum: 0x80 followed by zeros (big-endian view),
        // i.e. low byte 31 = 0x80 in little-endian layout.
        let mut bytes = [0u8; 32];
        bytes[31] = 0x80;
        assert_eq!(
            convert_bcs_hex("i256".to_string(), hex_of(&bytes)),
            Some(
                "-57896044618658097711785492504343953926634992332820282019728792003956564819968"
                    .to_string()
            )
        );
    }

    #[test]
    fn test_token_object_u256_decode() {
        // Token v2 property maps identify u256 by numeric type 6.
        let mut bytes = [0u8; 32];
        bytes[0] = 100;
        assert_eq!(
            convert_bcs_hex_new(6, hex_of(&bytes)),
            Some("100".to_string())
        );
    }
}
