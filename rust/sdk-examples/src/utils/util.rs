pub fn truncate_str(val: &str, max_chars: usize) -> String {
    let mut trunc = val.to_string();
    trunc.truncate(max_chars);
    trunc
}

/// Standardizes all addresses and table handles to be length 66 (0x-64 length hash)
pub fn standardize_address(handle: &str) -> String {
    if let Some(handle) = handle.strip_prefix("0x") {
        format!("0x{:0>64}", handle)
    } else {
        format!("0x{:0>64}", handle)
    }
}
