# Utils

## Chain ID Check

The `chain_id_check.rs` file provides tools to manage and verify the chain ID during processing. It helps to ensure the processor is indexing the correct chain ID. 

### ChainIdChecker Trait

This trait has two main functions that need to be implemented:

- `save_chain_id`: Saves the current chain ID to storage.
- `get_chain_id`: Retrieves the chain ID from storage.


### `check_or_update_chain_id` Function

This function checks if the chain ID from a `TransactionStream` matches the one in storage. If they match, processing continues. If not, it updates the storage with the new chain ID. This helps prevent processing errors due to mismatched chain IDs.

Use this function in your processor to manage the chain ID. 

