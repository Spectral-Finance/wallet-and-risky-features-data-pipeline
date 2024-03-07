from pyspark.sql.types import TimestampType, LongType, StringType, DecimalType, ArrayType, BooleanType

ETHEREUM_TABLES_SCHEMA = {
    "ethereum_blocks": {
        "description": "The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.\nSchedule Interval: every 5 minutes",
        "fields": {
            "uuid": {"dtype": StringType(), "comment": "Unique identifier for the block"},
            "timestamp": {"dtype": LongType(), "comment": "The timestamp for when the block was collated"},
            "timestamp_readable": {
                "dtype": TimestampType(),
                "comment": "The timestamp for when the block was collated in readable format",
            },
            "number": {"dtype": LongType(), "comment": "The block number"},
            "hash": {"dtype": StringType(), "comment": "The hash of the block"},
            "parent_hash": {"dtype": StringType(), "comment": "The hash of the parent block"},
            "nonce": {"dtype": StringType(), "comment": "Hash of the generated proof-of-work"},
            "sha3_uncles": {"dtype": StringType(), "comment": "SHA3 of the uncles data in the block"},
            "logs_bloom": {"dtype": StringType(), "comment": "The bloom filter for the logs of the block"},
            "transactions_root": {"dtype": StringType(), "comment": "The root of the transaction trie of the block"},
            "state_root": {"dtype": StringType(), "comment": "The root of the final state trie of the block"},
            "receipts_root": {"dtype": StringType(), "comment": "The root of the receipts trie of the block"},
            "miner": {
                "dtype": StringType(),
                "comment": "The address of the beneficiary to whom the mining rewards were given",
            },
            "difficulty": {"dtype": DecimalType(38, 9), "comment": "Integer of the difficulty for this block"},
            "total_difficulty": {
                "dtype": DecimalType(38, 9),
                "comment": "Integer of the total difficulty of the chain until this block",
            },
            "size": {"dtype": LongType(), "comment": "Integer the size of this block in bytes"},
            "extra_data": {"dtype": StringType(), "comment": 'The "extra data" field of this block'},
            "gas_limit": {"dtype": LongType(), "comment": "The maximum gas allowed in this block"},
            "gas_used": {"dtype": LongType(), "comment": "The total used gas by all transactions in this block"},
            "transaction_count": {"dtype": LongType(), "comment": "	The number of transactions in the block"},
            "base_fee_per_gas": {
                "dtype": LongType(),
                "comment": "Protocol base fee per gas, which can move up or down",
            },
            "date_partition": {
                "dtype": StringType(),
                "comment": "Partition column based on the timestamp of the block",
            },
        },
    },
    "ethereum_transactions": {
        "description": """Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes.
                    \nThis table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.\nSchedule Interval: every 5 minutes""",
        "fields": {
            "uuid": {"dtype": StringType(), "comment": "Unique identifier for the transaction"},
            "hash": {"dtype": StringType(), "comment": "The hash of the transaction"},
            "nonce": {
                "dtype": LongType(),
                "comment": "The number of transactions made by the sender prior to this one",
            },
            "transaction_index": {
                "dtype": LongType(),
                "comment": "Integer of the transactions index position in the block",
            },
            "from_address": {"dtype": StringType(), "comment": "Address of the sender"},
            "to_address": {
                "dtype": StringType(),
                "comment": "Address of the receiver. null when its a contract creation transaction",
            },
            "value": {"dtype": DecimalType(38, 9), "comment": "Value transferred in Ether"},
            "gas": {"dtype": LongType(), "comment": "Gas provided by the sender"},
            "gas_price": {"dtype": LongType(), "comment": "Gas price provided by the sender in Wei"},
            "input": {"dtype": StringType(), "comment": "The data sent along with the transaction"},
            "receipt_cumulative_gas_used": {
                "dtype": LongType(),
                "comment": "The total amount of gas used when this transaction was executed in the block",
            },
            "receipt_gas_used": {
                "dtype": LongType(),
                "comment": "The amount of gas used by this specific transaction alone",
            },
            "receipt_contract_address": {
                "dtype": StringType(),
                "comment": "The contract address created, if the transaction was a contract creation, otherwise null",
            },
            "receipt_root": {"dtype": StringType(), "comment": "The post-transaction state root of the block"},
            "receipt_status": {"dtype": LongType(), "comment": "Either 1 (success) or 0 (failure)"},
            "block_timestamp": {"dtype": LongType(), "comment": "The timestamp for when the block was collated"},
            "block_timestamp_readable": {
                "dtype": TimestampType(),
                "comment": "The timestamp for when the block was collated in readable format",
            },
            "block_number": {"dtype": LongType(), "comment": "The block number"},
            "block_hash": {"dtype": StringType(), "comment": "The hash of the block"},
            "max_fee_per_gas": {"dtype": LongType(), "comment": "Protocol max fee per gas, which can move up or down"},
            "max_priority_fee_per_gas": {
                "dtype": LongType(),
                "comment": "Protocol max priority fee per gas, which can move up or down",
            },
            "transaction_type": {"dtype": StringType(), "comment": "The type of transaction"},
            "receipt_effective_gas_price": {
                "dtype": LongType(),
                "comment": "The effective gas price of the transaction",
            },
            "date_partition": {
                "dtype": StringType(),
                "comment": "Partition column based on the timestamp of the block",
            },
        },
    },
    "ethereum_logs": {
        "description": "Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. \nThis table is generally useful for reporting on any logged event type on the Ethereum blockchain.\nSchedule Interval: every 5 minutes",
        "fields": {
            "uuid": {"dtype": StringType(), "comment": "Unique identifier for the log"},
            "log_index": {"dtype": LongType(), "comment": "Integer of the log index position in the block"},
            "transaction_hash": {"dtype": StringType(), "comment": "Hash of the transaction"},
            "transaction_index": {
                "dtype": LongType(),
                "comment": "Integer of the transactions index position in the block",
            },
            "address": {"dtype": StringType(), "comment": "Address from which this log originated"},
            "data": {
                "dtype": StringType(),
                "comment": "Contains one or more 32 Bytes non-indexed arguments of the log",
            },
            "topics": {
                "dtype": ArrayType(StringType()),
                "comment": "Indexed log arguments (0 to 4 32-byte hex strings). (In solidity: The first topic is the hash of the signature of the event (e.g. Deposit(address,bytes32,uint256)), except you declared the event with the anonymous specifier.)",
            },
            "block_timestamp": {"dtype": LongType(), "comment": "The timestamp for when the block was collated"},
            "block_timestamp_readable": {
                "dtype": TimestampType(),
                "comment": "The timestamp for when the block was collated in readable format",
            },
            "block_number": {"dtype": LongType(), "comment": "The block number"},
            "block_hash": {"dtype": StringType(), "comment": "The hash of the block"},
            "date_partition": {
                "dtype": StringType(),
                "comment": "Partition column based on the timestamp of the block",
            },
        },
    },
    "ethereum_token_transfers": {
        "description": """The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address.
                \nThis table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events. \nSchedule Interval: every 5 minutes""",
        "fields": {
            "uuid": {"dtype": StringType(), "comment": "Unique identifier for the token transfer"},
            "token_address": {"dtype": StringType(), "comment": "The address of the token contract"},
            "from_address": {"dtype": StringType(), "comment": "The address of the sender"},
            "to_address": {"dtype": StringType(), "comment": "The address of the receiver"},
            "value": {"dtype": DecimalType(38, 9), "comment": "The number of tokens transferred"},
            "transaction_hash": {"dtype": StringType(), "comment": "The hash of the transaction"},
            "log_index": {
                "dtype": LongType(),
                "comment": "The index of the log entry created for the event in the block",
            },
            "block_timestamp": {"dtype": LongType(), "comment": "The timestamp for when the block was collated"},
            "block_timestamp_readable": {
                "dtype": TimestampType(),
                "comment": "The timestamp for when the block was collated in readable format",
            },
            "block_number": {"dtype": LongType(), "comment": "The block number"},
            "block_hash": {"dtype": StringType(), "comment": "The hash of the block"},
            "date_partition": {
                "dtype": StringType(),
                "comment": "Partition column based on the timestamp of the block",
            },
        },
    },
    "ethereum_traces": {
        "description": "The traces table contains data for all transactions that have been executed on the Ethereum blockchain. \nSchedule Interval: every 5 minutes",
        "fields": {
            "uuid": {"dtype": StringType(), "comment": "Unique identifier for the trace"},
            "transaction_hash": {"dtype": StringType(), "comment": "Hash of the transaction"},
            "transaction_index": {
                "dtype": LongType(),
                "comment": "Integer of the transactions index position in the block",
            },
            "from_address": {"dtype": StringType(), "comment": "Address of the sender"},
            "to_address": {
                "dtype": StringType(),
                "comment": "Address of the receiver. null when its a contract creation transaction",
            },
            "value": {
                "dtype": DecimalType(38, 9),
                "comment": "Value transferred in Wei (the smallest denomination of ether)",
            },
            "input": {"dtype": StringType(), "comment": "The data send along with the transaction"},
            "output": {"dtype": StringType(), "comment": "The return value of executed contract"},
            "trace_type": {"dtype": StringType(), "comment": "The type of the trace"},
            "call_type": {"dtype": StringType(), "comment": "The call type of the trace"},
            "reward_type": {"dtype": StringType(), "comment": "The reward type of the trace"},
            "gas": {"dtype": LongType(), "comment": "The amount of gas used by this specific trace"},
            "gas_used": {"dtype": LongType(), "comment": "The amount of gas used in this transaction"},
            "subtraces": {
                "dtype": LongType(),
                "comment": "An integer of the total number of traces created by this transaction",
            },
            "trace_address": {"dtype": StringType(), "comment": "Array of trace addresses of this transaction"},
            "error": {"dtype": StringType(), "comment": "Error string"},
            "status": {"dtype": LongType(), "comment": "The status of the transaction"},
            "block_timestamp": {"dtype": LongType(), "comment": "The timestamp for when the block was collated"},
            "block_timestamp_readable": {
                "dtype": TimestampType(),
                "comment": "The timestamp for when the block was collated in readable format",
            },
            "block_number": {"dtype": LongType(), "comment": "The block number"},
            "block_hash": {"dtype": StringType(), "comment": "The hash of the block"},
            "trace_id": {"dtype": StringType(), "comment": "The trace id"},
            "date_partition": {
                "dtype": StringType(),
                "comment": "Partition column based on the timestamp of the block",
            },
        },
    },
    "ethereum_tokens": {
        "description": "The tokens table contains data for all tokens that have been deployed on the Ethereum blockchain. \nSchedule Interval: every 5 minutes",
        "fields": {
            "uuid": {"dtype": StringType(), "comment": "Unique identifier for the token"},
            "address": {"dtype": StringType(), "comment": "The address of the token contract"},
            "name": {"dtype": StringType(), "comment": "The name of the token"},
            "symbol": {"dtype": StringType(), "comment": "The symbol of the token"},
            "decimals": {"dtype": LongType(), "comment": "The number of decimals the token uses"},
            "total_supply": {
                "dtype": DecimalType(38, 9),
                "comment": "The total supply of the token in its smallest unit",
            },
            "block_timestamp": {"dtype": TimestampType(), "comment": "The timestamp for when the block was collated"},
            "date_partition": {
                "dtype": StringType(),
                "comment": "Partition column based on the timestamp of the block",
            },
        },
    },
    "ethereum_contracts": {
        "description": "The contracts table contains data for all contracts that have been deployed on the Ethereum blockchain. \nSchedule Interval: every 5 minutes",
        "fields": {
            "uuid": {"dtype": StringType(), "comment": "Unique identifier for the contract"},
            "address": {"dtype": StringType(), "comment": "The address of the contract"},
            "bytecode": {"dtype": StringType(), "comment": "The bytecode of the contract"},
            "function_sighashes": {"dtype": StringType(), "comment": "The function sighashes of the contract"},
            "is_erc20": {"dtype": BooleanType(), "comment": "Whether the contract is an ERC20 token"},
            "is_erc721": {"dtype": BooleanType(), "comment": "Whether the contract is an ERC721 token"},
            "block_timestamp": {"dtype": TimestampType(), "comment": "The timestamp for when the block was collated"},
            "date_partition": {
                "dtype": StringType(),
                "comment": "Partition column based on the timestamp of the block",
            },
        },
    },
}
