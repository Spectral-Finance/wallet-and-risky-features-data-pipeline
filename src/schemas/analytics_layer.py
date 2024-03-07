from pyspark.sql.types import (
    TimestampType,
    LongType,
    StringType,
    DecimalType,
    ArrayType,
    BooleanType,
)

ETHEREUM_TABLES_SCHEMA = {
    "ethereum_normal_transactions": {
        "description": """Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes.
                    \nThis table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.\nSchedule Interval: every 5 minutes""",
        "fields": {
            "uuid": {"dtype": StringType(), "comment": "Unique identifier for the transaction"},
            "hash": {"dtype": StringType(), "comment": "The hash of the transaction"},
            "from": {"dtype": StringType(), "comment": "Address of the sender"},
            "to": {
                "dtype": StringType(),
                "comment": "Address of the receiver. null when its a contract creation transaction",
            },
            "current_value": {"dtype": DecimalType(38, 9), "comment": "The value transferred in Ether"},
            "contract_address": {"dtype": StringType(), "comment": "The address of the token contract"},
            "token_name": {"dtype": StringType(), "comment": "The name of the token"},
            "token_symbol": {"dtype": StringType(), "comment": "The symbol of the token"},
            "token_decimal": {"dtype": LongType(), "comment": "The number of decimals the token uses"},
            "gas": {"dtype": LongType(), "comment": "Gas provided by the sender"},
            "gas_price": {"dtype": LongType(), "comment": "Gas price provided by the sender in Wei"},
            "input": {"dtype": StringType(), "comment": "The data sent along with the transaction"},
            "gas_used": {
                "dtype": LongType(),
                "comment": "The amount of gas used by this specific transaction alone",
            },
            "timestamp": {"dtype": LongType(), "comment": "The timestamp for when the block was collated"},
            "block_number": {"dtype": LongType(), "comment": "The block number"},
            "block_hash": {"dtype": StringType(), "comment": "The hash of the block"},
            "is_error": {"dtype": BooleanType(), "comment": "Whether the transaction was an error"},
            "date_partition": {
                "dtype": StringType(),
                "comment": "Partition column based on the timestamp of the block",
            },
        },
    },
    "ethereum_erc20_transactions": {
        "description": """The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address.
                \nThis table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events. \nSchedule Interval: every 5 minutes""",
        "fields": {
            "uuid": {"dtype": StringType(), "comment": "Unique identifier for the token transfer"},
            "contract_address": {"dtype": StringType(), "comment": "The address of the token contract"},
            "token_name": {"dtype": StringType(), "comment": "The name of the token"},
            "token_symbol": {"dtype": StringType(), "comment": "The symbol of the token"},
            "token_decimal": {"dtype": LongType(), "comment": "The number of decimals the token uses"},
            "from": {"dtype": StringType(), "comment": "The address of the sender"},
            "to": {"dtype": StringType(), "comment": "The address of the receiver"},
            "current_value": {"dtype": DecimalType(38, 9), "comment": "The current value of the token"},
            "hash": {"dtype": StringType(), "comment": "The hash of the transaction"},
            "timestamp": {"dtype": LongType(), "comment": "The timestamp for when the block was collated"},
            "block_number": {"dtype": LongType(), "comment": "The block number"},
            "block_hash": {"dtype": StringType(), "comment": "The hash of the block"},
            "is_error": {"dtype": BooleanType(), "comment": "Whether the transaction was an error"},
            "input": {"dtype": StringType(), "comment": "The data sent along with the transaction"},
            "date_partition": {
                "dtype": StringType(),
                "comment": "Partition column based on the timestamp of the block",
            },
        },
    },
    "ethereum_internal_transactions": {
        "description": "The traces table contains data for all transactions that have been executed on the Ethereum blockchain. \nSchedule Interval: every 5 minutes",
        "fields": {
            "uuid": {"dtype": StringType(), "comment": "Unique identifier for the trace"},
            "hash": {"dtype": StringType(), "comment": "Hash of the transaction"},
            "from": {"dtype": StringType(), "comment": "Address of the sender"},
            "to": {
                "dtype": StringType(),
                "comment": "Address of the receiver. null when its a contract creation transaction",
            },
            "current_value": {
                "dtype": DecimalType(38, 9),
                "comment": "Value transferred in Ether",
            },
            "contract_address": {"dtype": StringType(), "comment": "The address of the token contract"},
            "token_name": {"dtype": StringType(), "comment": "The name of the token"},
            "token_symbol": {"dtype": StringType(), "comment": "The symbol of the token"},
            "token_decimal": {"dtype": LongType(), "comment": "The number of decimals the token uses"},
            "input": {"dtype": StringType(), "comment": "The data send along with the transaction"},
            "error_code": {"dtype": StringType(), "comment": "Error string"},
            "timestamp": {"dtype": LongType(), "comment": "The timestamp for when the block was collated"},
            "block_number": {"dtype": LongType(), "comment": "The block number"},
            "block_hash": {"dtype": StringType(), "comment": "The hash of the block"},
            "trace_id": {"dtype": StringType(), "comment": "The trace id"},
            "is_error": {"dtype": BooleanType(), "comment": "Whether the transaction was an error"},
            "date_partition": {
                "dtype": StringType(),
                "comment": "Partition column based on the timestamp of the block",
            },
        },
    },
}
