from dataclasses import asdict

from src.balance_calculation import ChainData
from typing import List, Dict, Union, OrderedDict, Any

WALLET_ETHERSCAN_DATA = List[Dict[str, Union[int, float, str]]]

BUNDLE_LIST = List[str]

WALLET_FEATURE_DOCUMENT = Dict[str, Union[int, float, Dict[str, Dict[str, Union[float, int]]]]]
WALLET_FEATURES_SCHEMA = List[WALLET_FEATURE_DOCUMENT]
GOLD_PROCESSED_WALLET_TRANSACTION_DOCUMENT = Dict[str, Union[str, float, int]]
GOLD_PROCESSED_WALLET_TRANSACTION_LIST = List[GOLD_PROCESSED_WALLET_TRANSACTION_DOCUMENT]
GOLD_PROCESSED_WALLET_TRANSACTION_DICTIONARY = Dict[int, GOLD_PROCESSED_WALLET_TRANSACTION_LIST]

WALLET_BALANCE_SCHEMA = ChainData.__dict__
