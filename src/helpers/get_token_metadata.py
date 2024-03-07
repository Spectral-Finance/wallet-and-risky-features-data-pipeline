from typing import List
import pandas as pd
import numpy as np
from web3 import Web3, contract
from web3.exceptions import BadFunctionCallOutput
from eth_abi.exceptions import InsufficientDataBytes
from spectral_data_lib.log_manager import Logger
from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count

from spectral_data_lib.helpers.get_secrets import get_secret


class TokenMetadata:
    """This class retrieves the metadata for a given token address"""

    def __init__(self, node_rpc_urls, logger_name: str = "get_token_metadata") -> None:
        self.logger = Logger(logger_name=logger_name)
        self.node_rpc_urls = node_rpc_urls
        self.retry = len(node_rpc_urls)

    def check_token_type(self, token_address: str, abi: str, node_rpc_urls: List[str], retries: int = 5):
        """This function checks if a given token is ERC20 or ERC721

        Args:
            token_address (str): address of the token
            abi (str): ABI of the token
            retries (int, optional): number of retries. Defaults to 5.
            node_rpc_urls (List[str]): list of node rpc urls

        Returns:
            str: type of the token
        """

        web3 = Web3(Web3.HTTPProvider(node_rpc_urls[0]))

        token_address = web3.toChecksumAddress(token_address)

        try:

            # create a contract object
            contract = web3.eth.contract(address=token_address, abi=abi)

            # check if the contract is ERC20
            is_erc20 = (
                hasattr(contract.functions, "balanceOf")
                and hasattr(contract.functions, "totalSupply")
                and not hasattr(contract.functions, "ownerOf")
            )

            # check if the contract is ERC721
            is_erc721 = (
                hasattr(contract.functions, "setApprovalForAll")
                and hasattr(contract.functions, "safeTransferFrom")
                and hasattr(contract.functions, "tokenURI")
            )

            # print the result
            if is_erc20:
                return "ERC20", contract
            elif is_erc721:
                return "ERC721", contract
            else:
                return "Unknown", None
        except Exception as e:
            self.logger.debug(f"Error on checking token type for {token_address} - {e}")
            self.logger.info(f"Retrying {retries} more times")
            if retries > 0:
                return self.check_token_type(
                    token_address=token_address, abi=abi, node_rpc_urls=node_rpc_urls[1:], retries=retries - 1
                )
            else:
                self.logger.info(f"Could not get token type for {token_address}")
                return "Unknown", None

    def get_token_metadata(self, contract: contract, token_type: str) -> tuple:
        """Function to get the token metadata for a given contract.

        Args:
            contract (contract): Contract object.
            token_type (str): Token type.

        Returns:
            tuple: Tuple with the token name, token symbol, and token decimals.
        """

        default_name = "Unknown"
        default_symbol = "Unknown"
        default_decimals = 18

        try:
            token_name = contract.functions.name().call() if hasattr(contract.functions, "name") else default_name
            token_symbol = (
                contract.functions.symbol().call() if hasattr(contract.functions, "symbol") else default_symbol
            )
            token_decimals = (
                contract.functions.decimals().call()
                if (hasattr(contract.functions, "decimals") and token_type == "ERC20")
                else default_decimals
            )

            return token_name, token_symbol, token_decimals

        except (Exception, BadFunctionCallOutput, InsufficientDataBytes) as e:
            self.logger.info(f"Error to get token metadata for - {e}")
            return default_name, default_symbol, default_decimals

    def set_token_metadata_by_row(self, row: pd.Series, retries: int = 5) -> pd.Series:
        """Function to get the token metadata for a given token address.

        Args:
            row (pd.Series): Pandas series with the token address.
            retries (int, optional): Number of retries. Defaults to 3.

        Returns:
            pd.Series: Pandas series with the token metadata.
        """

        token_address = row["token_address"]

        try:

            if row["abi"] != "Contract source code not verified" and row["abi"] != "Invalid Address format":

                token_type, contract = self.check_token_type(
                    token_address=token_address, abi=row["abi"], node_rpc_urls=self.node_rpc_urls, retries=retries
                )

                if token_type == "Unknown":
                    self.logger.info(f"Unknown token type for {token_address}")
                    row["name"] = "Unknown"
                    row["symbol"] = "Unknown"
                    row["decimals"] = 18
                    row["type"] = "Unknown"
                    row["status"] = "Unknown"

                    return row

                else:

                    if hasattr(contract.functions, "selfDestruct") or hasattr(contract.functions, "destroy"):

                        row["name"] = "Unknown"
                        row["symbol"] = "Unknown"
                        row["decimals"] = 18
                        row["type"] = token_type
                        row["status"] = "Destroyed"

                        return row

                    else:

                        token_name, token_symbol, token_decimals = self.get_token_metadata(contract, token_type)

                        self.logger.info(f"Token metadata found for {token_address}")

                        row["name"] = token_name
                        row["symbol"] = token_symbol
                        row["decimals"] = token_decimals
                        row["type"] = token_type
                        row["status"] = "Active"

                        return row
            else:
                self.logger.info(f"Contract source code not verified for {token_address}")
                row["name"] = "Unknown"
                row["symbol"] = "Unknown"
                row["decimals"] = 18
                row["type"] = "Unknown"
                row["status"] = "Unknown"

                return row

        except Exception as e:

            if retries > 0:
                self.logger.info(f"Error to get token metadata for {token_address} - {e}")
                self.logger.info(f"Retrying {retries} more times")

                return self.set_token_metadata_by_row(row, retries=retries - 1)
            else:
                raise Exception(f"Error to get token metadata for {token_address} - {e}")

    def set_token_metadata(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Function to get the token metadata for each token address in the dataframe.

        Args:
            dataframe (pd.DataFrame): Pandas dataframe with the token addresses.

        Returns:
            pd.DataFrame: Pandas dataframe with the token metadata.
        """

        dataframe = dataframe.apply(self.set_token_metadata_by_row, axis=1, args=(self.retry,))

        return dataframe

    def parallelize_dataframe(self, df: pd.DataFrame, func, n_chunks: int = 10) -> pd.DataFrame:
        """Function to parallelize pandas dataframe using multiprocessing pool.
        This function takes a dataframe, a function and the number of chunks to split the dataframe into.
        After splitting the dataframe into chunks, the function is applied to each chunk using a multiprocessing pool to get the token metadata.

        Args:
            df (pd.DataFrame): Pandas dataframe to be split into chunks.
            func (Callable): Function to be applied to each chunk.
            n_chunks (int, optional): Number of chunks to split the dataframe into. Defaults to 10.

        Returns:
            pd.DataFrame: Pandas dataframe with the token metadata.

        """
        df_split = np.array_split(df, n_chunks)
        with ThreadPool(processes=cpu_count()) as pool:
            results = list(pool.map(func, df_split))

        return pd.concat(results)

    def get_tokens_metadata(self, token_list: pd.DataFrame) -> pd.DataFrame:
        """Function to get the token metadata for a list of tokens.

        Args:
            token_list (pd.DataFrame): Pandas dataframe with the token list.

        Returns:
            pd.DataFrame: Pandas dataframe with the token metadata.
        """

        n_chunks = 1000 if len(token_list) >= 10000 else 100

        results = self.parallelize_dataframe(token_list, self.set_token_metadata, n_chunks=n_chunks)

        return results
