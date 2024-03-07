from spectral_data_lib.helpers.get_secrets import get_secret
import pandas as pd
import numpy as np
from spectral_data_lib.log_manager import Logger
import asyncio
from aiohttp import ClientError
import aiohttp
from collections import deque


class EtherscanABI:
    API_RATE_LIMIT = 3  # number of requests allowed per second

    def __init__(self, logger_name: str = "get_abi_etherscan") -> None:
        self.logger = Logger(logger_name=logger_name)
        self.etherscan_api_keys = get_secret("prod/etherscan_api/keys")["keys"]
        self.api_key_queue = deque(self.etherscan_api_keys)
        self.semaphores_by_api_key = {k: asyncio.Semaphore(self.API_RATE_LIMIT) for k in self.etherscan_api_keys}

    async def get_abi(self, session: aiohttp.ClientSession, token_address: str, retries=10) -> str:
        """This function retrieves the ABI for a given token address using the Etherscan API

        Args:
            session (aiohttp.ClientSession): aiohttp session object
            token_address (str): address of the token
            retries (int, optional): number of retries. Defaults to 6.

        Returns:
            str: ABI of the token

        """

        api_endpoint = "https://api.etherscan.io/api"
        semaphore = self.semaphores_by_api_key[self.api_key_queue[0]]

        params = {"module": "contract", "action": "getabi", "address": token_address, "apikey": self.api_key_queue[0]}

        try:
            async with semaphore:  # use semaphore to limit the rate of requests
                async with session.get(api_endpoint, params=params, timeout=60) as response:
                    if response.status == 200:
                        data = await response.json()
                        if (
                            data is not None
                            and "result" in data
                            and data["result"] != "Max rate limit reached"
                            and data["result"] != ""
                        ):
                            self.logger.info(f"ABI retrieved for {token_address}")
                            await asyncio.sleep(1 / self.API_RATE_LIMIT)
                            return data["result"]
                        else:
                            self.logger.error(f"Error on retrieving ABI for {token_address}. Retrying...")
                            self.api_key_queue.rotate(-1)  # rotate the list of API keys
                            if (
                                retries > 0 and self.api_key_queue[0] != self.etherscan_api_keys[0]
                            ):  # make sure we haven't already tried all keys
                                return await self.get_abi(session, token_address, retries=retries - 1)
                            else:
                                self.logger.error(f"Failed to retrieve ABI for {token_address} after all retries.")
                    else:
                        self.logger.error(f"Error on retrieving ABI for {token_address}. Retrying...")
                        return await self.get_abi(session, token_address, retries=retries - 1)
        except (asyncio.TimeoutError, ClientError) as e:
            if retries > 0:
                self.logger.error(f"Timeout error on retrieving ABI for {token_address}. Retrying...")
                self.api_key_queue.rotate(-1)  # rotate the list of API keys
                if self.api_key_queue[0] != self.etherscan_api_keys[0]:  # make sure we haven't already tried all keys
                    return await self.get_abi(session, token_address, retries=retries - 1)
                else:
                    self.logger.error(f"Failed to retrieve ABI for {token_address} after all retries with all keys.")
            else:
                raise Exception(f"Failed to retrieve ABI for {token_address} after all retries.")

    async def process_chunk(self, session: aiohttp.ClientSession, chunk: pd.DataFrame) -> pd.DataFrame:
        """This function processes a chunk of the token list

        Args:
            session (aiohttp.ClientSession): aiohttp session object
            chunk (pd.DataFrame): chunk of the token list

        Returns:
            pd.DataFrame: chunk of the token list with the ABI column populated

        """

        tasks = []
        for i, row in chunk.iterrows():
            tasks.append(self.get_abi(session, row["token_address"]))

        abis = await asyncio.gather(*tasks, return_exceptions=True)
        chunk["abi"] = abis

        return chunk

    async def get_abis(self, token_list: pd.DataFrame) -> pd.DataFrame:
        """Fetches the ABI for each token in the token list using the Etherscan API

        Args:
            token_list (pd.DataFrame): A dataframe containing the token list

        Returns:
            pd.DataFrame: A dataframe containing the token list with the ABI column populated

        """

        chunks = np.array_split(token_list, 100) if len(token_list) > 100 else np.array_split(token_list, 10)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for chunk in chunks:
                tasks.append(asyncio.create_task(self.process_chunk(session, chunk)))
                self.logger.info(f"Tasks: {len(tasks)}")

            results = await asyncio.gather(*tasks, return_exceptions=True)
            df_with_abis = pd.concat(results, ignore_index=True)

        return df_with_abis
