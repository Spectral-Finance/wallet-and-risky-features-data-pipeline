from web3 import Web3


def check_node_status(node_rpc_url: str) -> bool:

    """Function to check if the node is up and running.

    Args:
        node_rpc_url (str): The node RPC URL.

    Returns:
        bool: True if the node is up and running, False otherwise.
    """

    web3 = Web3(Web3.HTTPProvider(node_rpc_url))

    return web3.isConnected()
