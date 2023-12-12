"""Client tracking dataclass for the odin-fastcs controller.

This dataclass implements per-client tracking and parameter caching for the odin-fastcs controller.

Tim Nicholls, STFC Detector Systems Software Group
"""
import time
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Set

from .types import ParamDict


@dataclass
class FastCSClient:
    """Client tracking dataclass for the odin-fastcs controller.

    This dataclass implements per-client tracking and parameter caching for the odin-fastcs
    controller. Caching allows the delta of parameters to be determined, i.e. only thoses changed
    since the last update of the cache.

    Attributes:
        id (str): client ID
        msg_count (int): count of messages received from the client
        last_msg (float): timestamp of last message received from client
        param_cache (dict): dictionary of cached parameters, keyed on the parameter path
        sub_paths: (set[str]): set of parameter paths the client has subscribed to
    """
    id: str
    msg_count: int = 0
    last_msg: float = 0
    param_cache: dict = field(default_factory=lambda: {})
    sub_paths: Set[str] = field(default_factory=lambda: set())

    def msg_recvd(self) -> None:
        """Update message received attributes for client.

        This method updates the message count and last message timestamp for the client, allowing
        clients to be tracked by the controller.
        """
        self.msg_count += 1
        self.last_msg = time.time()

    def update_params(self, path: str, new: dict, with_delta: bool) -> ParamDict:
        """Update the parameter cache for a client.

        This method updates the parameters cached for the client. If requested, the delta of the
        parameters, i.e. the difference since the last call, is calculated. The full parameter data
        or delta are returned accordingly.

        :param path: string path of parameters to update
        :param new: dictionary of new value(s) of parameter(s)
        :param with_delta: boolean flag indicating if delta is requested
        :returns updated parameters or delta
        """
        def _build_delta(cached: dict, new: dict) -> Any:
            """Recursively calculate the delta of parameters.

            This inner function recursively traverses the specified parameters, updating the
            cached values and calculating the delta between the cache and the new.

            :param cached: dicitonary of cached parameters
            :param new: dictionary of new parameters
            :returns updated parameters
            """
            if isinstance(cached, dict):
                subtree = {}
                for key in cached.keys():
                    if new_val := _build_delta(cached[key], new[key]):
                        subtree[key] = new_val
                        cached[key] = new_val
                return subtree
            else:
                if cached != new:
                    cached = deepcopy(new)
                    return new

            return None

        # If the delta was requested and the specified path is already cached for this client,
        # build the delta and return it. Otherwise, update the cache with the new values and return
        # those
        if with_delta and path in self.param_cache:
            data = _build_delta(self.param_cache[path], new)
        else:
            data = new
            self.param_cache[path] = deepcopy(new)

        return data

    def as_tree(self) -> ParamDict:
        """Return the client state as a parameter tree.

        This method returns the state of the client as a parameter tree (dict) form, allowing the
        controller to inlcude client data in its own parameter tree.

        :return dictionary of client state parameters
        """
        return {
            "msg_count": self.msg_count,
            "last_msg": self.last_msg,
            "params_cached": len(self.param_cache),
            "subscribed_paths": list(self.sub_paths),
        }


