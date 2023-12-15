"""Utility functions for the odin-fastcs adapter.

This module provides utility functions for the odin-fastcs adapter, primarily used by the controller
for manipulating parameter accesses.

Tim Nicholls, STFC Detector Systems Group
"""
from collections.abc import Generator, Iterable

from .types import ParamDict


def resolve_paths(paths: Iterable[str]) -> Generator[tuple[str, str, str], None, None]:
    """Resolve parameter access paths from a client.

    This generator function resolves a list of specified paths sent by a client into their component
    parts, i.e paths of the form adapter/sub/path/param resolve to adapter and sub/path/param. The
    function yields a tuple of the full path, adapter and resolved subpath.

    :param paths: iterable of string paths from a client
    :return: yields a string tuple of path, adapter and subpath for each input
    """
    # Iterate through the specified paths
    for path in paths:

        # Resolve the adapter and subpath from the path
        split_path = path.split("/", 1)
        adapter = split_path[0]
        sub_path = split_path[1] if len(split_path) > 1 else ""

        # Yield the path, adapter and subpath
        yield (path, adapter, sub_path)

def normalize_params(path: str, params: ParamDict) -> ParamDict:
    """Normalize parameters returned by adapters.

    Adapter parameter tree get calls return leaf nodes as a key-value pair, i.e. path
    /a/b/c will return {"c": value}. To normalize the data returned to the client for
    such paths, normalize the key from the parameter data and replace it with just the value.

    :param path: path to the parameter to normalize
    :param params: parameter(s) to normalize
    :return: normalized parameter(s)
    """
    if len(param_keys := list(params.keys())) == 1:
        if (key := param_keys[0]) == path.split("/")[-1]:
            params = params[key]

    return params

def denormalize_params(path: str, params: ParamDict) -> ParamDict:
    """Denormalise parameters to pass to adapters.

    This function reverses the normalize operation, expanding the leaf node of a parameter path
    into a key-value dict pair to sending to an adapter during e.g. a put request.

    :param path: path to parameter to denormalize
    :param params: parameter(s) to denormalize
    :return: denormalized parameter(s)
    """
    if not isinstance(params, dict) and '/' in path:
        (path, param_name) = (path[:path.rindex('/')], path[path.rindex('/')+1:])
        params = {param_name: params}

    return (path, param_name, params)

