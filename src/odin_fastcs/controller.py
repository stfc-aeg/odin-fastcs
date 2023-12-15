"""Controller for the odin-fastcs adapter.

This class implements the controller for the odin-fastcs adapter, which manages connections from
clients and interaction with other adapters loaded into odin-control. The controller can cache
parameters from adapters on a per-client basis, allowing requests for updates to only return changed
parameters.

Tim Nicholls, STFC Detector Systems Software Group
"""
import json
import logging
from functools import partial
from typing import Any, Dict, Sequence

from odin.adapters.adapter import ApiAdapterRequest
from odin.adapters.parameter_tree import ParameterTree, ParameterTreeError
from odin_data.control.ipc_message import IpcMessage, IpcMessageException
from odin_data.control.ipc_tornado_channel import IpcTornadoChannel
from zmq.error import ZMQError

from .client import FastCSClient
from .types import ParamDict
from .utils import denormalize_params, normalize_params, resolve_paths


class FastCSControllerError(Exception):
    """Simple exception class to wrap lower-level exceptions."""

    pass


class FastCSController:
    """Controller class for the odin-fastcs adapter.

    This class implements the controller for the odin-fastcs adapter. It manages client connections
    from FastCS clients and interaction with other adapters.

    :param client_endpoint: endpoint URL for client IPC channel
    :param publisher_endpoint: endpoint URL for publisher IPC channel
    """

    def __init__(self, client_endpoint: str, publisher_endpoint: str) -> None:
        """Create the FastCSController object.

        This constructor sets up the controller object, creating an IPC channel for clients to
        connect to at the specified endpoint and setting up the parameter tree and data structures
        required to interact with clients and adapters.
        """
        # Create the IPC channel for clients to connect to, bind it to the endpoint and register
        # receive and monitoring callbacks.
        try:
            self.client_chan = IpcTornadoChannel(
                IpcTornadoChannel.CHANNEL_TYPE_ROUTER, client_endpoint
            )
            self.client_chan.bind()
            self.client_chan.register_callback(self.handle_receive)
            self.client_chan.register_monitor(partial(self.handle_monitor, "client"))
            logging.info("FastCS client IPC channel bound on endpoint %s", client_endpoint)
        except ZMQError as chan_error:
            self.client_chan = None
            logging.error(
                "Failed to bind FastCS client IPC channel to endpoint %s: %s",
                client_endpoint,
                chan_error,
            )

        # Create the IPC channel to publish data to and bind it to the endpoint
        try:
            self.publish_chan = IpcTornadoChannel(
                IpcTornadoChannel.CHANNEL_TYPE_PUB, publisher_endpoint
            )
            self.publish_chan.bind()
            self.publish_chan.register_monitor(partial(self.handle_monitor, "publisher"))
            logging.info("FastCS publisher IPC channel bound on endpoint %s", publisher_endpoint)
        except ZMQError as chan_error:
            self.publish_chan = None
            logging.error(
                "Failed ot bind FastCS publisher IPC channel to endpoint %s: %s",
                publisher_endpoint,
                chan_error,
            )

        # Initialise member variables
        self.adapters: Dict[str, Any] = {}
        self.clients: Dict[str, FastCSClient] = {}
        self.num_clients = 0

        # Create parameter tree for this controller, allowing the adapter to be monitored as any
        # other odin-control adapter
        self.param_tree = ParameterTree(
            {
                "adapters": (lambda: list(self.adapters.keys()), None),
                "num_clients": (lambda: self.num_clients, None),
                "clients": (self.get_clients, None),
            },
        )

    def initialize(self, adapters: ParamDict) -> None:
        """Initialize the controller.

        This method initializes the controller with information about the adapters loaded into the
        running application.

        :param adapters: dictionary of adapter instances
        """
        self.adapters = adapters

    def cleanup(self) -> None:
        """Clean up the controller.

        This method cleans up the state of the controller at shutdown, closing the IPC channels.
        """
        if self.client_chan:
            self.client_chan.close()

        if self.publish_chan:
            self.publish_chan.close()

    def get(self, path: str, with_metadata: bool = False) -> ParamDict:
        """Get parameter data from controller.

        This method gets data from the controller parameter tree.

        :param path: path to retrieve from the tree
        :param with_metadata: flag indicating if parameter metadata should be included
        :return: dictionary of parameters (and optional metadata) for specified path
        """
        try:
            return self.param_tree.get(path, with_metadata)
        except ParameterTreeError as error:
            raise FastCSControllerError(error)

    def set(self, path: str, data: ParamDict) -> None:
        """Set parameters in the controller.

        This method sets parameters in the controller parameter tree.

        :param path: path to set parameters at
        :param data: dictionary of parameters to set
        """
        try:
            self.param_tree.set(path, data)
        except ParameterTreeError as error:
            raise FastCSControllerError(error)

    def get_clients(self) -> ParamDict:
        """Get information about connected clients.

        This method returns the stored data about the currently connected clients in a dictionary
        structure.

        :return: dictionary of client parameter data
        """
        return {id: client.as_tree() for id, client in self.clients.items()}

    def handle_receive(self, msg: Sequence[bytes]) -> None:
        """Handle receive events from clients.

        This method implements the receive callback for clients connected to the IPC channel. Client
        activity is tracked in the client data retained by the controller, then the incoming IPC
        message is decoded and passed on to the client message handler. Finally an appropriate
        response message is returned to the client.

        :param msg: byte-encoded multipart client message
        """
        # Decode the client ID from the first element of the message
        client_id = msg[0].decode("utf-8")

        # If the client is new, add to the current clients, otherwise update the client tracking
        # information
        if client_id not in self.clients:
            logging.info("New client detected with id %s", client_id)
            self.clients[client_id] = FastCSClient(client_id)
        else:
            logging.debug("Message received from existing client id %s", client_id)
            self.clients[client_id].msg_recvd()

        # Attempt to decode an IPC message from the rest of the payload and pass on for processing.
        # If the message cannnot be decoded correctly, log an error and build an error response.
        try:
            client_msg = IpcMessage(from_str=msg[1])
            response = self.process_client_msg(client_id, client_msg)
        except IpcMessageException as ipc_error:
            logging.error("Got error decoding message from client %s: %s", client_id, ipc_error)
            response = IpcMessage(msg_type=IpcMessage.NACK, msg_val="error", id=0)
            response.set_param("error", str(ipc_error))

        # Send the response back to the client
        self.client_chan.send_multipart([msg[0], response.encode()])

    def handle_monitor(self, chan_name: str, monitor_msg: Dict[str, Any]) -> None:
        """Handle monitor events from the client IPC channel.

        This method implements the monitor callback on the client IPC channel. New connection and
        disconnection events are tracked to update the number of clients currently connected.

        :param monitor_msg: decoded monitor message dictionary (as per ZMQStream)
        """
        match monitor_msg["event"]:
            case IpcTornadoChannel.CONNECTED:
                logging.debug("New connection to %s channel: %s", chan_name, str(monitor_msg))
                self.num_clients += 1
            case IpcTornadoChannel.DISCONNECTED:
                logging.debug("Disconnection from %s channel: %s", chan_name, str(monitor_msg))
                self.num_clients -= 1
            case _:
                logging.warning(
                    "Unexpected monitor event received on %s channel: %s",
                    chan_name,
                    str(monitor_msg),
                )

    def process_client_msg(self, client_id: str, client_msg: IpcMessage) -> IpcMessage:
        """Process IPC messages received from clients.

        This method processes decoded IpcMessages received from clients. The message type and value
        are matched to recognised commands, which are then processed accordingly. An IpcMessage
        repsonse is generated to return to the client.

        :param client_id : client ID string
        :param client_msg: Decoded IPC message from client
        :returns: IpcMessage response
        """
        # Extract the message type and value
        msg_type = client_msg.get_msg_type()
        msg_val = client_msg.get_msg_val()

        # Generate a response message to be returned to the client
        response = IpcMessage(msg_type=IpcMessage.ACK, msg_val=msg_val, id=client_msg.get_msg_id())

        # Match and handle the message appropriately, based on the message type and value
        match (msg_type, msg_val):
            case ("cmd", "request_adapters"):
                logging.debug("Got adapter request command from client %s", client_id)
                adapter_list = {
                    name: adapter.__class__.__name__ for name, adapter in self.adapters.items()
                }
                response.set_param("adapters", adapter_list)

            case ("cmd", "get"):
                logging.debug("Got get command from client %s", client_id)
                response.set_params(self.process_client_get(client_id, client_msg))

            case ("cmd", "set"):
                logging.debug("Got set command from client %s", client_id)
                response.set_params(self.process_client_set(client_id, client_msg))

            case ("cmd", "subscribe"):
                logging.debug("Got subscribe command from client %s", client_id)
                self.process_client_subscribe(client_id, client_msg)

            case ("cmd", "bye"):
                logging.debug("Got bye command from client %s", client_id)
                del self.clients[client_id]

            case _:
                logging.error(
                    "Got unknown message from client %s with type: %s value: %s",
                    client_id,
                    msg_type,
                    msg_val,
                )
                response.set_msg_type(IpcMessage.NACK)

        # Return the client response
        return response

    def process_client_get(self, client_id: str, client_msg: IpcMessage) -> ParamDict:
        """Process a client get command.

        This method processes a get command from a client, returning the current state of parameters
        at the requested paths. The client can request metadata for the parameters, or request a
        delta response of just those parameters that have changed since the last request.

        :param client_id: client ID string
        :param client_msg: client IPC message for the get command
        :returns dictionary of parameters (or deltas) at the specified paths
        """
        # Extract relevant parameters from the client message
        paths = client_msg.get_param("paths", [])
        with_metadata = client_msg.get_param("metadata", False)
        with_delta = client_msg.get_param("delta", False)

        # Override the delta parameter if metadata is requested, as that will alter the structure
        # of the returned parameters
        if with_metadata:
            with_delta = False

        # Create an empty dictionary of parameters to return
        data = {}

        # Iterate through the requested paths
        for path, adapter, sub_path in resolve_paths(paths):

            # If the adapter is present in the application, request the appropriate data from it
            if adapter in self.adapters:

                # Create a request object to pass to the adapter
                request = ApiAdapterRequest(None)

                # If the client wants parameter metadata, add the appropriate qualifier to the
                # response type, i.e. the Accept header field in the request
                if with_metadata:
                    request.set_response_type(request.response_type + ";metadata=true")

                # Call the get method of the adapter with the subpath and request
                adapter_response = self.adapters[adapter].get(sub_path, request)
                logging.debug(
                    "Got response from adapter %s GET at path %s: %s",
                    adapter,
                    sub_path,
                    adapter_response.data,
                )

                # Normalize the paths of data returned from the adapter
                params = normalize_params(sub_path, adapter_response.data)

                # Update the parameter cache in the client and add to the returned data
                data[path] = self.clients[client_id].update_params(path, params, with_delta)

        # Return the requested parameter data
        return data

    def process_client_set(self, client_id: str, client_msg: IpcMessage) -> ParamDict:
        """Process a client set command.

        This method processes a set command from a client. The parameter payload of the client
        message should be a dictionary of valid paths and values to set in adapters loaded into
        the system.

        :param client_id: client ID string
        :param client_msg: client IPC message for the set command
        :return: dictionary of modified parameter paths and values
        """
        # Extract the list of parameter paths and values from the client message
        paths = client_msg.get_param("paths", {})

        # Create an empty dictionary of modified parameters to return
        data = {}

        # Iterate through the requested paths
        for path, adapter, sub_path in resolve_paths(paths.keys()):

            # If the adapter is present in the application, send the appropriate data to it
            if adapter in self.adapters:

                # Denormalize leaf node parameters into the correct format for adapters
                request_path, param_name, params = denormalize_params(sub_path, paths[path])

                # Build a request with the parameter data in the body
                request = ApiAdapterRequest(json.dumps(params))

                # Call the put method of the adapter with the request path and request data
                adapter_response = self.adapters[adapter].put(request_path, request)
                logging.debug(
                    "Got response from adapter %s PUT at path %s: %s",
                    adapter,
                    request_path,
                    adapter_response.data,
                )

                # Add the modified parameters response to the returned data
                data[path] = adapter_response.data[request_path][param_name]

        # Return the modified parameter data
        return data

    def process_client_subscribe(self, client_id: str, client_msg: IpcMessage) -> ParamDict:
        """Process a client subscribe command.

        This method processes a subscribe command from a client.
        :param client_id: client ID string
        :param client_msg: client IPC message for the get command
        :return: response to client
        """
        paths = client_msg.get_param("paths", [])
        logging.debug("Client %s wants to subscribe to paths: %s", client_id, ",".join(paths))
        return {}
