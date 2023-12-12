"""odin-fastcs adapter.

This module implements an odin-control adapter for odin-fastcs. This adapter provides an interface
between other adapters loaded into the odin-control instance and client connections from FastCS,
allowing the state of the two systems to be synchronised.

Tim Nicholls, STFC Detector Systems Software Group
"""
import logging

from odin.adapters.adapter import (
    ApiAdapter,
    ApiAdapterRequest,
    ApiAdapterResponse,
    request_types,
    response_types,
    wants_metadata,
)
from tornado.escape import json_decode

from .controller import FastCSController, FastCSControllerError
from .types import ParamDict


class FastCSAdapter(ApiAdapter):
    """FastCS interface adapter for odin-control.

    This class implements the odin-fastcs adapter, providing an interface between odin-control and
    FastCS clients.
    """

    def __init__(self, **kwargs):
        """Initialize the FastCSAdapter object.

        This method initializes the adapter object, resolving configuration options passed from
        odin-control and instantiating a controller object.

        :param kwargs: keyword arguments specifying options
        """
        # Initialize the superclass, which populates options from the specified arguments
        super().__init__(**kwargs)

        # Resolve configuration options for the controller
        client_endpoint = self.options.get("endpoint", "tcp://127.0.0.1:5000")
        publisher_endpoint = self.options.get("publisher_endpoint", "tcp://127.0.0.1:6000")

        # Instantiate the controller object
        self.controller = FastCSController(client_endpoint, publisher_endpoint)

        logging.debug("FastCSAdapter loaded")

    def initialize(self, adapters: ParamDict) -> None:
        """Initialize the adapter.

        This method is called by odin-control once all adapters are loaded. The dictionary of loaded
        adapters are passed to the initalize method of the controller.

        :param adapters: dictionary of adapters loaded into the odin-control instance
        """
        logging.debug("FastCSAdapter initialize called with %d adapters", len(adapters))
        self.controller.initialize(adapters)

    def cleanup(self) -> None:
        """Clean up the adapter.

        This method is called by odin-control at shutdown. The corresponding method in the
        controller is called to clean up the state of the adapter.
        """
        logging.debug("FastCSAdapter cleanup called")
        self.controller.cleanup()

    @request_types("application/json", "application/vnd.odin-native")
    @response_types("application/json", default="application/json")
    def get(self, path: str, request: ApiAdapterRequest) -> ApiAdapterResponse:
        """Handle an HTTP GET request.

        This method handles an HTTP GET request, returning a JSON response.

        :param path: URI path of request
        :param request: HTTP request object
        :return: an ApiAdapterResponse object containing the appropriate response
        """
        try:
            response = self.controller.get(path, wants_metadata(request))
            status_code = 200
        except FastCSControllerError as e:
            response = {"error": str(e)}
            status_code = 400

        content_type = "application/json"
        return ApiAdapterResponse(response, content_type=content_type, status_code=status_code)

    @request_types("application/json", "application/vnd.odin-native")
    @response_types("application/json", default="application/json")
    def put(self, path: str, request: ApiAdapterRequest) -> ApiAdapterResponse:
        """Handle an HTTP PUT request.

        This method handles an HTTP PUT request, returning a JSON response.

        :param path: URI path of request
        :param request: HTTP request object
        :return: an ApiAdapterResponse object containing the appropriate response
        """
        try:
            data = json_decode(request.body)
            self.controller.set(path, data)
            response = self.controller.get(path)
            status_code = 200
        except FastCSControllerError as e:
            response = {"error": str(e)}
            status_code = 400

        content_type = "application/json"
        return ApiAdapterResponse(response, content_type=content_type, status_code=status_code)
