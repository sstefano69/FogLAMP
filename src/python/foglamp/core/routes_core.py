# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

from aiohttp import web
from foglamp.core.service_registry import service_registry

__author__ = "Amarendra K Sinha"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


def setup(app):
    # Micro Service support - Core
    app.router.add_route('GET', '/foglamp/service/ping', service_registry.ping)

    app.router.add_route('POST', '/foglamp/service', service_registry.register)
    app.router.add_route('DELETE', '/foglamp/service/{service_id}', service_registry.unregister)
    app.router.add_route('GET', '/foglamp/service', service_registry.get_service)

    # TODO: shutdown, register_interest, unregister_interest and notify_changes - pending
    app.router.add_route('POST', '/foglamp/service/shutdown', service_registry.shutdown)
    app.router.add_route('POST', '/foglamp/service/interest', service_registry.register_interest)
    app.router.add_route('DELETE', '/foglamp/service/interest/{service_id}', service_registry.unregister_interest)
    app.router.add_route('POST', '/foglamp/change', service_registry.notify_change)

    # enable cors support
    enable_cors(app)

    # enable a live debugger (watcher) for requests, see https://github.com/aio-libs/aiohttp-debugtoolbar
    # this will neutralize error middleware
    # Note: pip install aiohttp_debugtoolbar

    # enable_debugger(app)


def enable_cors(app):
    """ implements Cross Origin Resource Sharing (CORS) support """
    import aiohttp_cors

    # Configure default CORS settings.
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
        )
    })

    # Configure CORS on all routes.
    for route in list(app.router.routes()):
        cors.add(route)


def enable_debugger(app):
    """ provides a debug toolbar for server requests """
    import aiohttp_debugtoolbar

    # dev mode only
    # this will be served at API_SERVER_URL/_debugtoolbar
    aiohttp_debugtoolbar.setup(app)
