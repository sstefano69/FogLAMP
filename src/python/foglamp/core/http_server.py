import asyncio
from aiohttp import web


class AppWrapper:

    def __init__(self, aioapp, port, loop):
        self.port = port
        self.aioapp = aioapp
        self.loop = loop
        self.uris = []
        self.servers = []

    def initialize(self):
        self.loop.run_until_complete(self.aioapp.startup())
        handler = self.aioapp.make_handler(loop=self.loop)

        server_creations, self.uris = web._make_server_creators(
            handler, loop=self.loop, ssl_context=None,
            host=None, port=self.port, path=None, sock=None,
            backlog=128)

        self.servers = self.loop.run_until_complete(
            asyncio.gather(*server_creations, loop=self.loop)
        )

    def shutdown(self):
        server_closures = []
        for srv in self.servers:
            srv.close()
            server_closures.append(srv.wait_closed())
        self.loop.run_until_complete(
            asyncio.gather(*server_closures, loop=self.loop))

        self.loop.run_until_complete(self.aioapp.shutdown())

    def cleanup(self):
         self.loop.run_until_complete(self.aioapp.cleanup())

    def show_info(self):
        print("======== Running on {} ========\n".format(', '.join(self.uris)))


class MultiApp:

    def __init__(self, loop=None):
        self._apps = []
        self.user_supplied_loop = loop is not None
        if loop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop

    def configure_app(self, app, port):
        app._set_loop(self.loop)
        self._apps.append(
            AppWrapper(app, port, self.loop)
        )

    def run_all(self):
        try:
            for app in self._apps:
                app.initialize()
            try:
                for app in self._apps:
                    app.show_info()
                print("(Press CTRL+C to quit)")
                self.loop.run_forever()
            except KeyboardInterrupt:  # pragma: no cover
                pass
            finally:
                for app in self._apps:
                    app.shutdown()
        finally:
            for app in self._apps:
                app.cleanup()

        if not self.user_supplied_loop:
            self.loop.close()
