"""Database Server.

A server that is accessible on http://localhost:4000/. When your server
receives a request on http://localhost:4000/set?somekey=somevalue it should
store the passed key and value in memory. When it receives a request on
http://localhost:4000/get?key=somekey it should return the value stored at
somekey.

Class diagram:

+------------+                  +--------------------+
| BaseServer |------------------| BaseRequestHandler |
+------------+                  +--------------------+
      ^                                   ^
      |                                   |
+-----+-----+                   +---------+------------+
| TCPServer |-------------------| StreamRequestHandler |
+-----------+                   +----------------------+
      ^                                   ^
      |                                   |
+-----+------+                  +---------+----------+
| HTTPServer |------------------| HTTPRequestHandler |
+------------+                  +--------------------+
      ^                                   ^
      |                                   |
+-----+----------+              +---------+--------------+
| DatabaseServer |--------------| DatabaseRequestHandler |
+----------------+              +------------------------+
      |
      |
+-----+---+
| Storage |
+---------+
      ^
      |
      +-----------------------------------+
      |                                   |
+-----+--------------+           +--------+--------+
| NonVolatileStorage |           | VolatileStorage |
+--------------------+           +-----------------+

BaseServer/BaseRequestHandler defines the interface, but does not implement
most of the methods, which is done in subclasses.

TCPServer/StreamRequestHandler uses the Internet TCP protocol, which
provides for continuous streams of data between the client and server.

HTTPServer/HTTPRequestHandler dispatches the HTTP requests to a handler and
implements the HTTP protocol.

DatabaseServer/DatabaseRequestHandler dispatches the get/set requests
(set?somekey=somevalue, get?key=somekey) to a method handler and makes the
apropriate calls to Storage.

Storage defines the interface, but does not implement most of the methods, which
is done in sublcasses Volatile and NonVolatile. Volatile
will save the key, value pair in memory. NonVolatile will save the
key-value pair in disk.

Example:
    Server::
        $ python -m database_server

Attributes:
    FILENAME::
        Filename for non-volatile storage.
"""


import http.server
import urllib.parse
import os
import json
import tempfile


FILENAME = "{0}/data.dat".format(os.path.dirname(os.path.abspath(__file__)))


class DatabaseServer(http.server.HTTPServer):
    """Datababse Server.

    Since we are building an HTTP server with no disk I/O where all data will be
    stored in memory, a synchronous server class is correct.

    Knowing though that the class Storage could potentially be extended with disk
    I/O it would better to anticipate for that. An HTTP server with disk I/0
    could render the service "deaf" while one request is being handled. A
    threading or forking server would be appropriate.

    I choose not to implement as asynchronous server class for the purposes of
    the interview.

    Attributes:
        storage: Server storage.
    """

    def __init__(self, address, handler):
        super().__init__(address, handler)

        # Database server storage
        self.storage = NonVolatile()


class DatabaseRequestHandler(http.server.BaseHTTPRequestHandler):
    """Database request handler."""

    def do_GET(self):
        """Handle HTTP GET request.

        Handle HTTP GET request by parsing the path to method, arguments and
        passing them for dispatch.
        """

        method, args = self.parse(self.path)
        self.dispatch(method, args)

    def parse(self, path):
        """Parse url.

        Parse url to path and query. Path is the method name and query are the
        method arguments.

        Returns:
            path: Method name.
            query: Key/Value pairs.
        """

        url = urllib.parse.urlparse(path)
        path = url.path[1:]
        query = urllib.parse.parse_qsl(url.query)[0]
        return path, query

    def dispatch(self, method, args):
        """Dispatch method.

        Dispatch method if exists else send error to client. This will enable
        extention of the database server interface through extention and not
        modification.
        """

        try:
            method = getattr(self, method)
        except AttributeError:
            self.send_error(400)
        else:
            method(args)

    def get(self, args):
        """Get value by key.

        Get value by key from server storage and send response with value to
        client.

        Since there is no clear status code for that case, if key has no value
        then a 200 response with an empty string as body is sent.
        """

        # Get value by key from server storage
        value = self.server.storage.get(args[1])

        # Send response with value to client
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(value.encode())

    def set(self, args):
        """Set value by key.

        Set value by key to server storage and send response to client.
        """

        # Set value by key to server storage
        self.server.storage.set(args[0], args[1])

        # Send response to client
        self.send_response(200)
        self.end_headers()


class Storage:
    """Storage interface.

    Storage defines an interface for setting and getting values from keys.
    Subclasses should implement the set and get methods.
    """

    def set(self, key, value):
        """Set value by key.
        """
        raise NotImplementedError

    def get(self, key):
        """Get value by key.

        Returns:
            value: If key exist return value else return empty string.
        """
        raise NotImplementedError


class Volatile(Storage):
    """Volatile storage.

    Volatitle storage implements set and get methods for storing values from
    keys in memory.
    """

    def __init__(self):
        self.__data = {}

    def set(self, key, value):
        self.__data[key] = value

    def get(self, key):
        return self.__data.get(key, "")


class NonVolatile(Storage):

    def __init__(self):
        super().__init__()

        try:
            with open(FILENAME, "r") as f:
                self.__data = json.load(f)
        except IOError:
            self.__data = {}

    def get(self, key):
        return self.__data.get(key, "")

    def set(self, key, value):
        self.__data[key] = value

        #with open(FILENAME, "w") as f:
        #    json.dump(self.__data, f)


        with tempfile.NamedTemporaryFile("w", dir=os.path.dirname(FILENAME), delete=False) as tf:

            json.dump(self.__data, tf)

            tf.flush()
            os.fsync(tf)
            tempname = tf.name
        os.rename(tempname, FILENAME) # atomic
        dirfd = os.open(os.path.dirname(FILENAME), os.O_DIRECTORY)
        os.fsync(dirfd)
        os.close(dirfd)


if __name__ == "__main__":

    HOST = "localhost"
    PORT = 4000

    server = DatabaseServer((HOST, PORT), DatabaseRequestHandler)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()

