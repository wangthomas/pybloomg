from grpc import insecure_channel

from bloomgpb_pb2 import (
    BloomStub,
    FilterRequest,
    KeyRequest,
    ListRequest,
)

class BloomgError(Exception):
    "Root of exceptions from the client library"
    pass


class BloomgClient(object):
    def __init__(self, server):
        self.channel = insecure_channel(server)
        self.conn = BloomStub(self.channel)

    def create_filter(self, name):
        req = FilterRequest()
        req.Name = name
        resp = self.conn.CreateFilter(req)
        if resp and resp.Status == 0:
            return BloomgFilter(self.conn, name)
        else:
            raise BloomgError("Bloomg create filter failed!")

    def __getitem__(self, name):
        "Gets a BloomgFilter object based on the name."
        return BloomgFilter(self.conn, name)


    def list_filters(self):
        responses = {}
        req = ListRequest()
        resp = self.conn.ListFilters(req)
        if resp:
            for name in resp.Names:
                responses[name] = 'info not implemented'
        return responses


class BloomgFilter(object):
    "Provides an interface to a single Bloomd filter"
    def __init__(self, conn, name):
        """
        Creates a new BloomgFilter object.

        :Parameters:
            - conn : The connection to use
            - name : The name of the filter
        """
        self.conn = conn
        self.name = name


    def _make_key_request(self, name, keys):
        req = KeyRequest()
        req.Name = name
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            if not isinstance(key, str):
                raise Exception("Invalid key type {}, must be string", type(key))
            req.Keys.append(key)
        return req


    def add(self, key):
        req = self._make_key_request(self.name, key)
        resp = self.conn.Add(req)
        if resp and resp.Status == 0:
            return True
        raise BloomgError("add failed: %s" % self.name)


    def bulk(self, keys):
        req = self._make_key_request(self.name, keys)
        resp = self.conn.Add(req)
        if resp and resp.Status == 0:
            return True
        raise BloomgError("add failed: %s" % self.name)


    def drop(self):
        "Deletes the filter from the server. This is permanent"
        req = FilterRequest()
        req.Name = self.name
        resp = self.conn.Drop(req)
        if not resp or resp.Status != 0:
            raise BloomgError("drop failed: %s" % self.name)


    def close(self):
        """
        Closes the filter on the server.
        """
        
        raise BloomgError("close is not supported on bloomg!")


    def clear(self):
        req = FilterRequest()
        req.Name = self.name
        resp = self.conn.Clear(req)
        if not resp or resp.Status != 0:
            raise BloomgError("clear failed: %s" % self.name)


    def __contains__(self, key):
        "Checks if the key is contained in the filter."
        req = self._make_key_request(self.name, key)
        resp = self.conn.Has(req)
        if resp:
            return resp.Has[0]
        raise BloomgError("has failed: %s %s" % (self.name, key))


    def multi(self, keys):
        "Performs a multi command, checks for multiple keys in the filter"
        req = self._make_key_request(self.name, keys)
        resp = self.conn.Has(req)
        if resp:
            return resp.Has
        raise BloomgError("multi failed: %s" % self.name)


    def info(self):
        req = ListRequest()
        resp = self.conn.Info(req)
        if resp:
            return resp.Filters
        raise BloomgError("info failed: %s" % self.name)


    def pipeline(self):
        "Creates a BloomgPipeline for pipelining multiple queries"
        return BloomgPipeline(self.conn, self.name)


class BloomgPipeline(object):
    "Provides an interface to a single Bloomd filter"
    def __init__(self, conn, name):
        """
        Creates a new BloomgPipeline object.

        :Parameters:
            - conn : The connection to use
            - name : The name of the filter
        """
        self.conn = conn
        self.name = name
        self.buf = []
        self.type = ""

    def _make_key_request(self, name, keys):
        req = KeyRequest()
        req.Name = name
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            if not isinstance(key, str):
                raise Exception("Invalid key type {}, must be string", type(key))
            req.Keys.append(key)
        return req

    def bulk(self, keys):
        "Performs a bulk set command, adds multiple keys in the filter"
        self.type = "bulk"
        self.buf.append((self.name, keys))
        return self

    def multi(self, keys):
        "Performs a multi command, checks for multiple keys in the filter"
        self.type = "multi"
        self.buf.append((self.name, keys))
        return self

    def merge(self, pipeline):
        """
        Merges this pipeline with another pipeline. Commands from the
        other pipeline are appended to the commands of this pipeline.
        """
        self.buf.extend(pipeline.buf)
        return self


    def execute(self):
        """
        Executes the pipelined commands. All commands are sent to
        the server in the order issued, and responses are returned
        in appropriate order.
        """
        buf = self.buf
        self.buf = []

        all_resp = []

        if self.type == "bulk":
            for name, keys in buf:
                req = self._make_key_request(name, keys)
                resp = self.conn.Add(req)
                if not resp or resp.Status != 0:
                    all_resp.append(BloomgError("pipeline bulk failed: %s" % name))
            
        elif self.type == "multi":
            for name, keys in buf:
                req = self._make_key_request(name, keys)
                resp = self.conn.Has(req)
                if resp and resp.Has:
                    for has in resp.Has:
                        all_resp.append(has)
                else:
                    all_resp.append(BloomgError("pipeline multi failed: %s" % name))
        else:
            raise Exception("Unknown command!")


        return all_resp


