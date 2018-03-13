"""
This module implements a client for the Bloomg server.
"""

import time
import requests
from json import dumps

class BloomgError(Exception):
    "Root of exceptions from the client library"
    pass


class BloomgClient(object):
    def __init__(self, server):
        self.server = server
        self.session = requests.Session()
        self.server_info = None
        self.info_time = 0

    def check_cache(self, name):
        if not self.server_info or time.time() - self.info_time > 300:
            self.server_info = self.list_filters()
            self.info_time = time.time()
            if name not in self.server_info:
                raise BloomgError("Filter not exist!")

        if name not in self.server_info:
            self.server_info = self.list_filters()
            self.info_time = time.time()
            if name not in self.server_info:
                raise BloomgError("Filter not exist!")

    def create_filter(self, name):
        try:
            headers = {"Content-Type": "application/json"}
            payload = {"filtername": name}

            resp = self.session.post(
                self.server + "/create",
                headers=headers,
                data=get_json_dump(payload),
                timeout=60
            )

            if resp.status_code == 200:
                return BloomgFilter(self.server, self.session, name)
            else:
                raise BloomgError("Bloomg create filter failed!")
        except Exception as e:
            print("Bloomg create filter failed: %s" % str(e))
            raise BloomgError("Bloomg create filter failed!")

    def __getitem__(self, name):
        "Gets a BloomgFilter object based on the name."
        self.check_cache(name)
        return BloomgFilter(self.server, self.session, name)


    def list_filters(self):
        try:
            headers = {"Content-Type": "application/json"}

            resp = self.session.get(
                self.server + "/list",
                headers=headers,
                timeout=60
            )

            if resp.status_code == 200:
                responses = {}
                data = resp.json().get('data', None)
                for name in data:
                    responses[name] = "info not implemented"
                return responses

            else:
                raise BloomgError("Bloomg list filter failed!")
        except Exception as e:
            print("Bloomg list filter failed: %s" % str(e))
            raise BloomgError("Bloomg list filter failed!")


class BloomgFilter(object):
    "Provides an interface to a single Bloomd filter"
    def __init__(self, server, session, name):
        """
        Creates a new BloomgFilter object.

        """
        self.server = server
        self.session = session
        self.name = name
        self.hash_keys = True


    def add(self, key):
        headers = {"Content-Type": "application/json"}
        payload = {
            "filtername": self.name,
            "keys": [key]
        }

        try:
            resp = self.session.post(
                self.server + "/add",
                headers=headers,
                data=get_json_dump(payload),
                timeout=60
            )

            if resp.status_code != 200:
                raise BloomgError("add failed: %s" % self.name)

        except Exception as e:
            print("add failed: %s" % str(e))
            raise BloomgError("add failed: %s" % self.name)


    def bulk(self, keys):
        headers = {"Content-Type": "application/json"}
        payload = {
            "filtername": self.name,
            "keys": keys
        }

        try:
            resp = self.session.post(
                self.server + "/add",
                headers=headers,
                data=get_json_dump(payload),
                timeout=60
            )

            if resp.status_code != 200:
                raise BloomgError("bulk failed: %s" % self.name)

        except Exception as e:
            print("bulk failed: %s" % str(e))
            raise BloomgError("bulk failed: %s" % self.name)


    def drop(self):
        "Deletes the filter from the server. This is permanent"

        raise BloomgError("drop is not supported on pybloomg!")


    def close(self):
        """
        Closes the filter on the server.
        """

        raise BloomgError("close is not supported on pybloomg!")


    def clear(self):
        """
        Clear the filter on the server.
        """

        raise BloomgError("clear is not supported on pybloomg!")


    def __contains__(self, key):
        "Checks if the key is contained in the filter."
        headers = {"Content-Type": "application/json"}
        payload = {
            "filtername": self.name,
            "keys": [key]
        }

        try:
            resp = self.session.post(
                self.server + "/has",
                headers=headers,
                data=get_json_dump(payload),
                timeout=60
            )

            if resp.status_code == 200:
                return resp.json()["data"][0]
            else:
                raise BloomgError("Bloomg has failed!")
        except Exception as e:
            print("has failed: %s" % str(e))
            raise BloomgError("Bloomg has failed!")


    def multi(self, keys):
        "Performs a multi command, checks for multiple keys in the filter"
        headers = {"Content-Type": "application/json"}
        payload = {
            "filtername": self.name,
            "keys": keys
        }

        try:
            resp = self.session.post(
                self.server + "/has",
                headers=headers,
                data=get_json_dump(payload),
                timeout=60
            )

            if resp.status_code == 200:
                return resp.json()["data"]
            else:
                raise BloomgError("Bloomg multi failed!")
        except Exception as e:
            print("Bloomg multi failed: %s" % str(e))
            raise BloomgError("Bloomg multi failed!")


    def info(self):
        raise BloomgError("info is not supported on pybloomg!")


    def pipeline(self):
        "Creates a BloomgPipeline for pipelining multiple queries"
        return BloomgPipeline(self.server, self.session, self.name)


class BloomgPipeline(object):
    "Provides an interface to a single Bloomd filter"
    def __init__(self, server, session, name):
        """
        Creates a new BloomgPipeline object.

        """
        self.server = server
        self.session = session
        self.name = name
        self.buf = []
        self.type = ""

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
        headers = {"Content-Type": "application/json"}

        if self.type == "bulk":
            for name, keys in buf:
                payload = {
                    "filtername": self.name,
                    "keys": keys
                }

                resp = self.session.post(
                    self.server + "/add",
                    headers=headers,
                    data=get_json_dump(payload),
                    timeout=60
                )

                if resp.status_code != 200:
                    all_resp.append(BloomgError("pipeline bulk failed: %s" % name))

        elif self.type == "multi":
            for name, keys in buf:
                payload = {
                    "filtername": self.name,
                    "keys": keys
                }

                resp = self.session.post(
                    self.server + "/has",
                    headers=headers,
                    data=get_json_dump(payload),
                    timeout=60
                )

                if resp.status_code == 200:
                    all_resp.append(resp.json()["data"])
                else:
                    all_resp.append(BloomgError("pipeline multi failed: %s" % name))
        else:
            raise Exception("Unknown command!")


        return all_resp

def get_json_dump(obj, encode_class=None):
    '''
    Returns the output form json.dumps.

    '''
    # If there is a character such as 'registered mark' in obj that is encoded using
    # latin-1 encoding, json.dumps() will fail. Such cases are handled in
    # the exception block.
    try:
        json_dump = dumps(obj, cls=encode_class)
    except UnicodeDecodeError:
        json_dump = dumps(obj, cls=encode_class, encoding='latin-1')

    return json_dump
