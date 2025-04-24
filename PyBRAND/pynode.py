import sys
import argparse
import signal
import json
import sys
import os
from . import pynode_tools as PNT
import redis
import builtins
import traceback


class BRANDNode():
    def __init__(self):

        # parse input arguments
        parser = argparse.ArgumentParser()
        parser.add_argument('-n', '--nickname', type=str, required=True, default='node')
        parser.add_argument('-i', '--host', type=str, required=True, default='localhost')
        parser.add_argument('-p', '--port', type=str, required=True, default=6379)
        parser.add_argument('-a', '--password', type=str, required=False)
        args = vars(parser.parse_args())
        self.NAME = args["nickname"]
        args.pop("nickname")

        # connect to Redis
        self.realtime_database, self.persistant_database = self.connect_to_redis(**args)

        # initialize parameters
        self.supergraph_id = '0-0'
        self.__stream_ids = {}
        self.parameters = self.get_parameters()

    

        signal.signal(signal.SIGTERM, self.terminate)

        #silence all print statements based on silence parameters. I lowkey think this might be a bad feature
        self.silence = self.parameters.get('silence', False)
        print(f"[{self.NAME}] Silence: {self.silence}")
        builtins._original_print = builtins.print
        def custom_print(*args, **kwargs):
            if not self.silence:
                builtins._original_print(*args, **kwargs)
        builtins.print = custom_print

        #print the pid to stream for process tracking
        self.realtime_database.xadd("pid_stream", {self.NAME: os.getpid()})


    def connect_to_redis(self, **kwargs):
        """
        Establish connection to Redis and post initialized status to respective Redis stream
        If we supply a -h flag that starts with a number, then we require a -p for the port
        If we fail to connect, then exit status 1
        # If this function completes successfully then it executes the following Redis command:
        # XADD nickname_state * code 0 status "initialized"
        """

        try:
            if "socket" in kwargs:
                #r = redis.StrictRedis(unix_socket_path=redis_socket)
                print("redis socket isn't currently implemented on windows and will never be lmao")
                #print(f"[{self.NAME}] Redis connection established on socket:"f" {redis_socket}")

            r_temp = redis.StrictRedis(**kwargs, db=0)
            r_pers = redis.StrictRedis(**kwargs, db=1)
            #print(f"[{self.NAME}] Redis connection established on host:
        except ConnectionError as e:
            print(f"[{self.NAME}] Error with Redis connection, check again: {e}")
            sys.exit(1)

        initial_data = {
            'code': 0,
            'status': 'initialized',
        }
        r_temp.xadd(self.NAME + '_state', initial_data)

        return r_temp, r_pers

    # check to see that the inputed dtype is a valid numpy dtype by making a test array and catching any errors

    # dtype is always a string. not a dict of dtypes because dicts can not be converted to binary and then serialized

    # we need to change the payloading system because pickled dictionaries are not shareable across different languages
    '''
    what type of data is actually allowed? arraylikes, strings, numbers, and dictionaries that are serializeable in json
    arraylikes and strings have the encoding code for their dtype. dictionaries and strings have the encoding type serial
    maybe give a special status to armature structure for easy serialization
    only special values are "serial" and "armature" 

    '''

    def init_stream(self, stream_name, dtype: str):
        """
        you should always do this as best practise. makes a stream named {stream_name}_init with the dype details 
        """
        PNT.init_stream(self.realtime_database, stream_name, dtype)

    def get_stream_init(self, stream_name):
        """
        gets the full stream init entry
        """
        return PNT.get_stream_init(self.realtime_database, stream_name)

    def get_stream_dtype(self, stream_name):
        """
        gets just the dtype from the stream init 
        """
        return PNT.get_stream_dtype(self.realtime_database, stream_name)

    # get the latest enread entry from the redis stream
    def read_latest(self, stream_name):
        """
        reads the latest entry from the stream. won't read duplicates and will return null instead 
        """
        return PNT.read_latest(self.realtime_database, stream_name, self.__stream_ids)

    # take a series of stream entries and decode them into kvps with the time stamp as index 1 and the value dictionary as index 2 
    def decode(self, redis_entries, stream_name = None, dtype = None):
        """
        decodes the stream entries into a list of dictionaries using the stream init or the provided dtype. 
        """
        return PNT.decode(self.realtime_database, redis_entries, stream_name, dtype)

    def encode_from_dtype(self, data, dtype):
        """
        encodes the data from the dype 
        """
        PNT.encode_from_dtype(data, dtype)

    def add_to_stream(self, stream_name: str, data, dtype=None):
        """
        encodes the data and adds it to the redis stream
        """
        PNT.add_to_stream(self.realtime_database, stream_name, data, dtype)

    def get_parameters(self):
        return json.loads(self.persistant_database.hget("PARAMETERS", self.NAME).decode())

    #run the function. override with caution because the logic to capture the error trace is implemented here
    def run(self):
        try:
            while True:
                self.work()
        except Exception:
            #uncaught error occured. logging it to the redis error stream before exiting
            error = traceback.format_exc()
            print(f"Uncaught error occured during runtime. Error: {error}")
            self.realtime_database.xadd("error_stream", {self.NAME: error})

    def work(self):
        pass

    # meant to be overridden 
    def terminate(self, sig, frame):
        #logging.info('SIGINT received, Exiting')
        self.realtime_database.close()
        # self.sock.close()
        sys.exit(0)

    def cleanup(self):
        # Does whatever cleanup is required for when a SIGINT is caught
        # When this function is done, it wriest the following:
        #     XADD nickname_state * code 0 status "done"
        pass
