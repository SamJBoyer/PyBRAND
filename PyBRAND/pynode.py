# BRAND node template
# Author: Sam Boyer
# Adapted from code by: David Brandman and Kushant Patel and Mattia Rigotti

import sys
import argparse
import logging
import signal
import json
import sys
import os
import sys
from . import pynode_tools as PNT
from litework import python_analysis_tools as PAT
from redis import Redis
import builtins
import traceback


class BRANDNode():
    def __init__(self):

        print(f"file comes from {os.path.abspath(__file__)}")
        # parse input arguments
        argp = argparse.ArgumentParser()
        
        argp.add_argument('-n', '--nickname', type=str, required=True, default='node')
        argp.add_argument('-i', '--redis_host', type=str, required=True, default='localhost')
        argp.add_argument('-p', '--redis_port', type=int, required=True, default=6379)
        argp.add_argument('-s', '--redis_socket', type=str, required=False)
        argp.add_argument('-a', '--password', type=str, required=False)
        args = argp.parse_args()

        len_args = len(vars(args))
        if (len_args < 3):
            print("Arguments passed: {}".format(len_args))
            print("Please check the arguments passed")
            sys.exit(1)

        self.NAME = args.nickname
        redis_host = args.redis_host
        redis_port = args.redis_port
        redis_socket = args.redis_socket
        password = args.password

        # connect to Redis
        self.r = self.connect_to_redis(redis_host, redis_port, redis_socket)

        # initialize parameters
        self.supergraph_id = '0-0'
        self.__stream_ids = {}
        self.parameters = self.init_params(self.__stream_ids)



        signal.signal(signal.SIGINT, self.terminate)

        #silence all print statements based on silence parameters
        self.silence = self.parameters.get('silence', False)
        print(f"[{self.NAME}] Silence: {self.silence}")
        builtins._original_print = builtins.print
        def custom_print(*args, **kwargs):
            if not self.silence:
                builtins._original_print(*args, **kwargs)
        builtins.print = custom_print

        #print the pid to stream for process tracking
        self.r.xadd("pid_stream", {self.NAME: os.getpid()})

    def init_params(self, stream_ids):
        print(self.NAME)
        param_entry = PNT.read_latest(self.r, self.NAME, stream_ids)
        decode = PNT.decode(self.r, param_entry, "parameters", "serial")
        parameters = decode[0][1]['parameters']
        return parameters

    def connect_to_redis(self, redis_host, redis_port, redis_password=None, redis_socket=None):
        """
        Establish connection to Redis and post initialized status to respective Redis stream
        If we supply a -h flag that starts with a number, then we require a -p for the port
        If we fail to connect, then exit status 1
        # If this function completes successfully then it executes the following Redis command:
        # XADD nickname_state * code 0 status "initialized"
        """

        # redis_connection_parse = argparse.ArgumentParser()
        # redis_connection_parse.add_argument('-i', '--redis_host', type=str, required=True, default='localhost')
        # redis_connection_parse.add_argument('-p', '--redis_port', type=int, required=True, default=6379)
        # redis_connection_parse.add_argument('-n', '--nickname', type=str, required=True, default='redis_v0.1')

        # args = redis_connection_parse.parse_args()
        # len_args = len(vars(args))
        # print("Redis arguments passed:{}".format(len_args))

        try:
            if redis_socket:
                r = Redis(unix_socket_path=redis_socket)
                print(f"[{self.NAME}] Redis connection established on socket:"
                      f" {redis_socket}")
            else:
                r = Redis(redis_host, redis_port, password = 'oogert', retry_on_timeout=True)
                print(f"[{self.NAME}] Redis connection established on host:"
                      f" {redis_host}, port: {redis_port}")
        except ConnectionError as e:
            print(f"[{self.NAME}] Error with Redis connection, check again: {e}")
            sys.exit(1)

        initial_data = {
            'code': 0,
            'status': 'initialized',
        }
        r.xadd(self.NAME + '_state', initial_data)

        return r

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
        PNT.init_stream(self.r, stream_name, dtype)

    def get_stream_init(self, stream_name):
        """
        gets the full stream init entry
        """
        return PNT.get_stream_init(self.r, stream_name)

    def get_stream_dtype(self, stream_name):
        """
        gets just the dtype from the stream init 
        """
        return PNT.get_stream_dtype(self.r, stream_name)

    # get the latest enread entry from the redis stream
    def read_latest(self, stream_name):
        """
        reads the latest entry from the stream. won't read duplicates and will return null instead 
        """
        return PNT.read_latest(self.r, stream_name, self.__stream_ids)

    # take a series of stream entries and decode them into kvps with the time stamp as index 1 and the value dictionary as index 2 
    def decode(self, redis_entries, stream_name = None, dtype = None):
        """
        decodes the stream entries into a list of dictionaries using the stream init or the provided dtype. 
        """
        return PNT.decode(self.r, redis_entries, stream_name, dtype)

    def encode_from_dtype(self, data, dtype):
        """
        encodes the data from the dype 
        """
        PNT.encode_from_dtype(data, dtype)

    def add_to_stream(self, stream_name: str, data, dtype=None):
        """
        encodes the data and adds it to the redis stream
        """
        PNT.add_to_stream(self.r, stream_name, data, dtype)


    #likely to get phased out
    def getParametersFromSupergraph(self, complete_supergraph=False):
        """
        Read node parameters from Redis

        Parameters
        ----------
        complete_supergraph : (optional) boolean
            False returns just the node's parameters.
            True returns the complete supergraph
            straight from the Redis xrange call.

        Returns
        -------
        new_params : list of dict
            Each list item will be a dictionary of the
            node's parameters in that supergraph
        """
        model_stream_entries = self.r.xrange(
            b'supergraph_stream', '('+self.supergraph_id, '+')

        if not model_stream_entries:
            return None

        self.supergraph_id = model_stream_entries[-1][0]
        self.supergraph_id = self.supergraph_id.decode('utf-8')

        if complete_supergraph:
            return model_stream_entries

        # {} means the node was not listed in the corresponding supergraph
        new_params = [{} for i in model_stream_entries]

        for i, entry in enumerate(model_stream_entries):

            model_data = json.loads(entry[1][b'data'].decode())

            for node in model_data['nodes']:
                if model_data['nodes'][node]['nickname'] == self.NAME:
                    new_params[i] = model_data['nodes'][node]['parameters']

        return new_params

    def initializeParameters(self):
        """
        Read node parameters from Redis.
        ...
        """
        node_parameters = self.getParametersFromSupergraph()
        if node_parameters is None:
            print(f"[{self.NAME}] No model published to supergraph_stream in Redis")
            #sys.exit(1)
        else:
            # for parameter in node_parameters:
            # self.parameters[parameter['name']] = parameter['value']
            for key, value in node_parameters[-1].items():
                self.parameters[key] = value

        # print(self.parameters)


    #run the function. override with caution because the logic to capture the error trace is implemented here
    def run(self):
        try:
            while True:
                self.work()
                self.updateParameters()
        except Exception:
            #uncaught error occured. logging it to the redis error stream before exiting
            error = traceback.format_exc()
            print(f"Uncaught error occured during runtime. Error: {error}")
            self.r.xadd("error_stream", {self.NAME: error})


    def work(self):
        """
        # This is the business logic for the function.
        # At the end of its cycle, it should output the following:
        # XADD nickname_streamOutputName [inputRun M] run nRuns parameter N name1 value1 name2 value2
        # where streamOutputName is the name provided in the YAML file (usually: output)
        # and the name1 is the name variable for the output stream, and the value being the payload
        # and N is the value of parameter_count global variable
        # Note there is an exact match between the name value pairs and what is specified in the YAML
        # If inputs is not [] in the YAML file, then inputRun contains the
        # nRuns count of the previous input used for populating this stream output
        """
        pass

    # def write_brand(self):

    #     self.sync_dict_json = json.dumps(self.sync_dict)

    #     self.output_entry[self.time_key] = time.monotonic()
    #     self.output_entry[self.sync_key] = self.sync_dict_json.encode()

    #     self.r.xadd(self.output_stream, self.output_entry)

    def updateParameters(self):
        """
        This function reads from the nickname_parameters stream,
        and knows how to parse all parameters that (1) do not have static variable
        specified, or (2) are static: false specified
        It does not block on the XREAD call. Whenever there is a new stream value,
        it assumes that the new value is meaningful (since it should have been checked
        by the supervisor node) and then updates the parameters = {} value
        If this function updates the parameters{} dictionary, then it increments parameter_count
        """
        pass

    def terminate(self, sig, frame):
        logging.info('SIGINT received, Exiting')
        self.r.close()
        # self.sock.close()
        sys.exit(0)

    def cleanup(self):
        # Does whatever cleanup is required for when a SIGINT is caught
        # When this function is done, it wriest the following:
        #     XADD nickname_state * code 0 status "done"
        pass
