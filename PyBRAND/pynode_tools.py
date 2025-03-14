#!/usr/bin/env python
import json
import struct
import numpy as np
from litework import python_analysis_tools as PAT, ArmatureStruct
import warnings


'''
author: Samuel James Boyer
gmail: sam.james.boyer@gmail.com

this file contains a collection of functions used in the python node
the node is essentially a wrapper for these functions. these functions
are not implemented in the node itself so they can be tested using pytest.
brandnodes are designed to only be launched by the supervisor, and are therefore 
unnecessarily difficult to test. This is a work around

ALSO! BECAUSE ALL THESE METHODS WILL BE WRAPPED, ALL WARNINGS ARE REPLACED WITH "WarningError"s. THE WRAPPING FUNCTION
SHOULD HANDLE THESE WARNINGS APPROPRIATELY TO CRASH/NOCRASH THE SYSTEM AS NEEDED. REPLACING WARNINGS WITH ERRORS
ALSO MAKES PYTESTNG EASIER
'''

#class used in place of a warning because this will be wrapped. raises an error instead of a warning
class WarningError(Exception):
    pass 

#MUST TAKE STREAM IDS AS A PARAM AND UPDATE IT. SHOULDN'T BE EXPODED TO THE NODE END WRAPPER BUT NEEDED HERE
def init_stream(redis_client, stream_name, dtype: str):

    #check to see if an init stream already exists for this stream
    init_name = f"{stream_name}_init"
    if redis_client.exists(init_name):
        first_entry = redis_client.xrange(init_name, '-', '+', count=1)
        if first_entry is not None:
            existing_dtype = first_entry[0][1][b'dtype'].decode()
            print(f"existing dtype: {existing_dtype} and new dtype: {dtype}")
            if dtype != existing_dtype:
                raise WarningError(f"can not init. Stream {stream_name} has already been started with a different dtype")
        return
    
    #check if a string represents a valid dtype, if so, return the string 
    def get_valid_dstring(dstring) -> str:
        dstring = dstring.lower()
        if dstring == "serial" or PAT.get_struct_format(dstring) is not None:
            return dstring
        raise ValueError(f"dtype {dstring} is not a valid type")


    dtype_dict = {}
    #accept either a string or a dict. 
    if isinstance(dtype, str):
        dtype_dict = {"dtype": get_valid_dstring(dtype)}
    elif isinstance(dtype, dict):
        for key, value in dtype.items():
            try:
                dtype_dict[key] = get_valid_dstring(value)
            except ValueError as e:
                print(f"dictionary dtype {value} is not valid. this kvp will not be included in the stream init")
                warnings.warn("") #only time warnigns.warn is used because this in a loop and can't be hanled by a wrapper
                pass
        if len(dtype_dict) > 0:
            dtype_dict = {"dtype": json.dumps(dtype_dict)}
        else:
            raise ValueError("could not init stream. EVERY dtype in dict was invalid")
    else:
        pass
    redis_client.xadd(init_name, dtype_dict)


#check to see if the stream's first entry... exits? how is this useful in any context? couldn't this just combined with below? 
def get_stream_init(redis_client, stream_name):
    init_name = f"{stream_name}_init"
    first_entry = redis_client.xrange(init_name, '-', '+', count=1)
    if len(first_entry) > 0:
        return first_entry[0][1]
    else:
        raise IndexError("can not get stream init because stream is empty")

#gets the stream's dtype from the stream init entry. raises error if no stream init
def get_stream_dtype(redis_client, stream_name):
    init_entry = get_stream_init(redis_client, stream_name)
    dtype_data = init_entry[b'dtype']
    # see if the data is a dict, or a string
    try:
        dtype_dict = json.loads(dtype_data.decode())
        return dtype_dict
    except ValueError:
        try:
            dtype = dtype_data.decode()
            return dtype
        except Exception as e:
            raise ValueError(f"could not decode dtype data. original error: {e}")

# get the latest enread entry from the redis stream
#ALSO NEEDS TO RECIEVE THE HEAD IDS AND UPDATE THEM 
def read_latest(redis_client, stream_name, stream_head_id):
    # get the top entry from the redis data base
    last_entry = redis_client.xrevrange(stream_name, '+', '-', count=1)
    # get the id by only looking at the linux epoch time since all entries will share that
    if len(last_entry) != 0:
        redis_time_id = last_entry[0][0].decode()

        # if the this stream hasn't been tracked yet or the entry is newer than the last entry, return entry and update the tracker
        if stream_name not in stream_head_id or stream_head_id[stream_name] != redis_time_id:
            stream_head_id[stream_name] = redis_time_id
            return last_entry[0]
    return None

# take a series of stream entries and decode them into kvps with the time stamp as index 1 and the value dictionary as index 2 
def decode(redis_client, redis_entries, stream_name = None, dtype = None):
    #check to see if a dtype is provided
    if dtype is None:
        try:
            dtype = get_stream_dtype(redis_client, stream_name)
        except: 
            print(f"could not get dtype from stream init and none provided. can not decode for {stream_name}")
            return

    #code expects an array. if the output is just a single entry, make it an array
    if isinstance(redis_entries, tuple):
        redis_entries = [redis_entries]
    
    decoded_entries = PAT.decode_entries(redis_entries, dtype)
    return decoded_entries
            


#method that takes accepted formats and encodes them appropriately
def encode_from_dtype(data, dtype):
    try:
        if dtype == "serial":
            encoded_data = json.dumps(data)
            return encoded_data
        else:
            try:
                encoded_data = np.array(data, dtype=dtype).tobytes()
                return encoded_data
            except ValueError:
                struct_format = PAT.get_struct_format(dtype)
                encoded_data = struct.pack(struct_format, data)
                return encoded_data
    except Exception as e:
        print(f"could not encode data. original error: {e}")



'''
data handling:
numpy or array type: if the data is an arraylike and has tobytes, then it is converted to bytes and sent to the stream
dict type: if the data is a dict, then it is pickled and sent to the stream
int type: if the data is an int, then it is converted to a struct and sent to the stream
float type: if the data is a float, then it is converted to a struct and sent to the stream
else: if the data is none of the above, then it is converted to a json and sent to the stream
'''

#auto decodes the content of your data and adds it to the redis stream. has special handling for armature structs
def add_to_stream(redis_client, stream_name: str, data, dtype=None):
    #check if the dtype can be gotten from the stream or is provided
    if dtype is None:
        try:
            dtype = get_stream_dtype(redis_client, stream_name)
        except Exception as e: 
            print(f"could not get dtype from stream init and none provided. can not encode for {stream_name}. original error {e}")
            return

    #see if the data is an armature struct or not a dict
    if isinstance(data, ArmatureStruct):
        data = data.summarize()
    elif not isinstance(data, dict):
        data = {"data": data}
    
    encoded_data_dict = {}
    for key, value in data.items():
        try:
            # get dtype for each key if the keys are not uniform
            key_dtype = dtype[key]
            encoded_data_dict[key] = encode_from_dtype(value, key_dtype)
        except (KeyError, TypeError):
            encoded_data_dict[key] = encode_from_dtype(value, dtype)
        except Exception as e:
            print(f"could not serialize data. original error: {e}")
    redis_client.xadd(stream_name, encoded_data_dict)

