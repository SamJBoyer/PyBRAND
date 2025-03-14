import pytest
import redis
import brandlite.pynode_tools as pnt

'''
Author: Sam Boyer
Gmail: sam.james.boyer@gmail.com

Test of the level 2 node features. To see the deliniation, see the node.txt file in BRAND.
YOU MUST LAUNCH REDIS SERVER BEFORE RUNNING THIS SCRIPT SO THERE IS A DATABASE TO CONNECT TO
'''

@pytest.fixture
def redis_client():
    r = redis.Redis(host='localhost', port=6379, db=0)
    yield r
    r.flushdb()
    r.close()

def test_init_stream(redis_client):
    r = redis_client
    temp_ids = {}

    #STREAM INIT SUCCESS MODES

    #init with single dtype 
    pnt.init_stream(r, 'T1', 'int8')
    pnt.init_stream(r,'T2', 'b')
    pnt.init_stream(r,'T3', 'serial')
    
    #test getting the entire stream init
    a = pnt.get_stream_init(r, "T1")
    print(f"a: {a}")
    assert a == {b'dtype': b'int8'}

    b = pnt.get_stream_init(r, "T2")
    print(f"b: {b}")
    assert b == {b'dtype': b'b'}

    c = pnt.get_stream_init(r, "T3")
    print(f"c: {c}")
    assert c == {b'dtype': b'serial'}
    
    #test getting the stream dtype for simple
    a = pnt.get_stream_dtype(r, "T1")
    print(f"a: {a}")
    assert a == 'int8'

    b = pnt.get_stream_dtype(r, "T2")
    print(f"b: {b}")
    assert b == 'b'

    c = pnt.get_stream_dtype(r, "T3")
    print(f"c: {c}")
    assert c == 'serial'

    #STREAM INIT FAILURE MODES

    #init with invalid dtype
    with pytest.raises(ValueError):
        pnt.init_stream(r, 'INVALIDDTYPE', dtype = 'int9')
    
    #init when the stream has already been started
    with pytest.raises(pnt.WarningError):
        pnt.init_stream(r, 'T1', 'int32')

    #init with 1 invalid dtype in a dictionary
    with pytest.warns(UserWarning):
        pnt.init_stream(r, 'INVALIDDICT', {"a": "int8", "b": "int9"})


    



#Reminder: WHEN DOING NEW TESTS, REMEMBER THE REDIS LINUX TIME STAMP WILL NEVER BE THE SAME (unless you found a way to freeze time)
#DON'T ASSERT IT!! ALSO REMEMBER TO FLUSH THE DATABASE AFTER EACH TEST 
def test_read_stream(redis_client):
    r = redis_client
    temp_heads = {}

    #test reading from the stream WITH a stream init with simple data no name 
    pnt.init_stream(r, 'READTEST1', 'int8')
    pnt.add_to_stream(r, "READTEST1", 15)
    a = pnt.read_latest(r, "READTEST1", temp_heads)
    print(f"a: {a}")
    assert a[1] == {b'data': b'\x0f'}

    #decode a
    a_dec = pnt.decode(r, a, "READTEST1", "int8")
    print(f"a_dec: {a_dec}")
    assert str(a_dec[0][1]) == "{'data': array([15], dtype=int8)}"

    #test reading with more complicated data 
    pnt.init_stream(r, 'READTEST2', {"a": "int8", "b": "int16"})
    pnt.add_to_stream(r, "READTEST2", {"a": 15, "b": 20})
    b = pnt.read_latest(r, "READTEST2", temp_heads)
    print(f"b: {b}")

    #decode b 
    b_dec = pnt.decode(r, b, "READTEST2")
    print(f"b_dec: {b_dec}")
    assert str(b_dec[0][1]) == "{'a': array([15], dtype=int8), 'b': array([20], dtype=int16)}"







    '''
    pnt.add_to_stream("Add1", 15, "int8")
    Add1Top = pnt.read_latest("Add1")
    print(f"Add1Top: {Add1Top}")

    Add1D = pnt.decode("Add1", Add1Top, "int8")
    print(f"add1 decoded: {Add1D}") 

    pnt.add_to_stream("T1", 15)
    T1Top = pnt.read_latest("T1")
    print(f"T1top: {T1Top}")

    T1Decoded = pnt.decode("T1", T1Top)
    print(f"T1 decoded: {T1Decoded}") 

    print(" ------ testing adding to stream with larger dataset---- ")
    pnt.add_to_stream("Add2", {"a": 15, "b": 20}, {"a": "int8", "b": "int16"})

    Add2Top = pnt.read_latest("Add2")
    print(f"Add2Top: {Add2Top}")
    Add2D = pnt.decode("Add2", Add2Top, {"a": "int8", "b": "int16"})
    print(f"Add2 decoded: {Add2D}")

    print("--- test adding more complicated pieces of data ---")
    pnt.init_stream("Large", {"a": "int8", "b": "int16", "c": "serial"})
    pnt.add_to_stream("Large", {"a": [12,32,12,1,0], "b": [10, 2, 421, 2], "c": {"a": 1, "b": 2, "c": 3}})
    dataset = pnt.read_latest("Large")
    data = pnt.decode("Large", dataset)
    print(data)

    # Step 1: Create a dictionary with column names as keys and lists as values
    data = {
        'Column1': [1, 2, 3, 4],
        'Column2': [5, 6, 7, 8],
        'Column3': [9, 10, 11, 12]
    }
    df = pd.DataFrame(data)
    print(df)
    pnt.init_stream("Matrix", {"a": "int8", "b": "int16", "c": "serial"})
    pnt.add_to_stream("Matrix", {"a": df, "b": [10, 2, 421, 2], "c": {"a": 1, "b": 2, "c": 3}})
    dataset = pnt.read_latest("Matrix")
    data = pnt.decode("Matrix", dataset)
    print(data)
    '''

r = redis.Redis(host='localhost', port=6379, db=0)
r.flushdb()
#test_init_stream(r)
test_read_stream(r)
print("done")