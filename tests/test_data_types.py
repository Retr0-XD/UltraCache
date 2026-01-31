#!/usr/bin/env python3
"""Comprehensive test for all data types"""
import socket
import sys

def send_cmd(sock, *args):
    """Send RESP array command"""
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        arg_bytes = str(arg).encode('utf-8')
        cmd += f"${len(arg_bytes)}\r\n{arg_bytes.decode('utf-8')}\r\n"
    sock.sendall(cmd.encode('utf-8'))

def read_resp(sock):
    """Read RESP response"""
    first_byte = sock.recv(1)
    if not first_byte:
        return None
    
    if first_byte == b'+':  # Simple string
        line = b''
        while not line.endswith(b'\r\n'):
            line += sock.recv(1)
        return line[:-2].decode('utf-8')
    elif first_byte == b'-':  # Error
        line = b''
        while not line.endswith(b'\r\n'):
            line += sock.recv(1)
        return f"ERROR: {line[:-2].decode('utf-8')}"
    elif first_byte == b':':  # Integer
        line = b''
        while not line.endswith(b'\r\n'):
            line += sock.recv(1)
        return int(line[:-2].decode('utf-8'))
    elif first_byte == b'$':  # Bulk string
        line = b''
        while not line.endswith(b'\r\n'):
            line += sock.recv(1)
        length = int(line[:-2].decode('utf-8'))
        if length == -1:
            return None
        data = sock.recv(length)
        sock.recv(2)  # \r\n
        return data.decode('utf-8')
    elif first_byte == b'*':  # Array
        line = b''
        while not line.endswith(b'\r\n'):
            line += sock.recv(1)
        count = int(line[:-2].decode('utf-8'))
        result = []
        for _ in range(count):
            result.append(read_resp(sock))
        return result
    
    return None

def test_data_types():
    """Test all data types work correctly"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 6379))
    
    try:
        # AUTH as tenant
        send_cmd(sock, "AUTH", "data_test_tenant")
        resp = read_resp(sock)
        assert resp == "OK", f"AUTH failed: {resp}"
        print("✓ AUTH successful")
        
        # Test String operations
        send_cmd(sock, "SET", "name", "UltraCache")
        assert read_resp(sock) == "OK"
        send_cmd(sock, "GET", "name")
        assert read_resp(sock) == "UltraCache"
        print("✓ String operations work")
        
        # Test Hash operations
        send_cmd(sock, "HSET", "user:1", "name", "Alice")
        assert read_resp(sock) == 1
        send_cmd(sock, "HSET", "user:1", "age", "30")
        assert read_resp(sock) == 1
        send_cmd(sock, "HGET", "user:1", "name")
        assert read_resp(sock) == "Alice"
        send_cmd(sock, "HGET", "user:1", "age")
        assert read_resp(sock) == "30"
        print("✓ Hash operations work")
        
        # Test Set operations
        send_cmd(sock, "SADD", "colors", "red")
        assert read_resp(sock) == 1
        send_cmd(sock, "SADD", "colors", "green")
        assert read_resp(sock) == 1
        send_cmd(sock, "SADD", "colors", "blue")
        assert read_resp(sock) == 1
        send_cmd(sock, "SMEMBERS", "colors")
        members = read_resp(sock)
        assert set(members) == {"red", "green", "blue"}
        print("✓ Set operations work")
        
        # Test Sorted Set operations
        send_cmd(sock, "ZADD", "scores", "85", "Alice")
        assert read_resp(sock) == 1
        send_cmd(sock, "ZADD", "scores", "92", "Bob")
        assert read_resp(sock) == 1
        send_cmd(sock, "ZADD", "scores", "78", "Charlie")
        assert read_resp(sock) == 1
        send_cmd(sock, "ZRANGE", "scores", "0", "-1")
        zrange = read_resp(sock)
        assert zrange == ["Charlie", "Alice", "Bob"]  # Sorted by score
        print("✓ Sorted Set operations work")
        
        # Test type isolation - operations on wrong types should fail
        send_cmd(sock, "HGET", "name", "field")  # name is a string, not hash
        resp = read_resp(sock)
        assert "WRONGTYPE" in str(resp)
        
        send_cmd(sock, "SADD", "user:1", "member")  # user:1 is a hash, not set
        resp = read_resp(sock)
        assert "WRONGTYPE" in str(resp)
        
        send_cmd(sock, "ZADD", "colors", "100", "member")  # colors is a set, not zset
        resp = read_resp(sock)
        assert "WRONGTYPE" in str(resp)
        
        send_cmd(sock, "GET", "scores")  # scores is a zset, not string
        resp = read_resp(sock)
        assert "WRONGTYPE" in str(resp)
        print("✓ Type safety enforced - WRONGTYPE errors work")
        
        # Test that different keys can have different types
        send_cmd(sock, "SET", "key1", "string_value")
        assert read_resp(sock) == "OK"
        
        send_cmd(sock, "HSET", "key2", "field", "hash_value")
        assert read_resp(sock) == 1
        
        send_cmd(sock, "SADD", "key3", "set_member")
        assert read_resp(sock) == 1
        
        send_cmd(sock, "ZADD", "key4", "100", "zset_member")
        assert read_resp(sock) == 1
        
        send_cmd(sock, "GET", "key1")
        assert read_resp(sock) == "string_value"
        
        send_cmd(sock, "HGET", "key2", "field")
        assert read_resp(sock) == "hash_value"
        
        send_cmd(sock, "SMEMBERS", "key3")
        assert read_resp(sock) == ["set_member"]
        
        send_cmd(sock, "ZRANGE", "key4", "0", "-1")
        assert read_resp(sock) == ["zset_member"]
        print("✓ Multiple types can coexist")
        
        # Test deletion across types
        send_cmd(sock, "DEL", "key1")
        assert read_resp(sock) == 1
        send_cmd(sock, "GET", "key1")
        assert read_resp(sock) is None
        
        send_cmd(sock, "DEL", "key2")
        assert read_resp(sock) == 1
        send_cmd(sock, "HGET", "key2", "field")
        assert read_resp(sock) is None
        
        send_cmd(sock, "DEL", "key3")
        assert read_resp(sock) == 1
        send_cmd(sock, "SMEMBERS", "key3")
        assert read_resp(sock) == []
        
        send_cmd(sock, "DEL", "key4")
        assert read_resp(sock) == 1
        send_cmd(sock, "ZRANGE", "key4", "0", "-1")
        assert read_resp(sock) == []
        print("✓ DEL works across all types")
        
        print("\n✅ All data types working correctly!")
        
    finally:
        sock.close()

if __name__ == "__main__":
    try:
        test_data_types()
    except Exception as e:
        print(f"\n❌ Test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
