#!/usr/bin/env python3
"""Test Hash commands (HGET, HSET, HDEL)"""
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

def test_hash():
    """Test Hash operations"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 6379))
    
    try:
        # AUTH as tenant
        send_cmd(sock, "AUTH", "tenant1")
        resp = read_resp(sock)
        assert resp == "OK", f"AUTH failed: {resp}"
        print("✓ AUTH successful")
        
        # HSET - create new field
        send_cmd(sock, "HSET", "user:1", "name", "Alice")
        resp = read_resp(sock)
        assert resp == 1, f"HSET new field should return 1, got {resp}"
        print("✓ HSET new field returns 1")
        
        # HGET - retrieve field
        send_cmd(sock, "HGET", "user:1", "name")
        resp = read_resp(sock)
        assert resp == "Alice", f"HGET should return 'Alice', got {resp}"
        print("✓ HGET returns correct value")
        
        # HSET - update existing field
        send_cmd(sock, "HSET", "user:1", "name", "Bob")
        resp = read_resp(sock)
        assert resp == 0, f"HSET existing field should return 0, got {resp}"
        print("✓ HSET existing field returns 0")
        
        # HGET - verify update
        send_cmd(sock, "HGET", "user:1", "name")
        resp = read_resp(sock)
        assert resp == "Bob", f"HGET should return 'Bob', got {resp}"
        print("✓ HGET returns updated value")
        
        # HSET - add another field
        send_cmd(sock, "HSET", "user:1", "age", "25")
        resp = read_resp(sock)
        assert resp == 1, f"HSET new field should return 1, got {resp}"
        print("✓ HSET adds second field")
        
        # HGET - get second field
        send_cmd(sock, "HGET", "user:1", "age")
        resp = read_resp(sock)
        assert resp == "25", f"HGET should return '25', got {resp}"
        print("✓ HGET returns second field")
        
        # HGET - non-existent field
        send_cmd(sock, "HGET", "user:1", "email")
        resp = read_resp(sock)
        assert resp is None, f"HGET non-existent field should return nil, got {resp}"
        print("✓ HGET non-existent field returns nil")
        
        # HDEL - delete field
        send_cmd(sock, "HDEL", "user:1", "age")
        resp = read_resp(sock)
        assert resp == 1, f"HDEL should return 1, got {resp}"
        print("✓ HDEL returns 1")
        
        # HGET - verify deletion
        send_cmd(sock, "HGET", "user:1", "age")
        resp = read_resp(sock)
        assert resp is None, f"HGET deleted field should return nil, got {resp}"
        print("✓ HGET deleted field returns nil")
        
        # HDEL - delete non-existent field
        send_cmd(sock, "HDEL", "user:1", "age")
        resp = read_resp(sock)
        assert resp == 0, f"HDEL non-existent field should return 0, got {resp}"
        print("✓ HDEL non-existent field returns 0")
        
        # HGET - non-existent hash
        send_cmd(sock, "HGET", "user:999", "name")
        resp = read_resp(sock)
        assert resp is None, f"HGET non-existent hash should return nil, got {resp}"
        print("✓ HGET non-existent hash returns nil")
        
        # Test WRONGTYPE - set a string key
        send_cmd(sock, "SET", "mystring", "value")
        resp = read_resp(sock)
        assert resp == "OK", f"SET failed: {resp}"
        
        # Try hash operation on string key
        send_cmd(sock, "HGET", "mystring", "field")
        resp = read_resp(sock)
        assert "WRONGTYPE" in str(resp), f"HGET on string should return WRONGTYPE, got {resp}"
        print("✓ HGET on string returns WRONGTYPE error")
        
        print("\n✅ All Hash tests passed!")
        
    finally:
        sock.close()

if __name__ == "__main__":
    try:
        test_hash()
    except Exception as e:
        print(f"\n❌ Test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
