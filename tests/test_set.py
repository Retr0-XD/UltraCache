#!/usr/bin/env python3
"""Test Set commands (SADD, SREM, SMEMBERS)"""
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

def test_set():
    """Test Set operations"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 6379))
    
    try:
        # AUTH as tenant
        send_cmd(sock, "AUTH", "tenant1")
        resp = read_resp(sock)
        assert resp == "OK", f"AUTH failed: {resp}"
        print("✓ AUTH successful")
        
        # SADD - add new member
        send_cmd(sock, "SADD", "myset", "apple")
        resp = read_resp(sock)
        assert resp == 1, f"SADD new member should return 1, got {resp}"
        print("✓ SADD new member returns 1")
        
        # SADD - add duplicate member
        send_cmd(sock, "SADD", "myset", "apple")
        resp = read_resp(sock)
        assert resp == 0, f"SADD duplicate member should return 0, got {resp}"
        print("✓ SADD duplicate member returns 0")
        
        # SADD - add more members
        send_cmd(sock, "SADD", "myset", "banana")
        resp = read_resp(sock)
        assert resp == 1, f"SADD should return 1, got {resp}"
        
        send_cmd(sock, "SADD", "myset", "cherry")
        resp = read_resp(sock)
        assert resp == 1, f"SADD should return 1, got {resp}"
        print("✓ SADD adds multiple members")
        
        # SMEMBERS - get all members
        send_cmd(sock, "SMEMBERS", "myset")
        resp = read_resp(sock)
        assert isinstance(resp, list), f"SMEMBERS should return array, got {type(resp)}"
        assert len(resp) == 3, f"SMEMBERS should return 3 members, got {len(resp)}"
        assert set(resp) == {"apple", "banana", "cherry"}, f"SMEMBERS returned wrong members: {resp}"
        print("✓ SMEMBERS returns all members")
        
        # SREM - remove member
        send_cmd(sock, "SREM", "myset", "banana")
        resp = read_resp(sock)
        assert resp == 1, f"SREM should return 1, got {resp}"
        print("✓ SREM removes member")
        
        # SMEMBERS - verify removal
        send_cmd(sock, "SMEMBERS", "myset")
        resp = read_resp(sock)
        assert len(resp) == 2, f"SMEMBERS should return 2 members after removal, got {len(resp)}"
        assert set(resp) == {"apple", "cherry"}, f"SMEMBERS returned wrong members: {resp}"
        print("✓ SMEMBERS shows removed member is gone")
        
        # SREM - remove non-existent member
        send_cmd(sock, "SREM", "myset", "banana")
        resp = read_resp(sock)
        assert resp == 0, f"SREM non-existent member should return 0, got {resp}"
        print("✓ SREM non-existent member returns 0")
        
        # SMEMBERS - non-existent set
        send_cmd(sock, "SMEMBERS", "nonexistent")
        resp = read_resp(sock)
        assert isinstance(resp, list) and len(resp) == 0, f"SMEMBERS non-existent set should return empty array, got {resp}"
        print("✓ SMEMBERS non-existent set returns empty array")
        
        # Test WRONGTYPE - set a string key
        send_cmd(sock, "SET", "mystring", "value")
        resp = read_resp(sock)
        assert resp == "OK", f"SET failed: {resp}"
        
        # Try set operation on string key
        send_cmd(sock, "SADD", "mystring", "member")
        resp = read_resp(sock)
        assert "WRONGTYPE" in str(resp), f"SADD on string should return WRONGTYPE, got {resp}"
        print("✓ SADD on string returns WRONGTYPE error")
        
        print("\n✅ All Set tests passed!")
        
    finally:
        sock.close()

if __name__ == "__main__":
    try:
        test_set()
    except Exception as e:
        print(f"\n❌ Test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
