#!/usr/bin/env python3
"""Test Sorted Set commands (ZADD, ZREM, ZRANGE)"""
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

def test_zset():
    """Test Sorted Set operations"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 6379))
    
    try:
        # AUTH as tenant
        send_cmd(sock, "AUTH", "tenant1")
        resp = read_resp(sock)
        assert resp == "OK", f"AUTH failed: {resp}"
        print("✓ AUTH successful")
        
        # ZADD - add new member
        send_cmd(sock, "ZADD", "leaderboard", "100", "alice")
        resp = read_resp(sock)
        assert resp == 1, f"ZADD new member should return 1, got {resp}"
        print("✓ ZADD new member returns 1")
        
        # ZADD - update existing member score
        send_cmd(sock, "ZADD", "leaderboard", "150", "alice")
        resp = read_resp(sock)
        assert resp == 0, f"ZADD existing member should return 0, got {resp}"
        print("✓ ZADD existing member returns 0")
        
        # ZADD - add more members
        send_cmd(sock, "ZADD", "leaderboard", "200", "bob")
        resp = read_resp(sock)
        assert resp == 1, f"ZADD should return 1, got {resp}"
        
        send_cmd(sock, "ZADD", "leaderboard", "50", "charlie")
        resp = read_resp(sock)
        assert resp == 1, f"ZADD should return 1, got {resp}"
        
        send_cmd(sock, "ZADD", "leaderboard", "175", "diana")
        resp = read_resp(sock)
        assert resp == 1, f"ZADD should return 1, got {resp}"
        print("✓ ZADD adds multiple members")
        
        # ZRANGE - get all members (0 to -1)
        send_cmd(sock, "ZRANGE", "leaderboard", "0", "-1")
        resp = read_resp(sock)
        assert isinstance(resp, list), f"ZRANGE should return array, got {type(resp)}"
        # Expected order by score: charlie(50), alice(150), diana(175), bob(200)
        assert resp == ["charlie", "alice", "diana", "bob"], f"ZRANGE returned wrong order: {resp}"
        print("✓ ZRANGE returns all members in score order")
        
        # ZRANGE - get first 2 members
        send_cmd(sock, "ZRANGE", "leaderboard", "0", "1")
        resp = read_resp(sock)
        assert resp == ["charlie", "alice"], f"ZRANGE(0,1) should return first 2 members, got {resp}"
        print("✓ ZRANGE(0,1) returns first 2 members")
        
        # ZRANGE - get last 2 members
        send_cmd(sock, "ZRANGE", "leaderboard", "-2", "-1")
        resp = read_resp(sock)
        assert resp == ["diana", "bob"], f"ZRANGE(-2,-1) should return last 2 members, got {resp}"
        print("✓ ZRANGE(-2,-1) returns last 2 members")
        
        # ZRANGE - get middle members
        send_cmd(sock, "ZRANGE", "leaderboard", "1", "2")
        resp = read_resp(sock)
        assert resp == ["alice", "diana"], f"ZRANGE(1,2) should return middle members, got {resp}"
        print("✓ ZRANGE(1,2) returns middle members")
        
        # ZREM - remove member
        send_cmd(sock, "ZREM", "leaderboard", "alice")
        resp = read_resp(sock)
        assert resp == 1, f"ZREM should return 1, got {resp}"
        print("✓ ZREM removes member")
        
        # ZRANGE - verify removal
        send_cmd(sock, "ZRANGE", "leaderboard", "0", "-1")
        resp = read_resp(sock)
        assert len(resp) == 3, f"ZRANGE should return 3 members after removal, got {len(resp)}"
        assert resp == ["charlie", "diana", "bob"], f"ZRANGE returned wrong members: {resp}"
        print("✓ ZRANGE shows removed member is gone")
        
        # ZREM - remove non-existent member
        send_cmd(sock, "ZREM", "leaderboard", "alice")
        resp = read_resp(sock)
        assert resp == 0, f"ZREM non-existent member should return 0, got {resp}"
        print("✓ ZREM non-existent member returns 0")
        
        # ZRANGE - non-existent zset
        send_cmd(sock, "ZRANGE", "nonexistent", "0", "-1")
        resp = read_resp(sock)
        assert isinstance(resp, list) and len(resp) == 0, f"ZRANGE non-existent zset should return empty array, got {resp}"
        print("✓ ZRANGE non-existent zset returns empty array")
        
        # Test WRONGTYPE - set a string key
        send_cmd(sock, "SET", "mystring", "value")
        resp = read_resp(sock)
        assert resp == "OK", f"SET failed: {resp}"
        
        # Try zset operation on string key
        send_cmd(sock, "ZADD", "mystring", "100", "member")
        resp = read_resp(sock)
        assert "WRONGTYPE" in str(resp), f"ZADD on string should return WRONGTYPE, got {resp}"
        print("✓ ZADD on string returns WRONGTYPE error")
        
        print("\n✅ All Sorted Set tests passed!")
        
    finally:
        sock.close()

if __name__ == "__main__":
    try:
        test_zset()
    except Exception as e:
        print(f"\n❌ Test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
