#!/usr/bin/env python3
"""
Test List commands: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE
"""
import socket
import sys

def send_cmd(sock, *args):
    """Send a RESP command."""
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        arg_str = str(arg)
        cmd += f"${len(arg_str)}\r\n{arg_str}\r\n"
    sock.sendall(cmd.encode())

def recv_resp(sock):
    """Receive a RESP response."""
    def read_line():
        line = b""
        while True:
            char = sock.recv(1)
            if not char:
                raise ConnectionError("Connection closed")
            line += char
            if line.endswith(b"\r\n"):
                return line[:-2].decode()
    
    first = read_line()
    if first.startswith("+"):
        return first[1:]
    elif first.startswith("-"):
        return first
    elif first.startswith(":"):
        return int(first[1:])
    elif first.startswith("$"):
        length = int(first[1:])
        if length == -1:
            return None
        data = sock.recv(length)
        sock.recv(2)  # \r\n
        return data.decode()
    elif first.startswith("*"):
        count = int(first[1:])
        if count == -1:
            return None
        return [recv_resp(sock) for _ in range(count)]
    return first

def test_lpush_rpush():
    """Test LPUSH and RPUSH commands."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    # Clean up
    send_cmd(sock, "DEL", "mylist")
    recv_resp(sock)
    
    # LPUSH adds to head
    send_cmd(sock, "LPUSH", "mylist", "world")
    resp = recv_resp(sock)
    assert resp == 1, f"Expected 1, got {resp}"
    
    send_cmd(sock, "LPUSH", "mylist", "hello")
    resp = recv_resp(sock)
    assert resp == 2, f"Expected 2, got {resp}"
    
    # RPUSH adds to tail
    send_cmd(sock, "RPUSH", "mylist", "!")
    resp = recv_resp(sock)
    assert resp == 3, f"Expected 3, got {resp}"
    
    # Verify order: [hello, world, !]
    send_cmd(sock, "LRANGE", "mylist", "0", "-1")
    resp = recv_resp(sock)
    assert resp == ["hello", "world", "!"], f"Expected ['hello', 'world', '!'], got {resp}"
    
    sock.close()
    print("✓ LPUSH and RPUSH work correctly")

def test_lpop_rpop():
    """Test LPOP and RPOP commands."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    # Setup
    send_cmd(sock, "DEL", "mylist")
    recv_resp(sock)
    
    send_cmd(sock, "RPUSH", "mylist", "a")
    recv_resp(sock)
    send_cmd(sock, "RPUSH", "mylist", "b")
    recv_resp(sock)
    send_cmd(sock, "RPUSH", "mylist", "c")
    recv_resp(sock)
    
    # LPOP removes from head
    send_cmd(sock, "LPOP", "mylist")
    resp = recv_resp(sock)
    assert resp == "a", f"Expected 'a', got {resp}"
    
    # RPOP removes from tail
    send_cmd(sock, "RPOP", "mylist")
    resp = recv_resp(sock)
    assert resp == "c", f"Expected 'c', got {resp}"
    
    # Only 'b' remains
    send_cmd(sock, "LLEN", "mylist")
    resp = recv_resp(sock)
    assert resp == 1, f"Expected 1, got {resp}"
    
    send_cmd(sock, "LPOP", "mylist")
    resp = recv_resp(sock)
    assert resp == "b", f"Expected 'b', got {resp}"
    
    # Empty list
    send_cmd(sock, "LPOP", "mylist")
    resp = recv_resp(sock)
    assert resp is None, f"Expected None, got {resp}"
    
    sock.close()
    print("✓ LPOP and RPOP work correctly")

def test_llen():
    """Test LLEN command."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    # Clean up
    send_cmd(sock, "DEL", "mylist")
    recv_resp(sock)
    
    # Empty list
    send_cmd(sock, "LLEN", "mylist")
    resp = recv_resp(sock)
    assert resp == 0, f"Expected 0, got {resp}"
    
    # Add elements
    send_cmd(sock, "RPUSH", "mylist", "one")
    recv_resp(sock)
    send_cmd(sock, "RPUSH", "mylist", "two")
    recv_resp(sock)
    send_cmd(sock, "RPUSH", "mylist", "three")
    recv_resp(sock)
    
    send_cmd(sock, "LLEN", "mylist")
    resp = recv_resp(sock)
    assert resp == 3, f"Expected 3, got {resp}"
    
    sock.close()
    print("✓ LLEN works correctly")

def test_lrange():
    """Test LRANGE command."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    # Setup
    send_cmd(sock, "DEL", "mylist")
    recv_resp(sock)
    
    for i in range(10):
        send_cmd(sock, "RPUSH", "mylist", str(i))
        recv_resp(sock)
    
    # Get all
    send_cmd(sock, "LRANGE", "mylist", "0", "-1")
    resp = recv_resp(sock)
    assert resp == [str(i) for i in range(10)], f"Expected [0..9], got {resp}"
    
    # Get first 3
    send_cmd(sock, "LRANGE", "mylist", "0", "2")
    resp = recv_resp(sock)
    assert resp == ["0", "1", "2"], f"Expected ['0', '1', '2'], got {resp}"
    
    # Get last 3 using negative indices
    send_cmd(sock, "LRANGE", "mylist", "-3", "-1")
    resp = recv_resp(sock)
    assert resp == ["7", "8", "9"], f"Expected ['7', '8', '9'], got {resp}"
    
    # Get middle range
    send_cmd(sock, "LRANGE", "mylist", "3", "5")
    resp = recv_resp(sock)
    assert resp == ["3", "4", "5"], f"Expected ['3', '4', '5'], got {resp}"
    
    # Empty range
    send_cmd(sock, "LRANGE", "mylist", "20", "30")
    resp = recv_resp(sock)
    assert resp == [], f"Expected [], got {resp}"
    
    sock.close()
    print("✓ LRANGE works correctly")

def test_wrongtype():
    """Test WRONGTYPE error for list operations."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    # Set a string key
    send_cmd(sock, "SET", "mykey", "value")
    recv_resp(sock)
    
    # Try list operation on string key
    send_cmd(sock, "LPUSH", "mykey", "item")
    resp = recv_resp(sock)
    assert resp.startswith("-WRONGTYPE"), f"Expected WRONGTYPE error, got {resp}"
    
    sock.close()
    print("✓ WRONGTYPE errors work correctly")

def test_list_on_nonexistent_key():
    """Test list operations on non-existent keys."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    send_cmd(sock, "DEL", "nokey")
    recv_resp(sock)
    
    # LLEN on non-existent key
    send_cmd(sock, "LLEN", "nokey")
    resp = recv_resp(sock)
    assert resp == 0, f"Expected 0, got {resp}"
    
    # LPOP on non-existent key
    send_cmd(sock, "LPOP", "nokey")
    resp = recv_resp(sock)
    assert resp is None, f"Expected None, got {resp}"
    
    # LRANGE on non-existent key
    send_cmd(sock, "LRANGE", "nokey", "0", "-1")
    resp = recv_resp(sock)
    assert resp == [], f"Expected [], got {resp}"
    
    sock.close()
    print("✓ List operations on non-existent keys work correctly")

if __name__ == "__main__":
    try:
        test_lpush_rpush()
        test_lpop_rpop()
        test_llen()
        test_lrange()
        test_wrongtype()
        test_list_on_nonexistent_key()
        print("\n✅ All List tests passed!")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
