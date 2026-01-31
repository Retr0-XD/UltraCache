#!/usr/bin/env python3
import socket
import time

def send_command(sock, *args):
    """Send RESP array command"""
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        arg_str = str(arg)
        cmd += f"${len(arg_str)}\r\n{arg_str}\r\n"
    sock.sendall(cmd.encode())
    
def read_response(sock):
    """Read RESP response"""
    data = sock.recv(1024).decode()
    return data.strip()

# Connect to UltraCache
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('127.0.0.1', 6379))

print("Testing TTL functionality...")

# Test 1: PING
send_command(sock, "PING")
resp = read_response(sock)
print(f"PING -> {resp}")
assert "+PONG" in resp

# Test 2: SET + EXPIRE
send_command(sock, "SET", "mykey", "myvalue")
resp = read_response(sock)
print(f"SET mykey myvalue -> {resp}")

send_command(sock, "EXPIRE", "mykey", "2")
resp = read_response(sock)
print(f"EXPIRE mykey 2 -> {resp}")

# Test 3: TTL before expiration
send_command(sock, "TTL", "mykey")
resp = read_response(sock)
print(f"TTL mykey (before expire) -> {resp}")
assert ":1" in resp or ":2" in resp

# Test 4: GET before expiration
send_command(sock, "GET", "mykey")
resp = read_response(sock)
print(f"GET mykey (before expire) -> {resp}")
assert "myvalue" in resp

# Test 5: Wait for expiration
print("Waiting 3 seconds for TTL expiration...")
time.sleep(3)

# Test 6: TTL after expiration
send_command(sock, "TTL", "mykey")
resp = read_response(sock)
print(f"TTL mykey (after expire) -> {resp}")
assert ":-2" in resp or "$-1" in resp

# Test 7: GET after expiration
send_command(sock, "GET", "mykey")
resp = read_response(sock)
print(f"GET mykey (after expire) -> {resp}")
assert "$-1" in resp

print("\n✅ TTL tests passed!")

sock.close()
