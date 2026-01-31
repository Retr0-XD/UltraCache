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
    sock.settimeout(2.0)
    data = sock.recv(4096).decode()
    return data.strip()

# Connect to UltraCache
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('127.0.0.1', 6379))

print("Testing CPU throttling...")

# Create a tenant
send_command(sock, "AUTH", "throttle_test")
resp = read_response(sock)
print(f"AUTH throttle_test -> {resp}")

# Test basic functionality first
send_command(sock, "SET", "test", "value")
resp = read_response(sock)
print(f"SET test -> {resp}")

# Rapid fire commands to try to exhaust CPU quota
print("\nSending 1000 rapid SET commands...")
throttled_count = 0
success_count = 0

for i in range(1000):
    try:
        send_command(sock, "SET", f"key{i}", f"value{i}")
        resp = read_response(sock)
        if "CPU quota exceeded" in resp:
            throttled_count += 1
            if throttled_count == 1:
                print(f"✅ First throttle at command {i}: {resp}")
        elif "+OK" in resp:
            success_count += 1
    except socket.timeout:
        print(f"Timeout at command {i}")
        break

print(f"\nResults: {success_count} successful, {throttled_count} throttled")

if throttled_count > 0:
    print("✅ CPU throttling is working!")
    
    # Wait for quota reset
    print("\nWaiting 1.5 seconds for quota reset...")
    time.sleep(1.5)
    
    # Try again after reset
    send_command(sock, "SET", "afterreset", "value")
    resp = read_response(sock)
    print(f"SET after reset -> {resp}")
    if "+OK" in resp:
        print("✅ CPU quota reset correctly!")
else:
    print("⚠️  CPU quota not triggered (commands too fast or quota too high)")

sock.close()
print("\n✅ CPU throttling test complete!")
