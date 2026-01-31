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

print("Testing CPU throttling...")

# Create a tenant with limited CPU quota (already set to 100ms/sec by default)
send_command(sock, "AUTH", "throttle_test")
resp = read_response(sock)
print(f"AUTH throttle_test -> {resp}")

# Rapid fire commands to exhaust CPU quota
print("Sending rapid commands to exhaust CPU quota...")
throttled = False
for i in range(10000):
    send_command(sock, "SET", f"key{i}", f"value{i}")
    resp = read_response(sock)
    if "CPU quota exceeded" in resp:
        print(f"✅ Throttled after {i} commands: {resp}")
        throttled = True
        break

if not throttled:
    print("⚠️  Warning: CPU throttling did not trigger (quota may be too high)")
else:
    # Wait for quota to reset
    print("Waiting 1.5 seconds for quota reset...")
    time.sleep(1.5)
    
    # Try again after reset
    send_command(sock, "SET", "afterreset", "value")
    resp = read_response(sock)
    print(f"SET after reset -> {resp}")
    if "+OK" in resp:
        print("✅ CPU quota reset correctly!")
    else:
        print("❌ CPU quota did not reset")

sock.close()
print("\n✅ CPU throttling tests complete!")
