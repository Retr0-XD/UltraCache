#!/usr/bin/env python3
"""Test STATS admin command"""
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
    
    return None

def test_stats():
    """Test STATS command"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 6379))
    
    try:
        # AUTH as tenant
        send_cmd(sock, "AUTH", "stats_test_tenant")
        resp = read_resp(sock)
        assert resp == "OK", f"AUTH failed: {resp}"
        print("✓ AUTH successful")
        
        # Get initial stats
        send_cmd(sock, "STATS")
        stats = read_resp(sock)
        assert stats is not None, "STATS should return data"
        assert "tenant_id: stats_test_tenant" in stats
        assert "memory_used_bytes:" in stats
        assert "total_commands:" in stats
        assert "latency_p99_micros:" in stats
        print("✓ STATS command returns data")
        print(f"\nInitial stats:\n{stats}\n")
        
        # Parse initial stats
        initial_stats_dict = {}
        for line in stats.split('\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                initial_stats_dict[key.strip()] = value.strip()
        initial_commands = int(initial_stats_dict['total_commands'])
        
        # Perform some operations (paced to avoid CPU throttling)
        successful_sets = 0
        for i in range(10):
            send_cmd(sock, "SET", f"key{i}", f"value{i}")
            resp = read_resp(sock)
            if resp == "OK":
                successful_sets += 1
            # Small delay to avoid triggering CPU throttling
            import time
            time.sleep(0.01)
        assert successful_sets > 0, "Expected at least one successful SET"
        print(f"✓ Performed {successful_sets} successful SET operations")
        
        # Get stats again
        send_cmd(sock, "STATS")
        stats = read_resp(sock)
        assert "memory_used_bytes:" in stats
        
        # Parse stats to check values
        stats_dict = {}
        for line in stats.split('\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                stats_dict[key.strip()] = value.strip()
        
        # Verify stats make sense
        commands_executed = int(stats_dict['total_commands']) - initial_commands
        assert commands_executed >= successful_sets, (
            f"Should have recorded at least {successful_sets} new commands, got {commands_executed}"
        )
        assert int(stats_dict['memory_used_bytes']) > 0, "Should have memory usage"
        assert int(stats_dict['key_count']) == successful_sets, (
            f"Should have {successful_sets} keys, got {stats_dict['key_count']}"
        )
        
        print("✓ Stats values are correct")
        print(f"\nFinal stats:\n{stats}\n")
        
        # Test p99 latency tracking
        p99_micros = int(stats_dict['latency_p99_micros'])
        p99_ms = float(stats_dict['latency_p99_ms'])
        assert p99_micros > 0, "P99 latency should be tracked"
        assert p99_ms > 0, "P99 latency in ms should be tracked"
        print(f"✓ P99 latency tracked: {p99_micros}µs ({p99_ms:.3f}ms)")
        
        print("\n✅ All STATS tests passed!")
        
    finally:
        sock.close()

if __name__ == "__main__":
    try:
        test_stats()
    except Exception as e:
        print(f"\n❌ Test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
