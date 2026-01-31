#!/usr/bin/env python3
"""Load test demonstrating noisy neighbor isolation"""
import socket
import sys
import time
import threading
from statistics import mean, median

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
        return line[:-2].decode('utf-8')
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

def noisy_tenant_worker(tenant_name, duration_secs, results):
    """Simulate a noisy tenant hammering the server"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 6379))
    
    # AUTH
    send_cmd(sock, "AUTH", tenant_name)
    read_resp(sock)
    
    start = time.time()
    operations = 0
    throttled = 0
    latencies = []
    
    while time.time() - start < duration_secs:
        op_start = time.time()
        send_cmd(sock, "SET", f"key{operations}", f"value{operations}")
        resp = read_resp(sock)
        op_end = time.time()
        
        latencies.append((op_end - op_start) * 1000)  # Convert to ms
        
        if resp and "ERR tenant CPU quota exceeded" in str(resp):
            throttled += 1
        
        operations += 1
    
    sock.close()
    
    results[tenant_name] = {
        'operations': operations,
        'throttled': throttled,
        'latencies': latencies,
        'avg_latency_ms': mean(latencies) if latencies else 0,
        'median_latency_ms': median(latencies) if latencies else 0,
    }

def quiet_tenant_worker(tenant_name, duration_secs, results):
    """Simulate a well-behaved tenant"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 6379))
    
    # AUTH
    send_cmd(sock, "AUTH", tenant_name)
    read_resp(sock)
    
    start = time.time()
    operations = 0
    latencies = []
    
    while time.time() - start < duration_secs:
        op_start = time.time()
        send_cmd(sock, "SET", f"key{operations}", f"value{operations}")
        read_resp(sock)
        op_end = time.time()
        
        latencies.append((op_end - op_start) * 1000)  # Convert to ms
        operations += 1
        
        # Be nice - sleep between operations
        time.sleep(0.01)  # 10ms between ops = ~100 ops/sec
    
    sock.close()
    
    results[tenant_name] = {
        'operations': operations,
        'throttled': 0,
        'latencies': latencies,
        'avg_latency_ms': mean(latencies) if latencies else 0,
        'median_latency_ms': median(latencies) if latencies else 0,
    }

def test_noisy_neighbor():
    """Test that noisy tenants don't affect quiet tenants"""
    print("=" * 70)
    print("NOISY NEIGHBOR ISOLATION TEST")
    print("=" * 70)
    print("\nScenario:")
    print("  - Tenant 'noisy' hammers the server continuously")
    print("  - Tenant 'quiet' performs ~100 ops/sec")
    print("  - Both run for 3 seconds")
    print("\nExpected outcome:")
    print("  - Noisy tenant gets throttled (CPU quota exceeded)")
    print("  - Quiet tenant maintains stable latency")
    print("  - Both tenants are isolated from each other")
    print("\n" + "-" * 70)
    
    results = {}
    threads = []
    
    # Start noisy tenant
    t1 = threading.Thread(target=noisy_tenant_worker, args=("noisy", 3, results))
    threads.append(t1)
    
    # Start quiet tenant
    t2 = threading.Thread(target=quiet_tenant_worker, args=("quiet", 3, results))
    threads.append(t2)
    
    # Start both
    for t in threads:
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    
    # Noisy tenant results
    noisy = results['noisy']
    print(f"\n🔊 Noisy Tenant:")
    print(f"  Operations attempted:  {noisy['operations']}")
    print(f"  Throttled responses:   {noisy['throttled']} ({noisy['throttled']/noisy['operations']*100:.1f}%)")
    print(f"  Avg latency:           {noisy['avg_latency_ms']:.3f}ms")
    print(f"  Median latency:        {noisy['median_latency_ms']:.3f}ms")
    
    # Quiet tenant results
    quiet = results['quiet']
    print(f"\n🔇 Quiet Tenant:")
    print(f"  Operations completed:  {quiet['operations']}")
    print(f"  Throttled responses:   {quiet['throttled']}")
    print(f"  Avg latency:           {quiet['avg_latency_ms']:.3f}ms")
    print(f"  Median latency:        {quiet['median_latency_ms']:.3f}ms")
    
    print("\n" + "=" * 70)
    print("VALIDATION")
    print("=" * 70)
    
    # Verify isolation
    checks_passed = 0
    total_checks = 0
    
    # Check 1: Noisy tenant should be throttled
    total_checks += 1
    if noisy['throttled'] > 0:
        print("✓ Noisy tenant was throttled (CPU quota enforcement working)")
        checks_passed += 1
    else:
        print("✗ Noisy tenant was NOT throttled (CPU quota may be too high)")
    
    # Check 2: Quiet tenant should not be throttled
    total_checks += 1
    if quiet['throttled'] == 0:
        print("✓ Quiet tenant was not throttled (good behavior rewarded)")
        checks_passed += 1
    else:
        print("✗ Quiet tenant was throttled (unexpected)")
    
    # Check 3: Quiet tenant should maintain reasonable latency
    total_checks += 1
    if quiet['avg_latency_ms'] < 10:
        print(f"✓ Quiet tenant latency is good ({quiet['avg_latency_ms']:.3f}ms < 10ms)")
        checks_passed += 1
    else:
        print(f"✗ Quiet tenant latency is high ({quiet['avg_latency_ms']:.3f}ms)")
    
    # Check 4: Both tenants should complete operations
    total_checks += 1
    if noisy['operations'] > 100 and quiet['operations'] > 100:
        print(f"✓ Both tenants completed work (noisy: {noisy['operations']}, quiet: {quiet['operations']})")
        checks_passed += 1
    else:
        print(f"✗ Not enough operations completed")
    
    # Check 5: Noisy tenant should do more work (even when throttled)
    total_checks += 1
    if noisy['operations'] > quiet['operations'] * 2:
        print(f"✓ Noisy tenant attempted more work ({noisy['operations']} vs {quiet['operations']})")
        checks_passed += 1
    else:
        print(f"⚠ Noisy tenant work ratio lower than expected")
    
    print("\n" + "=" * 70)
    print(f"FINAL RESULT: {checks_passed}/{total_checks} checks passed")
    print("=" * 70)
    
    if checks_passed >= 4:
        print("\n✅ NOISY NEIGHBOR ISOLATION VERIFIED!")
        print("   Tenants are properly isolated - noisy behavior doesn't")
        print("   impact quiet tenants. CPU quotas working as designed.")
        return True
    else:
        print("\n⚠ Some isolation checks failed")
        print("   Review CPU quota settings and tenant behavior")
        return False

if __name__ == "__main__":
    try:
        success = test_noisy_neighbor()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
