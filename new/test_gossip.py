#!/usr/bin/env python3
"""
Test script for gossip protocol.
This will start 3 nodes and show how they discover each other via gossip.
"""

import sys
import time
import subprocess
import signal

def test_gossip():
    """Test gossip by starting multiple nodes."""

    print("="*70)
    print("GOSSIP PROTOCOL TEST")
    print("="*70)
    print("\nThis test will:")
    print("1. Start Node A on port 5000 (seed node)")
    print("2. Start Node B on port 5001 (joins via A)")
    print("3. Start Node C on port 5002 (joins via A)")
    print("4. Node C will discover Node B via gossip (not direct join)")
    print("5. After 10 seconds, kill Node B and observe failure detection")
    print("\nPress Ctrl+C to stop all nodes\n")

    processes = []

    try:
        # Start Node A (seed)
        print("Starting Node A (seed)...")
        p1 = subprocess.Popen(
            ['python', 'Node.py', 'A', '100', '5000'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        processes.append(('A', p1))
        time.sleep(2)

        # Start Node B (joins A)
        print("Starting Node B (joins A)...")
        p2 = subprocess.Popen(
            ['python', 'Node.py', 'B', '200', '5001', 'localhost:5000'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        processes.append(('B', p2))
        time.sleep(2)

        # Start Node C (joins A, should discover B via gossip)
        print("Starting Node C (joins A)...")
        p3 = subprocess.Popen(
            ['python', 'Node.py', 'C', '150', '5002', 'localhost:5000'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        processes.append(('C', p3))

        print("\n" + "="*70)
        print("All nodes started. Watch for gossip messages...")
        print("="*70 + "\n")

        # Let gossip run for a bit
        time.sleep(10)

        # Kill Node B to test failure detection
        print("\n" + "="*70)
        print("KILLING NODE B - Watch for failure detection...")
        print("="*70 + "\n")
        p2.terminate()
        p2.wait()

        # Wait for failure detection
        time.sleep(15)

        print("\n" + "="*70)
        print("Test complete! Check above for gossip and failure messages.")
        print("="*70)

    except KeyboardInterrupt:
        print("\n\nStopping all nodes...")

    finally:
        # Clean up
        for name, p in processes:
            try:
                p.terminate()
                p.wait(timeout=2)
            except:
                p.kill()
        print("All nodes stopped.")


if __name__ == "__main__":
    test_gossip()
