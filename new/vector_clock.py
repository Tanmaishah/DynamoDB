"""
Vector Clock Implementation for DynamoDB

A vector clock tracks causality between events in a distributed system.
Each node has a counter that increments when it performs a write.

Example:
    VC1 = {A: 1, B: 2}  # Node A wrote once, Node B wrote twice
    VC2 = {A: 2, B: 2}  # Node A wrote again
    
    VC1 < VC2  # VC1 happened before VC2
    
    VC3 = {A: 1, C: 1}  # Node C wrote
    VC1 || VC3  # VC1 and VC3 are concurrent (conflict!)
"""

from typing import Dict, List, Optional
import copy


class VectorClock:
    """Vector clock for tracking causality."""
    
    def __init__(self, clock: Optional[Dict[str, int]] = None):
        """
        Initialize vector clock.
        
        Args:
            clock: Dictionary mapping node_id -> counter
        """
        self.clock = clock if clock else {}
    
    def increment(self, node_id: str):
        """
        Increment the counter for a specific node.
        Called when that node performs a write.
        
        Args:
            node_id: ID of the node performing the write
        """
        self.clock[node_id] = self.clock.get(node_id, 0) + 1
    
    def update(self, other: 'VectorClock'):
        """
        Merge with another vector clock.
        Takes the maximum counter for each node.
        
        Used when receiving a vector clock from another node.
        
        Args:
            other: Another vector clock to merge with
        """
        for node_id, count in other.clock.items():
            self.clock[node_id] = max(self.clock.get(node_id, 0), count)
    
    def is_before(self, other: 'VectorClock') -> bool:
        """
        Check if self happened before other (self < other).
        
        True if:
        - All counters in self <= counters in other
        - At least one counter in self < counter in other
        
        Args:
            other: Vector clock to compare with
            
        Returns:
            True if self happened before other
        """
        if not self.clock and other.clock:
            return True  # Empty clock is before any non-empty clock
        
        if not other.clock:
            return False
        
        all_nodes = set(self.clock.keys()) | set(other.clock.keys())
        at_least_one_less = False
        
        for node in all_nodes:
            self_count = self.clock.get(node, 0)
            other_count = other.clock.get(node, 0)
            
            if self_count > other_count:
                return False  # Not happened before
            if self_count < other_count:
                at_least_one_less = True
        
        return at_least_one_less
    
    def is_concurrent(self, other: 'VectorClock') -> bool:
        """
        Check if self and other are concurrent (conflicting).
        
        Returns True if neither self < other nor other < self.
        This means the writes happened concurrently.
        
        Args:
            other: Vector clock to compare with
            
        Returns:
            True if clocks are concurrent (conflict exists)
        """
        return not self.is_before(other) and not other.is_before(self)
    
    def dominates(self, other: 'VectorClock') -> bool:
        """
        Check if self happened after other (other < self).
        
        Args:
            other: Vector clock to compare with
            
        Returns:
            True if self dominates other
        """
        return other.is_before(self)
    
    def copy(self) -> 'VectorClock':
        """Return a deep copy of this vector clock."""
        return VectorClock(copy.deepcopy(self.clock))
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary for serialization."""
        return self.clock.copy()
    
    @classmethod
    def from_dict(cls, clock_dict: Dict[str, int]) -> 'VectorClock':
        """Create from dictionary (deserialization)."""
        return cls(clock_dict.copy())
    
    def __eq__(self, other) -> bool:
        """Check if two vector clocks are equal."""
        if not isinstance(other, VectorClock):
            return False
        return self.clock == other.clock
    
    def __repr__(self) -> str:
        return f"VectorClock({self.clock})"
    
    def __str__(self) -> str:
        return str(self.clock)


class VersionedValue:
    """A value with its vector clock."""
    
    def __init__(self, key: str, value: str, vector_clock: VectorClock):
        self.key = key
        self.value = value
        self.vector_clock = vector_clock
    
    def __repr__(self) -> str:
        return f"VersionedValue(key={self.key}, value={self.value}, vc={self.vector_clock})"


def resolve_conflicts(values: List[VersionedValue]) -> List[VersionedValue]:
    """
    Given multiple versions of a value, resolve conflicts.
    
    Strategy:
    1. Remove versions that are dominated by others (old versions)
    2. Keep all concurrent versions (true conflicts)
    
    Args:
        values: List of versioned values for the same key
        
    Returns:
        List of non-dominated versions (may be 1 if no conflict, or multiple if conflict)
    """
    if not values:
        return []
    
    if len(values) == 1:
        return values
    
    # Remove dominated versions
    result = []
    for i, val1 in enumerate(values):
        is_dominated = False
        for j, val2 in enumerate(values):
            if i != j and val2.vector_clock.dominates(val1.vector_clock):
                # val2 happened after val1, so val1 is old
                is_dominated = True
                break
        
        if not is_dominated:
            result.append(val1)
    
    return result


# ============================================
# Testing
# ============================================

if __name__ == "__main__":
    print("="*60)
    print("VECTOR CLOCK TESTS")
    print("="*60)
    
    # Test 1: Basic increment
    print("\n1. Basic increment:")
    vc1 = VectorClock()
    vc1.increment("A")
    vc1.increment("B")
    print(f"   VC1 after A and B increment: {vc1}")
    
    # Test 2: Happened-before relationship
    print("\n2. Happened-before:")
    vc2 = VectorClock({"A": 2, "B": 2})
    print(f"   VC1: {vc1}")
    print(f"   VC2: {vc2}")
    print(f"   VC1 < VC2? {vc1.is_before(vc2)}")  # True
    print(f"   VC2 < VC1? {vc2.is_before(vc1)}")  # False
    
    # Test 3: Concurrent (conflict)
    print("\n3. Concurrent writes (conflict):")
    vc3 = VectorClock({"A": 1, "C": 1})
    print(f"   VC1: {vc1}")
    print(f"   VC3: {vc3}")
    print(f"   VC1 concurrent with VC3? {vc1.is_concurrent(vc3)}")  # True
    
    # Test 4: Merge
    print("\n4. Merging vector clocks:")
    vc4 = VectorClock({"A": 5, "B": 1})
    vc5 = VectorClock({"A": 3, "B": 7, "C": 2})
    print(f"   Before: VC4={vc4}, VC5={vc5}")
    vc4.update(vc5)
    print(f"   After merge: VC4={vc4}")  # {A: 5, B: 7, C: 2}
    
    # Test 5: Conflict resolution
    print("\n5. Conflict resolution:")
    val1 = VersionedValue("key1", "value_A", VectorClock({"A": 1, "B": 0}))
    val2 = VersionedValue("key1", "value_B", VectorClock({"A": 0, "B": 1}))
    val3 = VersionedValue("key1", "value_latest", VectorClock({"A": 2, "B": 2}))
    
    conflicts = [val1, val2, val3]
    resolved = resolve_conflicts(conflicts)
    print(f"   Given: {len(conflicts)} versions")
    print(f"   Resolved to: {len(resolved)} version(s)")
    for v in resolved:
        print(f"      {v.value} with {v.vector_clock}")
    
    # Test 6: True conflict (concurrent)
    print("\n6. True conflict (concurrent):")
    conflicts2 = [val1, val2]
    resolved2 = resolve_conflicts(conflicts2)
    print(f"   Given: {len(conflicts2)} concurrent versions")
    print(f"   Resolved to: {len(resolved2)} version(s) (both kept!)")
    for v in resolved2:
        print(f"      {v.value} with {v.vector_clock}")
    
    print("\n" + "="*60)