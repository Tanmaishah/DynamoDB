from typing import Dict, Optional
import copy

class VectorClock:
    """
    Vector clock implementation for tracking causality and detecting conflicts.
    
    Example:
        VC1 = {A: 1, B: 2}  # Node A updated once, Node B twice
        VC2 = {A: 2, B: 2}  # Node A updated again
        VC1 < VC2  # True (VC1 happened before VC2)
        
        VC3 = {A: 1, C: 1}  # Conflict with VC1
        VC1 || VC3  # Concurrent (conflict)
    """
    
    def __init__(self, clock: Optional[Dict[str, int]] = None):
        self.clock = clock if clock else {}
    
    def increment(self, node_id: str):
        """Increment the counter for the given node."""
        self.clock[node_id] = self.clock.get(node_id, 0) + 1
    
    def update(self, other: 'VectorClock'):
        """Merge with another vector clock (take max of each counter)."""
        for node_id, count in other.clock.items():
            self.clock[node_id] = max(self.clock.get(node_id, 0), count)
    
    def is_before(self, other: 'VectorClock') -> bool:
        """
        Check if self happened before other (self < other).
        True if all counters in self <= other AND at least one is strictly less.
        """
        if not self.clock and other.clock:
            return True
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
        """
        return not self.is_before(other) and not other.is_before(self)
    
    def dominates(self, other: 'VectorClock') -> bool:
        """Check if self happened after other (other < self)."""
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
    
    def __eq__(self, other: 'VectorClock') -> bool:
        """Check if two vector clocks are equal."""
        if not isinstance(other, VectorClock):
            return False
        return self.clock == other.clock
    
    def __repr__(self) -> str:
        return f"VectorClock({self.clock})"
    
    def __str__(self) -> str:
        return str(self.clock)


class VersionedValue:
    """
    A value with its vector clock for versioning.
    """
    
    def __init__(self, key: str, value: str, vector_clock: VectorClock):
        self.key = key
        self.value = value
        self.vector_clock = vector_clock
    
    def __repr__(self) -> str:
        return f"VersionedValue(key={self.key}, value={self.value}, vc={self.vector_clock})"


def resolve_conflicts(values: list[VersionedValue]) -> list[VersionedValue]:
    """
    Given multiple versions of a value, resolve conflicts.
    Returns a list of non-dominated versions (concurrent versions if conflict exists).
    
    Strategy:
    1. Deduplicate identical versions (same value + same vector clock)
    2. Remove dominated versions
    3. Return remaining versions
    
    Args:
        values: List of versioned values for the same key
        
    Returns:
        List of non-dominated, unique versions
    """
    if not values:
        return []
    
    if len(values) == 1:
        return values
    
    # Step 1: Deduplicate - remove exact duplicates (same value AND same vector clock)
    unique_versions = []
    seen = set()
    
    for val in values:
        # Create a hashable key from value and vector clock
        vc_tuple = tuple(sorted(val.vector_clock.clock.items()))
        version_key = (val.value, vc_tuple)
        
        if version_key not in seen:
            seen.add(version_key)
            unique_versions.append(val)
    
    if len(unique_versions) == 1:
        return unique_versions
    
    # Step 2: Remove dominated versions
    result = []
    for i, val1 in enumerate(unique_versions):
        is_dominated = False
        for j, val2 in enumerate(unique_versions):
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
