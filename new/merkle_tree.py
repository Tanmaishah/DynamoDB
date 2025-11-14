import hashlib
from typing import List, Tuple, Optional
from vector_clock import VersionedValue


def hash_data(data: str) -> str:
    """Hash data using SHA256."""
    return hashlib.sha256(data.encode()).hexdigest()


class MerkleTreeNode:
    """A node in the Merkle tree."""
    
    def __init__(self, hash_value: str, left=None, right=None, is_leaf=False):
        self.hash = hash_value
        self.left = left
        self.right = right
        self.is_leaf = is_leaf
    
    def __eq__(self, other):
        if not isinstance(other, MerkleTreeNode):
            return False
        return self.hash == other.hash


class MerkleTree:
    """
    Merkle tree for detecting differences between replicas.
    
    Structure:
    - Leaf nodes: Hash of key-value-vclock data in a bucket
    - Internal nodes: Hash of concatenated child hashes
    - Root: Single hash representing all data
    """
    
    def __init__(self, num_buckets: int = 256):
        """
        Initialize Merkle tree.
        
        Args:
            num_buckets: Number of leaf buckets (must be power of 2)
        """
        self.num_buckets = num_buckets
        self.root = None
        self.buckets = [[] for _ in range(num_buckets)]  # List of keys per bucket
        self.leaf_hashes = [None] * num_buckets  # Hash for each bucket
    
    def get_bucket_index(self, key: str) -> int:
        """Map a key to a bucket index."""
        key_hash = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return key_hash % self.num_buckets
    
    def build(self, versioned_values: List[VersionedValue]):
        """
        Build Merkle tree from list of versioned values.
        
        Args:
            versioned_values: List of VersionedValue objects to include in tree
        """
        # Reset buckets
        self.buckets = [[] for _ in range(self.num_buckets)]
        
        # Assign each key-value to a bucket
        for vv in versioned_values:
            bucket_idx = self.get_bucket_index(vv.key)
            self.buckets[bucket_idx].append(vv)
        
        # Build leaf hashes (hash of all data in each bucket)
        self.leaf_hashes = []
        for bucket in self.buckets:
            if bucket:
                # Sort by key for deterministic hashing
                bucket.sort(key=lambda x: x.key)
                # Hash all values in bucket together
                bucket_data = ""
                for vv in bucket:
                    # Include key, value, and vector clock in hash
                    vc_str = str(sorted(vv.vector_clock.clock.items()))
                    bucket_data += f"{vv.key}:{vv.value}:{vc_str};"
                leaf_hash = hash_data(bucket_data)
            else:
                # Empty bucket
                leaf_hash = hash_data("")
            
            self.leaf_hashes.append(leaf_hash)
        
        # Build tree bottom-up from leaf hashes
        self.root = self._build_tree_recursive(self.leaf_hashes)
    
    def _build_tree_recursive(self, hashes: List[str]) -> MerkleTreeNode:
        """
        Recursively build Merkle tree from list of hashes.
        
        Args:
            hashes: List of hashes at current level
            
        Returns:
            Root node of (sub)tree
        """
        if len(hashes) == 0:
            return MerkleTreeNode(hash_data(""), is_leaf=True)
        
        if len(hashes) == 1:
            return MerkleTreeNode(hashes[0], is_leaf=True)
        
        # Split into left and right halves
        mid = len(hashes) // 2
        left_hashes = hashes[:mid]
        right_hashes = hashes[mid:]
        
        # Recursively build left and right subtrees
        left_node = self._build_tree_recursive(left_hashes)
        right_node = self._build_tree_recursive(right_hashes)
        
        # Parent hash = hash(left_hash + right_hash)
        parent_hash = hash_data(left_node.hash + right_node.hash)
        
        return MerkleTreeNode(parent_hash, left=left_node, right=right_node, is_leaf=False)
    
    def get_root_hash(self) -> str:
        """Get root hash of the tree."""
        if self.root:
            return self.root.hash
        return hash_data("")
    
    def find_differing_buckets(self, other: 'MerkleTree') -> List[int]:
        """
        Find bucket indices where this tree differs from another tree.
        
        Args:
            other: Another MerkleTree to compare with
            
        Returns:
            List of bucket indices that differ
        """
        if not self.root or not other.root:
            return []
        
        # Quick check: if roots are same, no differences
        if self.root.hash == other.root.hash:
            return []
        
        # Recursively find differing buckets
        return self._find_differing_buckets_recursive(
            self.root, 
            other.root,
            0,
            self.num_buckets
        )
    
    def _find_differing_buckets_recursive(
        self,
        node1: MerkleTreeNode,
        node2: MerkleTreeNode,
        start_bucket: int,
        end_bucket: int
    ) -> List[int]:
        """
        Recursively find differing leaf buckets by drilling down the tree.
        
        Args:
            node1: Node in this tree
            node2: Node in other tree
            start_bucket: Starting bucket index for this subtree
            end_bucket: Ending bucket index for this subtree
            
        Returns:
            List of bucket indices that differ
        """
        # If hashes match, no differences in this subtree
        if node1.hash == node2.hash:
            return []
        
        # If we're at a leaf level, this bucket differs
        if node1.is_leaf or node2.is_leaf:
            # Return all bucket indices in this range
            return list(range(start_bucket, end_bucket))
        
        # Drill down to children
        differing = []
        mid = (start_bucket + end_bucket) // 2
        
        # Check left subtree
        if node1.left and node2.left:
            differing.extend(
                self._find_differing_buckets_recursive(
                    node1.left,
                    node2.left,
                    start_bucket,
                    mid
                )
            )
        
        # Check right subtree
        if node1.right and node2.right:
            differing.extend(
                self._find_differing_buckets_recursive(
                    node1.right,
                    node2.right,
                    mid,
                    end_bucket
                )
            )
        
        return differing
    
    def get_keys_in_bucket(self, bucket_idx: int) -> List[VersionedValue]:
        """Get all versioned values in a specific bucket."""
        if 0 <= bucket_idx < self.num_buckets:
            return self.buckets[bucket_idx]
        return []
    
    def __repr__(self):
        num_keys = sum(len(bucket) for bucket in self.buckets)
        return f"MerkleTree(buckets={self.num_buckets}, keys={num_keys}, root_hash={self.get_root_hash()[:8]}...)"