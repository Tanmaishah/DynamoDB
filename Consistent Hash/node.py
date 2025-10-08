class Node:
    def __init__(self,node_id,capacity=100):
        self.node_id=node_id
        self.capacity=capacity
        self.storage={}
    
    def put(self,key,value):
        self.storage[key]=value
        return True
    
    def get(self,key):
        return self.storage.get(key)
    
    def hash_key(self,key):
        return key in self.storage
    
    def remove(self,key):
        if key in self.storage:
            del self.storage[key]
            return True
        return False

    
    def get_all_keys(self):
        return list(self.storage.keys())
    def __repr__(self):
        return f"Node({self.node_id}, capacity={self.capacity})"
