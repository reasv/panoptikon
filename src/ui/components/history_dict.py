from collections import OrderedDict
import copy

class HistoryDict(OrderedDict):
    def __setitem__(self, key, value):
        # If the key exists and has the same value, delete it first
        if key in self and self[key] == value:
            del self[key]
        super().__setitem__(key, value)
        
    def add(self, key, value):
        self.__setitem__(key, value)
    
    def reverse_chronological_order(self):
        # Convert to a list of items and reverse it
        return list(self.items())[::-1]
    
    def __deepcopy__(self, memo):
        # Create a new instance of this class
        new_instance = self.__class__()
        # Deepcopy the items into the new instance
        for k, v in self.items():
            new_instance[copy.deepcopy(k, memo)] = copy.deepcopy(v, memo)
        return new_instance
    
    def remove(self, key):
        # Remove the key if it exists
        if key in self:
            del self[key]
    
    def reset(self):
        # Clear the dictionary
        self.clear()
    
    def keep_latest_n(self, n):
        if n <= 0:
            self.reset()
        else:
            keys_to_keep = list(self.keys())[-n:]
            for key in list(self.keys()):
                if key not in keys_to_keep:
                    del self[key]