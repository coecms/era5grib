class NonExistentKey():
    pass

class NoData():
    pass

class NoChildren():
    pass

class ConfTree():
    ### Constructors
    def __init__(self,name: str='root',data=NoData,children=NoChildren,none_is_default=True):
        self.name = name
        if data is not NoData and children is not NoChildren:
            raise Exception("A ConfTree cannot have both data and children")
        self.data = data
        if children is NoChildren:
            self.children = {}
        else:
            self.children = children
        if none_is_default:
            self._default=None
        else:
            self._default=NonExistentKey

    @classmethod
    def from_dict(cls,d,name='root',none_is_default=True):
        out=cls(name)
        out.children = {}
        out.data = NoData
        for k,v in d.items():
            if isinstance(v,dict):
                out.children[k]=ConfTree.from_dict(v,k,none_is_default=none_is_default)
            else:
                out.children[k]=cls(k,v,none_is_default=none_is_default)
        return out

    ### Reset methods
    def prune(self):
        ### Delete all children
        self.children={}

    def reset_data(self):
        self.data = NoData

    ### Getters
    def __getitem__(self,k):
        out = self.get(k)
        if out == self._default:
            raise KeyError(k)
        return out
    
    def to_dict(self):
        out = {}
        if self.children:
            for k,v in self.children.items():
                out[k] = v.to_dict()
            return out
        else:
            return self.data

    def get(self,k=None,default=NonExistentKey):
        if k is None:
            if self.data is not NoData:
                return self.data
            else:
                return self.to_dict()
        k_arr = k.split('.',maxsplit=1)
        if k_arr[0] in self.children:
            if len(k_arr)==1:
                return self.children[k_arr[0]].get(default=default)
            else:
                return self.children[k_arr[0]].get(k_arr[1],default)
        else:
            if default is NonExistentKey:
                return self._default
            else:
                return default

    ### Setters
    def set_data(self,value):
        self.data = value
        self.prune()

    def set(self,key,value):
        k_arr = key.split('.',maxsplit=1)
        if k_arr[0] in self.children:
            if len(k_arr)==1:
                if isinstance(value,dict):
                    self.children[k_arr[0]] = ConfTree.from_dict(value)
                else:
                    self.children[k_arr[0]].set_data(value)
            else:
                self.reset_data()
                self.children[k_arr[0]].set(k_arr[1],value)
        else:
            self.reset_data()
            if len(k_arr)==1:
                if isinstance(value,dict):
                    self.children[k_arr[0]] = ConfTree.from_dict(value)
                else:
                    self.children[k_arr[0]] = ConfTree(k_arr[0])
                    self.children[k_arr[0]].set_data(value)
            else:
                self.children[k_arr[0]] = ConfTree(k_arr[0])
                self.children[k_arr[0]].set(k_arr[1],value)

    def __setitem__(self,key,value):
        self.set(key,value)

    ### Updaters
    def update_data(self,value):
        ### Update is set with extra rules
        if self.children:
            raise Exception("Attempted to update data on a ConfTree with children")
        if self.data is NoData:
            self.set_data(value)
        else:
            if type(value) != type(self.data):
                raise Exception(f"Attempted to update data with conflicting type: Expected {type(self.data)}, Got {type(value)}")
            #elif isinstance(self.data,list):
            #    self.data = list(dict.fromkeys(self.data + value))
            else:
                self.set_data(value)

    def update(self,key,value):
        ### Update is set with extra rules
        k_arr = key.split('.',maxsplit=1)
        if k_arr[0] in self.children:
            if len(k_arr)==1:
                if isinstance(value,dict):
                    if self.children[k_arr[0]].data is not None:
                        raise Exception("Attempted to update children on ConfTree with data")
                    if isinstance(self.children[k_arr[0]],ConfTree):
                        self.children[k_arr[0]].merge(value)
                    else:
                        self.children[k_arr[0]] = ConfTree.from_dict(value)
                else:
                    self.children[k_arr[0]].update_data(value)
            else:
                self.children[k_arr[0]].update(k_arr[1],value)
        else:
            if self.data is not NoData:
                raise Exception("Attempted to update children on ConfTree with data")
            if len(k_arr)==1:
                if isinstance(value,dict):
                    self.children[k_arr[0]] = ConfTree.from_dict(value)
                else:
                    self.children[k_arr[0]] = ConfTree(k_arr[0])
                    self.children[k_arr[0]].update_data(value)
            else:
                self.children[k_arr[0]] = ConfTree(k_arr[0])
                self.children[k_arr[0]].update(k_arr[1],value)

    ### Traversal
    def get_all_keys(self):
        out = []
        if self.children:
            for k,v in self.children.items():
                out.append(k)
                if v.children:
                    out.extend( [ f'{k}.{i}' for i in v.get_all_keys() ] )
        return out

    ### Merge
    def merge(self,other):
        if isinstance(other,dict):
            other = ConfTree.from_dict(other)
        elif not isinstance(other,ConfTree):
            raise TypeError("Can only merge with a dict or another ConfTree")
        for k in other.get_all_keys():
            self.update(k,other.get(k))
