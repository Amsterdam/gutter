""" Simple class to give back errors

"""

class GutterStoreError:
    
    def __init__(self, msg=None, status_code=None):
        
        self.msg = msg
        self.status_code = status_code
        
    # ----
        
    # TODO  