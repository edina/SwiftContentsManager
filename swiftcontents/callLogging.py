"""
Decorator for logging all method calls of a class
"""

import logging
import sys

__all__ = ['logMethods']

def logFunctionCall(log,oFunc,clsName=None):
    def loggedFunction(*args,**kwargs):
        msg = 'calling '
        if clsName is not None:
            msg+=clsName+'.'
        msg += '%s('%oFunc.__name__
        for a in args:
            msg+=repr(a)+','
        for k in kwargs:
            msg+='%s=%s,'%(k,repr(kwargs[k]))
        if msg[-1] == ',':
            msg = msg[:-1]
        msg +=')'
        log.debug(msg)
        return oFunc(*args,**kwargs)
    return loggedFunction

def logMethods(Cls):
    class NewCls(object):
        def __init__(self,*args,**kwargs):
            self.oInstance = Cls(*args,**kwargs)
            if not hasattr( self.oInstance,'log'):
                self.oInstance.log = logging.getLogger(__name__)

        def __getattribute__(self,s):
            """called whenever a method is access"""

            try:
                x = super(NewCls,self).__getattribute__(s)
            except AttributeError:      
                pass
            else:
                return x
            x = self.oInstance.__getattribute__(s)
            # check it is an instance method
            if type(x) == type(self.__init__):
                return logFunctionCall(self.log,x,clsName=self.oInstance.__class__.__name__)
            else:
                return x
    return NewCls
            

if __name__ == '__main__':

    @logMethods
    class TestClass(object):

        def a(self,b):
            print ('hello',b)

        def add(self,a,b):
            return a+b

    @logMethods
    class TestClass2(object):
        def __init__(self,log):
            self.log = log

        def add(self,a,b):
            return a+b
       
    
    t = TestClass()

    t.a('magi')
    t.a(['a','b'])
    print (t.add(10,2))


    t2 = TestClass2(logging.getLogger('test'))
    print (t2.add(10,2))
