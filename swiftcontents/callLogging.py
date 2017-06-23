"""
Decorator for logging all method calls of a class
"""

import logging
import sys

__all__ = ['LogMethod','LogMethodResults']

class LogMethod(object):
    def __init__(self,log=None,logResult=False):
        self.log = logging.getLogger('CallLog')
        self.logResult = logResult

    def __call__(self,oFunc):
        def loggedFunction(*args,**kwargs):
            fName = args[0].__class__.__name__ + '.' + oFunc.__name__

            if hasattr(args[0],'log'):
                log = args[0].log
            else:
                log = self.log
            
            msg = 'calling %s('%fName
            for a in args[1:]:
                msg+=repr(a)+','
            for k in kwargs:
                msg+='%s=%s,'%(k,repr(kwargs[k]))
            if msg[-1] == ',':
                msg = msg[:-1]
            msg +=')'
            log.debug(msg)

            results = oFunc(*args,**kwargs)

            if self.logResult:
                log.debug('%s returned: %s',fName,repr(results))

            return results
        
        return loggedFunction                            

class LogMethodResults(LogMethod):
    def __init__(self,log=None):
        super(LogMethodResults,self).__init__(log=log,logResult=True)
    
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    class TestClass(object):

        @LogMethod()
        def a(self,b):
            print ('hello',b)

        @LogMethodResults()
        def add(self,a,b):
            return a+b

    class TestClass2(object):
        def __init__(self,log):
            self.log = log

        @LogMethod()
        def add(self,a,b):
            return a+b
       
    
    t = TestClass()

    t.a('magi')
    t.a(['a','b'])
    print (t.add(10,2))


    t2 = TestClass2(logging.getLogger('test'))
    print (t2.add(10,2))
