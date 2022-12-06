from abc import ABCMeta, abstractmethod

#creates an AbstractTask class that requires the implementation of run. method and returns an error if not implemented
class AbstractTask(metaclass = ABCMeta): 
    @abstractmethod
    def run(self):
        raise NotImplementedError('No run method defined!!!')
    
class SimpleTask (AbstractTask): #concrete class, that gets instanciated by the callable and provides implementation details for the run method. 
    def __init__(self, func: callable):
       self.func = func

    def run(self, *args, **kwargs): 
        return self.func(*args, **kwargs)