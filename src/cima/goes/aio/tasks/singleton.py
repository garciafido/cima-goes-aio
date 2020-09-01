from multiprocessing import Lock


class SingletonType(type):
    def __new__(mcs, name, bases, attrs):
        # Assume the target class is created (i.e. this method to be called) in the main thread.
        cls = super(SingletonType, mcs).__new__(mcs, name, bases, attrs)
        cls.__shared_instance_lock__ = Lock()
        return cls

    def __call__(cls, *args, **kwargs):
        with cls.__shared_instance_lock__:
            try:
                return cls.__shared_instance__
            except AttributeError:
                cls.__shared_instance__ = super(SingletonType, cls).__call__(*args, **kwargs)
                return cls.__shared_instance__
