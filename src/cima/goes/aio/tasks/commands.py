class Command(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs


class BreakCommand(Command):
    pass
