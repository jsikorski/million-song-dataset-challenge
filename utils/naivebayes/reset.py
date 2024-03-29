from utils.naivebayes.mode import Mode
from utils.naivebayes.status import Status
from utils.naivebayes.db import Db

class Reset(Mode):
    def validate(self, args):
        if len(args) != 2:
            raise ValueError('Usage: %s reset' % args[0])

    def execute(self):
        Db().reset()
        Status().execute()

    def output(self, _):
        print 'Reset Complete'