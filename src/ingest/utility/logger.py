import logging
import sys
import traceback
from datetime import datetime

class Loggers(object):

    def __init__(self, logfilename):
        self.terminal = sys.stderr
        self.logfilename = logfilename
        self.logfile = open(self.logfilename, "a")

    def write(self, message):
        self.terminal.write(message)
        self.logfile.write(message)

    def flush(self):
        pass

    def loggings(self):

        file_handler = logging.FileHandler(filename=self.logfilename)
        stdout_handler = logging.StreamHandler(sys.stdout)
        #stderr_handler = logging.StreamHandler(sys.stderr)
        #handlers = [file_handler, stderr_handler]
        handlers = [file_handler, stdout_handler]
        logging.basicConfig(
            level=logging.DEBUG,
            format='[%(asctime)s] {%(name)s:%(lineno)d} %(levelname)s - %(message)s',
            handlers=handlers
        )
        logger = logging.getLogger('LOGGER_NAME')





"""
filename = 'tmp2.log'
filepath  = '/Users/z0030zf/PycharmProjects/file_ingestion/ingest/utility'
now = datetime.now()
datetime1 = now.strftime('%Y-%m-%dT%H_%M_%S')
log_filename = filename + datetime1 + ".log"
sys.stderr =Loggers(log_filename,filepath)
log = Loggers(log_filename,filepath)
log.loggings()
logging.error('jdsdjosdjs')
#a=100/0
"""