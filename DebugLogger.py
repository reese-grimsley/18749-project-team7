# DebugLogger
# Written by Reese Grimsley, Sept 6, 2021

'''
A file to provide logging functionality using the builtin logging library. 

Copied from another project (written by Reese Grimsley)

'''
import logging



# setup default logging characteristics
TOP_LEVEL_NAME = '18749-T7'
DEFAULT_LEVEL = logging.DEBUG
logging.basicConfig()
logger = logging.getLogger(TOP_LEVEL_NAME) #base logger
logger.setLevel(DEFAULT_LEVEL)
logger.handlers.clear()

#setup a handler for the console window
ch = logging.StreamHandler()
ch.setLevel(DEFAULT_LEVEL)
# setup a handler for files
fh = None
format = logging.Formatter('%(name)s:%(levelname)s::  %(message)s')
ch.setFormatter(format)


logger.addHandler(ch)
logger.propagate = False

def get_logger(module_name):
    '''
    Create a logger for another module, which uses 'TTPython' as the root level name

    :param module_name: The name of the module to use this logger; it will be included in the header of each message
    :type module_name: string
    :return: the logger to be used for printing output to the console, file, etc. File output is diabled by default
    :rtype: ``logging.Logger``

    '''
    return logging.getLogger(TOP_LEVEL_NAME + '.' + module_name)

def get_base_logger():
    '''
    Use this to get the base logger and set configurations to it. The base name is 'TTPython'
    
    If this is going to modified, it should be done very early in the import sequence.
    :return: the logger to be used for printing output to the console, file, etc. File output is diabled by default
    :rtype: ``logging.Logger``
    '''
    return logger 

def set_base_logger_level(level):
    '''
    Set the level of the base logger, which dictates what messages are displayed and which are hidden

    :param level: The logging level (nominally between 0 and 50)
    :type level: int
    :return: None
    '''
    if level == 0:
        logger.warning('Note that setting the logger level to zero automatically uses the "WARNING" level')

    logger.setLevel(level)
    for handler in logger.handlers:
        handler.setLevel(level)

def set_console_level(level):
    '''
    Set the logging level of the console handler (prints to terminal)

    :param level: The logging level (nominally between 0 and 50)
    :type level: int
    :return: None
    '''
    global ch
    ch.setLevel(level)

def setup_file_handler(path, level=logging.DEBUG):
    '''
    Configure a file handler so that logger also prints output to file. It will use the same format as the console logger
    
    This may use a different logging level than the console handler to print more or less to file

    :param path: The file path of the log file
    :type path: string
    :param level: The logging level, specifically for the file handler (nominally a value between 0 and 50)
    :type level: int
    '''

    global fh
    fh = logging.FileHandler(path)
    fh.setLevel(level)
    fh.setFormatter(format)
    logger.addHandler(fh)


def set_file_level(level):
    '''
    Set the logging level of the file handler (prints to file)


    :param level: The logging level, specifically for the file handler (nominally a value between 0 and 50)
    :type level: int
    '''
    global fh
    assert not fh is None, 'File handler is none; setup_file_handler should be run first'
    fh.setLevel(level)



if __name__ == "__main__":

 
    setup_file_handler('./test.log',level=1)
    logger.critical('critical message')
    logger.error('error message')
    logger.warning('warning message')
    logger.info('info message')
    logger.debug('debug message')
    logger.log(2, 'minimal level')



