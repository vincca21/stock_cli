# logging.py

"""
Logging system set up for the project. Basic logging system using 3 levels of logging: DEBUG, INFO, ERROR.
    - DEBUG: Detailed information, used in code sections prone to error or where there is a need to debug
    - INFO: General information, logs general info for markers of start, end/completion/triggering of different sections, etc
    - ERROR: Logs errors, used to log errors that occur in the code
Logs will show shortened time format, level of logging, location of logging, and the message.
logs will use subtle colors and formatting (spacing/indentation) to make it easier to read and understand.
"""

import logging
import sys

from colorama import (  # Fore allows for specific colors & Style allows for specific styles (bold, underline, etc)
    Fore,
    Style,
)

# Set up logging

# Def log levels & characteristics for each lvl
LOG_COLORS = {
    'DEBUG': Fore.CYAN,
    'INFO': Fore.GREEN,
    'ERROR': Fore.RED
}

LOG_STYLES = {
    'DEBUG': Style.BRIGHT,
    'INFO': Style.NORMAL,
    'ERROR': Style.BRIGHT
}

LOG_INDENT = {
    'DEBUG': '  ',
    'INFO': '',
    'ERROR': '    '
}

# create new formatter class (CustomFormatter) that inherits from logging.Formatter
class CustomFormatter(logging.Formatter):
    """
    CustomFormatter class -> apply color & formatting to console logs
    File logs unchanged for readability
    """
    
    # override the format method
    def format(self, record):
        log_color = LOG_COLORS.get(record.levelno, "")      # get color for log level
        log_style = LOG_STYLES.get(record.levelno, "")      # get style for log level
        log_indent = LOG_INDENT.get(record.levelno, "")     # get indent for log level
        
        # set log message format 
        log_msg = f"{log_color}{log_style}{log_indent}{record.asctime} - {record.levelname} - {record.filename} - {record.message}{Style.RESET_ALL}"

        return log_msg
    
    
# create logger
def setup_logger(log_file='logs.log'):
    """
    Set up logger with custom formatter
    """
    
    # create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # cap all log lvl
    
    #format
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(message)s')
    
    #file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(CustomFormatter())
    logger.addHandler(console_handler)
    
    return logger

# access point for logger 
def get_logger():
    return logging.getLogger(__name__)

#Init
logger = setup_logger()


"""
How to use logger in different parts of the code:
import logger from logs/logging.py

"""
