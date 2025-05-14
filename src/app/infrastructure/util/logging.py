# core python
import datetime
import logging
import logging.config
from logging.handlers import BaseRotatingHandler
import os
import socket
import sys

# native
from infrastructure.util.config import AppConfig
from infrastructure.util.file import prepare_dated_file_path


LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s.%(msecs)03d %(levelname)-8s: %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        'with_pid_and_thread': {
            'format': '%(asctime)s.%(msecs)03d %(levelname)-8s: [PID:%(process)-6s, Thread:%(thread)-6s] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        'no_format': {
            'format': None,
            'datefmt': None,
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        }
    },
    'loggers': {
        '': {
            'handlers' : ['console'],
            'level'    : 'INFO',
            'propagate': True
        }
    }
}


class YYYYMMDDRotatingFileHandler(BaseRotatingHandler):
    def __init__(self, base_dir, log_name, encoding=None, delay=False):
        self.base_dir = base_dir
        self.log_name = log_name
        self.current_date = datetime.date.today()
        self.baseFilename = self.get_full_log_path()  # Store the base file name
        logging.info(f'#{os.getpid()} has baseFilename: {self.baseFilename}')
        super().__init__(self.baseFilename, mode='a', encoding=encoding, delay=delay)

    def shouldRollover(self, record):
        # Check if it's a new day. If so, the rollover should happen:
        new_date = datetime.date.today()
        return new_date != self.current_date

    def doRollover(self):
        # Update current date & filename; ensure dir exists
        self.current_date = datetime.date.today()
        self.baseFilename = self.get_full_log_path()
        os.makedirs(os.path.dirname(self.baseFilename), exist_ok=True)

        # Close current stream if open
        if self.stream:
            self.stream.close()
            self.stream = None

        # Reopen the stream
        self.stream = self._open()

        # Log standard startup info
        log_startup()

    def get_full_log_path(self):
        log_dir = os.path.join(self.base_dir, self.current_date.strftime('%Y%m'), self.current_date.strftime('%d'))
        os.makedirs(log_dir, exist_ok=True)
        return os.path.join(log_dir, self.log_name)


def add_yyyymmdd_file_handler(logger, base_dir, log_file_name=None, formatter_override=None):
    """
    Add auto-rotating file handler to existing handlers

    Args:
    - logger (logger): logger to add file handler to
    - base_dir (str): Base dir, before YYYYMM\DD
    - log_file_name (str): Log file name, including extension
    - formatter_override (str): Optionally use this formatter, rather than the 'standard' formatter

    Returns: logger
    """
    
    # Default log file name = app name
    if not log_file_name:
        log_file_name = f"{os.environ['APP_NAME']}.log"

    yyyymmdd_file_handler = YYYYMMDDRotatingFileHandler(base_dir, log_file_name)

    # Use same format as default handler
    if logger.handlers:

        # Override if requested
        if formatter_override:
            formatter = logging.Formatter(
                fmt=LOGGING_CONFIG['formatters'][formatter_override]['format'], 
                datefmt=LOGGING_CONFIG['formatters'][formatter_override]['datefmt']
            )
        else:  # default
            formatter = logger.handlers[0].formatter

        yyyymmdd_file_handler.setFormatter(formatter)

    logger.addHandler(yyyymmdd_file_handler)

    logging.info(f'#{os.getpid()} successfully added YYYYMMDD file handler with formatter override {formatter_override}')
    logging.info(f'#{os.getpid()} now logging to file: {yyyymmdd_file_handler.baseFilename}')

    return logger

def setup_logging(base_dir, log_file_name=None, log_level_override=None, formatter_override=None, rotate_for_date=None):
    """
    Log to stdout and auto-rotating YYYYMM\DD file at specified log level

    Args:
    - base_dir (str): base dir (without YYYYMM\DD)
    - log_file_name (str): Optionally specify file name (without path as this will be auto-generated based on APP_NAME), including extension
    - log_level_override (str): Optionally specify logging level (CRITICAL/ERROR/WARNING/INFO/DEBUG)
    - formatter_override (str): Optionally use this formatter, rather than the 'standard' formatter
    - rotate_for_date (datetime.date): 

    Returns: None
    """

    # Default log file name = app name
    if not log_file_name:
        log_file_name = f"{os.environ['APP_NAME']}.log"

    # Initialize from dict config
    logging.config.dictConfig(LOGGING_CONFIG)
    
    # Get the root logger
    root_logger = logging.getLogger()

    # PID for usage below
    pid = os.getpid()

    if log_level_override:
        root_logger.setLevel(log_level_override)

    # Rotate existing file, if specified
    if rotate_for_date:
        prepare_dated_file_path(folder_name=base_dir, date=rotate_for_date, file_name=log_file_name)

    # Add YYYYMMDD file handler
    root_logger = add_yyyymmdd_file_handler(root_logger, base_dir, log_file_name, formatter_override)
    logging.info(f'PID #{pid} logging to file: {base_dir}\\YYYYMM\\DD\\{log_file_name}')
    
    # Log startup details
    log_startup()

    logging.info('Logging setup completed.')

def log_startup():
    # Log hostname, user, and other information
    logging.info(f'Running on {socket.gethostname()} as {os.getlogin()}')
    logging.info(f'Running cmd: {sys.executable} ' + ' '.join(sys.argv))
    logging.info(f'App config upon startup (or log file rollover): '
                    f'{os.path.abspath(AppConfig().config_file_path)}'
                    f'\n{AppConfig()}\n'
                    )

def get_log_file_full_path():
    # Loop through log handlers
    for h in logging.getLoggerClass().root.handlers:
        if isinstance(h, (logging.FileHandler, YYYYMMDDRotatingFileHandler)):
            # Found a file handler! If there is a baseFilename, return it:
            if hasattr(h, 'baseFilename'):
                return h.baseFilename

    # If we reached here, no file handler was found. Return None.
    return None

def get_log_file_name():
    log_file_full_path = get_log_file_full_path()
    if log_file_full_path:
        # Found a log file! So we can use the log file name to derive the app name (matching the log file name):
        file_name = os.path.splitext(os.path.basename(log_file_full_path))[0]
        return file_name

    # If we reached here, no log file was found. Return None.
    return None
