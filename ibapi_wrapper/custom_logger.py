import logging

from datetime import datetime
import logging.handlers
import os

global_level = logging.DEBUG

# Create the global logger
def setup_custom_logger(global_name, filename, logdirname = './logs',  file_level = None, console_level = None, console = False):
  # Check if logdir exists
  now = datetime.now().strftime('%Y%m%dT%H%M%S')
  logdirname = os.path.join(logdirname, now)
  if not os.path.exists(logdirname):
    # Create it if not there 
      os.makedirs(logdirname)

  
  if not file_level:
    file_level = global_level
  if not console_level:
    console_level = global_level

  logger = logging.getLogger(global_name)
  logger.setLevel(global_level)
  
  logfilename = os.path.join(logdirname, now + "." + filename)

  # Set up a log line format
  log_fmt  = '%(asctime)s.%(msecs)03d - %(levelname)-7s - %(threadName)s - %(filename)s:%(lineno)d [%(funcName)s] - %(message)s'
  date_fmt = '%Y-%m-%d %H:%M:%S'
  # Create formatter
  formatter = logging.Formatter(fmt=log_fmt, datefmt=date_fmt)

  # logging.basicConfig(level=global_level, format=log_fmt, datefmt=date_fmt)

  # Create file handler
  fh = logging.handlers.RotatingFileHandler(
        logfilename,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=50,
  )
  fh.setLevel(file_level)
  fh.setFormatter(formatter)
  logger.addHandler(fh)

  if console:
    # Create console handler and set debug_level
    ch = logging.StreamHandler()
    ch.setLevel(console_level)

 
    # Add formatter to ch
    ch.setFormatter(formatter)
    
    # Add ch to logger
    logger.addHandler(ch)

  logger.info(f"Logging to {logfilename}")

  return logger
