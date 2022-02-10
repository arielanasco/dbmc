import logging
import datetime
import os.path
date_ =datetime.datetime.now()

if not os.path.exists("logs"):
    os.mkdir("logs")
LOG_FILE = f"{date_.month}-{date_.day}-{date_.year}"
if os.path.exists(f"logs/{LOG_FILE}.log"):
    fh = logging.FileHandler(f'logs/{LOG_FILE}.log','a','utf8')
else:
    fh = logging.FileHandler(f'logs/{LOG_FILE}.log','w','utf8')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(name)10s.%(funcName)-15s %(message)-10s')
def setup_log(script):
    logger = logging.getLogger(script)
    logger.setLevel(logging.DEBUG) 
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger



