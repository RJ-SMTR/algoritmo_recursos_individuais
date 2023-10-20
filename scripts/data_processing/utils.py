import logging
from datetime import timedelta, datetime

### --- 1 - Config do arquivo de log ---###
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f"./log/log_{current_time}.txt"
logging.basicConfig(
    filename=log_filename, 
    encoding='utf-8', 
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # This format excludes milliseconds
)

### --- Função de log --- ###
def log_info(message: str):
    logging.debug(message)
    print(message)