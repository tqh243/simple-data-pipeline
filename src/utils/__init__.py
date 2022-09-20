import os
import logging
import shutil
from datetime import datetime, timedelta
from functools import wraps
from fnmatch import fnmatch

import emoji
import pendulum

from src.utils import constants

current_date_format_utc = datetime.utcnow().strftime('%Y-%m-%d')

logging.basicConfig(
    format='[%(levelname)s] %(asctime)s %(name)s: %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

def log_time(logger):
    def timing(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            ret = func(*args, **kwargs)
            end_time = datetime.now()
            diff = end_time - start_time
            diff_secs = diff.total_seconds()
            diff_mins = 0
            if diff_secs > 60:
                diff_mins = diff_secs // 60
                diff_secs -= (diff_mins * 60)
            logger.info(f'Duration: {diff_mins} mins {int(diff_secs)} seconds')
            return ret

        return wrapper
    return timing

def remove_emoji(text):
    return emoji.get_emoji_regexp().sub(r'', text)

def js_parse_int(string):
    return int(''.join([x for x in string if x.isdigit()]))

def get_current_hcm_datetime():
    return pendulum.now(tz=constants.HCM_TZ)

def get_current_utc_datetime():
    return datetime.now()

def split_array_to_chunks(input_list: list, chunk_size: int):
    for i in range(0, len(input_list), chunk_size):
        yield input_list[i:i + chunk_size]

def get_date_record():
    # set date when we get the stats
    # if processing time just pass the next day then date record should be for previous day
    current_datetime = get_current_hcm_datetime()
    if current_datetime.hour == 0:
        date_record = current_datetime - timedelta(hours=2)
    else:
        date_record = current_datetime

    return date_record.strftime(constants.DATE_FORMAT)

def convert_epoch_to_timestamp(epoch_time: int):
    return datetime.fromtimestamp(epoch_time).strftime(constants.DATETIME_FORMAT)


def get_all_files_in_folder(folder_path: str, pattern: str = '*'):
    all_files = []
    for path, _, files in os.walk(folder_path):
        for name in files:
            if fnmatch(name, pattern):
                all_files.append(os.path.join(path, name))

    return all_files

def remove_empty_folder(folder_path: str, file_format: str):
    for path, _, files in os.walk(folder_path, topdown=False):
        for name in files:
            if not(name.endswith(file_format)):
                os.remove(os.path.join(path, name))

        if path == folder_path:
            break
        try:
            os.rmdir(path)
        except OSError as e:
            logging.warning(e)

def clear_local_files(folder_name):
    logging.info(f'Clear folder {folder_name}')
    if os.path.exists(folder_name):
        logging.info(f"Delete data from {folder_name}")
        try:
            shutil.rmtree(folder_name)
        except Exception as e:
            logging.error(e)
            raise Exception
    else:
        logging.info('Folder has not been created')
