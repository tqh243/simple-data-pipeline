import os
import logging
from datetime import datetime, timedelta
from functools import wraps

import emoji
import pendulum

from src.utils import constants

current_date_format_utc = datetime.utcnow().strftime('%Y-%m-%d')

def init_log(pipeline_name: str):
    logging.basicConfig(
        format='[%(levelname)s] %(asctime)s %(name)s: %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG
    )
    log_folder = os.path.join(constants.LOG_BASE_FOLDER, pipeline_name, current_date_format_utc)
    os.makedirs(log_folder, exist_ok=True)

    logger = logging.getLogger(pipeline_name)
    logger.setLevel(logging.DEBUG)
    log_filename = datetime.utcnow().hour

    # log file handler
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s %(name)s: %(message)s')
    file_handler = logging.FileHandler(f'{log_folder}/{log_filename}.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger


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
