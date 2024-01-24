import logging
import os
import random
import yaml
from logging import Logger
from typing import Union

def parse_defaults() -> dict:
    conf = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), 'defaults.yaml'), 'r').read())
    return conf

def parse_conf(conf_file: str = None) -> dict:
    """Parse conf from a YAML custom file, otherwise gets conf values
    from a default file.

    Args:
        conf_file (str, optional): Configuration file (YAML).

    Returns:
        dict: configuration values
    """
    default_conf = parse_defaults()
    custom_conf = yaml.safe_load(open(conf_file, 'r').read())
    final_conf = {}
    for k in default_conf:
        if k in custom_conf:
            final_conf[k] = custom_conf[k]
        else:
            final_conf[k] = default_conf[k]
    return final_conf

def set_logging(log_file: str = "data/logs/ad_stats_processing.log", 
                overwrite_file_handler: bool = False) -> [Union[Logger, None], Union[Logger, None]]:
    """Set up logging.

    Two logging streams will be set up:
    - console_logger printing log messages to the console
    - db_logger, saving log messages to a jsonl logfile which can be ingested into an appropriate database.

    Args:
        log_file (str, optional): jsonl file to save files to. Defaults to data/logs/ad_stats_processing.log.
        overwrite_file_handler (bool, optional): should the log_file be overwritten? Defaults to False.

    Returns:
        [Union[Logger, None], Union[Logger, None]]: _description_
    """
    # set logging
    log_folder = os.path.dirname(log_file)
    try:
    # set up the logger for printing to the screen (console)
        console_logger = logging.getLogger('console_logger')
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(funcName)s - %(message)s")
        console_handler.setFormatter(console_formatter)
        console_logger.addHandler(console_handler)
        console_logger.setLevel(logging.INFO)

        # set up the logger for producing logs to a database
        # create log folder if it does not exist
        os.makedirs(log_folder, exist_ok=True)
        db_logger = logging.getLogger('db_logger')
        logfile_mode = 'w' if overwrite_file_handler else 'a'
        db_handler = logging.FileHandler(log_file, mode=logfile_mode)
        db_logger.addHandler(db_handler)
        db_logger.setLevel(logging.INFO)

        console_logger.info(f"Logging setup complete, logfile: {log_file}")
        return console_logger, db_logger
    except:
        logging.error("Logging setup failed!")
        return None, None

def generate_random_null_value(value_list, none_percentage):
    """_summary_

    # Example usage
    defined_list = [1, 2, 3, 4, 5]
    none_percentage = 10

    # Generate 10 values to demonstrate the distribution
    none_c = 0
    for _ in range(100):
        result = generate_random_null_value(defined_list, none_percentage)
        if not result:
            none_c += 1

    :param value_list: _description_
    :type value_list: _type_
    :param none_percentage: _description_
    :type none_percentage: _type_
    :return: _description_
    :rtype: _type_
    """
    if random.random() < none_percentage / 100:
        return None
    else:
        return random.choice(value_list)
