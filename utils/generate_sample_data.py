import argparse
import logging
import os, sys
import pandas as pd
import random
import string
from datetime import datetime, timedelta

from funcs import parse_conf, parse_defaults

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

conf_defaults = parse_defaults()

n_events_default  = conf_defaults['n_events']
n_banners_default = conf_defaults['n_banners']
n_pages_default   = conf_defaults['n_pages']
# n_batches_default = conf_defaults['n_batches']
n_users_default   = conf_defaults['n_users']

def generate_user_id_list(n_users: int = n_users_default) -> list:
    """
    In order to make the simulation more realistic, we generate a list of unique user_id
    outside the simulations to keep it consistent across simulation batches.
    
    """
    user_ids = [''.join(random.choices(string.ascii_letters + string.digits, k=10)) for _ in range(n_users)]
    return user_ids

def generate_sample_data_hourly(n_events: int = n_events_default, 
                                n_banners: int = n_banners_default,
                                n_pages: int = n_pages_default, 
                                users: list = generate_user_id_list(),
                                start_time: datetime = datetime.utcnow()):
    """Generate a batch of one hour of events

    Args:
        n_events (int, optional): _description_. Defaults to n_events_default.
        n_banners (int, optional): _description_. Defaults to n_banners_default.
        n_pages (int, optional): _description_. Defaults to n_pages_default.
        users (list, optional): _description_. Defaults to generate_user_id_list().
        start_time (datetime, optional): _description_. Defaults to datetime.utcnow().

    Returns:
        _type_: _description_
    """
    # event types:
    # - 0 is view
    # - 1 is click
    event_types = [0, 1]
    # each banner is a unique ad which can have different placements
    banner_ids = [f'banner{n}' for n in range(n_banners)]
    # placement number is constant, can't be changed
    placement_ids = [f'placement{n}' for n in range(5)]
    page_ids = [f'page{n}' for n in range(n_pages)]
    logging.debug(f"n_pages: {n_pages}")
    logging.debug(f"page_ids: {page_ids}")
    
    # simulate timestamps in a range of 1hr
    time_range = int(start_time.timestamp()), int((start_time + timedelta(minutes=59, seconds=59)).timestamp())

    data = [{
        'timestamp': random.choice(range(time_range[0], time_range[1])),
        'event_type': random.choice(event_types),
        'banner_id': random.choice(banner_ids),
        'placement_id': random.choice(placement_ids),
        'page_id': random.choice(page_ids),
        'user_id': random.choice(users)
    } for _ in range(n_events)]
    
    return data

if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser(
                    description='Generate a daily equivalent (24 batches) of hourly website traffic data.')

    parser.add_argument('-c', '--conf-file',
                        help="YAML configuration file. Please note: if other options are provided, they will overseed the ones provided by the YAML file.")
    parser.add_argument('-e', '--n-events',
                        help=f"Number of events to be generated (default: {n_events_default})")
    parser.add_argument('-b', '--n-banners',
                        help=f"Number of banner_ids to be generated (default: {n_banners_default})")
    parser.add_argument('-p', '--n-pages',
                        help=f"Number of page_ids to be generated (default: {n_pages_default})")
    parser.add_argument('-u', '--n-users',
                        help=f"Number of user_ids to be generated (default: {n_users_default})")
        
    args = parser.parse_args()

    kws = ['n_events', 'n_banners', 'n_pages', 'n_users']
    if args.conf_file:
        conf = parse_conf(args.conf_file)
        logging.debug("Custom conf file parsed")
    else:
        conf = parse_defaults()
        logging.debug("Default conf file parsed")
    # if any command line option is provided, it will overseed the value
    # provided in the conf file
    arg_dict = vars(args)
    logging.debug(f"arg_dict: {arg_dict}")
    for var in arg_dict:
        if arg_dict[var]:
            conf[var] = arg_dict[var]
    print(conf)    
    
    # Generate appropriate out folder
    out_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data/sample_data")
    os.makedirs(out_folder, exist_ok=True)

    start_time = datetime.utcnow() \
                         .replace(hour=0, minute=0, second=0)
    logging.info(f"start_time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    # sys.exit()

    n_users = args.n_users if args.n_users else n_users_default

    logging.info(f"Website traffic sample data generation params:") 
    logging.info(f"n_events={conf['n_events']}")
    logging.info(f"n_banners={conf['n_banners']}")
    logging.info(f"n_pages={conf['n_pages']}")
    logging.info(f"n_users={conf['n_users']}")

    users_id_list = generate_user_id_list()
    for i in range(24):
        logging.info(f"Generating batch n. {i+1} starting from timestamp {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        sample_data = generate_sample_data_hourly(n_events=int(conf['n_events']),
                                                  n_pages=int(conf['n_pages']),
                                                  n_banners=int(conf['n_banners']),
                                                  users=users_id_list,
                                                  start_time=start_time)
        sample_data = pd.DataFrame(sample_data)
        # sample_data_outfile = f"{start_time.strftime('%Y-%m-%d_%H:%M:%S')}_sample_data_{str(i+1).zfill(2)}.csv"
        sample_data_outfile = f"{start_time.strftime('%Y-%m-%d_%H-%M-%S')}_sample_data.csv"
        sample_data.to_csv(os.path.join(out_folder, f'{sample_data_outfile}'), index=False)
        logging.info(f'Exported file {sample_data_outfile}')
        start_time += timedelta(hours=1)