import argparse
import logging
import os
import pandas as pd
import random
import string
from datetime import datetime, timedelta

from .funcs import parse_conf

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# conf = parse_conf()

n_events_default  = 100000
n_banners_default = 10
n_pages_default   = 20
n_batches_default = 10
n_users_default   = 20000

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
    # event types:
    # - 0 is view
    # - 1 is click
    event_types = [0, 1]
    # each banner is a unique ad which can have different placements
    banner_ids = [f'banner{n}' for n in range(n_banners)]
    # placement number is constant, can't be changed
    placement_ids = [f'placement{n}' for n in range(5)]
    page_ids = [f'page{n}' for n in range(n_pages)]
    
    # simulate timestamps in a range of 1hr
    time_range = int(start_time.timestamp()), int((start_time + timedelta(hours=1)).timestamp())

    data = [{
        'timestamp': random.choice(range(time_range[0], time_range[1])),
        'event_type': random.choice(event_types),
        'banner_id': random.choice(banner_ids),
        'placement_id': random.choice(placement_ids),
        'page_id': random.choice(page_ids),
        'user_id': random.choice(users)
    } for _ in range(n_events)]
    
    # return pd.DataFrame(data)
    return data

if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser(
                    description='Generate one or more batches (tables) with ad sample data spanning one hour.')

    parser.add_argument('-e', '--n-events', default=n_events_default,
                        help="Number of events to be generated (%(default)s)")
    parser.add_argument('-b', '--n-banners', default=n_banners_default,
                        help="Number of banners to be generated (%(default)s)")
    parser.add_argument('-p', '--n-pages', default=n_pages_default,
                        help="Number of pages to be generated (%(default)s)")
    parser.add_argument('-t', '--n-batches', default=n_batches_default,
                        help="Number of data batches (tables) to be generated (%(default)s)")
    parser.add_argument('-u', '--n-users', default=n_users_default,
                        help="Number of user_ids to be generated (%(default)s)")
        
    args = parser.parse_args()
    
    # Generate appropriate out folder
    out_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data/sample_data")
    os.makedirs(out_folder, exist_ok=True)

    # Generate 10 batches of data spanning one hour each
    start_time = datetime.utcnow()
    n_batches = args.n_batches if args.n_batches else n_batches_default
    n_users = args.n_users if args.n_users else n_users_default

    logging.info(f"Ad sample data generation params:") 
    logging.info(f"n_events={args.n_events}")
    logging.info(f"n_banners={args.n_banners}")
    logging.info(f"n_pages={args.n_pages}")
    logging.info(f"n_batches={n_batches}")
    logging.info(f"n_users={n_users}")

    for i in range(n_batches):
        logging.debug(f"Generating batch n. {i+1} starting from timestamp {start_time}")
        sample_data = generate_sample_data_hourly(n_events=200000,
                                                  start_time=start_time)
        sample_data_outfile = f'sample_data_{str(i+1).zfill(2)}.csv'
        sample_data.to_csv(os.path.join(out_folder, f'{sample_data_outfile}'), index=False)
        logging.debug(f'Exported file {sample_data_outfile}')
        start_time += timedelta(hours=1)