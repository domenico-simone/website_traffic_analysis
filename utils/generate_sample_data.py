import argparse
import logging
import os
import pandas as pd
import random
import string
from datetime import datetime, timedelta

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)

n_events_default  = 100000
n_banners_default = 10
n_pages_default   = 20
n_batches_default = 10

def generate_sample_data_hourly(n_events: int = n_events_default, 
                                n_banners: int = n_banners_default,
                                n_pages: int = n_pages_default, 
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

    # simulate n users so that n = num_events*0.90
    # so we can simulate users with >1 event
    users = [''.join(random.choices(string.ascii_letters + string.digits, k=10)) for _ in range(int(n_events*0.90))]
    data = {
        'Timestamp': [random.choice(range(time_range[0], time_range[1])) for _ in range(n_events)],
        'Event_type': [random.choice(event_types) for _ in range(n_events)],
        'Banner_id': [random.choice(banner_ids) for _ in range(n_events)],
        'Placement_id': [random.choice(placement_ids) for _ in range(n_events)],
        'Page_id': [random.choice(page_ids) for _ in range(n_events)],
        'User_id': [random.choice(users) for _ in range(n_events)]
    }
    
    return pd.DataFrame(data)

if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser(
                    # prog='Ad sample data generator',
                    description='Generate a table with 1hr of ad sample data.')

    parser.add_argument('-e', '--n-events', default=n_events_default,
                        help="Number of events to be generated (%(default)s)")
    parser.add_argument('-b', '--n-banners', default=n_banners_default,
                        help="Number of banners to be generated (%(default)s)")
    parser.add_argument('-p', '--n-pages', default=n_pages_default,
                        help="Number of pages to be generated (%(default)s)")
    parser.add_argument('-t', '--n-batches', default=n_batches_default,
                        help="Number of data batches to be generated (%(default)s)")
        
    args = parser.parse_args()
    # print(args.filename, args.count, args.verbose)
    
    # Generate appropriate out folder
    out_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data/sample_data")
    os.makedirs(out_folder, exist_ok=True)

    # Generate 10 hourly batches of data
    start_time = datetime.utcnow()

    if args.n_batches:
        n_batches = args.n_batches
    else:
        n_batches = n_batches_default

    logging.info(f"Ad sample data generation params:") 
    logging.info(f"n_events={args.n_events}")
    logging.info(f"n_banners={args.n_banners}")
    logging.info(f"n_pages={args.n_pages}")
    logging.info(f"n_batches={n_batches}")

    for i in range(n_batches):
        logging.debug(f"Generating batch n. {i+1} starting from timestamp {start_time}")
        sample_data = generate_sample_data_hourly(n_events=200000,
                                                  start_time=start_time)
        sample_data.to_csv(os.path.join(out_folder, f'sample_data_0{i+1}.csv'), index=False)
        logging.debug(f'Exported file sample_data_{str(i+1).zfill(2)}.csv')
        start_time += timedelta(hours=1)