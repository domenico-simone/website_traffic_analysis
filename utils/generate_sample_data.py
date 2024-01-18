import os
import pandas as pd
import random
import string
from datetime import datetime, timedelta

def generate_sample_data_hourly(n_events: int = 100000, n_banners: int = 10,
                                n_pages: int = 20, start_time: datetime = datetime.utcnow()):
    # event types:
    # - 0 is view
    # - 1 is click
    event_types = [0, 1]
    # each banner is a unique ad which can have different placements
    banner_ids = [f'banner{n}' for n in range(n_banners)]
    # placement number is constant, can't be changed
    placement_ids = ['placement1', 'placement2', 'placement3']
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

# Generate appropriate out folder
out_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data/sample_data")
os.makedirs(out_folder, exist_ok=True)

# Generate 10 hourly batches of data
start_time = datetime.utcnow()

for i in range(10):
    print(f"Generating batch n. {i+1} starting from timestamp {start_time}")
    sample_data = generate_sample_data_hourly(n_events=200000, start_time=start_time)
    sample_data.to_csv(os.path.join(out_folder, f'sample_data_0{i+1}.csv'), index=False)
    print(f'Exported file sample_data_{str(i+1).zfill(2)}.csv')
    start_time += timedelta(hours=1)