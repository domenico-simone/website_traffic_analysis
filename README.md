# Website ad stats

## Overview

### Business request

_As per the task description_, with the final goal of getting stats about how efficient the ads are in terms of banner (_ie_ the product advertised) and placement in the page.

### Goals

- Set up a service that will receive and process data related to views and clicks of ads.  
- Compute hourly and daily statistics for each placement_id (number of views and clicks of each placement_id).
- Count distinct users which viewed or clicked on a placement.
- Compute the same statistics above (number of views, clicks and distinct users) for each page.

## Technical overview

### Execution plan

_Describe about proposed system architecture. This could include an architecture diagram and description._

The service could be implemented with one of the following architectures:

- use batch processing
- use real-time processing
- use both (lambda architecture)



## Other

The service to be developed should be able to handle a huge amount of data. The business requirements involved in this architecture could

- use a batch system
- use a stream messaging architecture
- use both (lambda architecture)
- consider using a scheduler

## Service requirements

## Technical requirements

## Technical design

Ad clicks message processing: architecture design

## Run the services

### Generate sample data

Please run the script `utils/generate_sample_data.py` which has the following options:

```
usage: generate_sample_data.py [-h] [-e N_EVENTS] [-b N_BANNERS] [-p N_PAGES] [-t N_BATCHES] [-u N_USERS]

Generate one or more batches (tables) with ad sample data spanning one hour.

options:
  -h, --help            show this help message and exit
  -e N_EVENTS, --n-events N_EVENTS
                        Number of events to be generated (100000)
  -b N_BANNERS, --n-banners N_BANNERS
                        Number of banners to be generated (10)
  -p N_PAGES, --n-pages N_PAGES
                        Number of pages to be generated (20)
  -t N_BATCHES, --n-batches N_BATCHES
                        Number of data batches (tables) to be generated (10)
  -u N_USERS, --n-users N_USERS
                        Number of user_ids to be generated (20000)
```

setting placements is not provided as option since the number is set by design (n=5).

```bash
python utils/generate_sample_data.py
```