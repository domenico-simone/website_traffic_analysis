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

#### Batch processing

We assume we are receiving the events in hourly batches. Therefore, the job can be set up in this way:
- For every batch, the hourly stats will be computed and shown
- Every 24 batches, hourly and daily stats will be shown


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

Generation of sample data is performed with the script `utils/generate_sample_data.py`. The following parameters can be tweaked: 

```
usage: generate_sample_data.py [-h] [-c CONF_FILE] [-e N_EVENTS] [-b N_BANNERS] [-p N_PAGES] [-u N_USERS]

Generate a daily equivalent (24 batches) of hourly website traffic data.

options:
  -h, --help            show this help message and exit
  -c CONF_FILE, --conf-file CONF_FILE
                        YAML configuration file. Please note: if other options are provided, they will overseed the ones provided by the YAML file.
  -e N_EVENTS, --n-events N_EVENTS
                        Number of events to be generated (default: 100000)
  -b N_BANNERS, --n-banners N_BANNERS
                        Number of banners to be generated (default: 10)
  -p N_PAGES, --n-pages N_PAGES
                        Number of pages to be generated (default: 100)
  -u N_USERS, --n-users N_USERS
                        Number of user_ids to be generated (default: 50000)
```

Number of placements cannot be provided as option since it is set by design (n=5).

```bash
python utils/generate_sample_data.py
```

### Compute hourly statistics

This script will compute hourly stats for a single file (by default, `data/sample_data/sample_data_01.csv`)

```
python hourly_stats.py
```

## Installation

Create conda env with python==3.10 and activate environment

```bash
conda create -n website_traffic_analysis python=3.10
conda activate website_traffic_analysis
```

Install pip requirements

```bash
pip install -r requirements.txt
```

Install Apache Airflow

```bash
# Set desired Airflow version
AIRFLOW_VERSION=2.8.1

# Extract the version of Python installed in the conda environment to get the constraints file
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

For testing purposes, we can launch Airflow as standalone

```bash
# set HOME dir for Airflow
mkdir -p airflow
export AIRFLOW_HOME=`pwd`/airflow

airflow standalone
```