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

## Installation

If you already have a working Java installation, you can skip to the [next section](#install-in-venv) to install the requirements in a venv

### Install in conda env

```bash
# create conda env with python==3.10 and activate environment
conda create -n website_traffic_analysis python=3.10
conda activate website_traffic_analysis
```

If you don't have java installed on your machine, please install it within the created conda env:

```bash
conda install -c conda-forge openjdk
```

Install requirements

```bash
pip install -r requirements.txt
```

### Install in venv

Install pip requirements

```bash
# create venv and install requirements
python -m venv website_traffic_analysis
source website_traffic_analysis/bin/activate

pip install -r requirements.txt
```

## Run the services

### Generate sample data

Generation of sample data is performed with the script `utils/generate_sample_data.py`. Parameters for this script can be set in two ways:

- through a configuration file (in YAML format: please refer to the `conf.yaml` file in the root directory for an example) provided with the `-c` (`--conf-file`) option
- through the command line options (which will overseed the conf file provided with `-c`), as follows: 

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

Please note that number of placements can not be provided as option since it is set by design (n=5).

```bash
python utils/generate_sample_data.py
```

#### About user_ids

The user_id list is computed **before** running the event generation in order to keep users consistent across generated batches. 

### Compute hourly statistics

This job script will compute hourly stats for a single file (by default, `data/sample_data/sample_data_01.csv`)

```
python hourly_stats.py
```

### Compute daily statistics

This job script will compute daily stats, using the 24 batch files available in the folder `data/sample_data`.

## Event processing: architecture design & features

### Job set up and scheduling

The infrastructure has been designed in the scenario of receiving hourly batches of data. The system is scheduled to receive a new file at each round hour, with the following naming template: `YYYY-MM-DD_hh-00-00_sample_data.csv` (for testing purposes, the files are located in the folder `data/sample_data`). Therefore, the architecture for processing these batch files should schedule two jobs:

- a **hourly** processing to get hourly statistics. Scheduling of this job should find a tradeoff between providing the statistics in a timely fashion (_ie_, as soon as the batch file is _supposed_ to be collected) and ensuring the batch file is actually available, since there might be delays in the batch file delivery. An Airflow DAG could be triggered 5 seconds after the beginning of the hour, with a maximum of 10 retries scheduled every 15 seconds.

- a **daily** processing to get daily statistics. Scheduling of this job should find the above mentioned tradeoff, with the additional consideration that the job is expected to find 24 batch files to be processed as once. Triggering of this job should be dependent upon this check, therefore we could implement a retry strategy of 10 retries scheduled every 15 seconds, starting from 5-10 seconds after midnight. However, the job is scheduled to run in any case, with the logging of the number of expected missing files. 

Upon successful completion of the the daily processing, the related batch files could be removed, based on storage needs.

### Data processing

#### Settings

The core data processing (filtering/aggregation) is implemented in PySpark, in order to leverage on the reliability and scalability of this framework. Special attention is required in the allocation of computational resources. Preliminary tests performed on a laptop (**provide features**) with (**provide spark resource allocation**) have shown that a scenario of 1M events/hr distributed across 50K users / 10 banners / 20 pages is handled by the system quite seamlessly. **Talk about partitioning etc**

#### Results

The dataframe output by the hourly processing has the following fields:

- **placement_id** or **page_id**, depending on the chosen field to aggregate by
- **start_time** in UTC format
- **end_time** in UTC format
- **number of hourly views** for a placement_id or page_id
- **number of hourly clicks** for a placement_id or page_id
- **number of distinct users** for a placement_id or page_id.

The dataframe output by the daily processing has the following fields:

- **placement_id** or **page_id**, depending on the chosen field to aggregate by
- **date**
- **number of daily views** for a placement_id or page_id
- **number of daily clicks** for a placement_id or page_id
- **number of distinct users** for a placement_id or page_id.

#### Logs

Logs are an essential feature of the data ingestion pipeline. Two loggers have been configured:
 - **console_logger** that prints messages on the screen (Pyspark console messages will be printed on screen as well)
 - **db_logger** that stores logging messages in jsonl format, then appends the logged events to a logfile (`data/logs/ad_stats_processing.log`). To this purpose, a custom Python class (`DbLogger`) has been developed, with the following attributes:
   - **status** reporting task status (_eg_ `SUCCESS`, `FAILED`) or progress (`INFO`, `WARNING`);
   - **message** _eg_ `Daily report for 2024-01-24: DONE`
   - **timestamp**: the timestamp (UTC) of the _logged event_
   - **batch_type**: the aggregation level of the job (`daily` or `hourly`)
   - **datetime**: the timestamp (UTC) of the batch whose processing generated the logging message, rounded to the beginning of the reported hour for hourly processing (_eg_ `2024-01-24T18:55:29` becomes `2024-01-24T18:00:00`) or to the beginning of the reported day for daily processing (_eg_ `2024-01-24T18:55:29` becomes `2024-01-24T00:00:00`).

More attributes could be added. Regardless, the jsonl file can be used to produce the logs to a dedicated database.

Features to be implemented:

**Data QC.** Some events might harbor missing values for certain fields due to some upstream issue. If the missing fields are `user_id` and/or the field for which we are collecting statistics (`placement_id` or `page_id` depending on the task), such events should be filtered out, with a report of event survival rate _eg_ in the stats table (TODO).

**Enforce a schema when reading the batch file.** This has been implemented for both jobs. In case the validation of incoming data raises an error, this will be  

**Export computed stats to a db**, _e.g._ MySQL or Redis.

**Write logs to a db**:
  - schema errors
  - missing files (for daily statistics)

In the code included in this repository, these logs are written to a log file. This log file could be used as source for writing logs to an external db, although the logs could also be written during the execution of the scripts.

**Scheduling**

In the examples provided in this repository, a daily equivalent of hourly batches (n=24) is simulated in a single run [stg about file naming]. In a real life scenario, one batch file per hour will be provided. These batch files should be processed with the logic implemented in the `hourly_stats.py` script as soon as they are received, while the logic implemented in the script `daily_stats.py` should be scheduled to run every 24 hours. If the whole procedure succeeds, the whole daily dataset could be deleted for storage purposes.

### Real-time streaming processing

Although code for real-time streaming processing is not provided here, it is worth mentioning that this is a quite common scenario for the streaming of website traffic data, therefore 