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

```bash
python utils/generate_sample_data.py
```