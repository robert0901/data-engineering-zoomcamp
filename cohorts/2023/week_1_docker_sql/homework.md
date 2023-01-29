## Week 1 Homework

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- `--imageid string`
- `--iidfile string`
- `--idimage string`
- `--idfile string`
## Solution
``` 
docker build --help
--iidfile string Write the image ID to the file
```
## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- 1
- 6
<mark>-3<mark>
- 7
## Solution
```
$ docker run -it --entrypoint=bash python:3.9

root@1cca9b9858d4:/# pip list
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4

```
# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

## Solution
## Reused Ingestion File
```
import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        file_name = 'output.csv.gz'
    elif url.endswith('.parquet'):
        file_name = 'output.parquet'
    else:
        file_name = 'output.csv'

    os.system(f"wget {url} -O {file_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    if url.endswith('.parquet'):
        df = pd.read_parquet(file_name)
        file_name = 'output.csv.gz'
        df.to_csv(file_name, index=False, compression="gzip")

    df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime  = pd.to_datetime(df.lpep_pickup_datetime )
    df.lpep_dropoff_datetime  = pd.to_datetime(df.lpep_dropoff_datetime )

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True:

        try:
            t_start = time()

            df = next(df_iter)

            df.lpep_pickup_datetime  = pd.to_datetime(df.lpep_pickup_datetime )
            df.lpep_dropoff_datetime  = pd.to_datetime(df.lpep_dropoff_datetime )

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)

```
## Dockerized Ingestion Script
```
$ URL ='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz'
$ python3 ingest_data.py \>     --user=root \
>     --password=root \
>     --host=localhost \
>     --port=5433 \
>     --db=ny_taxi \
>     --table_name=green_taxi_trips \
>     --url=${URL}

```


## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 20689
<mark>-20530<mark>
- 17630
- 21090

## Solution
```
/*How many trips on 2019-01=15? */
select count(*) from green_taxi_trips WHERE
lpep_pickup_datetime >='2019-01-15 00:00:00'::timestamp and 
  lpep_dropoff_datetime < '2019-01-16 00:00:00'::timestamp
```


## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- 2019-01-18
- 2019-01-28
<mark>- 2019-01-15<mark>
- 2019-01-10
## Solution
```
 SELECT date(lpep_pickup_datetime) as pick_up_date, max(trip_distance) as max_trip_distance
FROM   green_taxi_trips
group by 1
order by 2 desc
```

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
<mark>- 2: 1282 ; 3: 254<mark>
- 2: 1282 ; 3: 274

## Solution
```
 /*In 2019-01-01 how many trips had 2 and 3 passengers? */

SELECT sum(case when passenger_count = 2 then 1 else 0 end) as Two_Passenger,
	sum(case when passenger_count = 3 then 1 else 0 end) as Three_Passenger

FROM   green_taxi_trips where date(lpep_pickup_datetime) = '2019-01-01'
```
 ## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- South Ozone Park
<mark>- Long Island City/Queens Plaza<mark>
 
 ```
 /*Largest tip */
SELECT 
zpu."Zone" as Pick_Up_Zone,
zdo."Zone" Drop_Off_Zone,
t.tip_amount
FROM
  green_taxi_trips t,
  zones zpu,
  zones zdo
WHERE
  t."PULocationID" = zpu."LocationID" AND
  t."DOLocationID" = zdo."LocationID" and 
zpu."Zone" = 'Astoria'
order by 3 desc limit 1


  t."PULocationID" = zpu."LocationID" AND
  t."DOLocationID" = zdo."LocationID" limit 10
 ```
 


## Submitting the solutions

* Form for submitting: [form](https://forms.gle/EjphSkR1b3nsdojv7)
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 30 January (Monday), 22:00 CET


## Solution

We will publish the solution here
