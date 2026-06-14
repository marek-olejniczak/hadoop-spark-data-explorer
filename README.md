# Flight Delay Analysis (Apache Hadoop + PySpark)

Analysis of **US domestic flight delays and cancellations 2009-2018**
([Kaggle dataset](https://www.kaggle.com/datasets/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018),
10 CSV files, 7.2 GB, ~65M flights) processed with Apache Hadoop (HDFS + YARN)
and PySpark, browsable through an interactive Streamlit app.

The pipeline follows a **batch precompute** pattern:

1. **Download** — raw CSVs are fetched from Kaggle via `kagglehub` (public dataset, no token needed)
2. **Ingest** — CSVs are uploaded to HDFS and converted by a PySpark job (running on YARN) to
   columnar Parquet partitioned by year. Size drops from 7.2 GB to 1.3 GB (5.5x).
3. **Aggregate** — a second PySpark job scans the full Parquet dataset and computes 8 aggregates
   (2.4 MB total, ~3000x reduction of the raw data) covering **6 analysis points** plus data for
   a **carrier recommendation** feature.
4. **Serve** — the small aggregates are exported from HDFS to local Parquet files and served by a
   Streamlit application. No Spark is invoked at runtime; every interaction is instant.

The cluster consists of a **master** node (NameNode, ResourceManager, Spark driver) and a
**slave** node (DataNode, NodeManager), both running on identical Hadoop 3.3.4 inside Docker
containers.

## Architecture

```
Kaggle CSV (7.2 GB)
       │  scripts/download_dataset.py
       ▼
data/datasets/.../*.csv
       │  scripts/ingest_all.sh
       │  ├─ hdfs dfs -put → HDFS /flights/raw/{year}.csv
       │  └─ docker exec → jobs/ingest_to_parquet.py (PySpark on YARN)
       ▼
HDFS /flights/parquet/year={2009..2018}/  (1.3 GB, Parquet)
       │  jobs/compute_aggregates.py (PySpark on YARN)
       ▼
HDFS /flights/results/  (8 aggregates, ~2.4 MB)
       │  scripts/export_results.sh
       ▼
results/*.parquet  (local)
       │
       ▼
app/app.py  (Streamlit, http://localhost:8501)
```

## Docker cluster

### Network

All containers share a single bridge network so they can reach each other by hostname:

```bash
docker network create --driver=bridge hadoop
```

### Master container

Runs all four Hadoop daemons plus PySpark. Ports are exposed for web UIs.

```bash
docker run -itd --name hadoop-master --hostname hadoop-master \
  -p 9870:9870 -p 8088:8088 -p 8888:8888 -p 9864:9864 \
  --net=hadoop \
  -v "$PWD:/project" \
  adamgput/hadoop-pyspark-put:1.0
```

Daemons are started manually inside the container:

```bash
docker exec hadoop-master bash -c 'source /etc/profile && $HADOOP_HOME/sbin/start-dfs.sh'
docker exec hadoop-master bash -c 'source /etc/profile && $HADOOP_HOME/sbin/start-yarn.sh'
```

The image `adamgput/hadoop-pyspark-put:1.0` has baked-in Hadoop config that points to
`hadoop-master` (`fs.defaultFS=hdfs://hadoop-master:9000`,
`yarn.resourcemanager.hostname=hadoop-master`). This means slave containers from the same
image automatically connect to the master without any additional configuration.

### Slave containers

Two separate containers, one for HDFS storage and one for YARN compute. Both use the same
image as the master, so Hadoop versions match exactly (3.3.4) and Spark executors can find
all required classes on the slave.

```bash
# DataNode — provides HDFS storage
docker run -d --name slave1-dn --hostname slave1-dn \
  --net=hadoop \
  adamgput/hadoop-pyspark-put:1.0 hdfs datanode

# NodeManager — runs Spark executor containers
docker run -d --name slave1-nm --hostname slave1-nm \
  --net=hadoop \
  adamgput/hadoop-pyspark-put:1.0 yarn nodemanager
```

### Cluster state after startup

- HDFS: 2 live DataNodes (master + slave), replication factor 2
- YARN: 2 NodeManagers (master + slave)
- HDFS directories for the project: `/flights/raw`, `/flights/parquet`, `/flights/results`

### Memory considerations

With 16 GB of host RAM, resource flags for Spark jobs should be set conservatively:

```
--num-executors 1 --executor-cores 2 --executor-memory 1536m --driver-memory 768m
```

If NodeManagers become UNHEALTHY (disk above 90%), clean Docker build cache:

```bash
docker builder prune -f
```

## Repository layout

```
app/           Streamlit application (reads results/*.parquet only)
data/          Dataset cache (gitignored, populated by download_dataset.py)
jobs/
  ingest_to_parquet.py    PySpark: CSV -> Parquet on HDFS
  compute_aggregates.py   PySpark: compute 8 aggregates on the full dataset
scripts/
  download_dataset.py     Fetch dataset from Kaggle via kagglehub
  ingest_all.sh           Batch ingest: all years, idempotent, cleans up CSVs
  export_results.sh       Export aggregates from HDFS to local results/
results/       Exported aggregates (8 small Parquet files, ~2.4 MB)
```

## Components in detail

### jobs/ingest_to_parquet.py

Reads a single year's CSV from HDFS (`/flights/raw/{year}.csv`) with an explicit schema
(27 columns: dates, strings, ints, doubles) to avoid `inferSchema`'s extra full scan.
Drops the empty artifact column `Unnamed: 27`, then writes the DataFrame to
`/flights/parquet/year={year}` as Parquet with Hive-style partitioning. `mode("overwrite")`
makes the job idempotent.

The Parquet format compresses the data ~5.5x and allows column pruning: later aggregates
reading only 3 out of 27 columns will physically read only those columns.

### jobs/compute_aggregates.py

Reads the full partitioned Parquet dataset (the `year` column is derived from the Hive
partition directories). Computes 8 aggregates via `groupBy().agg()`, each saved as a
single file (`coalesce(1)` since results are small). Every table keeps both averages
(`avg_*`) and counts (`n_*`) so the app can correctly merge years via weighted means.

| Aggregate | Grouping | Key metrics | Analysis point |
|---|---|---|---|
| carrier_delays | carrier, year | avg dep/arr delay, n_delayed (>=15 min), n_cancelled | 1. Airline delays |
| cancellations_time | year, month, day_of_week | n_flights, n_cancelled | 2. Flight cancellations (when) |
| cancellation_causes | year, cancellation_code | n_cancelled | 2b. Cancellation causes |
| airport_taxi_out | origin, year | avg taxi_out, avg taxi_in | 3. Airport taxi-out |
| airport_delay_causes | origin, year | sum of minutes per 5 delay causes | 4. Delay causes by airport |
| distance_carriers | carrier, distance_bucket, year | n_flights, avg_arr_delay, avg_distance | 5. Short vs long haul |
| routes | origin, dest, year | n_flights, avg_arr_delay, n_delayed, n_cancelled, avg_distance | 6. Busiest routes |
| route_carrier_stats | origin, dest, carrier, year | n_flights, avg_arr_delay, n_delayed, n_cancelled | 7. Recommendation data |

A flight is considered "delayed" when `ARR_DELAY >= 15` minutes (US DOT standard). Cancelled
flights are excluded from delay statistics but counted separately under `n_cancelled`.

### scripts/ingest_all.sh

Iterates over every `*.csv` in the local dataset directory. For each year:

1. **Check** if `/flights/parquet/year={YEAR}` already exists on HDFS → skip if present
   (idempotent: re-runs continue from where they left off).
2. **Put** the CSV to HDFS (`hdfs dfs -put -f`).
3. **Run** the PySpark ingest job via `spark-submit` on YARN.
4. **Verify** the Parquet output directory exists.
5. **Clean up** the CSV from HDFS and locally (keeps peak disk usage at ~11 GB instead of ~21 GB).

### scripts/export_results.sh

Downloads `/flights/results/*` from HDFS to the local `results/` directory via the container's
`/project` mount. Because `compute_aggregates.py` uses `coalesce(1)`, each aggregate is stored
in a single-file directory; the script flattens these to `results/{name}.parquet` and fixes file ownership for the host user.

### app/app.py (Streamlit)

Seven browsable views, selected via a radio button in the sidebar, with a global year-range
slider (2009–2018):

1. **Airline delays** — bar charts of average arrival delay and % delayed per carrier,
   plus a line chart of trends over time for selected carriers.
2. **Flight cancellations** — cancellation rate by month and day of week, pie chart of
   cancellation causes (carrier, weather, NAS, security).
3. **Airport taxi-out times** — airports with the longest average taxi-out time (configurable
   threshold and top-N).
4. **Delay causes at an airport** — chosen airport's delay-minute breakdown by the five
   cause categories, with a stacked bar over time.
5. **Short vs long haul** — per-carrier distribution of flight distance buckets
   (short <500 mi, medium 500–1500 mi, long >1500 mi) and their average delays.
6. **Busiest routes** — ranked by traffic volume, coloured by punctuality, with optional
   origin-airport filter.
7. **Carrier recommendation** — for a chosen origin → destination pair, ranks carriers by
   a risk score: `w_d * P(delay>=15) + w_c * P(cancellation) + w_a * avg_delay / 30`.
   All three weights are user-adjustable via sliders; carriers with fewer than a configurable
   number of flights on the route are filtered out.

## How to run

Prerequisites: Docker, Python 3.10+ with `kagglehub`, `pip`.

```bash
# 1. Set up the Docker network and cluster
cd flight-delay-analysis
docker network create --driver=bridge hadoop 2>/dev/null

docker run -itd --name hadoop-master --hostname hadoop-master \
  -p 9870:9870 -p 8088:8088 -p 8888:8888 -p 9864:9864 \
  --net=hadoop \
  -v "$PWD:/project" \
  adamgput/hadoop-pyspark-put:1.0

docker run -d --name slave1-dn --hostname slave1-dn \
  --net=hadoop \
  adamgput/hadoop-pyspark-put:1.0 hdfs datanode

docker run -d --name slave1-nm --hostname slave1-nm \
  --net=hadoop \
  adamgput/hadoop-pyspark-put:1.0 yarn nodemanager

# 2. Start Hadoop daemons on the master
docker exec hadoop-master bash -c 'source /etc/profile && $HADOOP_HOME/sbin/start-dfs.sh'
docker exec hadoop-master bash -c 'source /etc/profile && $HADOOP_HOME/sbin/start-yarn.sh'
docker exec hadoop-master hdfs dfs -mkdir -p /flights/raw /flights/parquet /flights/results

# 3. Download the dataset
pip install kagglehub
python3 scripts/download_dataset.py

# 4. Ingest: CSV -> HDFS -> Parquet, year by year
bash scripts/ingest_all.sh

# 5. Compute aggregates on the full dataset
docker exec hadoop-master spark-submit \
  --master yarn --num-executors 1 --executor-cores 2 \
  --executor-memory 1536m --driver-memory 768m \
  /project/jobs/compute_aggregates.py

# 6. Export results and start the application
bash scripts/export_results.sh
pip install -r app/requirements.txt
streamlit run app/app.py --server.headless true
```

The application will be available at **http://localhost:8501**.

### Web UIs

| Address | Service |
|---|---|
| http://localhost:9870 | HDFS NameNode browser |
| http://localhost:8088 | YARN ResourceManager |
| http://localhost:8501 | Streamlit application |

## Notes

- Resource flags are tuned for an 8-16 GB host. On larger machines, increase
  `--num-executors`, `--executor-memory`, and `--driver-memory` accordingly.
- The `ingest_all.sh` script is **idempotent**: years with existing Parquet are skipped,
  so an interrupted run can simply be re-executed.
- If NodeManagers become UNHEALTHY (disk usage > 90%), run `docker builder prune -f`
  to free Docker build cache.
