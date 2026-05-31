# Sharding Gains

**Horizontal Scaling Efficiency with PostgreSQL, Docker Compose, and a Python Coordinator**

Sharding Gains is a distributed database benchmark project that demonstrates how horizontal sharding improves aggregation query performance, and how simple replica fallback keeps the benchmark running when a primary shard is manually stopped.

The system benchmarks this query over a synthetic `User_Logs` dataset:

```sql
SELECT user_id, COUNT(*)
FROM user_logs
GROUP BY user_id;
```

The same dataset is evaluated across three layouts:

| Scenario | Logical shards | Purpose |
|---:|---:|---|
| 1 shard | 1 | Baseline execution time |
| 2 shards | 2 | Medium horizontal split |
| 4 shards | 4 | Maximum split in this project |

The final report shows:

- Run time for each benchmark attempt
- Median time
- Speedup
- Efficiency
- Counted logs
- Completeness
- Which node served each shard: primary, replica, unavailable, or unused

## Table Of Contents

- [System Design](#system-design)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Command Reference](#command-reference)
- [Benchmark Output](#benchmark-output)
- [Failure Demonstrations](#failure-demonstrations)
- [Result Files](#result-files)
- [How The Benchmark Works](#how-the-benchmark-works)
- [Troubleshooting](#troubleshooting)
- [Clean Reset](#clean-reset)

## System Design

The project uses a Python coordinator and eight PostgreSQL containers.

```text
Python Coordinator
  |-- Dataset Generator
  |-- Data Loader
  |-- Shard Router
  |-- Benchmark Runner
  |-- Query-time Fallback Handler
  |-- Result Merger
  `-- Terminal Reporter

PostgreSQL Containers
  |-- shard1_primary
  |-- shard1_replica
  |-- shard2_primary
  |-- shard2_replica
  |-- shard3_primary
  |-- shard3_replica
  |-- shard4_primary
  `-- shard4_replica
```

Each logical shard has one primary container and one replica container:

| Logical shard | Primary container | Replica container | Primary port | Replica port |
|---|---|---|---:|---:|
| Shard 1 | `shard1_primary` | `shard1_replica` | `5433` | `5443` |
| Shard 2 | `shard2_primary` | `shard2_replica` | `5434` | `5444` |
| Shard 3 | `shard3_primary` | `shard3_replica` | `5435` | `5445` |
| Shard 4 | `shard4_primary` | `shard4_replica` | `5436` | `5446` |

All containers use the same database credentials:

```text
Database: userlogs
User:     benchmark
Password: benchmark
```

Replication is implemented at application level during data loading. When the loader writes a shard partition to a primary node, it writes the exact same partition to the matching replica. PostgreSQL streaming replication is intentionally not required for this benchmark because the dataset is static and the workload is read-only.

## Project Structure

```text
.
|-- docker-compose.yml
|-- README.md
|-- requirements.txt
|
|-- coordinator/
|   |-- main.py
|   |-- config.py
|   |-- dataset_generator.py
|   |-- loader.py
|   |-- router.py
|   |-- benchmark.py
|   |-- db.py
|   |-- merger.py
|   `-- reporter.py
|
|-- db/
|   `-- init.sql
|
|-- data/
|   `-- user_logs.csv
|
`-- results/
    |-- benchmark_results.csv
    `-- benchmark_results.json
```

## Prerequisites

Install these tools before running the project:

- Docker Desktop
- Docker Compose
- Python 3.10 or newer
- `pip`

Check your environment:

```bash
docker --version
docker compose version
python --version
pip --version
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Quick Start

Run the complete benchmark from a fresh checkout:

```bash
docker compose up -d --build
python -m coordinator.main generate --rows 1000000
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark
```

Expected normal result:

```text
Completeness = 100%
Scenario 4 shards: S1 = P, S2 = P, S3 = P, S4 = P
```

After the first full setup, you do not need to rebuild or reload data just to benchmark again. Run:

```bash
python -m coordinator.main benchmark
```

Run only the 4-shard scenario:

```bash
python -m coordinator.main benchmark --nodes 4
```

Run each scenario 5 times instead of 3:

```bash
python -m coordinator.main benchmark --runs 5
```

## Command Reference

### 1. Start PostgreSQL Containers

```bash
docker compose up -d --build
```

Use this when:

- Running the project for the first time
- `docker-compose.yml` has changed
- Containers do not exist yet

For later runs, if containers already exist, this is usually enough:

```bash
docker compose up -d
```

### 2. Generate Dataset

```bash
python -m coordinator.main generate --rows 1000000
```

This creates:

```text
data/user_logs.csv
```

The dataset contains:

- `1,000,000` rows by default
- `100,000` possible users
- Random actions such as `login`, `logout`, `search`, `checkout`
- Deterministic random seed for repeatable benchmark input

If the file already exists, it is reused. To regenerate it:

```bash
python -m coordinator.main generate --rows 1000000 --force
```

### 3. Initialize Database Tables

```bash
python -m coordinator.main init-db
```

This creates the benchmark tables on every primary and replica container:

```text
user_logs_n1
user_logs_n2
user_logs_n4
```

### 4. Load Data

```bash
python -m coordinator.main load
```

This loads all benchmark layouts:

- `user_logs_n1` for the 1-shard scenario
- `user_logs_n2` for the 2-shard scenario
- `user_logs_n4` for the 4-shard scenario

The loader writes both primary and replica nodes, so the replica can serve the same data if the primary is later stopped manually.

### 5. Run Benchmark

Run all scenarios:

```bash
python -m coordinator.main benchmark
```

Run one scenario:

```bash
python -m coordinator.main benchmark --nodes 4
```

Set custom run count:

```bash
python -m coordinator.main benchmark --runs 5
```

## Benchmark Output

The benchmark prints one unified table for normal runs and failure runs.

Example:

```text
================ SHARDING BENCHMARK ================

Query: SELECT user_id, COUNT(*) FROM user_logs GROUP BY user_id
Dataset: 1,000,000 User_Logs
Runs per scenario: 3
Representative time: Median

+---------+-----------------------+----------+-----------+--------------+-----------+----------------+------+------+------+------+
|   Nodes | Run times (seconds)   |   Median | Speedup   | Efficiency   |   Counted | Completeness   | S1   | S2   | S3   | S4   |
+=========+=======================+==========+===========+==============+===========+================+======+======+======+======+
|       1 | [6.12, 6.03, 6.18]    |     6.12 | 1.00      | 1.00         |   1000000 | 100%           | P    | -    | -    | -    |
|       2 | [3.41, 3.36, 3.48]    |     3.41 | 1.79      | 0.89         |   1000000 | 100%           | P    | P    | -    | -    |
|       4 | [1.95, 1.88, 1.92]    |     1.92 | 3.19      | 0.80         |   1000000 | 100%           | P    | P    | P    | P    |
+---------+-----------------------+----------+-----------+--------------+-----------+----------------+------+------+------+------+
```

Column meanings:

| Column | Meaning |
|---|---|
| `Nodes` | Number of logical shards in the scenario |
| `Run times` | Wall-clock times for each run |
| `Median` | Representative time for the scenario |
| `Speedup` | `T1 / Tn` |
| `Efficiency` | `Speedup / number_of_shards` |
| `Counted` | Total logs counted after merging shard results |
| `Completeness` | `Counted / 1,000,000 * 100%` |
| `S1` to `S4` | Source used for each logical shard |

Shard source symbols:

| Symbol | Meaning |
|---|---|
| `P` | Data was read from primary |
| `R` | Primary was unavailable, data was read from replica |
| blank | Both primary and replica were unavailable |
| `-` | Shard is not used in this scenario |

## Failure Demonstrations

The project is designed for manual failure testing. It does not automatically stop containers. You stop and start nodes yourself with Docker commands, then rerun the benchmark.

### Case 1: Normal Benchmark

Start from a complete setup:

```bash
docker compose up -d --build
python -m coordinator.main generate --rows 1000000
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark
```

Expected behavior:

```text
Completeness = 100%
All active shards use P
```

For the 4-shard scenario:

```text
S1 = P
S2 = P
S3 = P
S4 = P
```

### Case 2: Stop One Primary Shard

Stop `shard2_primary` manually:

```bash
docker stop shard2_primary
```

Run the 4-shard benchmark again:

```bash
python -m coordinator.main benchmark --nodes 4
```

Expected behavior:

```text
S2 = R
Completeness = 100%
```

Explanation: `shard2_primary` is unavailable, but `shard2_replica` still has the same data, so the benchmark remains complete.

### Case 3: Stop Both Primary And Replica Of One Shard

If `shard2_primary` is already stopped, stop its replica too:

```bash
docker stop shard2_replica
```

Run the benchmark:

```bash
python -m coordinator.main benchmark --nodes 4
```

Expected behavior:

```text
S2 is blank
Completeness is around 75%
Warning about partial result is printed
```

Explanation: the coordinator cannot read logical shard 2 from either primary or replica. It still queries shards 1, 3, and 4, merges the available results, and prints a partial benchmark instead of crashing.

### Case 4: Start The Failed Shard Again

Bring the stopped containers back:

```bash
docker start shard2_primary
docker start shard2_replica
```

Run benchmark again:

```bash
python -m coordinator.main benchmark --nodes 4
```

Expected behavior:

```text
S2 = P
Completeness = 100%
```

### Case 5: Stop A Different Shard

You can repeat the same experiment with any shard:

```bash
docker stop shard3_primary
python -m coordinator.main benchmark --nodes 4
```

Expected:

```text
S3 = R
Completeness = 100%
```

Then stop its replica:

```bash
docker stop shard3_replica
python -m coordinator.main benchmark --nodes 4
```

Expected:

```text
S3 is blank
Completeness is reduced
Partial result warning is printed
```

Start it again:

```bash
docker start shard3_primary
docker start shard3_replica
```

## Result Files

Every benchmark run saves results to:

```text
results/benchmark_results.csv
results/benchmark_results.json
```

The result files include:

```text
nodes
run_times_seconds
median_time_seconds
speedup
efficiency
counted_logs
expected_logs
completeness_percent
s1_source
s2_source
s3_source
s4_source
notes
```

Normal and failure benchmark results are stored in the same files. There is no separate failure dashboard or failure result table.

## How The Benchmark Works

### Data Generation

The generator creates `data/user_logs.csv` with:

```text
id
user_id
action
created_at
```

The `id` values are sequential. `user_id`, `action`, and `created_at` are generated with a fixed random seed so repeated dataset generation is reproducible.

### Horizontal Fragmentation

The router uses:

```text
shard_id = user_id % number_of_shards
```

For `1 shard`, all rows go to shard 1.

For `2 shards`, rows are split across shard 1 and shard 2.

For `4 shards`, rows are split across shard 1, shard 2, shard 3, and shard 4.

### Parallel Query Execution

For each benchmark scenario, the coordinator sends the aggregation query to active logical shards in parallel. This is essential because the goal is to measure horizontal scaling behavior, not sequential query execution.

### Query-time Fallback

There is no separate health check phase. The coordinator handles failures only when it actually queries a shard:

```text
1. Try primary.
2. If primary succeeds, use P.
3. If primary fails or times out, try replica.
4. If replica succeeds, use R.
5. If both fail, return an empty result for that shard.
6. Continue querying all other shards.
```

This keeps the benchmark resilient while preserving the manual nature of the failure demo.

### Result Merging

Each shard returns:

```text
user_id, log_count
```

The coordinator merges all available shard results with a dictionary:

```text
global_counts[user_id] += log_count
```

If all logical shards are available, `Counted = 1,000,000`.

If one logical shard is completely unavailable, `Counted` is lower and `Completeness` drops below `100%`.

### Speedup And Efficiency

Speedup:

```text
Speedup(n) = T1 / Tn
```

Efficiency:

```text
Efficiency(n) = Speedup(n) / n
```

Where:

```text
T1 = median time of the 1-shard baseline
Tn = median time of the n-shard scenario
```

If there is no complete 1-shard baseline, speedup and efficiency are shown as `N/A`.

If completeness is below `100%`, speedup should not be treated as a fair comparison because the benchmark processed incomplete data.

## Troubleshooting

### Docker Is Not Running

Error example:

```text
failed to connect to the docker API
```

Fix:

```text
Start Docker Desktop, wait until the engine is ready, then run docker compose up -d again.
```

### Port Is Already In Use

If ports `5433` to `5436` or `5443` to `5446` are occupied, stop the conflicting service or edit `docker-compose.yml` and `coordinator/config.py` consistently.

### Dataset Already Exists

If you run:

```bash
python -m coordinator.main generate --rows 1000000
```

and the dataset already exists, it will be reused. To regenerate:

```bash
python -m coordinator.main generate --rows 1000000 --force
```

### Benchmark Shows `N/A` For Speedup

This means there is no complete 1-shard baseline available for comparison.

Run:

```bash
python -m coordinator.main benchmark
```

or run a complete baseline first:

```bash
python -m coordinator.main benchmark --nodes 1
```

### Completeness Is Below 100%

At least one logical shard could not be read from primary or replica.

Check container status:

```bash
docker ps -a
```

Start missing containers:

```bash
docker start shard2_primary
docker start shard2_replica
```

Then rerun:

```bash
python -m coordinator.main benchmark --nodes 4
```

### Need To Run Benchmark Again

You do not need to rebuild, regenerate, initialize, or reload if nothing changed.

Just run:

```bash
python -m coordinator.main benchmark
```

Use rebuild only when Docker configuration changed:

```bash
docker compose up -d --build
```

Use reload only when database volumes were reset or you intentionally want to reload data:

```bash
python -m coordinator.main load
```

## Clean Reset

To stop containers:

```bash
docker compose down
```

To remove containers and database volumes:

```bash
docker compose down -v
```

After removing volumes, run the full setup again:

```bash
docker compose up -d --build
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark
```

If `data/user_logs.csv` still exists, you do not need to generate it again. To rebuild the dataset from scratch:

```bash
python -m coordinator.main generate --rows 1000000 --force
```

## Design Boundaries

This project intentionally keeps the benchmark focused and transparent:

- No 2PC
- No 3PC
- No web UI
- No API server
- No automatic node shutdown
- No separate failure dashboard
- No separate health-check table
- No separate failure benchmark report

The coordinator reports exactly what happened during query execution: which shards responded, whether primary or replica was used, how much data was counted, and whether the final result is complete.

## Recommended Demo Script

Use this sequence for a clean presentation:

```bash
docker compose up -d --build
python -m coordinator.main generate --rows 1000000
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark
```

Then demonstrate replica fallback:

```bash
docker stop shard2_primary
python -m coordinator.main benchmark --nodes 4
```

Then demonstrate partial result behavior:

```bash
docker stop shard2_replica
python -m coordinator.main benchmark --nodes 4
```

Finally recover:

```bash
docker start shard2_primary
docker start shard2_replica
python -m coordinator.main benchmark --nodes 4
```

This sequence shows the complete story: sharding improves query throughput, replica fallback preserves completeness when one primary is unavailable, and the coordinator remains stable even when a logical shard is completely down.
