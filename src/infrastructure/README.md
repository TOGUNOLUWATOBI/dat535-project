# Local Spark Cluster Setup

A lightweight shell utility (`setup_spark.sh`) for configuring and managing a single-node standalone Apache Spark cluster.

## Prerequisites

* **Apache Spark / PySpark `4.2.0`** installed on the system (defaults to `~/spark-env/bin/python`)
* **Java `17+`**

## Quick Start

Run the following three commands in order to initialize, launch, and verify your cluster:

1. **Make the file executable, also fix directory ownership permission** :
   ```bash
   chmod +x setup_spark.sh
   sudo mkdir -p /srv/dat535/spark /srv/dat535/data
   sudo chown -R ubuntu:ubuntu /srv/dat535
   ```

2. **Configure the cluster** (creates directories and generates Spark configuration files):
   ```bash
   bash setup_spark.sh setup
   ```

3. **Start the cluster** (launches the Spark Master and Worker daemons):
   ```bash
   bash setup_spark.sh start
   ```

4. **Verify the cluster** (runs a test executor job with Parquet read/write validation):
   ```bash
   bash setup_spark.sh check
   ```

## Additional Commands

| Command | Description |
| :--- | :--- |
| `bash setup_spark.sh status` | Check Master/Worker daemon status and registration |
| `bash setup_spark.sh stop` | Stop the Worker and Master daemons |
| `bash setup_spark.sh --help` | Display usage details and environment variable overrides |