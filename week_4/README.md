# Graded Assignment 3 (Week 4): Click Counts by Time of Day using PySpark on Dataproc

**Name:** Ayan Hussain
**Student ID:** 23f1000932

## What the assignment asks

We are given a file of website clicks. Each row has a `user_id` and a `timestamp`. The task is to count how many clicks happened in each of four time-of-day windows: 0-6, 6-12, 12-18 and 18-24 hours. The work must be done with PySpark on a Google Cloud Dataproc cluster, reading the input from Google Cloud Storage (GCS) and writing the output back to GCS. It has to be written twice, once with RDDs and once with DataFrames, and SparkSQL is not allowed.

## My approach

Both versions follow the same idea: take the hour from each timestamp, divide it by 6 (ignoring the remainder), and use that number to pick the bin. Hours 0 to 5 go to bin 0-6, hours 6 to 11 go to 6-12, and so on. Then count the rows in each bin.

**RDD version.** I read the file as text, drop the header line, and turn each line into a pair like `("6-12", 1)`. Then `reduceByKey` adds up the ones for each bin. I also add a zero count for every bin so that all four windows always show up, even if one had no clicks.

**DataFrame version.** I read the file as a CSV with a header and convert the timestamp column to a real timestamp. I use `F.hour()` to get the hour, `when/otherwise` to label the bin, and `groupBy().count()` to count. I then join with a small list of the four bins so a missing bin would show as 0.

Rows that cannot be parsed are skipped in both versions, so one bad line does not break the job. No SparkSQL is used anywhere.

## What I did on GCP

1. Created a Cloud Storage bucket `ayan-week4-bucket-1234` in `us-central1` with two folders: `input/` (holds `click_file.txt`) and `code/` (holds `click_counts.py`).
2. Opened Cloud Shell and enabled the Dataproc and Compute Engine APIs.
3. Gave the default Compute Engine service account the `Dataproc Worker` role, which Dataproc needs to create a cluster.
4. Created a single-node Dataproc cluster named `click-cluster` (n2-standard-2, 50 GB disk) from the terminal.
5. Submitted the PySpark job to the cluster with `gcloud dataproc jobs submit pyspark`.
6. Checked the output files in the bucket and downloaded them.
7. Deleted the cluster afterwards so it does not keep costing money.

## Commands used

```bash
# create the cluster
gcloud dataproc clusters create click-cluster \
  --region=us-central1 \
  --single-node \
  --master-machine-type=n2-standard-2 \
  --master-boot-disk-size=50GB

# submit the job
gcloud dataproc jobs submit pyspark gs://ayan-week4-bucket-1234/code/click_counts.py \
  --cluster=click-cluster --region=us-central1 \
  -- gs://ayan-week4-bucket-1234/input/click_file.txt gs://ayan-week4-bucket-1234/output

# look at the results
gsutil cat gs://ayan-week4-bucket-1234/output/rdd_output/part-00000
gsutil cat gs://ayan-week4-bucket-1234/output/dataframe_output/part-*.csv

# delete the cluster when finished
gcloud dataproc clusters delete click-cluster --region=us-central1
```

## Result

The input file has 200 clicks. Both the RDD and the DataFrame version gave the same answer:

| Time window (hours) | Clicks |
|---------------------|--------|
| 0-6                 | 44     |
| 6-12                | 53     |
| 12-18               | 48     |
| 18-24               | 55     |

The four counts add up to 200, so no click was lost or counted twice. I also checked the numbers separately with a plain Python script on the same file, and they matched.

## Files in this submission

| File | What it is |
|------|------------|
| `click_counts.py` | The PySpark program with both the RDD and DataFrame implementations |
| `rdd_output.txt` | Output of the RDD version, downloaded from the GCS bucket |
| `dataframe_output.csv` | Output of the DataFrame version, downloaded from the GCS bucket |
| `README.md` | This file |
| Screencast video | Walkthrough of the code, GCP setup, live run and results |

## Other ways to run a Spark job (optional part)

Besides the `gcloud` command above, the same script can be run from the Dataproc **Submit job** form in the Cloud Console, or by connecting to the cluster's master node over SSH and running `spark-submit click_counts.py <input_path> <output_path>`.
