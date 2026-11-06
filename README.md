# Intro to Big Data

This repository contains the practical assignments for the **Introduction to Big Data** course. Each weekly folder contains the source code, input data, generated output, and assignment-specific documentation for a different Google Cloud or Apache Spark exercise.

## Repository structure

```text
Intro-to-Big-Data/
├── README.md
├── week_2/
│   ├── README.md
│   ├── longest.py
│   ├── sample_input.txt
│   └── output.txt
├── week_3/
│   ├── README.md
│   ├── main.py
│   ├── requirements.txt
│   ├── poem.txt
│   ├── poem_longest_lines.txt
│   └── Log.png
└── week_4/
    ├── README.md
    ├── click_counts.py
    ├── rdd_output.txt
    └── dataframe_output.csv
```

## Folder overview

| Folder | Topic | Main technology |
|---|---|---|
| [`week_2/`](./week_2/) | Find the longest line or lines in a text file stored in Google Cloud Storage | Python, Compute Engine VM, Cloud Storage |
| [`week_3/`](./week_3/) | Automatically find the longest line or lines when a file is uploaded | Python, Cloud Functions, Eventarc, Cloud Storage |
| [`week_4/`](./week_4/) | Count website clicks across four time-of-day windows | PySpark RDDs and DataFrames, Dataproc, Cloud Storage |

## Week 2: Longest lines on a VM

The [`week_2/`](./week_2/) assignment runs [`longest.py`](./week_2/longest.py) on a Google Cloud VM. The script:

1. Downloads `sample_input.txt` from a Cloud Storage bucket.
2. Finds the maximum line length.
3. Reports every line with that maximum length, including ties.
4. Uploads the result as `output.txt`.

### Files

- [`longest.py`](./week_2/longest.py): Cloud Storage client and longest-line logic.
- [`sample_input.txt`](./week_2/sample_input.txt): Sample input containing a tie for the longest line.
- [`output.txt`](./week_2/output.txt): Generated result downloaded from Cloud Storage.
- [`README.md`](./week_2/README.md): Full VM, bucket, permissions, and execution notes.

### Running

From a configured Google Cloud VM with `google-cloud-storage` installed:

```bash
python3 longest.py
```

The bucket name and input/output object names are configured near the top of the script.

## Week 3: Event-driven Cloud Function

The [`week_3/`](./week_3/) assignment deploys [`main.py`](./week_3/main.py) as a Python Cloud Function. An upload to a Cloud Storage bucket triggers the function, which:

1. Reads the uploaded text file.
2. Finds all longest lines.
3. Writes details to Cloud Logging.
4. Saves the result under `results/` in the same bucket.

The function ignores objects whose names begin with `results/`, preventing its own output from triggering an infinite loop.

### Files

- [`main.py`](./week_3/main.py): Cloud Function entry point, `find_longest_lines`.
- [`requirements.txt`](./week_3/requirements.txt): Function Framework and Google Cloud Storage dependencies.
- [`poem.txt`](./week_3/poem.txt): Test input file.
- [`poem_longest_lines.txt`](./week_3/poem_longest_lines.txt): Expected/generated result for the test input.
- [`Log.png`](./week_3/Log.png): Cloud Logging evidence.
- [`README.md`](./week_3/README.md): Deployment, trigger, IAM, and testing notes.

### Entry point

The deployment entry point is:

```text
find_longest_lines
```

Install the dependencies listed in [`requirements.txt`](./week_3/requirements.txt) when running or testing the function locally.

## Week 4: Click counts with PySpark

The [`week_4/`](./week_4/) assignment runs [`click_counts.py`](./week_4/click_counts.py) as a PySpark job on Google Cloud Dataproc. It reads a CSV-style click file from Cloud Storage and counts clicks in these windows:

- `0-6`
- `6-12`
- `12-18`
- `18-24`

The script implements the calculation twice:

- **RDD version:** parses each row and uses `reduceByKey`.
- **DataFrame version:** parses timestamps, derives the hour with Spark functions, and aggregates with `groupBy`.

SparkSQL is not used. Both implementations include zero-count rows for empty time windows and write their results to separate output directories.

### Files

- [`click_counts.py`](./week_4/click_counts.py): RDD and DataFrame implementations.
- [`rdd_output.txt`](./week_4/rdd_output.txt): Output from the RDD implementation.
- [`dataframe_output.csv`](./week_4/dataframe_output.csv): Output from the DataFrame implementation.
- [`README.md`](./week_4/README.md): Dataproc setup, submission commands, and result verification.

### Running

Submit the job to Dataproc with two Cloud Storage paths:

```bash
gcloud dataproc jobs submit pyspark click_counts.py \
  --cluster=<cluster-name> \
  --region=<region> \
  -- gs://<bucket>/input/click_file.txt gs://<bucket>/output
```

The script writes results beneath:

```text
gs://<bucket>/output/rdd_output/
gs://<bucket>/output/dataframe_output/
```

## Recommended reading order

1. Start with this file for the repository map.
2. Read the README in the relevant weekly folder for the complete assignment context.
3. Review the source file for the implementation.
4. Compare the generated output with the documented expected result.

## Notes

- The cloud resource names and paths in the weekly READMEs describe the resources used for the assignments. Replace them with your own values before re-running the code.
- Cloud Storage buckets, Cloud Functions, Dataproc clusters, and related services may incur charges. Delete temporary resources after testing.
- The scripts expect the permissions and dependencies described in their corresponding weekly README.
