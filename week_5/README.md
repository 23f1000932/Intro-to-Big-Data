# Week 5 Graded Assignment 4: Customer and Transaction Analysis with PySpark on Dataproc

**Student:** Udghosh Rao (24f2000612)

## Overview

This project uses Google Cloud Dataproc and the PySpark DataFrame API to clean, join and aggregate two small datasets stored in Google Cloud Storage (GCS): a customer dataset (155 rows) and a transaction dataset (306 rows). Both datasets contain deliberately dirty records, so most of the work is cleaning them correctly before any join or aggregation. All processing runs on a Dataproc cluster, with GCS used for both input and output. Only the DataFrame API is used; there is no SparkSQL anywhere in the code.

## Files in this submission

| File | What it is |
|---|---|
| `week5_pyspark.py` | The PySpark program that does all the work |
| `outputs/` | All result CSVs downloaded from the GCS bucket (listed below) |
| Dataproc job screenshot | Shows the job with status Succeeded and the driver log output |
| Screencast | Walks through GCP setup, the code logic and a live run |

## Cloud setup

I created a GCS bucket named `ayan-week5-bucket-1234` in `us-central1` and uploaded the two input files to its `input/` folder. The Dataproc cluster was created from Cloud Shell, because the console defaults hit a memory/quota error. It is a single-node cluster with an `e2-standard-2` master, a 50 GB standard disk and image version 2.2.

```bash
gcloud dataproc clusters create week5-cluster-as4 \
  --region=us-central1 --single-node \
  --master-machine-type=e2-standard-2 \
  --master-boot-disk-type=pd-standard \
  --master-boot-disk-size=50GB \
  --image-version=2.2-debian12
```

The job is submitted like this, with the bucket name passed as the script argument:

```bash
gcloud dataproc jobs submit pyspark week5_pyspark.py \
  --cluster=week5-cluster --region=us-central1 \
  -- udghosh_week5
```

## What the program does

**1. Load and inspect.** Both CSVs are read from GCS with every column as a string, so that bad values are not silently changed on load. The program prints each schema, the row count and a few sample rows.

**2. Cleaning, done separately for each dataset before any join.** Every column is trimmed and blank strings are turned into nulls. Each row then gets an `invalid_reason` column that lists every rule it broke, so rows with no reason are clean and the rest go to `invalid_rows` with the reason attached.

For customers, a row is invalid if `customer_id`, `customer_name`, `city` or `status` is missing, if the status is not active or inactive, or if it is a duplicate. For transactions, a row is invalid if `transaction_id`, `customer_id` or `payment_mode` is missing, if the amount is missing, non-numeric, zero or negative, if the date is missing or malformed (including impossible dates like 2025/02/30), or if it is a duplicate.

Duplicates are found with `row_number()` over a window partitioned by all columns. The first copy is kept and every other copy is marked invalid. A second check on `customer_id` and `transaction_id` stops two different rows from sharing the same key.

**3. City standardization.** A lookup map converts known variants to one spelling (for example `cmbt` to Coimbatore, `trivandrm` to Trivandrum, `Poona` to Pune, `bangalore` to Bengaluru, `hyd` to Hyderabad). Anything not in the map falls back to title case. After this, the 8 distinct cities are Bengaluru, Chennai, Coimbatore, Hyderabad, Kochi, Mumbai, Pune and Trivandrum.

**4. Type and format checks.** `transaction_amount` is cast to double and `transaction_date` is converted to a real date and written back as `YYYY-MM-DD`.

**5. New columns.** For transactions I add `month` (a number), `month_name`, and `amount_category`: Low for amounts under 3000, Medium from 3000 up to 7000, and High from 7000 upward.

**6. Referential integrity.** A transaction is only kept if its `customer_id` exists in the cleaned customer table. In this data every valid transaction passed this check.

**7. Join.** The cleaned transactions are inner-joined with the cleaned customers on `customer_id`.

**8. Aggregations.** From the joined data the program computes the total and average amount per customer, the total amount per city, and the top 3 customers by total amount.

Each major stage prints a few rows to the driver log, including the invalid and cleaned rows, as the assignment asks.

## Results

| Dataset | Input rows | Invalid rows | Clean rows |
|---|---|---|---|
| Customers | 155 | 5 (2 duplicates, 3 with missing values) | 150 |
| Transactions | 306 | 6 (2 duplicates, missing transaction_id, missing customer_id, amount of -300, invalid date 2025/02/30) | 300 |

All 300 clean transactions matched a customer, so the joined dataset has 300 rows.

Top 3 customers by total transaction amount:

| Customer | City | Total | Average | Transactions |
|---|---|---|---|---|
| c111 | Coimbatore | 40066 | 6677.67 | 6 |
| c070 | Chennai | 31927 | 7981.75 | 4 |
| c068 | Kochi | 29998 | 5999.60 | 5 |

Total transaction amount per city:

| City | Total | Transactions |
|---|---|---|
| Coimbatore | 241741 | 45 |
| Trivandrum | 232665 | 45 |
| Chennai | 230836 | 42 |
| Hyderabad | 212826 | 44 |
| Kochi | 204217 | 41 |
| Bengaluru | 167627 | 38 |
| Pune | 112960 | 26 |
| Mumbai | 84248 | 19 |

## Output files

The program writes each result to GCS under `gs://ayan-week5-bucket-1234/output/` as a single CSV with a header. The downloaded copies in `outputs/` were renamed from the long `part-0000...` file names.

| GCS folder | Downloaded file | Contents |
|---|---|---|
| `cleaned_data/customers` | `cleaned_data_customers.csv` | 150 cleaned customers |
| `cleaned_data/transactions` | `cleaned_data_transactions.csv` | 300 cleaned transactions with month and category |
| `invalid_rows/customers` | `invalid_rows_customers.csv` | 5 invalid customer rows with reason |
| `invalid_rows/transactions` | `invalid_rows_transactions.csv` | 6 invalid transaction rows with reason |
| `joined_data` | `joined_data.csv` | 300 joined rows |
| `aggregates/per_customer` | `aggregates_per_customer.csv` | Total, average and count per customer |
| `aggregates/per_city` | `aggregates_per_city.csv` | Total and count per city |
| `aggregates/top3_customers` | `aggregates_top3_customers.csv` | Top 3 customers by total |

## Notes

The spelling-variant list in the city map was built from the variants that actually appear in this dataset, so a new dataset with other misspellings would need new entries. Date parsing uses the `CORRECTED` time parser policy so that impossible dates become null and are flagged instead of crashing the job.
