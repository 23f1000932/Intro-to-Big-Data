# Week 3 – Graded Assignment 2: Longest Line Finder (Cloud Function on GCP)

**Name:** Ayan Hussain
**Student ID:** 23f1000932

## What this does

A Cloud Function (Cloud Run functions, 2nd gen, Python 3.12) runs automatically whenever a text file is uploaded to a Google Cloud Storage bucket. It downloads the file, finds the longest line(s) by character count, prints the result to Cloud Logging, and saves the result back into the same bucket under `results/`. If several lines tie for the maximum length, all of them are reported.

## Architecture

```
Upload file to GCS bucket
        |
        v
Eventarc trigger (object.v1.finalized)
        |
        v
Cloud Function: find_longest_lines
   1. Download the uploaded file
   2. Find max line length and every line with that length
   3. Print results to Cloud Logging
   4. Save results/<name>_longest_lines.txt in the same bucket
```

Saving the result into the same bucket fires the trigger again, so the function ignores any object whose name starts with `results/`. Without this check it would loop forever.

## GCP resources

| Resource | Value |
|---|---|
| Bucket | `ayan-week3-bucket-1234` (us-central1, Standard) |
| Function / service | `longest-line-fn` (us-central1) |
| Runtime | Python 3.12 |
| Entry point | `find_longest_lines` |
| Trigger | `longest-line-trigger`, Cloud Storage, `google.cloud.storage.object.v1.finalized` |
| Runtime service account | default Compute Engine service account |

Roles that were needed: `roles/pubsub.publisher` for the Cloud Storage service agent, `roles/eventarc.eventReceiver` and `roles/run.invoker` for the compute service account, and `roles/storage.objectAdmin` on the bucket so the function can write the result file. The last one was missing at first and the first run failed with a 403 when saving the output, which the logs made obvious.

## Setup steps

1. Created the bucket `ayan-week3-bucket-1234` in us-central1.
2. Enabled the Cloud Run, Eventarc, Cloud Build, Artifact Registry and Logging APIs.
3. Created the service `longest-line-fn` with the inline editor (Python 3.12), then pasted `main.py` and `requirements.txt` and set the entry point to `find_longest_lines`.
4. Added the Eventarc trigger for the bucket (object finalized) pointing at the service.
5. Granted the IAM roles listed above.
6. Uploaded `poem.txt` to the bucket and checked the logs and the `results/` folder.

## Test input and result

Input file `poem.txt` has six lines, and the last two tie for the longest at 41 characters.

Output saved to `results/poem_longest_lines.txt`:

```
Source file: poem.txt
Maximum line length: 41
Number of longest lines: 2

Smoke curls up from every kitchen chimney
Children run along the dusty winding road
```

Cloud Logging shows the same values (file name, maximum length, count, and each longest line), then the "Result saved" message, followed by a second invocation that logs "Skipping result file" for the output object.

## Files in this submission

`main.py` (function code), `requirements.txt` (dependencies), `poem.txt` (input file), `poem_longest_lines.txt` (output downloaded from the bucket), the Logs Explorer screenshot, and the screencast video.

## requirements.txt

```
functions-framework==3.*
google-cloud-storage
```
