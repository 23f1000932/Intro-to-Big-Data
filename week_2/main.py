import functions_framework
from google.cloud import storage

storage_client = storage.Client()

@functions_framework.cloud_event
def find_longest_lines(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    # Skip our own output files to avoid an infinite trigger loop
    if file_name.startswith("results/"):
        print(f"Skipping result file: {file_name}")
        return

    bucket = storage_client.bucket(bucket_name)
    text = bucket.blob(file_name).download_as_text()
    lines = text.splitlines()

    if not lines:
        print(f"{file_name} is empty, nothing to analyze.")
        return

    max_len = max(len(line) for line in lines)
    longest = [line for line in lines if len(line) == max_len]

    # Print to Cloud Logging
    print(f"File: {file_name}")
    print(f"Maximum line length: {max_len} characters")
    print(f"Number of longest line(s): {len(longest)}")
    for i, line in enumerate(longest, 1):
        print(f"Longest line {i}: {line}")

    # Save result back to the same bucket
    result = (
        f"Source file: {file_name}\n"
        f"Maximum line length: {max_len}\n"
        f"Number of longest lines: {len(longest)}\n\n"
        + "\n".join(longest)
        + "\n"
    )
    out_name = f"results/{file_name.rsplit('.', 1)[0]}_longest_lines.txt"
    bucket.blob(out_name).upload_from_string(result, content_type="text/plain")
    print(f"Result saved to gs://{bucket_name}/{out_name}")
