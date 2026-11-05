from google.cloud import storage

BUCKET = "ayan-week2-bucket-1234"
INPUT = "sample_input.txt"
OUTPUT = "output.txt"

client = storage.Client()
bucket = client.bucket(BUCKET)

text = bucket.blob(INPUT).download_as_text()
lines = text.splitlines()

max_len = max(len(l) for l in lines)
longest = [l for l in lines if len(l) == max_len]

result = f"Max length: {max_len}\n" + "\n".join(longest)
print(result)

bucket.blob(OUTPUT).upload_from_string(result)
print("Saved output.txt to bucket")
