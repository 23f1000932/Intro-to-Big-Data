# Graded Assignment 1, Week 2: Longest Line(s) in a Text File on GCP

Name: Ayan Hussain
Student ID: 23f1000932

## What this assignment does

The task was to run a small Python program on a Google Cloud VM that reads a text file from a Cloud Storage bucket, finds the longest line (or lines, if several tie), prints them in the terminal, and saves the result back into the same bucket. Everything was done on GCP, nothing ran locally.

## Files in this zip

- `longest.py`: the Python script that I ran on the VM.
- `output.txt`: the result file, downloaded from the bucket after the script ran.
- `README.md`: this file.
- `sample_input.txt`: the input file I used. I made it myself with ten lines, and two of them share the maximum length of 75 characters, so the tie case gets tested.

## How I did it

### 1. Creating the bucket

In the Cloud Console I went to Cloud Storage, then Buckets, then Create. I named the bucket `ayan-week2-bucket-1234`, since bucket names have to be unique across all of Google Cloud. I picked an Asia location, kept the Standard storage class, left public access prevention on, and clicked Create. I then opened the bucket and used Upload files to put `sample_input.txt` at the top level, so the object path is simply `sample_input.txt`.

### 2. Creating the VM instance

Next I opened Compute Engine, then VM instances, and enabled the Compute Engine API the first time it asked. I clicked Create instance and named it `week2-vm`. I chose a small general-purpose E2 machine in the same area as my bucket, because the job is tiny and doesn't need more, and I kept the default Debian boot disk. Under Identity and API access I set the Storage API access to Read Write so the VM would be allowed to talk to Cloud Storage. After clicking Create I waited for the green tick, then pressed the SSH button to open a terminal in the browser.

### 3. Setting up Python and writing the script

Inside the VM, Python 3.13.5 was already installed. I ran `sudo apt update`, then `sudo apt install -y python3-pip`, then `pip3 install google-cloud-storage --break-system-packages`. The last flag is needed on newer Debian because system Python is protected. I then created the script with `nano longest.py`.

The script creates a storage client, downloads `sample_input.txt` as text, splits it into lines, and finds the maximum line length. It then keeps every line whose length equals that maximum, so ties are all reported. It prints the result and uploads it to the bucket as `output.txt`.

### 4. The permission error and how I fixed it

This was the main trouble in the assignment, and it came in two steps.

The first time I ran the script I got a 400 error saying `Invalid bucket name: 'YOUR-BUCKET-NAME'`. I had forgotten to replace the placeholder in the script with my real bucket name. I edited line 3 to `BUCKET = "ayan-week2-bucket-1234"` and ran it again.

That led to the real problem, a 403 Forbidden error. The message said that the VM's service account, `1023290947498-compute@developer.gserviceaccount.com`, did not have `storage.objects.get` access to the object. So the bucket name was right and the file existed, but the account the VM runs as had no permission to read from the bucket.

To fix it, I went to Cloud Storage, opened my bucket, and clicked the Permissions tab. I pressed Grant access, pasted the service account email into New principals, and picked the role Storage Object Admin, since the script needs to read the input and also write the output. I saved it, waited about a minute for the change to apply, and ran the script again. This time it worked.

The takeaway is that the VM's access scope and the bucket's own permissions both matter. Giving the VM Read Write storage access was not enough on its own, because the bucket also has to allow the VM's service account.

### 5. Running it and checking the output

I ran `python3 longest.py` on the VM. The terminal printed:

```
Max length: 75
Cloud computing lets you rent computers by the hour instead of buying them.
Storage and compute are separate services that work really well together :)
Saved output.txt to bucket
```

After refreshing the bucket page, `output.txt` appeared (166 bytes). I downloaded it from the bucket and included it in this zip.
