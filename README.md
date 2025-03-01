# CitiBike 🚲 Deep Dive

This projects allows the processing of Citibike yearly datasets into a [duckdb file](https://ghn-public-data.s3.us-east-1.amazonaws.com/citibike-data/CitibikeData.db) that used by [this website](https://www.gabrielhn.com/citibike-deep-dive) to render graphs.

The database holds 4 tables that this code updates based on the latest citibike datasets and then uploads them into the S3.

![website](/citibike.gif)

# Installs
```
# Create Virtual Env
uv venv

# Activate Env
source .venv/bin/activate

# Install Requirements
uv pip install -r requirements.txt
```

Additionally, would need to create [a Mapbox Directions API to use found here](https://docs.mapbox.com/help/glossary/access-token/).

And configure this Mapbox Token, and S3 credentials inside the `.env` file.

# Usage
Different commands allows for local test, pure remote calls, or a combination of the two.

The more local you run the script the faster the data pipeline gets

```
# To read remote citibike S3 files, read the existing duckdb file in S3, and upload the updated duckdb file into S3. Can do 

python -m citibike_data_process --file-remote --read-remote --make-remote

#  To upload the local citibike files, read the local duckdb file, and main the duckdb file locally.

python -m citibike_data_process --file-local --read-local --make-local

```