# olop

## Pre-reqs
´´´
sudo dnf install tesseract
sudo dnf install tesseract-langpack-swe
´´´

Start Mongodb
´´´
podman run --network host -dt --name olop_mongo -v '/home/moneyman/mongodb/data:/data/db:Z' docker.io/library/mongo:latest
´´

Run the ingestion
```
python main.py
```



