# Summary
This project contains some tooling around importing NOAA daily climate data into Elasticsearch. This provides
an interesting data set to play around with and learn more about Elasticsearch.

# Cluster
Currently I've used it to import a subset of data into an Elasticsearch cluster running across 4x Raspberry Pis. I intentionally went with low end hardware to force scaling issues to arise. Each cluster member is a Raspberry Pi 4, with 4GB of RAM and a 128GB SD card for storage.

For Elasticsearch, I'm using the OSS tar file for 7.8.0 and built the coresponding opendistro from source to get ssl and authentication. I also installed Kibana with the opendistro plugin on the first cluster member.

# Data
I'm using the daily entries from here https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/
They are all in CSV format, so my tooling currently converts them to json and then inserts into Elasticsearch in batches. I'm also adding fields that are formats that are easier for people to read. Celcius and Farenheit for example, instead of just 10ths of a degree C.

# Code
Currently the code is a total mess and full of hardcoded values. I've intentionally not used the python Elasticsearch library, as I was interested in practicing some of the issues that it solves for you. Mostly around bulk inserts and retries.

# TODO
* move all hardcoded values to vars
* use HTTP HEAD to check if files have been updated online, and grab the latest data from them
* move logic into functions
* move data insert into a function and make it more DRY
* add dashboards to repo
* add multithreading similar to https://github.com/thomascate/Backup_Scripts
* add dedicated master and kibana nodes to cluster to bring down load issues
