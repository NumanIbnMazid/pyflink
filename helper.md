# Project Guideline

## Example workflow to run word_count demo

### Run Docker

$ `docker compose up`

### Access Pyflink Job Manager Container's Bash

$ `docker exec -it -u flink pyflink-jobmanager-1 bash`

### Go to project folder

$ `cd /project`

### GO to `word_count` folder

$ `cd word_count/`

### Execute target python file

$ `python word_count.py`

### Execute target python file specifying input

$ `python word_count.py --input word_text.txt`

### Execute target python file using docker compose

$ `docker-compose run --rm --entrypoint '/bin/sh' jobmanager -c 'cd /project && cd word_count/ && python word_count.py'`

### Execute target python file specifying input using docker compose

$ `docker-compose run --rm --entrypoint '/bin/sh' jobmanager -c 'cd /project && cd word_count/ && python word_count.py --input word_text.txt'`
