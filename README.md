# Spark with Docker

- Project build using: https://github.com/big-data-europe/docker-spark

## How to start

```bash
docker-compose up
```

- Master: http://localhost:8080
- Workers:
- - http://localhost:8081
- - http://localhost:8082

## How to execute container with worker

use a name spark-worker-{id} with worker id.

```bash
docker exec -it spark-worker-1 bash
```

## How to run Python examples

Into worker container:

- You can run _pyspark CLI_
  ```bash
  ./spark/bin/pyspark
  ```
- or you can execute a file:
  ```bash
  cd home
  ../../spark/bin/spark-submit example.py data.csv
  ```
