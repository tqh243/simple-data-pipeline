# eq-data-pipeline

## Install new package

```
pipenv install package_name
pipenv install pyyaml
```


## Run docker

```
docker-compose -f docker/docker-compose.yml build

docker-compose -f docker/docker-compose.yml up -d
```


## Run code
```
docker exec -t eq_data_pipeline python -m src.pipeline.getfly.etl
```
