# eq-data-pipeline

## Install new package

```
pip install pipenv
pipenv install package_name
```


## Run docker

```
docker-compose -f docker/docker-compose.yml build

docker-compose -f docker/docker-compose.yml up -d
```


## Run code
```
docker exec -t eq_data_pipeline python -m src.pipeline.getfly.etl
docker exec -t eq_data_pipeline python -m src.pipeline.getfly.etl -j broward_accounts
docker exec -t eq_data_pipeline python -m src.pipeline.getfly.etl -j apc_accounts
docker exec -t eq_data_pipeline python -m src.pipeline.getfly.etl -j broward_products
```
