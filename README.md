# Project structure
```bash
├── README.md
├── airflow
│   ├── korolev_check_dag.py
│   ├── korolev_fetch_dag.py
│   └── utils
│       ├── korolev_classes.py
│       └── korolev_functions.py
├── data
│   ├── generation.json
│   ├── move.json
│   ├── pokemon-species.json
│   ├── pokemon.json
│   ├── stat.json
│   └── type.json
└── snowflake
    ├── Scheme.png
    └── script.sql

4 directories, 13 files
```
## airflow
Folder contains DAGs and required functions.

- **korolev_check_dag.py**

Defiles DAG that gets number of generations daily and prints it in airflow logs.

- **korolev_fetch_dag.py**

Defiles DAG that fetches endpoint data and writes it in S3 folder in `.json` format.

### utils
- **korolev_classes.py**

Defines classes used in `korolev_fetch_dag.py`.

- **korolev_functions.py**

Defines functions used in Airflow `PythonOperator` in DAG definition files.

## data
Folder contains all essential data for data marts
## snowflake
Folder contains Snowflake script and schema
- **script.sql**

All-in-one script to create data warehouse with staging layer, storage layer and required Data Marts.
- **Schema.png**

Entity-Relation diagram for DWH storage layer.
# Data Marts top 10 entries
## Question A

| NAME     | POKEMON_COUNT | DIFFERENCE_WITH_PREVIOUS | DIFFERENCE_WITH_NEXT |
|----------|---------------|--------------------------|----------------------|
| shadow   | 0             |                          | 0                    |
| unknown  | 0             | 0                        | 58                   |
| ice      | 58            | 58                       | 14                   |
| fairy    | 72            | 14                       | 1                    |
| ghost    | 73            | 1                        | 4                    |
| steel    | 77            | 4                        | 1                    |
| fighting | 78            | 1                        | 0                    |
| dark     | 78            | 0                        | 1                    |
| dragon   | 79            | 1                        | 4                    |
| ground   | 83            | 4                        | 2                    |

## Question B

| MOVE_NAME       | POKEMON_COUNT | DIFFERENCE_WITH_PREVIOUS | DIFFERENCE_WITH_NEXT |
|-----------------|---------------|--------------------------|----------------------|
| ice-burn        | 1             |                          | 0                    |
| sizzly-slide    | 1             | 0                        | 0                    |
| heal-order      | 1             | 0                        | 0                    |
| glitzy-glow     | 1             | 0                        | 0                    |
| sappy-seed      | 1             | 0                        | 0                    |
| freezy-frost    | 1             | 0                        | 0                    |
| hyperspace-fury | 1             | 0                        | 0                    |
| tar-shot        | 1             | 0                        | 0                    |
| magic-powder    | 1             | 0                        | 0                    |
| v-create        | 1             | 0                        | 0                    |

## Question C

| NAME                | STAT_SUM |
|---------------------|----------|
| eternatus-eternamax | 1125     |
| rayquaza-mega       | 780      |
| mewtwo-mega-x       | 780      |
| mewtwo-mega-y       | 780      |
| kyogre-primal       | 770      |
| groudon-primal      | 770      |
| necrozma-ultra      | 754      |
| arceus              | 720      |
| zamazenta-crowned   | 720      |
| zacian-crowned      | 720      |

## Question D

| TYPE     | GENERATION      | POKEMON_COUNT |
|----------|-----------------|---------------|
| bug      | generation-i    | 15            |
| bug      | generation-ii   | 12            |
| bug      | generation-iii  | 14            |
| bug      | generation-iv   | 11            |
| bug      | generation-v    | 18            |
| bug      | generation-vi   | 3             |
| bug      | generation-vii  | 14            |
| bug      | generation-viii | 9             |
| dark     | generation-i    | 9             |
