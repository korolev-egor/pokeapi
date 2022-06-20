from typing import Dict

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests

from .korolev_classes import PokeAPIParser, parsers


def fetch_endpoints(**kwargs: Dict) -> None:
    def fetch_endpoint(parser: PokeAPIParser):
        parser.fetch_resources_concurrently(limit=kwargs['resources_limit_per_page'])
        parser.dump_unprocessed()

    for parser in parsers:
        fetch_endpoint(parser(**kwargs))


def process_endpoints(**kwargs: Dict) -> None:
    def process_endpoint(parser: PokeAPIParser):
        parser.load_unprocessed_resources()
        parser.fetch_endpoint_concurrently()
        parser.dump_processed()

    for parser in parsers:
        process_endpoint(parser(**kwargs))


def cleanup_unprocessed_folder(**kwargs: Dict) -> None:
    """Remove all files from 's3://de-school-mwaa/unprocessed_files/Korolev/'."""
    hook = S3Hook()
    bucket, prefix = hook.parse_s3_url(kwargs['unprocessed_folder'])
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix, delimiter='/')
    hook.delete_objects(bucket=bucket, keys=keys)


def check_generations(url: str) -> None:
    response = requests.get(url)
    response_content = response.json()
    number_of_generations = response_content['count']
    print(f'Current number of generations is: {number_of_generations}')
