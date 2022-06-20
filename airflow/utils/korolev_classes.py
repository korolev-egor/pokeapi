import abc
from typing import Callable, List, Dict, Any, Optional
import time
import json
import queue
import threading

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests


class PokeAPIParser(abc.ABC):
    """Abstract class to parse PokeAPI endpoints."""
    def __init__(self, poke_api_url: str, sleep_duration: float, threads_number: int,
                 processed_folder: str = None, unprocessed_folder: str = None, **kwargs) -> None:
        self.poke_api_url = poke_api_url
        self.sleep_duration = sleep_duration
        self.threads_number = threads_number
        self.processed_folder = processed_folder
        self.unprocessed_folder = unprocessed_folder

        self.processed_resources = queue.Queue()
        self.unprocessed_resources = queue.Queue()
        self.hook = S3Hook()

    @classmethod
    @property
    @abc.abstractmethod
    def ENDPOINT(cls) -> str:
        """Endpoint name."""
        raise NotImplementedError

    @classmethod
    @property
    @abc.abstractmethod
    def PATHS(cls) -> List[str]:
        """List of sequences of JSON keys separated by dot symbol."""
        raise NotImplementedError

    def _update_value(self, path: str, json: Dict, res: Dict) -> None:
        """
        Recursively update `res` dictionary with values by `path` from `json`.

        Changes `res` dictionary in place.
        Path must be a string containing sequence of string keys
        for `json` separated by dot symbol.
        If key sequence from `path` maps to list in `json` -
        get remaining key sequence in every element of the list and put in `res`.
        If key is url
        Examples
        --------
        Example1:

        >>> json = {'k1': [{'k21': 1, 'k22': 2}, {'k21': 3, 'k22': 4}]}
        >>> path = 'k1.k21'
        >>> res = dict()
        >>> get_value(path, json, res)
        >>> res
        {'k1': [{'k21': 1}, {'k21': 3}]}

        Example2:

        >>> json = {'Garden': {'Flowers': {'Red flower': 'Rose'}}}
        >>> path = 'Garden.Flowers.Red flower'
        >>> res = dict()
        >>> get_value(path, json, res)
        >>> res
        {'Garden': {'Flowers': {'Red flower': 'Rose'}}}
        """
        keys = path.split('.')
        key = keys.pop(0)
        json = json[key]
        if isinstance(json, dict):
            if res.get(key) is None:
                res[key] = dict()
            self._update_value('.'.join(keys), json, res[key])
        elif isinstance(json, list):
            if res.get(key) is None:
                res[key] = [dict() for _ in json]
            for index, item in enumerate(json):
                self._update_value('.'.join(keys), item, res[key][index])
        else:
            if key == 'url':
                res['id'] = int(json.split('/')[-2])  # save id as int to save space
            else:
                res[key] = json

    def parse_json(self, json: Dict) -> Dict:
        """Parse JSON fetched from PokeAPI resource."""
        res = dict()
        res['id'] = json['id']
        res['name'] = json['name']
        for path in self.PATHS:
            self._update_value(path, json, res)
        return res

    def _get_json_content(self, url: str) -> Dict:
        """Send `Get` request and get dictionary from response."""
        return requests.get(url).json()

    def _fetch_resource(self, offset: int, limit: int) -> Optional[str]:
        """Fetch from list of available NamedAPIResources by endpoint.
        NamedAPIResource is dict that contain two keys: `name` and `url`.
        NamedAPIResource is put in `unprocessed_resources` queue."""
        url = f'{self.poke_api_url}{self.ENDPOINT}?limit={limit}&offset={offset}'
        content = self._get_json_content(url)
        for resource in content['results']:
            self.unprocessed_resources.put(resource)
        return content['next']

    def _fetch_resources(self, begin: int, step: int, limit: int) -> None:
        """Fetch resources from PokeAPI endpoint."""
        counter = 0
        while self._fetch_resource(begin + counter * step, limit):
            time.sleep(self.sleep_duration)
            counter += 1

    def _concurrent_running(self, target: Callable, kwargs_list: List[Dict[str, Any]]) -> None:
        """Run `target` function concurrently."""
        threads = []
        for kwargs in kwargs_list:
            thread = threading.Thread(target=target, kwargs = kwargs)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def fetch_resources_concurrently(self, limit: int) -> None:
        """Fetch resources from PokeAPI endpoint concurrently."""
        kwargs_list = [
            {'begin': limit * i, 'step': limit * self.threads_number, 'limit': limit}
            for i in range(self.threads_number)
        ]
        self._concurrent_running(self._fetch_resources, kwargs_list)

    def _fetch_endpoint(self) -> None:
        """Get from unprocessed queue resource, process it and put in processed queue."""
        while True:
            time.sleep(self.sleep_duration)
            try:
                resource = self.unprocessed_resources.get(block=False)
            except queue.Empty:
                return
            content = self._get_json_content(resource['url'])
            parsed_content = self.parse_json(content)
            self.processed_resources.put(parsed_content)

    def fetch_endpoint_concurrently(self) -> None:
        """Fetch endpoint in threads."""
        kwargs_list = [None] * self.threads_number
        self._concurrent_running(self._fetch_endpoint, kwargs_list)

    def load_unprocessed_resources(self) -> None:
        """Load unprocessed data from S3."""
        bucket, key = self.hook.parse_s3_url(f'{self.unprocessed_folder}{self.ENDPOINT}.json')
        data_string = self.hook.read_key(
            bucket_name=bucket,
            key=key
        )
        decoded_data = json.loads(data_string)
        for resource in decoded_data:
            self.unprocessed_resources.put(resource)

    def dump_unprocessed(self) -> None:
        """Dump unprocessed data into S3."""
        self._load_string(self.unprocessed_folder, self.unprocessed_resources)

    def dump_processed(self) -> None:
        """Dump processed data into S3."""
        self._load_string(self.processed_folder, self.processed_resources)

    def _load_string(self, prefix, resources_queue) -> None:
        """Load queue data as JSON string file into S3."""
        encoded_data = json.dumps(list(resources_queue.queue))
        self.hook.load_string(
            string_data=encoded_data,
            key=f'{prefix}{self.ENDPOINT}.json', replace=True
        )


class PokemonParser(PokeAPIParser):
    ENDPOINT = 'pokemon'
    PATHS = ['stats.base_stat', 'stats.stat.url']


class TypeParser(PokeAPIParser):
    ENDPOINT = 'type'
    PATHS = ['pokemon.pokemon.url']


class StatParser(PokeAPIParser):
    ENDPOINT = 'stat'
    PATHS = []


class GenerationParser(PokeAPIParser):
    ENDPOINT = 'generation'
    PATHS = []


class SpeciesParser(PokeAPIParser):
    ENDPOINT = 'pokemon-species'
    PATHS = ['generation.url', 'varieties.pokemon.url']


class MoveParser(PokeAPIParser):
    ENDPOINT = 'move'
    PATHS = ['learned_by_pokemon.url']


parsers: List[PokeAPIParser] = [
    SpeciesParser,
    GenerationParser,
    StatParser,
    TypeParser,
    PokemonParser,
    MoveParser,
]