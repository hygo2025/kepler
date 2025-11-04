from __future__ import annotations

import json
import os
import requests
import time

import pandas as pd
from tqdm.auto import tqdm

from src.utils.data_utils import ensure_directory_exists
from src.utils.enviroment import geo_data_path

CEP_CACHE_PATH = os.path.join(geo_data_path(), "cep_geocache.csv")
ensure_directory_exists(CEP_CACHE_PATH)
CEP_CACHE_COLUMNS = [
    'cep',
    'state',
    'city',
    'neighborhood',
    'street',
    'service',
    'location_type',
    'longitude',
    'latitude',
    'raw_data_json'
]

def _geocode_cep_brasilapi(cep: str) -> dict | None:
    if not cep or not cep.isdigit():
        return None

    url = f"https://brasilapi.com.br/api/cep/v2/{cep}"

    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()

        data = response.json()

        flat_data = {
            'cep': data.get('cep'),
            'state': data.get('state'),
            'city': data.get('city'),
            'neighborhood': data.get('neighborhood'),
            'street': data.get('street'),
            'service': data.get('service'),
            'location_type': None,
            'longitude': None,
            'latitude': None,
            'raw_data_json': json.dumps(data)
        }

        if data.get("location"):
            flat_data['location_type'] = data["location"].get("type")

            if data["location"].get("coordinates"):
                coords = data["location"]["coordinates"]
                flat_data['longitude'] = coords.get("longitude")
                flat_data['latitude'] = coords.get("latitude")

        return flat_data

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"\nCEP {cep} não encontrado na BrasilAPI.")
        else:
            print(f"\nErro HTTP ao buscar CEP {cep}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"\nErro de conexão ao buscar CEP {cep}: {e}")

    return None


def load_cep_cache() -> pd.DataFrame:
    print(f"\nCarregando cache de CEPs de: {CEP_CACHE_PATH}")

    if not os.path.exists(CEP_CACHE_PATH):
        print("\nArquivo de cache de CEPs não encontrado. Criando um novo.")
        return pd.DataFrame(columns=CEP_CACHE_COLUMNS)

    try:
        cache_df = pd.read_csv(CEP_CACHE_PATH, dtype=str)
        cache_df = cache_df.reindex(columns=CEP_CACHE_COLUMNS)
        cache_df = cache_df.fillna('')
        cache_df['cep'] = cache_df['cep'].astype(str)

        print(f"\nCache de CEPs carregado. {len(cache_df)} registros encontrados.")
        return cache_df

    except Exception as e:
        print(f"\nErro ao ler cache de CEPs, iniciando um novo: {e}")
        return pd.DataFrame(columns=CEP_CACHE_COLUMNS)


def geocode_new_ceps(new_ceps_list: list, cache_df: pd.DataFrame, checkpoint_interval=5) -> pd.DataFrame:
    if not new_ceps_list:
        print("\nNenhum CEP novo para geocodificar.")
        return cache_df

    print(f"\nGeocodificando {len(new_ceps_list)} novos CEPs...")

    new_results = []
    total = len(new_ceps_list)

    updated_cache = cache_df.copy()

    for i, cep in enumerate(tqdm(new_ceps_list, desc="Geocodificando CEPs")):
        api_data = _geocode_cep_brasilapi(cep)

        if api_data:
            new_results.append(api_data)
        else:
            failed_entry = {col: '' for col in CEP_CACHE_COLUMNS}
            failed_entry['cep'] = cep
            new_results.append(failed_entry)

        time.sleep(0.05)

        if (i + 1) % checkpoint_interval == 0 or (i + 1) == total:

            if not new_results:
                continue

            temp_df = pd.DataFrame(new_results, columns=CEP_CACHE_COLUMNS)
            updated_cache = pd.concat([updated_cache, temp_df], ignore_index=True)
            updated_cache.drop_duplicates(subset=["cep"], keep="last", inplace=True)
            updated_cache.sort_values(by=["cep"], inplace=True)

            try:
                updated_cache.to_csv(CEP_CACHE_PATH, index=False)
            except Exception as e:
                print(f"\n\nERRO: Não foi possível salvar o checkpoint de CEPs: {e}")

            new_results = []

    print(f"\nGeocodificação de CEPs concluída.")
    return updated_cache