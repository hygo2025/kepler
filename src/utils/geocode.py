import json
import os

import pandas as pd
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from tqdm.auto import tqdm

from src.utils.data_utils import ensure_directory_exists
from src.utils.enviroment import geo_data_path

CACHE_FILE_PATH = os.path.join(geo_data_path(), "geocoding_cache.csv")
ensure_directory_exists(CACHE_FILE_PATH)

def load_geo_cache() -> pd.DataFrame:
    if os.path.exists(CACHE_FILE_PATH):
        cache_df = pd.read_csv(CACHE_FILE_PATH).fillna('')
    else:
        cache_df = pd.DataFrame(columns=[
            "state", "city", "neighborhood",
            "geopoint", "full_address", "raw_data_json"
        ])
    return cache_df


def geocode_new_locations(new_locs, cache_df, checkpoint_interval=5):
    if not new_locs:
        print("✨ Todas as localizações já estão no cache. Nada a fazer.")
        return cache_df

    print(f"Geocodificando {len(new_locs)} novas localidades...")

    geolocator = Nominatim(user_agent="listing_similarity_app/1.0")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.0)

    new_cache = []
    total = len(new_locs)

    for i, (state, city, neighborhood) in enumerate(
        tqdm(new_locs, desc="Geocodificando", unit="local")
    ):
        query = f"{neighborhood}, {city}, {state}, Brazil"
        try:
            loc = geocode(query)
            if loc:
                new_cache.append({
                    "state": state,
                    "city": city,
                    "neighborhood": neighborhood,
                    "geopoint": f"{loc.longitude},{loc.latitude}",
                    "full_address": loc.address,
                    "raw_data_json": json.dumps(loc.raw, ensure_ascii=False)
                })
            else:
                new_cache.append({
                    "state": state,
                    "city": city,
                    "neighborhood": neighborhood,
                    "geopoint": "",
                    "full_address": "",
                    "raw_data_json": ""
                })
        except Exception as e:
            print(f"Erro ao geocodificar '{query}': {e}")

        if (i + 1) % checkpoint_interval == 0 or (i + 1) == total:
            temp_df = pd.DataFrame(new_cache)
            updated_cache = pd.concat([cache_df, temp_df], ignore_index=True)
            updated_cache.drop_duplicates(
                subset=["state", "city", "neighborhood"], keep="last", inplace=True
            )
            updated_cache.sort_values(
                by=["state", "city", "neighborhood"], inplace=True
            )
            updated_cache.to_csv(CACHE_FILE_PATH, index=False)
            print(f"\nCheckpoint salvo ({i + 1}/{total}) em '{CACHE_FILE_PATH}'")

    print(f"Geocodificação concluída. {len(new_cache)} novas entradas adicionadas.")
    return updated_cache




