from pathlib import Path

import pandas as pd

from src.utils.config import Config


def convert_to_parquet(
        suffix: str,
        config: Config,
        glob_pattern: str = ""
):
    origin_path = Path(f"{config.data_path}/{suffix}")
    out_base = Path(config.parquet_path) / suffix
    out_base.mkdir(parents=True, exist_ok=True)

    files = sorted(origin_path.glob(glob_pattern))
    total = len(files)

    for idx, file in enumerate(files, start=1):
        print(f"[{idx}/{total}] Convertendo {file.name}...")
        df = pd.read_csv(file, compression="gzip", low_memory=False)

        out = out_base / f"{file.stem}.parquet"
        df.to_parquet(out, index=False, engine="pyarrow", compression="snappy")


if __name__ == "__main__":
    convert_to_parquet(
        suffix="listings",
        config=Config.from_args(Config.parse_args()),
        glob_pattern="listings_es_*.csv.gz"
    )

    convert_to_parquet(
        suffix="events",
        config=Config.from_args(Config.parse_args()),
        glob_pattern="clickstream_zap_*.csv.gz"
    )
