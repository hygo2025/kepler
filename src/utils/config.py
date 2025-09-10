import argparse
import os
from pathlib import Path

class Config:
    def __init__(self,
                 top_k: int = 10,
                 shuffle_parts: int = 8,
                 default_parts: int = 8,
                 concurrent_gpu_tasks: int = 3,
                 jar_path: str = "",
                 data_path: str = "",
                 use_gpu: bool = False,
                 ):
        self.top_k = top_k
        self.shuffle_parts = shuffle_parts
        self.default_parts = default_parts
        self.concurrent_gpu_tasks = concurrent_gpu_tasks
        self.jar_path = jar_path
        self.data_path = data_path
        base = Path(__file__).resolve().parent
        self.parquet_path =  (base / "../data").resolve()
        self.listings_path = f"{self.parquet_path}/listings"
        self.events_path = f"{self.parquet_path}/events"
        self.analytics_path = "../analytics"

        self.use_gpu = use_gpu

    @staticmethod
    def from_args(args):
        return Config(
            top_k=args.topk,
            shuffle_parts=args.shuffle_parts,
            default_parts=args.default_parts,
            concurrent_gpu_tasks=args.concurrent_gpu_tasks,
            jar_path=args.rapids_jar_path,
            data_path=args.data_path,
            use_gpu=args.use_gpu,
        )

    @staticmethod
    def parse_args():
        default_path_jar = os.environ.get("RAPIDS_JAR_PATH")
        default_data_path = os.environ.get("DATA_PATH")
        p = argparse.ArgumentParser("RAPIDS benchmark seguro (single-node)")
        p.add_argument("--topk", type=int, default=10)
        p.add_argument("--shuffle-parts", type=int, default=8, help="spark.sql.shuffle.partitions")
        p.add_argument("--default-parts", type=int, default=8, help="spark.default.parallelism")
        p.add_argument("--concurrent-gpu-tasks", type=int, default=3)
        p.add_argument("--rapids-jar-path", type=str, default=default_path_jar, help="Path to RAPIDS jar")
        p.add_argument("--data-path", type=str, default=default_data_path, help="Path to data files")
        p.add_argument("--use-gpu", type=bool, default=False, help="Use GPU (RAPIDS)")
        return p.parse_args()