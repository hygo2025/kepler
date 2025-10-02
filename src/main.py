import argparse
import os

import tensorflow as tf

from src.converter import convert_to_parquet
from src.enum.models_enum import ModelsEnum
from src.enum.operations_enum import OperationsEnum
from src.models.rnn_4rec import rnn_4rec
from src.pipelines.enrich_events import enrich_events
from src.converter.user_session import make_user_session
from src.utils.spark_session import make_spark

paths = {
    "LISTINGS_RAW_PATH": os.getenv("LISTINGS_RAW_PATH"),
    "EVENTS_RAW_PATH": os.getenv("EVENTS_RAW_PATH"),
    "EVENTS_RAW_RENTAL_PATH": os.getenv("EVENTS_RAW_RENTAL_PATH"),
    "EVENTS_PROCESSED_PATH": os.getenv("EVENTS_PROCESSED_PATH"),
    "LISTINGS_PROCESSED_PATH": os.getenv("LISTINGS_PROCESSED_PATH"),
    "ENRICHED_EVENTS_PATH": os.getenv("ENRICHED_EVENTS_PATH"),
    "USER_SESSIONS_PATH": os.getenv("USER_SESSIONS_PATH"),
    "LISTING_ID_MAPPING_PATH": os.getenv("LISTING_ID_MAPPING_PATH"),
    "USER_ID_MAPPING_PATH": os.getenv("USER_ID_MAPPING_PATH"),
}

def parse_and_validate(input_str, enum_class, item_name):
    validated_items = []
    valid_options = [member.value for member in enum_class]

    items_to_check = [item.strip() for item in input_str.split(',') if item.strip()]

    if not items_to_check:
        raise argparse.ArgumentTypeError(f"Nenhum(a) {item_name} foi fornecido(a).")

    for item in items_to_check:
        if item not in valid_options:
            raise argparse.ArgumentTypeError(
                f"{item_name.capitalize()} inválido(a): '{item}'. "
                f"As opções válidas são: {', '.join(valid_options)}"
            )
        validated_items.append(enum_class(item))

    return validated_items


def parse_and_validate_operations(operations_str):
    return parse_and_validate(operations_str, OperationsEnum, "operação")

def parse_and_validate_models(models_str):
    return parse_and_validate(models_str, ModelsEnum, "modelo")

def run_spark_job(job_func, *args, **kwargs):
    spark = make_spark()
    try:
        job_func(spark, paths, *args, **kwargs)
    finally:
        spark.stop()

def main():
    for key, value in paths.items():
        if value is None:
            raise EnvironmentError(f"A variável de ambiente '{key}' não está definida.")

    parser = argparse.ArgumentParser(
        description="Processa dados com base nas operações passadas."
    )

    parser.add_argument(
        "-o", "--operations",
        type=parse_and_validate_operations,
        help=(
            "Operações a serem executadas, separadas por vírgula. "
            f"Opções disponíveis: {', '.join([op.value for op in OperationsEnum])}"
        )
    )

    parser.add_argument(
        "-m", "--models",
        type=parse_and_validate_models,
        help=(
            "Modelos a serem executadas, separadas por vírgula. "
            f"Modelos disponíveis: {', '.join([op.value for op in ModelsEnum])}"
        )
    )

    args = parser.parse_args()

    operations=args.operations if args.operations else []
    models=args.models if args.models else []

    print(args  )

    for operation in operations:
        if operation == OperationsEnum.TO_PARQUET:
            run_spark_job(convert_to_parquet)
        if operation == OperationsEnum.MAKE_USER_SESSIONS:
            run_spark_job(make_user_session)
        if operation == OperationsEnum.ENRICH_EVENTS:
            run_spark_job(enrich_events)

    for model in models:
        print("Versão do TensorFlow:", tf.__version__)
        gpu_devices = tf.config.list_physical_devices('GPU')
        print("Num GPUs Disponíveis: ", len(gpu_devices))
        if gpu_devices:
            print("Detalhes da GPU:", gpu_devices)
        else:
            print("Nenhuma GPU foi encontrada pelo TensorFlow.")

        if model == ModelsEnum.RNN4Rec:
            rnn_4rec(paths)

if __name__ == "__main__":
    main()
