import argparse

from src.converter import prepare_data
from src.enum.models_enum import ModelsEnum
from src.enum.operations_enum import OperationsEnum
from src.converter.enrich_events import enrich_events
from src.converter.user_session import make_user_session
from src.utils.spark_session import make_spark

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
        job_func(spark, *args, **kwargs)
    finally:
        spark.stop()

def main():
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
        if operation == OperationsEnum.PREPARE_DATA:
            run_spark_job(prepare_data)

    # for model in models:
    #     print("Versão do TensorFlow:", tf.__version__)
    #     gpu_devices = tf.config.list_physical_devices('GPU')
    #     print("Num GPUs Disponíveis: ", len(gpu_devices))
    #     if gpu_devices:
    #         print("Detalhes da GPU:", gpu_devices)
    #     else:
    #         print("Nenhuma GPU foi encontrada pelo TensorFlow.")


if __name__ == "__main__":
    main()
