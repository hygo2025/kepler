import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

import matplotlib.pyplot as plt
import pandas as pd
from libreco.algorithms import RNN4Rec
from libreco.data import DatasetPure
from libreco.evaluation import evaluate
import tensorflow as tf

# Ativa o compilador XLA (Accelerated Linear Algebra)
# Isso pode otimizar o grafo computacional para a GPU
tf.config.optimizer.set_jit(True)

print("Otimizações XLA e Mixed Precision ativadas.")

def prepare_data_for_libreco(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df_exploded = df.explode('sequence', ignore_index=True)
    event_data = pd.json_normalize(df_exploded['sequence'])
    df_combined = df_exploded[['anonymized_user_id']].join(event_data)
    final_df = pd.DataFrame({
        'user': df_combined['anonymized_user_id'],
        'item': df_combined['listing_id'],
        'label': 1,  # Adiciona a coluna 'label' com valor constante 1
        'time': df_combined['event_ts'].astype('int64')  # Garante o tipo correto
    })
    return final_df


def plot_training_history(history_dict: dict, metrics: list, title: str, save_path: str):
    """Plota a evolução das métricas de validação ao longo das épocas."""
    history_df = pd.DataFrame(history_dict)
    plt.figure(figsize=(12, 7))
    for metric in metrics:
        if metric in history_df.columns:
            plt.plot(history_df.index + 1, history_df[metric], marker='o', linestyle='--', label=metric)
    plt.title(title, fontsize=16)
    plt.xlabel("Época")
    plt.ylabel("Score da Métrica")
    plt.grid(True)
    plt.xticks(history_df.index + 1)
    plt.legend()
    plt.tight_layout()
    plt.savefig(save_path)
    print(f"\nGráfico salvo em: {save_path}")
    plt.show()


def rnn_4rec(paths: dict):
    train_df = prepare_data_for_libreco(paths["TRAIN_DATA_PATH"])
    validation_df = prepare_data_for_libreco(paths["VALIDATION_DATA_PATH"])

    print("Filtrando validação para uma avaliação 'warm-start'...")
    train_users = set(train_df["user"])
    validation_df_warm = validation_df[validation_df["user"].isin(train_users)]

    print("Construindo datasets da LibRecommender...")
    train_data, data_info = DatasetPure.build_trainset(train_df)
    validation_data = DatasetPure.build_testset(validation_df_warm)
    print(data_info)

    # --- 3. Treinamento do Modelo ---
    rnn = RNN4Rec(task="ranking", data_info=data_info, rnn_type="gru", loss_type="bpr",
                  embed_size=256, hidden_units=(512, 256), n_epochs=50, lr=0.001,
                  batch_size=2048, recent_num=20, sampler="unconsumed", num_neg=5)

    print("\nIniciando treinamento e avaliação na validação...")
    rnn.fit(train_data, neg_sampling=True, verbose=2, eval_data=validation_data,
            metrics=["precision", "recall", "ndcg", "map"])

    # --- 4. Visualização dos Resultados ---
    plot_training_history(rnn.eval_results, ['ndcg', 'recall', 'precision'],
                          "Performance de Validação por Época", "validation_performance.png")

    # --- 5. Avaliação Final no Conjunto de Teste ---
    print("\n--- Iniciando Avaliação Final no Conjunto de Teste ---")
    test_df = prepare_data_for_libreco(paths["TEST_DATA_PATH"])
    # Filtra o teste para usuários de treino para uma avaliação justa
    test_df_warm = test_df[test_df["user"].isin(train_users)]
    test_data = DatasetPure.build_testset(test_df_warm)

    test_results = evaluate(model=rnn, data=test_data, neg_sampling=True,
                            metrics=["precision", "recall", "ndcg", "map"])

    print("\n--- RESULTADO FINAL NO CONJUNTO DE TESTE ---")
    print(test_results)