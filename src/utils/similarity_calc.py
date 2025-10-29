import pandas as pd
import ast


class PropertySimilarityCalculator:
    def __init__(self, dataframe, id_column, numerical_features, categorical_features, amenities_column='amenities'):
        self.id_column = id_column
        self.numerical_features = numerical_features
        self.categorical_features = categorical_features
        self.amenities_column = amenities_column

        self.df = dataframe.copy()

        self.df[f'{self.amenities_column}_set'] = self.df[self.amenities_column].apply(self._safe_literal_eval).apply(
            set)

        for col in self.numerical_features:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)

        self.df.set_index(self.id_column, inplace=True)

        self.value_ranges = {col: self.df[col].max() - self.df[col].min() for col in self.numerical_features}

        print("Calculador de Similaridade pronto!")

    @staticmethod
    def _safe_literal_eval(s):
        try:
            return ast.literal_eval(s)
        except (ValueError, SyntaxError, TypeError):
            return []

    @staticmethod
    def _numerical_similarity(val1, val2, val_range):
        if val_range == 0:
            return 1.0
        return 1 - (abs(val1 - val2) / val_range)

    @staticmethod
    def _categorical_similarity(cat1, cat2):
        return 1.0 if cat1 == cat2 else 0.0

    @staticmethod
    def _jaccard_similarity(set1, set2):
        if not isinstance(set1, set) or not isinstance(set2, set):
            return 0.0
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        return intersection / union if union != 0 else 1.0

    def calculate_similarity(self, prop_id1, prop_id2, weights):
        try:
            prop1 = self.df.loc[prop_id1]
            prop2 = self.df.loc[prop_id2]
        except KeyError as e:
            print(f"Erro: ID do imóvel não encontrado: {e}")
            return None

        total_score = 0
        total_weight = 0

        for feature in self.numerical_features:
            if feature in weights:
                sim = self._numerical_similarity(prop1[feature], prop2[feature], self.value_ranges[feature])
                total_score += sim * weights[feature]
                total_weight += weights[feature]

        for feature in self.categorical_features:
            if feature in weights:
                sim = self._categorical_similarity(prop1[feature], prop2[feature])
                total_score += sim * weights[feature]
                total_weight += weights[feature]

        if self.amenities_column in weights:
            sim = self._jaccard_similarity(prop1[f'{self.amenities_column}_set'], prop2[f'{self.amenities_column}_set'])
            total_score += sim * weights[self.amenities_column]
            total_weight += weights[self.amenities_column]

        return total_score / total_weight if total_weight > 0 else 0.0

    def analyze_recommendations(self, base_property_id, recommended_ids, weights):
        results = []
        for rec_id in recommended_ids:
            if base_property_id == rec_id:
                continue

            score = self.calculate_similarity(base_property_id, rec_id, weights)
            if score is not None:
                results.append({'recommended_id': rec_id, 'similarity_score': score})

        if not results:
            return pd.DataFrame(columns=['recommended_id', 'similarity_score'])

        return pd.DataFrame(results).sort_values(by='similarity_score', ascending=False).reset_index(drop=True)