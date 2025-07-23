import os
import pandas as pd
from google.cloud import bigquery
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import joblib
import numpy as np
from datetime import datetime

# Configurar Google Cloud
os.environ['GOOGLE_CLOUD_PROJECT'] = 'credtech-1'

# Configurações
PROJECT_ID = "credtech-1"
DATASET_ID = "dataclean"

class CreditRiskPredictor:
    def __init__(self):
        self.model = None
        self.label_encoders = {}
        self.scaler = StandardScaler()
        self.feature_columns = [
            'uf', 'modalidade', 'porte', 'cnae_secao', 'cnae_subclasse',
            'total_vencido_15d_segmento',
            'total_inadimplida_arrastada_segmento', 
            'media_taxa_inadimplencia_original',
            'contagem_clientes_unicos_segmento'
        ]
        self.target_column = 'taxa_inadimplencia_final_segmento'
        self.is_trained = False
        
        # Valores padrão apenas para PJ
        self.default_values = {
            'PJ - Médio': {
                'total_vencido_15d_segmento': 0,
                'total_inadimplida_arrastada_segmento': 0,
                'media_taxa_inadimplencia_original': 0.000000,
                'contagem_clientes_unicos_segmento': 1
            },
            'PJ - Pequeno': {
                'total_vencido_15d_segmento': 11045,
                'total_inadimplida_arrastada_segmento': 3207,
                'media_taxa_inadimplencia_original': 0.031025,
                'contagem_clientes_unicos_segmento': 1
            },
            'PJ - Indisponível': {
                'total_vencido_15d_segmento': 47399,
                'total_inadimplida_arrastada_segmento': 42163,
                'media_taxa_inadimplencia_original': 0.149176,
                'contagem_clientes_unicos_segmento': 1
            },
            'PJ - Micro': {
                'total_vencido_15d_segmento': 1266,
                'total_inadimplida_arrastada_segmento': 0,
                'media_taxa_inadimplencia_original': 0.018932,
                'contagem_clientes_unicos_segmento': 1
            },
            'PJ - Grande': {
                'total_vencido_15d_segmento': 0,
                'total_inadimplida_arrastada_segmento': 0,
                'media_taxa_inadimplencia_original': 0.000000,
                'contagem_clientes_unicos_segmento': 1
            }
        }
        
    def load_training_data(self, client: bigquery.Client) -> pd.DataFrame:
        print("📊 Carregando dados para treinamento...")
        
        query = f"""
        SELECT 
            uf,
            modalidade,
            porte,
            cnae_secao,
            cnae_subclasse,
            taxa_inadimplencia_final_segmento,
            total_carteira_ativa_segmento,
            total_vencido_15d_segmento,
            total_inadimplida_arrastada_segmento,
            media_taxa_inadimplencia_original,
            contagem_clientes_unicos_segmento
        FROM `{PROJECT_ID}.{DATASET_ID}.ft_scr_agregado_mensal`
        WHERE taxa_inadimplencia_final_segmento IS NOT NULL
            AND total_carteira_ativa_segmento > 1000
            AND taxa_inadimplencia_final_segmento BETWEEN 0 AND 1
            AND cliente = 'PJ'
        """
        
        df = client.query(query).to_dataframe()
        print(f"✅ Dados carregados: {len(df):,} registros")
        return df
    
    def preprocess_data(self, df: pd.DataFrame, is_training: bool = True) -> pd.DataFrame:
        df_processed = df.copy()
        
        # Features categóricas e numéricas
        categorical_features = ['uf', 'modalidade', 'porte', 'cnae_secao', 'cnae_subclasse']
        numerical_features = ['total_vencido_15d_segmento', 'total_inadimplida_arrastada_segmento', 
                             'media_taxa_inadimplencia_original', 'contagem_clientes_unicos_segmento']
        
        # Trata valores nulos nas categóricas
        for col in categorical_features:
            if col in df_processed.columns:
                df_processed[col] = df_processed[col].fillna('UNKNOWN')
        
        # Trata valores nulos nas numéricas
        for col in numerical_features:
            if col in df_processed.columns:
                df_processed[col] = df_processed[col].fillna(0)
        
        # Encoding apenas das variáveis categóricas
        for col in categorical_features:
            if col in df_processed.columns:
                if is_training:
                    le = LabelEncoder()
                    df_processed[col] = le.fit_transform(df_processed[col].astype(str))
                    self.label_encoders[col] = le
                else:
                    if col in self.label_encoders:
                        le = self.label_encoders[col]
                        unique_values = set(le.classes_)
                        df_processed[col] = df_processed[col].astype(str).apply(
                            lambda x: x if x in unique_values else 'UNKNOWN'
                        )
                        if 'UNKNOWN' not in unique_values:
                            le.classes_ = np.append(le.classes_, 'UNKNOWN')
                        df_processed[col] = le.transform(df_processed[col])
        
        return df_processed
    
    def train_model(self, client: bigquery.Client) -> dict:
        print("🤖 Iniciando treinamento do modelo...")
        
        # Carrega dados
        df = self.load_training_data(client)
        if df.empty:
            raise ValueError("Não foi possível carregar dados para treinamento")
        
        # Preprocessa dados
        df_processed = self.preprocess_data(df, is_training=True)
        
        # Separa features e target
        available_features = [col for col in self.feature_columns if col in df_processed.columns]
        X = df_processed[available_features]
        y = df_processed[self.target_column]
        
        print(f"📋 Features utilizadas: {available_features}")
        
        # Divide dados em treino e teste
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Treina modelo
        print("🔄 Treinando Random Forest...")
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1
        )
        
        self.model.fit(X_train, y_train)
        
        # Avalia modelo
        y_pred = self.model.predict(X_test)
        
        metrics = {
            'r2_score': r2_score(y_test, y_pred),
            'mse': mean_squared_error(y_test, y_pred),
            'mae': mean_absolute_error(y_test, y_pred),
            'rmse': np.sqrt(mean_squared_error(y_test, y_pred)),
            'feature_importance': dict(zip(available_features, self.model.feature_importances_)),
            'n_samples': len(df),
            'n_features': len(available_features)
        }
        
        # Cross-validation
        print("🔍 Executando validação cruzada...")
        cv_scores = cross_val_score(self.model, X_train, y_train, cv=5, scoring='r2')
        metrics['cv_r2_mean'] = cv_scores.mean()
        metrics['cv_r2_std'] = cv_scores.std()
        
        self.is_trained = True
        print(f"✅ Modelo treinado! R² = {metrics['r2_score']:.4f}")
        
        return metrics
    
    def save_model(self, filepath: str):
        model_data = {
            'model': self.model,
            'label_encoders': self.label_encoders,
            'scaler': self.scaler,
            'feature_columns': self.feature_columns,
            'target_column': self.target_column,
            'is_trained': self.is_trained,
            'default_values': self.default_values
        }
        
        joblib.dump(model_data, filepath)
        print(f"💾 Modelo salvo em: {filepath}")

def train_and_evaluate_model():
    print("🚀 Iniciando treinamento do modelo de risco de crédito...")
    
    try:
        # Cria cliente BigQuery
        client = bigquery.Client()
        
        # Cria instância do preditor
        predictor = CreditRiskPredictor()
        
        # Treina o modelo
        metrics = predictor.train_model(client)
        
        # Exibe métricas
        print("\n" + "="*50)
        print("📈 MÉTRICAS DE AVALIAÇÃO DO MODELO")
        print("="*50)
        print(f"R² Score (Teste): {metrics['r2_score']:.4f}")
        print(f"R² Score (CV): {metrics['cv_r2_mean']:.4f} ± {metrics['cv_r2_std']:.4f}")
        print(f"RMSE: {metrics['rmse']:.6f}")
        print(f"MAE: {metrics['mae']:.6f}")
        print(f"Amostras: {metrics['n_samples']:,}")
        print(f"Features: {metrics['n_features']}")
        
        # Avalia qualidade
        r2_score_val = metrics['r2_score']
        cv_r2 = metrics['cv_r2_mean']
        
        print("\n" + "="*50)
        print("🎯 AVALIAÇÃO DA QUALIDADE")
        print("="*50)
        
        if r2_score_val >= 0.7 and cv_r2 >= 0.65:
            quality = "EXCELENTE ✅"
        elif r2_score_val >= 0.5 and cv_r2 >= 0.45:
            quality = "BOM ⚠️"
        elif r2_score_val >= 0.3 and cv_r2 >= 0.25:
            quality = "REGULAR ⚠️"
        else:
            quality = "RUIM ❌"
        
        print(f"Qualidade: {quality}")
        
        # Verifica overfitting
        gap = r2_score_val - cv_r2
        if gap > 0.1:
            print(f"⚠️ Possível overfitting (gap: {gap:.3f})")
        else:
            print(f"✅ Sem overfitting (gap: {gap:.3f})")
        
        # Importância das features
        print("\n🔍 IMPORTÂNCIA DAS FEATURES:")
        for feature, importance in sorted(metrics['feature_importance'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {feature}: {importance:.4f}")
        
        # Salva modelo
        if not os.path.exists("models"):
            os.makedirs("models")
        
        predictor.save_model("models/credit_risk_model.pkl")
        
        print("\n🎉 Treinamento concluído com sucesso!")
        return True
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

if __name__ == "__main__":
    train_and_evaluate_model()