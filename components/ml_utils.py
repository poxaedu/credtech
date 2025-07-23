import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import joblib
import logging
import streamlit as st
from google.cloud import bigquery
from typing import Dict, Tuple, Any
import os
from datetime import datetime

logger = logging.getLogger(__name__)

# Configurações do BigQuery (consistentes com data_loader.py)
PROJECT_ID = "credtech-1"
DATASET_ID = "dataclean"

class CreditRiskPredictor:
    """
    Modelo de Machine Learning para predição de risco de crédito.
    Utiliza dados históricos de inadimplência por segmento para prever
    o risco de novos clientes baseado em suas características.
    """
    
    def __init__(self):
        self.model = None
        self.label_encoders = {}
        self.scaler = StandardScaler()
        # Features atualizadas conforme train_model_clean.py
        self.feature_columns = [
            'uf', 'modalidade', 'porte', 'cnae_secao', 'cnae_subclasse',
            'total_vencido_15d_segmento',
            'total_inadimplida_arrastada_segmento', 
            'media_taxa_inadimplencia_original',
            'contagem_clientes_unicos_segmento'
        ]
        self.target_column = 'taxa_inadimplencia_final_segmento'
        self.is_trained = False
        
        # Valores padrão apenas para PJ (conforme train_model_clean.py)
        self.default_values = {
            'Médio': {
                'total_vencido_15d_segmento': 0,
                'total_inadimplida_arrastada_segmento': 0,
                'media_taxa_inadimplencia_original': 0.000000,
                'contagem_clientes_unicos_segmento': 1
            },
            'Pequeno': {
                'total_vencido_15d_segmento': 11045,
                'total_inadimplida_arrastada_segmento': 3207,
                'media_taxa_inadimplencia_original': 0.031025,
                'contagem_clientes_unicos_segmento': 1
            },
            'Indisponível': {
                'total_vencido_15d_segmento': 47399,
                'total_inadimplida_arrastada_segmento': 42163,
                'media_taxa_inadimplencia_original': 0.149176,
                'contagem_clientes_unicos_segmento': 1
            },
            'Micro': {
                'total_vencido_15d_segmento': 1266,
                'total_inadimplida_arrastada_segmento': 0,
                'media_taxa_inadimplencia_original': 0.018932,
                'contagem_clientes_unicos_segmento': 1
            },
            'Grande': {
                'total_vencido_15d_segmento': 0,
                'total_inadimplida_arrastada_segmento': 0,
                'media_taxa_inadimplencia_original': 0.000000,
                'contagem_clientes_unicos_segmento': 1
            }
        }
        
        # Tenta carregar modelo existente automaticamente
        self._try_load_existing_model()
    
    def _try_load_existing_model(self):
        """
        Tenta carregar um modelo existente na inicialização.
        """
        model_path = "models/credit_risk_model.pkl"
        if os.path.exists(model_path):
            try:
                self.load_model(model_path)
                logger.info("Modelo carregado automaticamente na inicialização")
            except Exception as e:
                logger.warning(f"Falha ao carregar modelo existente: {e}")
    
    def is_model_available(self) -> bool:
        """
        Verifica se o modelo está disponível e treinado.
        """
        return self.is_trained and self.model is not None
    
    def get_model_info(self) -> dict:
        """
        Retorna informações sobre o modelo carregado.
        """
        model_path = "models/credit_risk_model.pkl"
        info = {
            'is_available': self.is_model_available(),
            'model_path': model_path,
            'model_exists': os.path.exists(model_path)
        }
        
        if info['model_exists']:
            try:
                stat = os.stat(model_path)
                info['last_modified'] = datetime.fromtimestamp(stat.st_mtime)
                info['file_size'] = stat.st_size
            except:
                pass
        
        return info

    def load_training_data(self, client: bigquery.Client) -> pd.DataFrame:
        """
        Carrega dados de treinamento do BigQuery (conforme train_model_clean.py).
        """
        logger.info("Carregando dados para treinamento do modelo ML...")
        
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
        
        try:
            df = client.query(query).to_dataframe()
            logger.info(f"Dados carregados: {len(df)} registros")
            return df
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {e}")
            return pd.DataFrame()
    
    def preprocess_data(self, df: pd.DataFrame, is_training: bool = True) -> pd.DataFrame:
        """
        Preprocessa os dados para o modelo (conforme train_model_clean.py).
        """
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
    
    def train_model(self, client: bigquery.Client) -> Dict[str, Any]:
        """
        Treina o modelo de machine learning (conforme train_model_clean.py).
        """
        logger.info("Iniciando treinamento do modelo...")
        
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
        
        # Divide dados em treino e teste
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Treina modelo (usando Random Forest)
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
        cv_scores = cross_val_score(self.model, X_train, y_train, cv=5, scoring='r2')
        metrics['cv_r2_mean'] = cv_scores.mean()
        metrics['cv_r2_std'] = cv_scores.std()
        
        self.is_trained = True
        logger.info(f"Modelo treinado com sucesso. R² = {metrics['r2_score']:.4f}")
        
        return metrics
    
    def predict_risk(self, input_data: Dict[str, str]) -> Dict[str, Any]:
        """
        Prediz o risco de crédito para um novo cliente PJ.
        
        Args:
            input_data: Dicionário com as características do cliente
                       {'uf': 'SP', 'modalidade': 'Cartão de Crédito', 
                        'porte': 'Pequeno', 'cnae_secao': 'A', 'cnae_subclasse': '01111'}
        
        Returns:
            Dicionário com a predição e informações adicionais
        """
        if not self.is_trained:
            raise ValueError("Modelo não foi treinado. Execute train_model() primeiro.")
        
        # Prepara dados de entrada com valores padrão
        processed_input = input_data.copy()
        
        # Adiciona valores padrão baseados no porte
        porte = input_data.get('porte', 'Indisponível')
        if porte in self.default_values:
            for key, value in self.default_values[porte].items():
                processed_input[key] = value
        else:
            # Valores padrão genéricos
            processed_input.update({
                'total_vencido_15d_segmento': 0,
                'total_inadimplida_arrastada_segmento': 0,
                'media_taxa_inadimplencia_original': 0.0,
                'contagem_clientes_unicos_segmento': 1
            })
        
        # Converte input para DataFrame
        df_input = pd.DataFrame([processed_input])
        
        # Preprocessa
        df_processed = self.preprocess_data(df_input, is_training=False)
        
        # Garante que todas as features necessárias estão presentes
        for feature in self.feature_columns:
            if feature not in df_processed.columns:
                # Se a feature não existe, adiciona com valor padrão
                if feature in ['total_vencido_15d_segmento', 'total_inadimplida_arrastada_segmento', 
                              'media_taxa_inadimplencia_original', 'contagem_clientes_unicos_segmento']:
                    df_processed[feature] = 0
                else:
                    df_processed[feature] = 0  # Para features categóricas já encodadas
        
        # Seleciona features na ordem correta
        X = df_processed[self.feature_columns]
        
        # Predição
        risk_prediction = self.model.predict(X)[0]
        
        # Calcula intervalo de confiança (aproximado)
        predictions_trees = np.array([tree.predict(X)[0] for tree in self.model.estimators_])
        confidence_interval = {
            'lower': np.percentile(predictions_trees, 25),
            'upper': np.percentile(predictions_trees, 75)
        }
        
        # Classifica o risco
        if risk_prediction <= 0.02:  # 2%
            risk_category = "BAIXO"
            risk_color = "green"
        elif risk_prediction <= 0.05:  # 5%
            risk_category = "MÉDIO"
            risk_color = "orange"
        else:
            risk_category = "ALTO"
            risk_color = "red"
        
        return {
            'risk_percentage': risk_prediction * 100,  # Converte para porcentagem
            'risk_category': risk_category,
            'risk_color': risk_color,
            'confidence_interval': {
                'lower': confidence_interval['lower'] * 100,
                'upper': confidence_interval['upper'] * 100
            },
            'input_data': input_data,
            'processed_input': processed_input
        }
    
    def save_model(self, filepath: str):
        """
        Salva o modelo treinado.
        """
        if not self.is_trained:
            raise ValueError("Modelo não foi treinado")
        
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
        logger.info(f"Modelo salvo em: {filepath}")
    
    def load_model(self, filepath: str):
        """
        Carrega um modelo previamente treinado.
        """
        try:
            model_data = joblib.load(filepath)
            self.model = model_data['model']
            self.label_encoders = model_data['label_encoders']
            self.scaler = model_data['scaler']
            self.feature_columns = model_data['feature_columns']
            self.target_column = model_data['target_column']
            self.is_trained = model_data['is_trained']
            # Carrega valores padrão se existirem
            if 'default_values' in model_data:
                self.default_values = model_data['default_values']
            logger.info(f"Modelo carregado de: {filepath}")
        except Exception as e:
            logger.error(f"Erro ao carregar modelo: {e}")
            raise

# Instância global do preditor
credit_risk_predictor = CreditRiskPredictor()

@st.cache_data(ttl=3600)
def get_unique_values_for_features(_client: bigquery.Client, force_refresh: bool = False) -> Dict[str, list]:
    """
    Busca valores únicos para cada feature para popular os seletores.
    Focado apenas em PJ conforme train_model_clean.py
    """
    
    query = f"""
    SELECT DISTINCT
        uf,
        modalidade,
        porte,
        cnae_secao,
        cnae_subclasse
    FROM `{PROJECT_ID}.{DATASET_ID}.ft_scr_agregado_mensal`
    WHERE uf IS NOT NULL
        AND cliente = 'PJ'
    ORDER BY uf, modalidade, porte, cnae_secao, cnae_subclasse
    """
    
    try:
        df = _client.query(query).to_dataframe()
        
        # Aplica tratamento de caracteres de replacement
        def substituir_replacement_char(df: pd.DataFrame) -> pd.DataFrame:
            for col in df.select_dtypes(include="object").columns:
                df[col] = df[col].str.replace('�', 'ç', regex=False)
            return df
        
        df = substituir_replacement_char(df)
        
        # Cria dicionário com valores únicos para cada coluna
        unique_values = {}
        for col in ['uf', 'modalidade', 'porte', 'cnae_secao', 'cnae_subclasse']:
            if col in df.columns:
                unique_values[col] = sorted(df[col].dropna().unique().tolist())
        
        return unique_values
        
    except Exception as e:
        return {}