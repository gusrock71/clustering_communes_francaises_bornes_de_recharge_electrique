# Image de service pour l'API de lecture des clusters (ve_pipeline/api/),
# déployée sur Cloud Run. Volontairement séparée du reste du pipeline
# (ingestion/dbt/entraînement) : seules requirements-api.txt et le
# sous-module ve_pipeline/api/ sont copiés, pour une image légère et un build
# rapide (pas de boto3/duckdb/pyspark/dbt/scikit-learn/mlflow, voir
# ve_pipeline/api/main.py).
FROM python:3.11-slim

WORKDIR /app

# Dépendances d'abord (layer cache Docker : ne se réinstalle que si
# requirements-api.txt change, pas à chaque modification du code).
COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

# Seul ve_pipeline/api/ (+ le package parent, vide) est nécessaire à
# l'exécution de l'API -- voir docstring de ve_pipeline/api/main.py.
COPY ve_pipeline/__init__.py ve_pipeline/__init__.py
COPY ve_pipeline/api/ ve_pipeline/api/

# Cloud Run fournit le port d'écoute attendu via la variable $PORT (8080 par
# défaut) -- ne jamais coder 8080 en dur, Cloud Run peut utiliser un autre
# port selon la configuration du service.
ENV PORT=8080
EXPOSE 8080

CMD exec uvicorn ve_pipeline.api.main:app --host 0.0.0.0 --port ${PORT}
