# 1. Image de base figée
FROM python:3.12-slim

# 2. Variables d'environnement Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. Répertoire de travail
WORKDIR /app

# 4. Copie des dépendances
COPY requirements.txt ./

# 5. Installation des dépendances
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copie du code applicatif
COPY . ./

# 7. Création de l'utilisateur non-root
RUN useradd -u 10001 appuser && chown -R appuser:appuser /app

# 8. Bascule sur l'utilisateur non-privilégié
USER appuser

# 9. Commande par défaut
CMD ["python", "producer.py"]