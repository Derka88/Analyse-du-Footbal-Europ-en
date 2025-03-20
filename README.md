# Pipeline de Données de Football avec Apache Airflow

Ce projet utilise Apache Airflow pour automatiser l'extraction et le stockage des données de football depuis l'API football-data.org.

## Prérequis

- Python 3.8 ou supérieur
- Apache Airflow
- MySQL
- Compte API football-data.org

## Installation

1. Créer un environnement virtuel :
```bash
python -m venv venv
source venv/bin/activate  # Sur Linux/Mac
.\venv\Scripts\activate   # Sur Windows
```

2. Installer les dépendances :
```bash
pip install -r requirements.txt
```

3. Initialiser la base de données Airflow :
```bash
airflow db init
```

4. Créer un utilisateur Airflow :
```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

5. Configurer les variables Airflow :
```bash
airflow variables set FOOTBALL_API_KEY "votre_clé_api"
airflow variables set MYSQL_HOST "127.0.0.1"
airflow variables set MYSQL_USER "root"
airflow variables set MYSQL_PASSWORD "root"
airflow variables set MYSQL_DATABASE "FootballData"
```

6. Copier le DAG dans le dossier dags d'Airflow :
```bash
cp football_data_dag.py ~/airflow/dags/
```

## Structure du Projet

- `football_data_dag.py` : Le DAG Airflow principal
- `requirements.txt` : Les dépendances Python
- `README.md` : Ce fichier

## Configuration

Le DAG est configuré pour s'exécuter quotidiennement à minuit (`schedule_interval='0 0 * * *'`).

Les compétitions suivantes sont traitées :
- Premier League (PL)
- Bundesliga (BL1)
- Serie A (SA)
- La Liga (PD)
- Ligue 1 (FL1)
- Champions League (CL)

## Utilisation

1. Démarrer le serveur Airflow :
```bash
airflow webserver
```

2. Démarrer le scheduler Airflow :
```bash
airflow scheduler
```

3. Accéder à l'interface web d'Airflow :
- Ouvrir un navigateur
- Aller à `http://localhost:8080`
- Se connecter avec les identifiants créés

4. Activer le DAG dans l'interface web d'Airflow

## Structure de la Base de Données

La table `matches` contient les colonnes suivantes :
- `id` : Identifiant unique auto-incrémenté
- `competition` : Nom de la compétition
- `match_date` : Date du match
- `home_team` : Équipe à domicile
- `away_team` : Équipe à l'extérieur
- `score` : Score du match
- `status` : Statut du match
- `created_at` : Date et heure d'insertion des données