from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import mysql.connector
from airflow.models import Variable

# Configuration par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Dictionnaire des compétitions
COMPETITIONS = {
    "PL": "Premier League",
    "BL1": "Bundesliga",
    "SA": "Serie A",
    "PD": "La Liga",
    "FL1": "Ligue 1",
    "CL": "Champions League"
}

def get_matches(competition_id):
    """Récupère les matchs d'une compétition depuis l'API"""
    api_key = Variable.get("FOOTBALL_API_KEY")
    base_url = "https://api.football-data.org/v4"
    headers = {"X-Auth-Token": api_key}
    
    url = f"{base_url}/competitions/{competition_id}/matches"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data["matches"])
    else:
        print(f"Erreur pour la compétition {competition_id}:", response.status_code, response.text)
        return None

def save_to_mysql(matches_info, competition_name):
    """Sauvegarde les données dans MySQL"""
    try:
        conn = mysql.connector.connect(
            host=Variable.get("MYSQL_HOST"),
            user=Variable.get("MYSQL_USER"),
            password=Variable.get("MYSQL_PASSWORD"),
            database=Variable.get("MYSQL_DATABASE")
        )
        
        cursor = conn.cursor()
        
        # Préparation de la requête d'insertion
        insert_query = """
        INSERT INTO matches (competition, match_date, home_team, away_team, score, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        # Insertion des données
        for _, row in matches_info.iterrows():
            data = (
                competition_name,
                row['Date'],
                row['Domicile'],
                row['Extérieur'],
                row['Score'],
                row['Statut']
            )
            cursor.execute(insert_query, data)
        
        conn.commit()
        print(f"Données de {competition_name} sauvegardées avec succès!")
        
    except Exception as e:
        print(f"Erreur lors de la sauvegarde des données: {str(e)}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def process_competition(**context):
    """Traite une compétition spécifique"""
    competition_id = context['competition_id']
    competition_name = COMPETITIONS[competition_id]
    
    print(f"Traitement de {competition_name}...")
    df_matches = get_matches(competition_id)
    
    if df_matches is not None and not df_matches.empty:
        matches_info = pd.DataFrame({
            'Date': pd.to_datetime(df_matches['utcDate']).dt.strftime('%Y-%m-%d'),
            'Domicile': df_matches['homeTeam'].apply(lambda x: x['name']),
            'Score': df_matches.apply(lambda x: f"{x['score']['fullTime']['home']} - {x['score']['fullTime']['away']}", axis=1),
            'Extérieur': df_matches['awayTeam'].apply(lambda x: x['name']),
            'Statut': df_matches['status']
        })
        
        save_to_mysql(matches_info, competition_name)
        print(f"Traitement de {competition_name} terminé avec succès!")
    else:
        print(f"Aucune donnée disponible pour {competition_name}")

# Création du DAG
dag = DAG(
    'football_data_pipeline',
    default_args=default_args,
    description='Pipeline d\'extraction des données de football',
    schedule_interval='0 0 * * *',  # Exécution quotidienne à minuit
    catchup=False,
    tags=['football', 'data'],
)

# Création des tâches pour chaque compétition
competition_tasks = []
for competition_id in COMPETITIONS.keys():
    task = PythonOperator(
        task_id=f'process_{competition_id.lower()}',
        python_callable=process_competition,
        op_kwargs={'competition_id': competition_id},
        dag=dag,
    )
    competition_tasks.append(task)

# Les tâches peuvent s'exécuter en parallèle car elles sont indépendantes 