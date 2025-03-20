import requests
import pandas as pd
import mysql.connector
from datetime import datetime

# Ta clé API (à récupérer sur le site)
API_KEY = "3b4d55318c19489f9690c28016ef8f46"
BASE_URL = "https://api.football-data.org/v4"

# Dictionnaire des compétitions disponibles
COMPETITIONS = {
    "PL": "Premier League",
    "BL1": "Bundesliga",
    "SA": "Serie A",
    "PD": "La Liga",
    "FL1": "Ligue 1",
    "CL": "Champions League"
}

# Headers avec l'API Key
headers = {
    "X-Auth-Token": API_KEY
}

def initialize_database():
    try:
        print("Initialisation de la base de données...")
        conn = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="root",
            database="FootballData"
        )
        cursor = conn.cursor()
        
        # Suppression de la table si elle existe
        cursor.execute("DROP TABLE IF EXISTS matches")
        print("Table matches supprimée si elle existait")
        
        # Création de la table
        create_table_query = """
        CREATE TABLE matches (
            id INT AUTO_INCREMENT PRIMARY KEY,
            competition VARCHAR(50),
            match_date DATE,
            home_team VARCHAR(100),
            away_team VARCHAR(100),
            score VARCHAR(20),
            status VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_query)
        print("Table matches créée avec succès")
        
        conn.commit()
    except mysql.connector.Error as err:
        print(f"Erreur MySQL lors de l'initialisation: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Connexion MySQL fermée.")

# Fonction pour récupérer les matchs d'une compétition
def get_matches(competition_id="PL"):  # "PL" pour Premier League
    print(f"Tentative de récupération des données pour {competition_id}...")
    url = f"{BASE_URL}/competitions/{competition_id}/matches"
    print(f"URL de l'API : {url}")
    
    try:
        response = requests.get(url, headers=headers)
        print(f"Code de statut de la réponse : {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            matches = data["matches"]
            print(f"Nombre de matchs trouvés : {len(matches)}")
            return pd.DataFrame(matches)
        else:
            print(f"Erreur pour la compétition {competition_id}:", response.status_code, response.text)
            return None
    except Exception as e:
        print(f"Exception lors de la récupération des données : {str(e)}")
        return None

# Fonction pour sauvegarder les données dans MySQL
def save_to_mysql(matches_info, competition_name):
    try:
        print(f"Tentative de connexion à MySQL pour {competition_name}...")
        # Connexion à MySQL
        conn = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="root",
            database="FootballData"
        )
        
        cursor = conn.cursor()
        
        # Préparation de la requête d'insertion
        insert_query = """
        INSERT INTO matches (competition, match_date, home_team, away_team, score, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        # Insertion des données
        print(f"Insertion des données pour {competition_name}...")
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
        print(f"\nDonnées de {competition_name} sauvegardées avec succès dans MySQL!")
        
    except mysql.connector.Error as err:
        print(f"Erreur MySQL: {err}")
    except Exception as e:
        print(f"Exception inattendue : {str(e)}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Connexion MySQL fermée.")

def process_competition(competition_id):
    print(f"\nRécupération des données pour {COMPETITIONS[competition_id]}...")
    df_matches = get_matches(competition_id)
    
    if df_matches is not None and not df_matches.empty:
        # Création d'un DataFrame plus lisible
        matches_info = pd.DataFrame({
            'Date': pd.to_datetime(df_matches['utcDate']).dt.strftime('%Y-%m-%d'),
            'Domicile': df_matches['homeTeam'].apply(lambda x: x['name']),
            'Score': df_matches.apply(lambda x: f"{x['score']['fullTime']['home']} - {x['score']['fullTime']['away']}", axis=1),
            'Extérieur': df_matches['awayTeam'].apply(lambda x: x['name']),
            'Statut': df_matches['status']
        })
        
        print(f"\nInformations sur les matchs de {COMPETITIONS[competition_id]}:")
        print("-" * 50)
        print(matches_info)
        print(f"\nNombre total de matchs : {len(matches_info)}")
        print("\nStatut des matchs :")
        print(matches_info['Statut'].value_counts())
        
        # Sauvegarde dans MySQL
        save_to_mysql(matches_info, COMPETITIONS[competition_id])
    else:
        print(f"Aucune donnée disponible pour {COMPETITIONS[competition_id]}")

# Exécution pour toutes les compétitions
print("Démarrage du script...")
# Initialisation de la base de données
initialize_database()

# Traitement de chaque compétition
for competition_id in COMPETITIONS.keys():
    process_competition(competition_id)
print("Fin du script.") 