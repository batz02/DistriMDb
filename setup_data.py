import pickle
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

# --- CONFIGURAZIONE ---
FILENAME = "tmdb_5000_movies.csv"  
COL_TITOLO = "original_title"    
COL_TRAMA = "overview"   
COL_ID = "id"       
# ----------------------

print(f"1. Caricamento {FILENAME}...")

# nrows=1000 serve per sicurezza: anche se scarichi un file gigante, 
# lui legge solo le prime 1000 righe così il pc non rallenta.
try:
    df = pd.read_csv(FILENAME, nrows=5000)
except FileNotFoundError:
    print(f"ERRORE: Non trovo il file '{FILENAME}'. Scaricalo e mettilo nella cartella del progetto!")
    exit()

# Pulizia: togliamo righe che non hanno la trama
df = df.dropna(subset=[COL_TRAMA])

# Creiamo la lista di dizionari che piace al tuo server
# Rinominiamo le colonne del CSV nelle chiavi standard "title" e "plot" che usiamo nel progetto
movies_db = []
for index, row in df.iterrows():
    movies_db.append({
        "id": row[COL_ID],
        "title": row[COL_TITOLO],
        "plot": str(row[COL_TRAMA])
    })

print(f"   -> Caricati {len(movies_db)} film.")

print("2. Training del modello AI (TF-IDF)...")
plots = [m["plot"] for m in movies_db]

# stop_words='english' è fondamentale perché questi dataset sono in inglese
vectorizer = TfidfVectorizer(stop_words='english', max_features=5000)
tfidf_matrix = vectorizer.fit_transform(plots)

# 3. Salvataggio (uguale a prima)
with open("client_model.pkl", "wb") as f:
    pickle.dump(vectorizer, f)

server_data = {
    "vectors": tfidf_matrix.toarray(),
    "metadata": movies_db
}
with open("server_db.pkl", "wb") as f:
    pickle.dump(server_data, f)

print("Setup completato! Database pronto.")