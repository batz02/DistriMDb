import pickle
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

FILENAME = "tmdb_5000_movies.csv"  
COL_TITOLO = "original_title"    
COL_TRAMA = "overview"   
COL_ID = "id"       

print(f"1. Caricamento {FILENAME}...")

try:
    df = pd.read_csv(FILENAME, nrows=5000)
except FileNotFoundError:
    print(f"ERRORE: Non trovo il file '{FILENAME}'. Scaricalo e mettilo nella cartella del progetto!")
    exit()

df = df.dropna(subset=[COL_TRAMA])

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

vectorizer = TfidfVectorizer(stop_words='english', max_features=5000)
tfidf_matrix = vectorizer.fit_transform(plots)

with open("client_model.pkl", "wb") as f:
    pickle.dump(vectorizer, f)

server_data = {
    "vectors": tfidf_matrix.toarray(),
    "metadata": movies_db
}
with open("server_db.pkl", "wb") as f:
    pickle.dump(server_data, f)

print("Setup completato! Database pronto.")