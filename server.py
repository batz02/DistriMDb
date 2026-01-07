import Pyro5.api
import Pyro5.errors
import pickle
import numpy as np
import threading
import json
import os
import sys
from sklearn.metrics.pairwise import cosine_similarity

@Pyro5.api.expose
class MovieSearchEngine(object):
    def __init__(self, replica_name):
        self.replica_name = replica_name
        print(f">> [{self.replica_name}] Server: Caricamento Database...")
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(base_dir, "server_db.pkl")

        self.votes_file = os.path.join(base_dir, f"votes_{self.replica_name}.json")

        with open(db_path, "rb") as f:
            data = pickle.load(f)
            self.vectors = data["vectors"]
            self.metadata = data["metadata"]
        
        self.lock = threading.Lock()
        self.votes = self._load_votes_from_disk()
        self._sync_at_startup()
        print(f">> [{self.replica_name}] Pronto! Indicizzati {len(self.metadata)} film.")

    @Pyro5.api.expose
    def get_all_votes(self):
        return self.votes

    def _sync_at_startup(self):
        
        print(f"   [{self.replica_name}] SYNC: Ricerca peer per allineamento...")
        try:
            ns = Pyro5.api.locate_ns()
            all_replicas = ns.list(prefix="movie.finder.")
            
            for name, uri in all_replicas.items():
                if name.endswith(self.replica_name): continue
                
                try:
                    print(f"   [{self.replica_name}] -> Richiesta dati a {name}...")
                    with Pyro5.api.Proxy(uri) as peer:
                        remote_votes = peer.get_all_votes() 
                        
                        with self.lock:
                            updated = False
                            for mid, count in remote_votes.items():
                                mid = int(mid)
                                local_count = self.votes.get(mid, 0)
                                if count > local_count:
                                    self.votes[mid] = count
                                    updated = True
                            
                            if updated:
                                self._save_votes_to_disk()
                                print(f"   [{self.replica_name}] SYNC COMPLETATA: Database aggiornato.")
                            else:
                                print(f"   [{self.replica_name}] SYNC: Database già allineato.")
                        return 
                except Exception as e:
                    continue
        except:
            print("   [SYNC] Nessun peer trovato. Avvio con i dati locali.")
    
    def _load_votes_from_disk(self):
        if not os.path.exists(self.votes_file) or os.path.getsize(self.votes_file) == 0:
            return {}
        try:
            with open(self.votes_file, "r") as f:
                raw_data = json.load(f)
                return {int(k): v for k, v in raw_data.items()}
        except Exception:
            return {}

    def _save_votes_to_disk(self):
        try:
            with open(self.votes_file, "w") as f:
                json.dump(self.votes, f, indent=4)
        except Exception as e:
            print(f"!! ERRORE SALVATAGGIO: {e}")

    def _propagate_update(self, movie_id, new_count):

        print(f"   [{self.replica_name}] ...Inizio propagazione aggiornamento agli altri peer...")
        try:
            ns = Pyro5.api.locate_ns()
            all_replicas = ns.list(prefix="movie.finder.")
            
            for name, uri in all_replicas.items():
                if name.endswith(self.replica_name):
                    continue
                
                try:
                    with Pyro5.api.Proxy(uri) as peer:
                        peer.receive_update(movie_id, new_count)
                        print(f"   [{self.replica_name}] -> Inviato sync a {name}")
                except Pyro5.errors.CommunicationError:
                    print(f"   [{self.replica_name}] -> Impossibile contattare {name} (Offline?)")
                    
        except Exception as e:
            print(f"!! Errore durante la propagazione: {e}")

    def receive_update(self, movie_id, new_count):
       
        with self.lock:
            self.votes[movie_id] = new_count
            self._save_votes_to_disk()
            print(f"   [{self.replica_name}] <--- RICEVUTO SYNC: Film {movie_id} ora ha {new_count} voti.")

    def search(self, client_vector_list):
        print(f"   [{self.replica_name}] Ricevuta richiesta di ricerca...")
        query_vec = np.array(client_vector_list).reshape(1, -1)
        scores = cosine_similarity(query_vec, self.vectors)
        best_idx = np.argmax(scores)
        best_score = scores[0, best_idx]
        
        if best_score > 0.1: 
            result = self.metadata[best_idx]
            return {
                "status": "OK",
                "replica": self.replica_name,
                "id": result["id"],
                "title": result["title"],         
                "plot_snippet": result["plot"], 
                "similarity_score": float(best_score),
                "votes": self.votes.get(result["id"], 0)
            }
        else:
            return {"status": "NOT_FOUND", "msg": "Nessun film rilevante trovato."}
        
    def upvote_movie(self, movie_id):
        
        with self.lock:
            current = self.votes.get(movie_id, 0)
            new_count = current + 1
            self.votes[movie_id] = new_count
            self._save_votes_to_disk()
            
            print(f"   [{self.replica_name}] CLIENT UPDATE: Film {movie_id} -> {new_count} voti.")
            
        threading.Thread(target=self._propagate_update, args=(movie_id, new_count)).start()
        
        return new_count

def start_server():
    print("--- AVVIO SERVER ---")
    
    # 1. Connettiamoci al Name Server (con gestione errori robusta)
    try:
        ns = Pyro5.api.locate_ns()
    except (Pyro5.errors.NamingError, OSError, Exception) as e:
        # Ora catturiamo anche OSError (il tuo Errno 65) e qualsiasi altro crash
        print(f"\n!!! ERRORE CRITICO: Name Server non raggiungibile.")
        print(f"    Dettaglio errore: {e}")
        print("-------------------------------------------------------")
        print("    SOLUZIONE:")
        print("    1. Apri un nuovo terminale ed esegui: python -m Pyro5.nameserver")
        print("    2. Se sei su MacOS o VPN, prova: python -m Pyro5.nameserver -n localhost")
        print("-------------------------------------------------------")
        return

    # 2. Logica di scelta del nome
    my_name = None
    
    if len(sys.argv) > 1:
        # Se l'utente ha specificato un nome, usiamo quello (es: python server.py Pippo)
        my_name = sys.argv[1]
    else:
        # Se nessun argomento, generiamo un nome univoco (replica_1, replica_2, ...)
        print(">> Nessun nome specificato. Cerco il primo nome disponibile...")
        
        # Chiediamo al NS chi esiste già
        existing_services = ns.list(prefix="movie.finder.")
        
        counter = 1
        while True:
            candidate_name = f"replica_{counter}"
            full_service_name = f"movie.finder.{candidate_name}"
            
            if full_service_name in existing_services:
                # Se esiste già, incrementiamo e riproviamo
                counter += 1
            else:
                # Trovato un buco libero!
                my_name = candidate_name
                break
    
    daemon = Pyro5.api.Daemon()
    uri = daemon.register(MovieSearchEngine(my_name))
    service_name = f"movie.finder.{my_name}"
    ns.register(service_name, uri)
    
    print(f"Server registrato come '{service_name}'. In attesa...")
    daemon.requestLoop()

if __name__ == "__main__":
    start_server()