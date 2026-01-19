import Pyro5.api
import Pyro5.errors
import pickle
import sys

def get_available_replicas(ns):
    all_objects = ns.list(prefix="movie.finder.")
    return list(all_objects.items())

def main():
    try:
        with open("client_model.pkl", "rb") as f:
            vectorizer = pickle.load(f)
    except FileNotFoundError:
        print("ERRORE: Esegui prima 'setup_data.py'!")
        return

    print("--- CLIENT TOLLERANTE AI GUASTI ---")
    print("Connessione al Name Server...")
    try:
        ns = Pyro5.api.locate_ns()
    except:
        print("ERRORE: Name Server non trovato. Esegui 'python -m Pyro5.nameserver'")
        return

    while True:
        query = input("\nCosa stai cercando? (o 'exit'): ")
        if query.lower() == "exit": break

        print(" -> [Client] Elaborazione NLP...")
        vector_sparse = vectorizer.transform([query])
        vector_list = vector_sparse.toarray()[0].tolist()

        replicas_list = get_available_replicas(ns)
        if not replicas_list:
            print("NESSUN SERVER DISPONIBILE! Assicurati di aver avviato le repliche.")
            continue

        response = None
        used_replica = None

        for name,uri in replicas_list:
            clean_name = name.split(".")[-1]
            print(f" -> [Network] Provo a contattare replica: {clean_name} ...")
            try:
                with Pyro5.api.Proxy(uri) as server:
                    response = server.search(vector_list)
                    print(f"    [SUCCESSO] Risposta ricevuta da {clean_name}!")
                    break

            except Pyro5.errors.CommunicationError:
                print(f"    [FALLITO] {clean_name} Questa replica non risponde. Passo alla prossima...")
                continue
        
        if response:
            if response["status"] == "OK":
                print(f"\n=== RISULTATO da {response['replica']} ===")
                print(f"Titolo: {response['title']}")
                print(f"Trama:  {response['plot_snippet']}")
                print(f"Score similarità: {response['similarity_score']:.2f}")
                print(f"Likes:  {response['votes']}")
                
                if input("Ti piace? (s/n): ").lower() == 's':
                    vote_success = False
                    
                    current_replicas = get_available_replicas(ns)
                    
                    print(" -> [Client] Invio voto a una replica disponibile...")
                    
                    for name,uri in current_replicas:
                        clean_name = name.split(".")[-1]
                        try:
                            with Pyro5.api.Proxy(uri) as server:
                                new_votes = server.upvote_movie(response['id'])
                                print(f" -> [SUCCESSO] Voto registrato da {clean_name}! Totale: {new_votes}")
                                vote_success = True
                                break 
                        except Pyro5.errors.CommunicationError:
                            print(f"    [FAIL] {clean_name} non risponde, provo il prossimo...")
                    
                    if not vote_success:
                        print("!!! ERRORE CRITICO: Impossibile salvare il voto. Nessun server raggiungibile.")

            else:
                print("Nessun film trovato.")
        else:
            print("!!! CRITICO: Tutte le repliche sono irraggiungibili. Riprova più tardi.")

if __name__ == "__main__":
    main()