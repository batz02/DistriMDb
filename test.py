import unittest
import numpy as np
import json
import os
from unittest.mock import MagicMock, patch, mock_open, call
import Pyro5.errors

from server import MovieSearchEngine

class TestMovieSearchEngine(unittest.TestCase):

    def setUp(self):
        self.fake_metadata = [
            {"id": 101, "title": "Matrix", "plot": "Un hacker scopre la realtà simulata."},
            {"id": 102, "title": "Titanic", "plot": "Una nave affonda nell'oceano."}
        ]
        
        self.fake_vectors = np.array([
            [0.9, 0.1],
            [0.1, 0.9]
        ])
        
        self.db_data = {
            "vectors": self.fake_vectors,
            "metadata": self.fake_metadata
        }

    @patch("server.Pyro5.api.locate_ns")
    @patch("server.pickle.load")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists")
    def test_search_logic(self, mock_exists, mock_file, mock_pickle, mock_ns):
        mock_pickle.return_value = self.db_data
        mock_exists.return_value = False
        
        engine = MovieSearchEngine("replica_test")
        fake_query_vector = [1.0, 0.0] 
        result = engine.search(fake_query_vector)
        
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["title"], "Matrix")
        self.assertEqual(result["id"], 101)

    @patch("server.threading.Thread")
    @patch("server.Pyro5.api.locate_ns")
    @patch("server.pickle.load")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists") 
    def test_upvote_and_propagation(self, mock_exists, mock_file, mock_pickle, mock_ns, mock_thread):
        mock_pickle.return_value = self.db_data
        mock_exists.return_value = False
        
        engine = MovieSearchEngine("replica_test")
        new_votes = engine.upvote_movie(101)
        
        self.assertEqual(new_votes, 1)
        mock_thread.assert_called_once()

    @patch("server.Pyro5.api.locate_ns")
    @patch("server.pickle.load")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists") 
    def test_receive_sync_update(self, mock_exists, mock_file, mock_pickle, mock_ns):
        mock_pickle.return_value = self.db_data
        mock_exists.return_value = False
        
        engine = MovieSearchEngine("replica_test")
        engine.receive_update(102, 10)
        
        self.assertEqual(engine.votes[102], 10)

    @patch("server.Pyro5.api.locate_ns")
    @patch("server.pickle.load")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists")
    def test_search_no_results(self, mock_exists, mock_file, mock_pickle, mock_ns):
       
        mock_pickle.return_value = self.db_data
        mock_exists.return_value = False
        engine = MovieSearchEngine("replica_test")

        # similarità negativa per non ritornare risultati 
        fake_query_vector = [-1.0, -1.0] 
        
        result = engine.search(fake_query_vector)
        self.assertEqual(result["status"], "NOT_FOUND")

    @patch("os.path.getsize")           
    @patch("server.Pyro5.api.locate_ns")
    @patch("server.pickle.load")
    @patch("builtins.open", new_callable=mock_open, read_data='{"101": 5}')
    @patch("os.path.exists")
    def test_load_existing_votes(self, mock_exists, mock_file, mock_pickle, mock_ns, mock_getsize):
        
        mock_pickle.return_value = self.db_data
        
        mock_exists.return_value = True 
        mock_getsize.return_value = 100 
        
        engine = MovieSearchEngine("replica_test")
        
        self.assertEqual(engine.votes[101], 5)

    @patch("os.path.getsize")          
    @patch("server.Pyro5.api.locate_ns")
    @patch("server.pickle.load")
    @patch("builtins.open", new_callable=mock_open, read_data='NON_UN_JSON_VALIDO')
    @patch("os.path.exists")
    def test_corrupted_votes_file(self, mock_exists, mock_file, mock_pickle, mock_ns, mock_getsize):
    
        mock_pickle.return_value = self.db_data
        
        mock_exists.return_value = True
        mock_getsize.return_value = 100
        
        with patch("json.load", side_effect=json.JSONDecodeError("msg", "doc", 0)):
            engine = MovieSearchEngine("replica_test")
            
        self.assertEqual(engine.votes, {})

    @patch("server.Pyro5.api.Proxy")
    @patch("server.Pyro5.api.locate_ns")
    @patch("server.pickle.load")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists")
    def test_startup_sync_pulls_data(self, mock_exists, mock_file, mock_pickle, mock_ns, mock_proxy):
        
        mock_pickle.return_value = self.db_data
        mock_exists.return_value = False
        
        mock_ns_instance = MagicMock()

        mock_ns_instance.list.return_value = {"movie.finder.replica_other": "PYRO:..."}
        mock_ns.return_value = mock_ns_instance
        
        mock_peer = MagicMock()
        mock_peer.get_all_votes.return_value = {102: 50}
        mock_proxy.return_value.__enter__.return_value = mock_peer
        
        engine = MovieSearchEngine("replica_test")
        
        self.assertEqual(engine.votes[102], 50)

    @patch("server.Pyro5.api.locate_ns")
    @patch("server.pickle.load")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists")
    def test_startup_sync_no_peers(self, mock_exists, mock_file, mock_pickle, mock_ns):
        
        mock_pickle.return_value = self.db_data
        mock_exists.return_value = False
        
        mock_ns_instance = MagicMock()
        mock_ns_instance.list.return_value = {} 
        mock_ns.return_value = mock_ns_instance
        
        try:
            engine = MovieSearchEngine("replica_test")
        except Exception:
            self.fail("Il server è crashato perché non ha trovato peer!")
            
        self.assertEqual(engine.votes, {})

    @patch("server.Pyro5.api.Proxy")
    @patch("server.Pyro5.api.locate_ns")
    @patch("server.pickle.load")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists")
    def test_propagate_network_error(self, mock_exists, mock_file, mock_pickle, mock_ns, mock_proxy):
       
        mock_pickle.return_value = self.db_data
        mock_exists.return_value = False
        
        engine = MovieSearchEngine("replica_test")
        
        mock_ns_instance = MagicMock()
        mock_ns_instance.list.return_value = {"movie.finder.replica_dead": "PYRO:..."}
        mock_ns.return_value = mock_ns_instance
        
        mock_proxy.side_effect = Pyro5.errors.CommunicationError("Timeout")
        
        try:
            engine._propagate_update(101, 5)
        except Pyro5.errors.CommunicationError:
            self.fail("La propagazione non ha gestito l'errore di rete!")
            
    @patch("json.dump")
    @patch("server.Pyro5.api.locate_ns")
    @patch("server.pickle.load")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists")
    def test_receive_update_saves_to_disk(self, mock_exists, mock_file, mock_pickle, mock_ns, mock_json_dump):

        mock_pickle.return_value = self.db_data
        mock_exists.return_value = False
        
        engine = MovieSearchEngine("replica_test")
        
        engine.receive_update(101, 99)
        
        mock_json_dump.assert_called()
        self.assertEqual(engine.votes[101], 99)

if __name__ == '__main__':
    unittest.main()