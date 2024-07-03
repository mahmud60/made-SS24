import unittest
import sqlite3
import pandas as pd
import os
from pipeline import Pipeline

class Test(unittest.TestCase):
    def test_data_pipeline(self):
        BASE_DIR = os.getcwd()
        print("The BASE path is: "+BASE_DIR)
        DB_PATH = os.path.join(BASE_DIR, "data\ProjectDatabase.db")
        print("The path is: "+DB_PATH)
        
        print(os.path.exists(DB_PATH))

        #check if the db is already exits, if so remove it first
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        

        #check if the db is removed successfully
        self.assertFalse(os.path.exists(DB_PATH))
        
        
        #run the data pipeline
        # Create an instance of the Pipeline class
        pipeline_instance = Pipeline()

        # Run the pipeline
        pipeline_instance.initialize_Pipeline()
        
        #check if the data pipeline created the db
        self.assertTrue(os.path.exists(DB_PATH))
                                
        #connect db and check the rows of the table are as expected
        conn = sqlite3.connect(DB_PATH)
        expected_row_counts = {
            'emissions': 6075,
            'renewable': 5439
        }
        
        for table_name, expected_row_count in expected_row_counts.items():
            db_rows = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            self.assertEqual(len(db_rows), expected_row_count)
        
        conn.close()

if __name__ == "__main__":
    unittest.main()