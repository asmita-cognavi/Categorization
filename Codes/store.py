import pymongo
import pandas as pd
import json
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def import_csv_to_mongodb():
    # MongoDB connection
    client = pymongo.MongoClient(
        "CONNECTION_STRING",
        serverSelectionTimeoutMS=30000
    )
    db = client["PROD_STUDENT"] #DEV_STUDENT
    
    try:
        # Read the CSV file
        # Update the filename to match your consolidated file name
        filename = 'GENERATED FILE F5OM SCRIPT.PY'
        logger.info(f"Reading CSV file: {filename}")
        
        # Read CSV in chunks to handle large files
        chunk_size = 1000
        chunks = pd.read_csv(filename, chunksize=chunk_size)
        
        # Create or get the temporary collection
        temp_collection = db["student_scores_temp"]
        
        # Drop existing collection if it exists
        temp_collection.drop()
        logger.info("Dropped existing temporary collection")
        
        total_documents = 0
        
        for chunk in chunks:
            # Convert the string representations of dictionaries to actual dictionaries
            chunk['metrics'] = chunk['metrics'].apply(eval)
            chunk['scores'] = chunk['scores'].apply(eval)
            
            # Convert DataFrame chunk to list of dictionaries
            records = chunk.to_dict('records')
            
            # Insert the chunk into MongoDB
            temp_collection.insert_many(records)
            
            total_documents += len(records)
            logger.info(f"Inserted {len(records)} documents. Total: {total_documents}")
        
        # Create indexes
        temp_collection.create_index("student_id")
        temp_collection.create_index([("scores.category", 1)])
        temp_collection.create_index([("scores.total_score", -1)])
        logger.info("Created indexes on student_id, category, and total_score")
        
        # Print collection stats
        stats = db.command("collstats", "student_scores_temp")
        logger.info(f"""
Import completed successfully:
- Total documents: {total_documents}
- Collection size: {stats['size'] / 1024 / 1024:.2f} MB
- Storage size: {stats['storageSize'] / 1024 / 1024:.2f} MB
        """)
        
    except Exception as e:
        logger.error(f"Error during import: {str(e)}")
        raise
    
    finally:
        client.close()

if __name__ == "__main__":
    import_csv_to_mongodb()