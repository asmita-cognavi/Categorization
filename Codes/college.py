import logging
from pymongo import MongoClient
from bson import ObjectId
import pandas as pd
from collections import defaultdict
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('student_update.log'),
        logging.StreamHandler()
    ]
)

def normalize_college_name(college_name):
    """Normalize college name for consistent comparison"""
    if not college_name:
        return None
    
    # Convert to uppercase for case-insensitive comparison
    normalized = college_name.upper()
    
    # Remove extra whitespace
    normalized = ' '.join(normalized.split())
    
    # Remove special characters except spaces
    normalized = re.sub(r'[^\w\s]', '', normalized)
    
    # Replace common variations
    replacements = {
        'UNIVERSITY': 'UNIV',
        'UNIVERSITY': 'UNI',
        'COLLEGE': 'COLL',
        'INSTITUTE': 'INST',
        'TECHNOLOGY': 'TECH',
        'ENGINEERING': 'ENGG'
    }
    
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)
    
    return normalized

# Connection string
connection_string = "CONNECTION_STRING"

# Dictionary to store college-wise category counts
college_stats = defaultdict(lambda: {'C1': 0, 'C2': 0, 'C3': 0, 'C4': 0, 'C5': 0, 'original_name': None})

def process_batch(batch, students_collection):
    """Process a batch of score documents"""
    updates = []
    
    for score_doc in batch:
        try:
            student_id = score_doc.get('student_id')
            
            if not student_id:
                logging.warning(f"No student_id found for score document {score_doc['_id']}")
                continue
                
            # Find matching student document
            student_doc = students_collection.find_one({'_id': ObjectId(student_id)})
            
            college_name = None
            if student_doc:
                # Get education records
                education_records = student_doc.get('education_records', [])
                
                # Find primary education record
                primary_record = next(
                    (record for record in education_records if record.get('is_primary') is True),
                    None
                )
                
                if primary_record:
                    college_name = primary_record.get('college_name')
                    
                    # Update college statistics
                    if college_name and 'scores' in score_doc and 'category' in score_doc['scores']:
                        normalized_name = normalize_college_name(college_name)
                        category = score_doc['scores']['category']
                        college_stats[normalized_name][category] += 1
                        # Keep track of the original name
                        if college_stats[normalized_name]['original_name'] is None:
                            college_stats[normalized_name]['original_name'] = college_name
            
            # Prepare update operation
            updates.append(UpdateOne(
                {'_id': score_doc['_id']},
                {'$set': {'college': college_name}}
            ))
            
        except Exception as e:
            logging.error(f"Error processing document {score_doc.get('_id')}: {str(e)}")
            continue
    
    return updates

try:
    # Connect to MongoDB
    logging.info("Connecting to MongoDB...")
    client = MongoClient(connection_string)
    
    # Get database and collection references
    db = client['PROD_STUDENT'] #DEV_STUDENT
    scores_collection = db['student_scores_temp']
    students_collection = db['students']  
    
    # Counter for tracking updates
    update_count = 0
    error_count = 0
    batch_size = 1000
    
    # Get total count of documents
    total_docs = scores_collection.count_documents({})
    logging.info(f"Total documents to process: {total_docs}")
    
    # Process in batches
    from pymongo import UpdateOne
    for i in range(0, total_docs, batch_size):
        batch = list(scores_collection.find().skip(i).limit(batch_size))
        logging.info(f"Processing batch {i//batch_size + 1} ({len(batch)} documents)")
        
        updates = process_batch(batch, students_collection)
        
        if updates:
            # Execute bulk update
            result = scores_collection.bulk_write(updates)
            update_count += result.modified_count
            logging.info(f"Batch update complete. Modified {result.modified_count} documents")
    
    logging.info("Creating Excel report...")
    
    # Convert college statistics to DataFrame
    df_data = []
    for normalized_name, stats in college_stats.items():
        row = {
            'College': stats['original_name'] if stats['original_name'] else 'Unknown',
            'Normalized Name': normalized_name,
            'C1 Count': stats['C1'],
            'C2 Count': stats['C2'],
            'C3 Count': stats['C3'],
            'C4 Count': stats['C4'],
            'C5 Count': stats['C5'],
            'Total Students': sum(v for k, v in stats.items() if k not in ['original_name'])
        }
        df_data.append(row)
    
    df = pd.DataFrame(df_data)
    
    # Sort by total students in descending order
    df = df.sort_values('Total Students', ascending=False)
    
    # Save to Excel
    excel_file = 'college_category_report_prod.xlsx'
    df.to_excel(excel_file, index=False)
    
    # Log some statistics about normalization
    total_colleges = len(college_stats)
    logging.info(f"Total unique colleges after normalization: {total_colleges}")
    
    logging.info(f"Update completed:")
    logging.info(f"Total documents updated: {update_count}")
    logging.info(f"Total errors encountered: {error_count}")
    logging.info(f"Report saved to {excel_file}")

except Exception as e:
    logging.error(f"Connection error: {str(e)}")

finally:
    # Close the connection
    if 'client' in locals():
        client.close()
        logging.info("MongoDB connection closed")