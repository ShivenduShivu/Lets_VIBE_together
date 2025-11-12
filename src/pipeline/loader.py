from pymongo import MongoClient
from src.config import COLLECTION_PROCESSED, COLLECTION_SCHEMA

class Loader:
    def __init__(self, mongo_uri, db_name):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.processed = self.db[COLLECTION_PROCESSED]
        self.schema_registry = self.db[COLLECTION_SCHEMA]
    
    def load_processed_data(self, doc):
        """Save processed document to MongoDB."""
        self.processed.insert_one(doc)
    
    def save_new_schema(self, version, schema, created_at):
        """Save new schema version to registry."""
        schema_doc = {
            "version": version,
            "schema": schema,
            "created_at": created_at
        }
        self.schema_registry.insert_one(schema_doc)
    
    def get_latest_schema(self):
        """Get the latest schema from registry."""
        latest = self.schema_registry.find().sort("created_at", -1).limit(1)
        return next(latest, None)