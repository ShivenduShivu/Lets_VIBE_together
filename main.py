"""
Main entry point for the ETL pipeline.
"""
import os
import datetime
from src.pipeline.extractor import Extractor          # ✅ CLASS, not function
from src.pipeline.parsers import ParserManager
from src.pipeline.schema import SchemaEvolver
from src.pipeline.loader import Loader
from src.config import MONGO_URI, DATABASE_NAME

def main():
    """Main pipeline orchestration logic."""
    print("Starting Dynamic ETL Pipeline...")
    
    # 1. Setup all components
    loader = Loader(MONGO_URI, DATABASE_NAME)
    schema_evolver = SchemaEvolver(loader)
    parser_manager = ParserManager()
    extractor = Extractor(raw_data_dir="data/raw")     # ✅ CLASS instance

    # 2. Get all raw file paths
    raw_files = extractor.discover_files()             # ✅ CLASS method
    if not raw_files:
        print("No new files found in data/raw. Exiting.")
        return

    print(f"Found {len(raw_files)} files to process.")

    for file_path in raw_files:
        print(f"\n--- Processing: {file_path} ---")
        try:
            # 3. Extract & Parse
            file_type, raw_content = extractor.read_file(file_path)    # ✅ CLASS method
            parsed_data = parser_manager.parse(raw_content, file_type, file_path)
            
            if parsed_data is None:
                print(f"Skipping {file_path}, parser returned None.")
                continue

            # 4. Infer & Evolve Schema (The core logic)
            schema_version, _ = schema_evolver.evolve(parsed_data)
            print(f"Data conforms to schema version: {schema_version}")

            # 5. Load
            processed_doc = {
                "source_file": os.path.basename(file_path),
                "processed_at": datetime.datetime.now(datetime.timezone.utc),
                "schema_version": schema_version,
                "data": parsed_data
            }
            loader.load_processed_data(processed_doc)
            print(f"Successfully loaded data for {file_path}.")
            
            # 6. Move processed file to avoid re-processing
            extractor.move_file(file_path)                         # ✅ CLASS method

        except Exception as e:
            print(f"FAILED to process {file_path}: {e}")
            # Optionally move to an 'error' folder
    
    print("\nPipeline run complete.")

if __name__ == "__main__":
    main()