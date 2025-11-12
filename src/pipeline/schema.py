import datetime
from src.pipeline.loader import Loader

class SchemaInferer:
    """Contains stateless methods for schema inference and merging.
    This is the 'pure' logic engine."""

    # Python type to schema type mapping
    TYPE_MAP = {
        str: 'string',
        int: 'integer',
        float: 'number',  # Use 'number' for floats (JSON Schema convention, covers int/float)
        bool: 'boolean',
        type(None): 'null',
        datetime.datetime: 'string'  # Treat datetimes as strings in schema
    }

    def infer_schema(self, data):
        """Recursively infers a schema from a single data object (dict, list, primitive)."""
        data_type = type(data)

        if data_type in self.TYPE_MAP:
            return {'type': self.TYPE_MAP[data_type]}

        if data_type == dict:
            properties = {}
            for key, value in data.items():
                # Sanitize keys (MongoDB doesn't like keys with '.' or starting with '$')
                safe_key = str(key).replace(".", "_").replace("$", "_")
                properties[safe_key] = self.infer_schema(value)  # Recursive call
            return {'type': 'object', 'properties': properties}

        if data_type == list:
            if not data:
                # Empty list, default to 'null' items
                return {'type': 'array', 'items': {'type': 'null'}}

            # Infer schema for all items and merge them
            item_schemas = [self.infer_schema(item) for item in data]
            merged_item_schema = item_schemas[0]
            for item_schema in item_schemas[1:]:
                merged_item_schema = self.merge_schemas(merged_item_schema, item_schema)  # Recursive merge
            return {'type': 'array', 'items': merged_item_schema}

        # Fallback for unhandled types (e.g., tuples, sets) - treat as string
        return {'type': 'string', 'description': f'unhandled_type_{str(data_type)}'}
    
    def merge_schemas(self, old_schema, new_schema):
        """Recursively merges two schemas, handling type promotions and object merging."""
        if old_schema == new_schema:
            return old_schema

        # Handle Type Promotion
        old_type = old_schema.get('type')
        new_type = new_schema.get('type')

        if old_type != new_type:
            # Standardize to lists (types can be str or list of str)
            old_list = old_type if isinstance(old_type, list) else [old_type]
            new_list = new_type if isinstance(new_type, list) else [new_type]

            combined_types = set(old_list + new_list) - {'null'}  # Ignore null

            # Promotion: int + float -> float (remove 'integer')
            if 'integer' in combined_types and 'number' in combined_types:
                combined_types.remove('integer')

            if not combined_types:
                return {'type': 'null'}
            elif len(combined_types) == 1:
                return {'type': list(combined_types)[0]}
            else:
                return {'type': sorted(list(combined_types))}

        # Recursive Merge for Objects
        if old_type == 'object':
            old_props = old_schema.get('properties', {})
            new_props = new_schema.get('properties', {})

            all_keys = set(old_props.keys()) | set(new_props.keys())
            merged_props = {}

            for key in all_keys:
                if key in old_props and key in new_props:
                    merged_props[key] = self.merge_schemas(old_props[key], new_props[key])  # Recursive
                elif key in old_props:
                    merged_props[key] = old_props[key]
                else:
                    merged_props[key] = new_props[key]

            return {'type': 'object', 'properties': merged_props}

        # Recursive Merge for Arrays
        if old_type == 'array':
            old_items = old_schema.get('items', {'type': 'null'})
            new_items = new_schema.get('items', {'type': 'null'})

            merged_items = self.merge_schemas(old_items, new_items)  # Recursive
            return {'type': 'array', 'items': merged_items}

        # If no changes or other types, return old
        return old_schema
    

class SchemaEvolver:
    """The stateful class that uses the Loader to manage schema evolution.
    This is the main 'brain' component called by main.py."""

    def __init__(self, loader: Loader):
        self.loader = loader
        self.inferer = SchemaInferer()  # Instance of the stateless inferer
        self.latest_schema_cache = None  # In-memory cache for efficiency

    def get_latest_schema(self):
        """Fetches the latest schema, using cache if available."""
        if self.latest_schema_cache:
            return self.latest_schema_cache

        schema_doc = self.loader.get_latest_schema()
        if schema_doc:
            self.latest_schema_cache = schema_doc
            return schema_doc

        # No schema in DB, create a "v0" empty schema
        print("No schema found in registry, creating v0.")
        v0_schema = {
            "version": "v0",
            "schema": {"type": "object", "properties": {}},
            "created_at": datetime.datetime.now(datetime.timezone.utc)
        }
        self.loader.save_new_schema(v0_schema["version"], v0_schema["schema"], v0_schema["created_at"])
        self.latest_schema_cache = v0_schema
        return v0_schema

    def evolve(self, parsed_data):
        """Process a new piece of parsed data and evolve the schema.
        Returns the schema_version this data conforms to, and the schema itself."""
        # 1. Get latest schema from registry (via cache or DB)
        latest_schema_doc = self.get_latest_schema()
        latest_schema = latest_schema_doc["schema"]
        latest_version = latest_schema_doc["version"]

        # 2. Infer schema from new data
        new_inferred_schema = self.inferer.infer_schema(parsed_data)

        # 3. Merge new schema with the latest schema
        merged_schema = self.inferer.merge_schemas(latest_schema, new_inferred_schema)

        # 4. Check if evolution has occurred
        if merged_schema == latest_schema:
            # No change, data conforms to latest version
            return latest_version, latest_schema
        else:
            # Schema changed! Create a new version.
            print(f"Schema evolution detected! Creating new version.")
            new_version_num = int(latest_version.lstrip('v')) + 1
            new_version_id = f"v{new_version_num}"
            new_created_at = datetime.datetime.now(datetime.timezone.utc)

            self.loader.save_new_schema(new_version_id, merged_schema, new_created_at)

            # Update cache
            self.latest_schema_cache = {
                "version": new_version_id,
                "schema": merged_schema,
                "created_at": new_created_at
            }

            return new_version_id, merged_schema