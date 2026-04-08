"""
Fetch Ireland Groundwater Wells and Springs data from data.gov.ie (XML format) 
and store in MongoDB.

Requirements:
- Create database if not present (don't drop if exists)
- Create collection if not present
- Drop and recreate collection if already present
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import urllib.request
import ssl
import xml.etree.ElementTree as ET
import json
import time
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# MongoDB Configuration
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "Rawdata"
COLLECTION_NAME = "Ireland_Groundwater_Wells_Springs"

# Data source - Ireland Groundwater Wells and Springs
# Using ESRI REST API with XML output format
BASE_URL = "https://gsi.geodata.gov.ie/server/rest/services/Groundwater/IE_GSI_Groundwater_Wells_Springs_100K_IE26_ITM/MapServer/0/query"

# Source info page
SOURCE_PAGE = "https://data.gov.ie/dataset/groundwater-wells-and-springs-ireland-roi-itm/resource/cba18cab-84b1-47a4-a145-736625974b9f"

# Settings
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def get_ssl_context():
    """Create SSL context that doesn't verify certificates (for corporate networks)."""
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def fetch_data_count():
    """Get total count of records available."""
    params = "where=1%3D1&returnCountOnly=true&f=json"
    url = f"{BASE_URL}?{params}"
    
    context = get_ssl_context()
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    
    try:
        response = urllib.request.urlopen(req, timeout=60, context=context)
        data = json.loads(response.read().decode('utf-8'))
        return data.get('count', 0)
    except Exception as e:
        print(f"    Warning: Could not get count: {e}")
        return None


def download_and_parse_xml():
    """
    Download data from ESRI REST API in XML format and parse it.
    Uses pagination to get all records.
    
    Returns:
        List of dictionaries containing the parsed records
    """
    context = get_ssl_context()
    records = []
    
    # Get total count first
    total_count = fetch_data_count()
    if total_count:
        print(f"    Total records available: {total_count}")
    
    # Fetch data in batches using pagination
    batch_size = 1000
    offset = 0
    
    while True:
        # Build URL with XML format
        params = (
            f"where=1%3D1"
            f"&outFields=*"
            f"&returnGeometry=true"
            f"&resultOffset={offset}"
            f"&resultRecordCount={batch_size}"
            f"&f=xml"  # Request XML format
        )
        url = f"{BASE_URL}?{params}"
        
        print(f"    Fetching records {offset + 1} to {offset + batch_size}...", end='\r')
        
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        
        try:
            response = urllib.request.urlopen(req, timeout=120, context=context)
            xml_content = response.read().decode('utf-8')
            
            # Parse XML
            batch_records = parse_xml_response(xml_content)
            
            if not batch_records:
                # No more records
                break
            
            records.extend(batch_records)
            print(f"    Fetched {len(records)} records so far...          ")
            
            if len(batch_records) < batch_size:
                # Last batch
                break
            
            offset += batch_size
            
        except Exception as e:
            print(f"\n    Error fetching batch at offset {offset}: {e}")
            # Try to continue with what we have
            break
    
    print(f"    Total records fetched: {len(records)}")
    return records


def parse_xml_response(xml_content: str) -> list:
    """
    Parse the XML response from ESRI REST API.
    
    Args:
        xml_content: Raw XML string
    
    Returns:
        List of record dictionaries
    """
    records = []
    
    try:
        root = ET.fromstring(xml_content)
        
        # Find all feature elements
        # ESRI XML format typically has features under FeatureSet/Features/Feature
        for feature in root.iter():
            if 'Feature' in feature.tag or feature.tag.endswith('Feature'):
                record = {}
                
                # Get attributes
                attrs_elem = feature.find('.//Attributes') or feature.find('.//attributes')
                if attrs_elem is not None:
                    for attr in attrs_elem:
                        key = attr.tag
                        value = attr.text
                        if value:
                            value = value.strip()
                            # Try to convert to appropriate type
                            if value.isdigit():
                                record[key] = int(value)
                            elif value.replace('.', '', 1).replace('-', '', 1).isdigit():
                                try:
                                    record[key] = float(value)
                                except:
                                    record[key] = value
                            else:
                                record[key] = value
                        else:
                            record[key] = None
                
                # Also try to get geometry
                geom_elem = feature.find('.//Geometry') or feature.find('.//geometry')
                if geom_elem is not None:
                    geom = {}
                    for g in geom_elem:
                        if g.text:
                            try:
                                geom[g.tag] = float(g.text)
                            except:
                                geom[g.tag] = g.text
                    if geom:
                        record['geometry'] = geom
                
                # If we couldn't find structured attributes, try to get all child elements
                if not record:
                    for child in feature:
                        if child.text and child.text.strip():
                            key = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                            value = child.text.strip()
                            if value.isdigit():
                                record[key] = int(value)
                            else:
                                try:
                                    record[key] = float(value)
                                except:
                                    record[key] = value
                
                if record:
                    records.append(record)
        
        # If no features found with Feature tag, try alternative parsing
        if not records:
            records = parse_esri_xml_alternative(root)
            
    except ET.ParseError as e:
        print(f"    XML Parse Error: {e}")
        # Try to extract what we can
        pass
    
    return records


def parse_esri_xml_alternative(root) -> list:
    """Alternative parsing for ESRI XML when standard parsing doesn't work."""
    records = []
    
    # Try to find any element that looks like a record
    for elem in root.iter():
        # Look for elements with multiple children (likely records)
        children = list(elem)
        if len(children) >= 3:  # At least 3 fields
            record = {}
            for child in children:
                tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                if child.text and child.text.strip():
                    value = child.text.strip()
                    if value.isdigit():
                        record[tag] = int(value)
                    else:
                        try:
                            record[tag] = float(value)
                        except:
                            record[tag] = value
            
            # Only add if it looks like a real record (has some content)
            if len(record) >= 2:
                records.append(record)
    
    return records


def get_requests_session():
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_json_fallback(collection=None, fetch_timestamp=None):
    """
    Fallback: Fetch data as JSON if XML parsing fails.
    Uses requests library for better timeout handling.
    Optionally inserts data incrementally if collection is provided.
    """
    print("    Using JSON format as fallback...")
    records = []
    
    batch_size = 500  # Smaller batches for faster response
    offset = 0
    consecutive_errors = 0
    max_consecutive_errors = 3
    total_inserted = 0
    
    session = get_requests_session()
    
    while consecutive_errors < max_consecutive_errors:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "resultOffset": offset,
            "resultRecordCount": batch_size,
            "f": "json"
        }
        
        try:
            response = session.get(BASE_URL, params=params, timeout=20, verify=False)
            response.raise_for_status()
            data = response.json()
            
            features = data.get('features', [])
            
            if not features:
                print(f"\n    No more records after offset {offset}")
                break
            
            batch_records = []
            for feature in features:
                record = feature.get('attributes', {})
                geometry = feature.get('geometry', {})
                if geometry:
                    record['geometry'] = geometry
                batch_records.append(record)
                records.append(record)
            
            # Insert incrementally if collection is provided
            if collection is not None and fetch_timestamp is not None and batch_records:
                documents = [transform_record(r, fetch_timestamp) for r in batch_records]
                try:
                    result = collection.insert_many(documents, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    print(f"    Batch at {offset}: {len(batch_records)} fetched, {total_inserted} total inserted")
                except Exception as e:
                    print(f"    Insert error at offset {offset}: {e}")
            else:
                print(f"    Fetched {len(records)} records so far...")
            
            if len(features) < batch_size:
                # Last batch
                break
            
            offset += batch_size
            consecutive_errors = 0
            time.sleep(0.1)
            
        except Exception as e:
            print(f"\n    Error at offset {offset}: {str(e)[:50]}")
            consecutive_errors += 1
            if records:
                print(f"    Continuing... ({len(records)} records so far)")
            time.sleep(1)
    
    print(f"    Total records fetched: {len(records)}")
    if collection is not None:
        print(f"    Total records inserted: {total_inserted}")
    return records


def connect_to_mongodb():
    """
    Connect to MongoDB and return client, database, and collection.
    Creates database if not present (don't drop).
    Creates collection if not present, drops and recreates if exists.
    
    Returns:
        Tuple of (client, database, collection)
    """
    # Connect with write concern to ensure data persistence
    client = MongoClient(
        MONGO_HOST, 
        MONGO_PORT,
        w=1,
        journal=True
    )
    
    # Test connection
    client.admin.command('ping')
    
    # Check if database exists
    existing_dbs = client.list_database_names()
    if DB_NAME in existing_dbs:
        print(f"     Database '{DB_NAME}' exists")
    else:
        print(f"    ℹ Database '{DB_NAME}' will be created")
    
    # Get database (creates if not exists when we write to it)
    db = client[DB_NAME]
    
    # Check if collection exists
    existing_collections = db.list_collection_names()
    collection_exists = COLLECTION_NAME in existing_collections
    
    if collection_exists:
        print(f"     Collection '{COLLECTION_NAME}' exists - will drop and recreate")
    else:
        print(f"    ℹ Collection '{COLLECTION_NAME}' will be created")
    
    # Get collection with write concern
    collection = db.get_collection(
        COLLECTION_NAME, 
        write_concern=WriteConcern(w=1, j=True)
    )
    
    return client, db, collection, collection_exists


def transform_record(record: dict, fetch_timestamp: datetime) -> dict:
    """
    Transform a record and add metadata.
    
    Args:
        record: Parsed record
        fetch_timestamp: Timestamp when data was fetched
    
    Returns:
        Document for MongoDB
    """
    # Clean up field names (remove any problematic characters)
    cleaned_record = {}
    for key, value in record.items():
        # MongoDB doesn't allow dots in field names
        clean_key = key.replace('.', '_').replace('$', '_')
        cleaned_record[clean_key] = value
    
    # Add metadata
    cleaned_record['fetched_at'] = fetch_timestamp
    cleaned_record['source'] = 'Geological Survey Ireland - Groundwater Wells and Springs'
    cleaned_record['source_url'] = SOURCE_PAGE
    
    return cleaned_record


def main():
    print("=" * 80)
    print("Ireland Groundwater Wells and Springs Data -> MongoDB Pipeline")
    print("=" * 80)
    print(f"\nData Source: data.gov.ie (XML format)")
    print(f"MongoDB: {MONGO_HOST}:{MONGO_PORT}")
    print(f"Database: {DB_NAME}")
    print(f"Collection: {COLLECTION_NAME}")
    print("=" * 80)
    
    # Step 1: Connect to MongoDB
    print("\n[1] Connecting to MongoDB...")
    try:
        client, db, collection, collection_exists = connect_to_mongodb()
        print("     Connected successfully!")
            
    except Exception as e:
        print(f"     Failed to connect to MongoDB: {e}")
        print("    Please ensure MongoDB is running on localhost:27017")
        return
    
    # Step 2: Drop existing collection if exists
    if collection_exists:
        print("\n[2] Dropping existing collection...")
        try:
            collection.drop()
            print(f"     Collection '{COLLECTION_NAME}' dropped")
            
            # Recreate collection reference after drop
            collection = db.get_collection(
                COLLECTION_NAME, 
                write_concern=WriteConcern(w=1, j=True)
            )
        except Exception as e:
            print(f"     Error dropping collection: {e}")
    else:
        print("\n[2] Collection does not exist - will be created")
    
    # Step 3: Download and fetch data with incremental inserts
    print("\n[3] Downloading data with incremental inserts...")
    fetch_timestamp = datetime.now(timezone.utc)
    
    try:
        records = download_and_parse_xml()
        
        # If XML parsing didn't get good results, try JSON fallback with incremental inserts
        if len(records) < 10:
            print("\n    XML parsing returned few records, trying JSON fallback...")
            # Pass collection for incremental inserts
            records = fetch_json_fallback(collection=collection, fetch_timestamp=fetch_timestamp)
            
            # Data already inserted incrementally, skip step 4
            if records:
                print(f"\n[4] Data insertion completed during fetch")
            else:
                print("     No records fetched. Exiting.")
                client.close()
                return
        else:
            # XML worked, need to insert
            print(f"     Total records from XML: {len(records)}")
            
            if records:
                sample_keys = list(records[0].keys())[:5]
                print(f"     Sample fields: {', '.join(sample_keys)}...")
                
                # Step 4: Transform and insert data
                print("\n[4] Inserting data into MongoDB...")
                documents = [transform_record(record, fetch_timestamp) for record in records]
                
                batch_size = 1000
                total_inserted = 0
                
                for i in range(0, len(documents), batch_size):
                    batch = documents[i:i + batch_size]
                    result = collection.insert_many(batch, ordered=False)
                    inserted_count = len(result.inserted_ids)
                    total_inserted += inserted_count
                    print(f"    Inserted batch {i // batch_size + 1}: {inserted_count} records (Total: {total_inserted})")
                
                print(f"     Total inserted: {total_inserted} records")
    except Exception as e:
        print(f"     Error: {e}")
        import traceback
        traceback.print_exc()
    
    # Step 5: Verify data
    print("\n[5] Verifying data in MongoDB...")
    try:
        count = collection.count_documents({})
        print(f"     Total documents in collection: {count}")
    except Exception as e:
        print(f"     Error verifying data: {e}")
        count = 0
    
    # Step 6: Show sample data
    print("\n[6] Sample records:")
    print("-" * 80)
    try:
        sample_records = list(collection.find().limit(3))
        for i, record in enumerate(sample_records, 1):
            print(f"\n  Record {i}:")
            field_count = 0
            for key, value in record.items():
                if key != '_id' and field_count < 10:
                    print(f"    {key}: {value}")
                    field_count += 1
    except Exception as e:
        print(f"     Error fetching sample: {e}")
    
    # Step 7: Show summary statistics
    print("\n" + "=" * 80)
    print("Summary Statistics:")
    print("-" * 80)
    try:
        # Try to group by well type or county if available
        for field in ['Well_Type', 'WELL_TYPE', 'WellType', 'TYPE', 'County', 'COUNTY']:
            pipeline = [
                {"$match": {field: {"$exists": True, "$ne": None}}},
                {"$group": {"_id": f"${field}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            results = list(collection.aggregate(pipeline))
            if results:
                print(f"\n  By {field}:")
                for item in results:
                    val = str(item['_id'])[:30] if item['_id'] else 'Unknown'
                    print(f"    {val:<30}: {item['count']:>6}")
                break
                
    except Exception as e:
        print(f"    ℹ Could not generate statistics: {e}")
    
    print("\n" + "=" * 80)
    print(f" Pipeline complete! {count} records stored in MongoDB")
    print(f"  Database: {DB_NAME}")
    print(f"  Collection: {COLLECTION_NAME}")
    print("=" * 80)
    
    # Close connection
    client.close()
    print("\n MongoDB connection closed.")


if __name__ == "__main__":
    main()
