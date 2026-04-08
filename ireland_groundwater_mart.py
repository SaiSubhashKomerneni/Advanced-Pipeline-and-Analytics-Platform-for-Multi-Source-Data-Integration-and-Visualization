"""
Ireland Groundwater Data - Mart Processing Layer
=================================================
Reads staged data from MongoDB, performs cleaning and transformation,
and stores processed data in PostgreSQL Mart database.

Processing Steps:
1. Read raw data from MongoDB (Rawdata.Ireland_Groundwater_Wells_Springs)
2. Clean and transform data
3. Store processed data in PostgreSQL (Mart.ireland_groundwater)

Improvements Applied:
1. Centralized Config: Uses shared.config for all settings
2. Logging: Replaced print() with structured logging
3. Error Handling: Retry logic via shared.db_helpers
4. Validation: Pydantic models for data validation
5. Upsert: Uses ON CONFLICT for idempotent operations
"""

import sys
from pathlib import Path
from datetime import datetime
import re

# Add parent directory to path for shared imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from psycopg2 import sql
from pymongo import MongoClient
from pydantic import BaseModel, Field, validator
from typing import Optional

# Import shared modules
from shared.config import config
from shared.logger import get_logger, PipelineLogger
from shared.db_helpers import (
    get_mongo_client,
    get_postgres_connection,
    ensure_database_exists,
    retry_on_failure
)

# Initialize logger
logger = get_logger(__name__)

# Configuration from shared config
MONGO_DB = config.MONGO_DB  # Rawdata
MONGO_COLLECTION = config.MONGO_COLLECTION_IRELAND  # Ireland_Groundwater_Wells_Springs
MART_DB = config.PG_MART_DB  # Mart
TARGET_TABLE = config.PG_TABLE_IRELAND  # ireland_groundwater


# Pydantic model for Ireland Groundwater record validation
class IrelandGroundwaterMartRecord(BaseModel):
    """Validated Ireland Groundwater record for Mart database."""
    well_id: str = Field(..., max_length=100)
    source_name: str = Field(default="Unknown", max_length=255)
    source_type: str = Field(default="Unknown", max_length=50)
    county: str = Field(default="Unknown", max_length=100)
    townland: Optional[str] = Field(default=None, max_length=255)
    hole_depth_m: float = Field(default=0, ge=0, le=5000)
    depth_to_bedrock_m: float = Field(default=0, ge=0, le=1000)
    drill_year: Optional[int] = Field(default=None, ge=1800, le=2030)
    yield_m3_day: float = Field(default=0, ge=0)
    yield_category: str = Field(default="Unknown", max_length=30)
    abstraction_purpose: str = Field(default="Unknown", max_length=100)
    coord_x: Optional[float] = None
    coord_y: Optional[float] = None
    location_accuracy: str = Field(default="Unknown", max_length=50)
    data_completeness_score: int = Field(default=0, ge=0, le=100)
    has_yield_data: bool = False
    has_depth_data: bool = False
    has_location_data: bool = False
    
    class Config:
        str_strip_whitespace = True


@retry_on_failure(max_retries=3)
def connect_mongodb():
    """Connect to MongoDB and return the collection with retry logic."""
    logger.info(f"Connecting to MongoDB ({MONGO_DB}.{MONGO_COLLECTION})...")
    client = get_mongo_client()
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    count = collection.count_documents({})
    logger.info(f"Connected! Found {count:,} documents in {MONGO_DB}.{MONGO_COLLECTION}")
    return client, collection


def create_mart_database():
    """Create Mart database in PostgreSQL if not exists."""
    logger.info("Ensuring Mart database exists...")
    ensure_database_exists(MART_DB)


@retry_on_failure(max_retries=3)
def connect_postgresql():
    """Connect to PostgreSQL Mart database with retry logic."""
    logger.info(f"Connecting to PostgreSQL Mart database ({MART_DB})...")
    conn = get_postgres_connection(database=MART_DB.lower())
    logger.info(f"Connected to PostgreSQL {MART_DB} database")
    return conn


def create_target_table(conn):
    """Create target table with unique constraint for upsert."""
    logger.info("Creating target table...")
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        )
    """, (TARGET_TABLE,))
    table_exists = cursor.fetchone()[0]
    
    if not table_exists:
        # Create optimized table - removed redundant columns, added defaults for nulls
        create_sql = f"""
        CREATE TABLE {TARGET_TABLE} (
            id SERIAL PRIMARY KEY,
            well_id VARCHAR(100) NOT NULL UNIQUE,
            source_name VARCHAR(255),
            source_type VARCHAR(50) DEFAULT 'Unknown',
            county VARCHAR(100) DEFAULT 'Unknown',
            townland VARCHAR(255),
            hole_depth_m DECIMAL(8, 2) DEFAULT 0,
            depth_to_bedrock_m DECIMAL(8, 2) DEFAULT 0,
            drill_year INTEGER,
            yield_m3_day DECIMAL(10, 2) DEFAULT 0,
            yield_category VARCHAR(30) DEFAULT 'Unknown',
            abstraction_purpose VARCHAR(100) DEFAULT 'Unknown',
            coord_x DECIMAL(12, 4),
            coord_y DECIMAL(12, 4),
            location_accuracy VARCHAR(50) DEFAULT 'Unknown',
            data_completeness_score INTEGER DEFAULT 0,
            has_yield_data BOOLEAN DEFAULT FALSE,
            has_depth_data BOOLEAN DEFAULT FALSE,
            has_location_data BOOLEAN DEFAULT FALSE,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_sql)
        conn.commit()
        logger.info(f"Created table '{TARGET_TABLE}' with unique constraint on well_id")
    else:
        logger.info(f"Table '{TARGET_TABLE}' already exists - using upsert")
    
    cursor.close()


def clean_string(value):
    """Clean string values."""
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        # Remove multiple spaces
        cleaned = re.sub(r'\s+', ' ', cleaned)
        return cleaned if cleaned and cleaned.lower() not in ['none', 'null', 'n/a', 'na'] else None
    return str(value)


def clean_float(value, default=None, min_val=None, max_val=None):
    """Clean and validate float values with optional default and range validation."""
    if value is None:
        return default
    try:
        val = float(value)
        if val != val:  # NaN check
            return default
        # Range validation
        if min_val is not None and val < min_val:
            return default
        if max_val is not None and val > max_val:
            return default
        return val
    except (ValueError, TypeError):
        return default


def clean_integer(value):
    """Clean and validate integer values."""
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def extract_year_from_timestamp(timestamp):
    """Extract year from Unix timestamp (milliseconds)."""
    if timestamp is None:
        return None
    try:
        # Handle negative timestamps (dates before 1970)
        ts = float(timestamp)
        if ts < -50000000000000:  # Invalid very negative value
            return None
        # Convert from milliseconds to seconds
        ts_seconds = ts / 1000
        dt = datetime.fromtimestamp(ts_seconds)
        year = dt.year
        if 1800 <= year <= 2030:
            return year
        return None
    except (ValueError, TypeError, OSError):
        return None


def categorize_yield(yield_value):
    """Categorize well yield into bands."""
    if yield_value is None:
        return 'Unknown'
    if yield_value < 10:
        return 'Low (<10 m³/day)'
    elif yield_value < 100:
        return 'Medium (10-100 m³/day)'
    elif yield_value < 500:
        return 'High (100-500 m³/day)'
    else:
        return 'Very High (500+ m³/day)'


def calculate_completeness_score(record):
    """Calculate data completeness score (0-100)."""
    score = 0
    
    # Essential fields (higher weight)
    essential = ['well_id', 'source_type', 'county']
    for field in essential:
        if record.get(field) is not None:
            score += 15
    
    # Important fields
    important = ['hole_depth_m', 'depth_to_bedrock_m', 'coord_x', 'coord_y']
    for field in important:
        if record.get(field) is not None:
            score += 10
    
    # Nice to have
    optional = ['yield_m3_day', 'drill_year', 'townland']
    for field in optional:
        if record.get(field) is not None:
            score += 5
    
    return min(score, 100)


def fetch_and_transform_data(collection):
    """Fetch data from MongoDB and perform transformations with Pydantic validation."""
    logger.info("Fetching and transforming data...")
    
    # Fetch all documents
    documents = list(collection.find({}))
    logger.info(f"Fetched {len(documents):,} raw documents")
    
    transformed_records = []
    stats = {
        'total': len(documents),
        'valid': 0,
        'missing_well_id': 0,
        'missing_county': 0,
        'invalid_depth': 0,
        'invalid_coordinates': 0,
        'has_yield': 0,
        'has_depth': 0,
        'validation_errors': 0,
        'duplicates': 0
    }
    
    seen_well_ids = set()
    
    for doc in documents:
        # Extract well_id - check various possible field names
        well_id = clean_string(doc.get('WELL_ID') or doc.get('Well_ID') or doc.get('well_id'))
        
        if not well_id:
            stats['missing_well_id'] += 1
            continue
        
        # Skip duplicates
        if well_id in seen_well_ids:
            stats['duplicates'] += 1
            continue
        seen_well_ids.add(well_id)
        
        # Extract and clean fields
        gsi_name = clean_string(doc.get('GSI_NAME') or doc.get('gsi_name'))
        original_name = clean_string(doc.get('ORIG_NAME') or doc.get('orig_name'))
        source_name = clean_string(doc.get('SOURCENAM') or doc.get('sourcenam'))
        source_type = clean_string(doc.get('SOURCETYP') or doc.get('sourcetyp'))
        county = clean_string(doc.get('COUNTY') or doc.get('county'))
        townland = clean_string(doc.get('TOWNLAND') or doc.get('townland'))
        
        # Depth fields
        hole_depth = clean_float(doc.get('HOLEDEPTHM') or doc.get('holedepthm'))
        depth_to_bedrock = clean_float(doc.get('DTB_M') or doc.get('dtb_m'))
        bedrock_confidence = clean_string(doc.get('DTB_CONFID') or doc.get('dtb_confid'))
        
        # Validate depths
        if hole_depth is not None and (hole_depth < 0 or hole_depth > 5000):
            stats['invalid_depth'] += 1
            hole_depth = None
        
        if depth_to_bedrock is not None and (depth_to_bedrock < 0 or depth_to_bedrock > 1000):
            depth_to_bedrock = None
        
        # Drill date
        drill_timestamp = doc.get('DRILL_DATE') or doc.get('drill_date')
        drill_year = extract_year_from_timestamp(drill_timestamp)
        
        # Yield data
        yield_value = clean_float(doc.get('YIELD_M3D') or doc.get('yield_m3d') or 
                                   doc.get('YIELD') or doc.get('yield'))
        
        # Abstraction purpose
        abstraction = clean_string(doc.get('ABSTRACTIO') or doc.get('abstractio') or
                                    doc.get('ABSTRACTION') or doc.get('abstraction'))
        
        # Coordinates
        coord_x = clean_float(doc.get('X') or doc.get('x') or 
                              (doc.get('geometry', {}).get('x') if isinstance(doc.get('geometry'), dict) else None))
        coord_y = clean_float(doc.get('Y') or doc.get('y') or 
                              (doc.get('geometry', {}).get('y') if isinstance(doc.get('geometry'), dict) else None))
        
        # Validate coordinates (Irish ITM coordinates range)
        has_location = False
        if coord_x and coord_y:
            # ITM coordinates for Ireland roughly: X: 400000-800000, Y: 500000-1000000
            if 0 < coord_x < 1000000 and 0 < coord_y < 1200000:
                has_location = True
            else:
                stats['invalid_coordinates'] += 1
                coord_x, coord_y = None, None
        
        location_accuracy = clean_string(doc.get('LOC_ACC') or doc.get('loc_acc') or
                                          doc.get('LOCACC') or doc.get('locacc'))
        
        # Track statistics
        if not county:
            stats['missing_county'] += 1
        
        has_yield = yield_value is not None
        has_depth = hole_depth is not None or depth_to_bedrock is not None
        
        if has_yield:
            stats['has_yield'] += 1
        if has_depth:
            stats['has_depth'] += 1
        
        # Create and validate record with Pydantic
        try:
            record_data = {
                'well_id': well_id[:100] if len(well_id) > 100 else well_id,
                'source_name': (source_name[:255] if source_name and len(source_name) > 255 else source_name) or 'Unknown',
                'source_type': source_type or 'Unknown',
                'county': county or 'Unknown',
                'townland': townland[:255] if townland and len(townland) > 255 else townland,
                'hole_depth_m': hole_depth if hole_depth is not None else 0,
                'depth_to_bedrock_m': depth_to_bedrock if depth_to_bedrock is not None else 0,
                'drill_year': drill_year,  # Keep NULL for missing years
                'yield_m3_day': yield_value if yield_value is not None else 0,
                'yield_category': categorize_yield(yield_value),
                'abstraction_purpose': (abstraction[:100] if abstraction and len(abstraction) > 100 else abstraction) or 'Unknown',
                'coord_x': coord_x,  # Keep NULL for missing coords
                'coord_y': coord_y,
                'location_accuracy': location_accuracy or 'Unknown',
                'has_yield_data': has_yield,
                'has_depth_data': has_depth,
                'has_location_data': has_location
            }
            record_data['data_completeness_score'] = calculate_completeness_score(record_data)
            
            # Validate with Pydantic
            validated = IrelandGroundwaterMartRecord(**record_data)
            transformed_records.append(validated.model_dump())
            stats['valid'] += 1
            
        except Exception as e:
            stats['validation_errors'] += 1
            continue
    
    logger.info(f"Transformed {len(transformed_records):,} records")
    logger.info("Statistics:")
    logger.info(f"  - Valid records: {stats['valid']:,}")
    logger.info(f"  - Missing well_id: {stats['missing_well_id']:,}")
    logger.info(f"  - Missing county: {stats['missing_county']:,}")
    logger.info(f"  - Invalid depths: {stats['invalid_depth']:,}")
    logger.info(f"  - Invalid coordinates: {stats['invalid_coordinates']:,}")
    logger.info(f"  - Duplicates: {stats['duplicates']:,}")
    logger.info(f"  - Records with yield data: {stats['has_yield']:,}")
    logger.info(f"  - Records with depth data: {stats['has_depth']:,}")
    logger.info(f"  - Validation errors: {stats['validation_errors']:,}")
    
    return transformed_records, stats


def insert_to_postgresql(conn, records):
    """Insert transformed records into PostgreSQL using upsert."""
    logger.info("Inserting data into PostgreSQL Mart using upsert...")
    
    cursor = conn.cursor()
    
    # Use ON CONFLICT for upsert (idempotent operations) on well_id unique constraint
    upsert_sql = f"""
    INSERT INTO {TARGET_TABLE} 
    (well_id, source_name, source_type, county, townland,
     hole_depth_m, depth_to_bedrock_m, drill_year,
     yield_m3_day, yield_category, abstraction_purpose, coord_x, coord_y,
     location_accuracy, data_completeness_score, has_yield_data, has_depth_data, has_location_data)
    VALUES (%s, COALESCE(%s, 'Unknown'), COALESCE(%s, 'Unknown'), COALESCE(%s, 'Unknown'), %s,
            COALESCE(%s, 0), COALESCE(%s, 0), %s,
            COALESCE(%s, 0), COALESCE(%s, 'Unknown'), COALESCE(%s, 'Unknown'), %s, %s,
            COALESCE(%s, 'Unknown'), COALESCE(%s, 0), COALESCE(%s, FALSE), COALESCE(%s, FALSE), COALESCE(%s, FALSE))
    ON CONFLICT (well_id)
    DO UPDATE SET
        source_name = EXCLUDED.source_name,
        source_type = EXCLUDED.source_type,
        county = EXCLUDED.county,
        townland = EXCLUDED.townland,
        hole_depth_m = EXCLUDED.hole_depth_m,
        depth_to_bedrock_m = EXCLUDED.depth_to_bedrock_m,
        drill_year = EXCLUDED.drill_year,
        yield_m3_day = EXCLUDED.yield_m3_day,
        yield_category = EXCLUDED.yield_category,
        abstraction_purpose = EXCLUDED.abstraction_purpose,
        coord_x = EXCLUDED.coord_x,
        coord_y = EXCLUDED.coord_y,
        location_accuracy = EXCLUDED.location_accuracy,
        data_completeness_score = EXCLUDED.data_completeness_score,
        has_yield_data = EXCLUDED.has_yield_data,
        has_depth_data = EXCLUDED.has_depth_data,
        has_location_data = EXCLUDED.has_location_data,
        processed_at = CURRENT_TIMESTAMP
    """
    
    batch_size = config.BATCH_SIZE
    total_inserted = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batch_data = [
            (
                r['well_id'], r['source_name'],
                r['source_type'], r['county'], r['townland'], r['hole_depth_m'],
                r['depth_to_bedrock_m'], r['drill_year'],
                r['yield_m3_day'], r['yield_category'], r['abstraction_purpose'],
                r['coord_x'], r['coord_y'], r['location_accuracy'],
                r['data_completeness_score'], r['has_yield_data'], r['has_depth_data'],
                r['has_location_data']
            )
            for r in batch
        ]
        
        cursor.executemany(upsert_sql, batch_data)
        conn.commit()
        total_inserted += len(batch)
        logger.info(f"Upserted batch {i // batch_size + 1}: {len(batch)} records (Total: {total_inserted:,})")
    
    logger.info(f"Total upserted: {total_inserted:,} records")
    cursor.close()
    return total_inserted


def verify_and_summarize(conn):
    """Verify inserted data and show summary."""
    logger.info("Running verification and summary...")
    
    cursor = conn.cursor()
    
    # Total count
    cursor.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
    total = cursor.fetchone()[0]
    logger.info(f"Total records in Mart: {total:,}")
    
    # Count by county
    cursor.execute(f"""
        SELECT county, COUNT(*) as cnt
        FROM {TARGET_TABLE}
        WHERE county IS NOT NULL
        GROUP BY county
        ORDER BY cnt DESC
        LIMIT 10
    """)
    results = cursor.fetchall()
    
    logger.info("Top 10 Counties by Well Count:")
    for row in results:
        logger.info(f"  {row[0]:<25} | {row[1]:>8,}")
    
    # Count by source type
    cursor.execute(f"""
        SELECT source_type, COUNT(*) as cnt
        FROM {TARGET_TABLE}
        WHERE source_type IS NOT NULL
        GROUP BY source_type
        ORDER BY cnt DESC
    """)
    type_results = cursor.fetchall()
    
    logger.info("By Source Type:")
    for row in type_results:
        logger.info(f"  {row[0]:<25} | {row[1]:>8,}")
    
    # Count by yield category
    cursor.execute(f"""
        SELECT yield_category, COUNT(*) as cnt
        FROM {TARGET_TABLE}
        GROUP BY yield_category
        ORDER BY cnt DESC
    """)
    yield_results = cursor.fetchall()
    
    logger.info("By Yield Category:")
    for row in yield_results:
        logger.info(f"  {row[0]:<25} | {row[1]:>8,}")
    
    # Data quality stats
    cursor.execute(f"""
        SELECT 
            ROUND(AVG(data_completeness_score), 2) as avg_score,
            SUM(CASE WHEN has_yield_data THEN 1 ELSE 0 END) as with_yield,
            SUM(CASE WHEN has_depth_data THEN 1 ELSE 0 END) as with_depth,
            SUM(CASE WHEN has_location_data THEN 1 ELSE 0 END) as with_location
        FROM {TARGET_TABLE}
    """)
    quality = cursor.fetchone()
    
    logger.info("Data Quality Metrics:")
    logger.info(f"  Average Completeness Score: {quality[0]}/100")
    logger.info(f"  Records with Yield Data:    {quality[1]:,}")
    logger.info(f"  Records with Depth Data:    {quality[2]:,}")
    logger.info(f"  Records with Location Data: {quality[3]:,}")
    
    # Sample records
    cursor.execute(f"""
        SELECT well_id, county, source_type, hole_depth_m, data_completeness_score
        FROM {TARGET_TABLE}
        WHERE county IS NOT NULL
        ORDER BY data_completeness_score DESC
        LIMIT 5
    """)
    samples = cursor.fetchall()
    
    logger.info("Sample High-Quality Records:")
    for row in samples:
        depth_str = f"{row[3]:.1f}m" if row[3] else "N/A"
        logger.info(f"  {row[0][:20]:<20} | {row[1]:<15} | {row[2]:<10} | {depth_str:>10} | Score: {row[4]}")
    
    cursor.close()
    return total


def main():
    logger.info("=" * 80)
    logger.info("Ireland Groundwater Data - Mart Processing Pipeline")
    logger.info("=" * 80)
    logger.info(f"Source: MongoDB ({MONGO_DB}.{MONGO_COLLECTION})")
    logger.info(f"Target: PostgreSQL ({MART_DB}.{TARGET_TABLE})")
    logger.info("=" * 80)
    
    mongo_client = None
    pg_conn = None
    
    try:
        with PipelineLogger(logger, "Ireland Groundwater Mart Processing"):
            # Step 1: Connect to databases
            logger.info("[1] Connecting to databases...")
            mongo_client, collection = connect_mongodb()
            
            create_mart_database()
            pg_conn = connect_postgresql()
            create_target_table(pg_conn)
            
            # Step 2: Fetch and transform data
            logger.info("[2] Fetching and transforming data...")
            records, stats = fetch_and_transform_data(collection)
            
            if not records:
                logger.error("No records to process. Exiting.")
                return
            
            # Step 3: Insert into PostgreSQL using upsert
            logger.info("[3] Upserting to PostgreSQL...")
            inserted = insert_to_postgresql(pg_conn, records)
            
            # Step 4: Verify and summarize
            logger.info("[4] Verification...")
            total = verify_and_summarize(pg_conn)
            
            logger.info("=" * 80)
            logger.info("Mart processing complete!")
            logger.info(f"  Source records: {stats['total']:,}")
            logger.info(f"  Processed records: {total:,}")
            logger.info(f"  Target: PostgreSQL {MART_DB}.{TARGET_TABLE}")
            logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        
    finally:
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB connection closed.")
        if pg_conn:
            pg_conn.close()
            logger.info("PostgreSQL connection closed.")


if __name__ == "__main__":
    main()
