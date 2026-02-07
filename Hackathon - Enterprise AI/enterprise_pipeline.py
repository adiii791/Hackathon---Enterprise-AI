"""
ENTERPRISE AI FOUNDATION PIPELINE
@scale Applied AI Challenge 2026 - Maharathwada

PROBLEM: Transform 4 messy CSV datasets into analytics-ready features
DATASETS: Weather, Station Region, Activity Logs, Reference Units

KEY FOCUS AREAS (from problem statement):
1. Data Quality: Handle missing values, duplicates, inconsistencies
2. Data Governance: Lineage tracking, auditability, quality metrics
3. Feature Engineering: Create versioned features (V1 and V2)
4. Reliability: Failure recovery, idempotent re-runs
5. Merge & Reconcile: Combine datasets intelligently


"""

import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
from pathlib import Path
import hashlib
import sys


# ============================================================================
# CONFIGURATION & SETUP
# ============================================================================

class PipelineConfig:
    """Central configuration for pipeline"""
    
    # File paths (MODIFY THESE based on actual filenames)
    RAW_DATA_DIR = "raw_data"
    OUTPUT_DIR = "output"
    
    # Data files provided by organizers
    WEATHER_FILE = "weather.csv"
    STATION_FILE = "station_region.csv"
    ACTIVITY_FILE = "activity_logs.csv"
    UNITS_FILE = "reference_units.csv"
    
    # Quality thresholds
    MAX_MISSING_PCT = 30  # Fail if >30% missing in critical columns
    MAX_DUPLICATE_PCT = 15  # Warn if >15% duplicates
    
    # Feature versions
    FEATURE_V1 = "v1"  # Basic features
    FEATURE_V2 = "v2"  # Enhanced features


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_execution.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# STAGE 1: DATA INGESTION WITH VALIDATION
# ============================================================================

class DataIngestion:
    """
    FOCUS: File validation, integrity checking, metadata capture
    WHY: Ensures we catch problems early, tracks data lineage
    """
    
    def __init__(self):
        self.metadata = {}
        
    def calculate_hash(self, filepath):
        """Calculate MD5 hash for data integrity"""
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def load_csv(self, filepath, description):
        """Load CSV with validation and metadata capture"""
        logger.info(f"Loading: {filepath}")
        
        try:
            # Read CSV
            df = pd.read_csv(filepath)
            
            # Capture metadata (GOVERNANCE requirement)
            meta = {
                'filename': filepath,
                'description': description,
                'timestamp': datetime.now().isoformat(),
                'rows': len(df),
                'columns': len(df.columns),
                'file_hash': self.calculate_hash(filepath),
                'column_names': list(df.columns),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                'missing_pct': (df.isnull().sum().sum() / (df.shape[0] * df.shape[1]) * 100)
            }
            
            self.metadata[filepath] = meta
            
            logger.info(f"✓ Loaded: {len(df)} rows, {len(df.columns)} columns")
            logger.info(f"  Missing: {meta['missing_pct']:.2f}%")
            
            return df
            
        except Exception as e:
            logger.error(f" Failed to load {filepath}: {str(e)}")
            raise
    
    def ingest_all(self):
        """Load all 4 datasets"""
        logger.info("="*60)
        logger.info("STAGE 1: DATA INGESTION")
        logger.info("="*60)
        
        datasets = {}
        
        # Load each dataset
        try:
            datasets['weather'] = self.load_csv(
                f"{PipelineConfig.RAW_DATA_DIR}/{PipelineConfig.WEATHER_FILE}",
                "Weather measurements from stations"
            )
            
            datasets['stations'] = self.load_csv(
                f"{PipelineConfig.RAW_DATA_DIR}/{PipelineConfig.STATION_FILE}",
                "Station location and region mapping"
            )
            
            datasets['activity'] = self.load_csv(
                f"{PipelineConfig.RAW_DATA_DIR}/{PipelineConfig.ACTIVITY_FILE}",
                "Activity logs from monitoring"
            )
            
            datasets['units'] = self.load_csv(
                f"{PipelineConfig.RAW_DATA_DIR}/{PipelineConfig.UNITS_FILE}",
                "Unit conversion references"
            )
            
        except FileNotFoundError as e:
            logger.error("Missing data files! Place CSV files in 'raw_data/' directory")
            raise
        
        return datasets


# ============================================================================
# STAGE 2: DATA CLEANING & QUALITY
# ============================================================================

class DataCleaning:
    """
    FOCUS: Handle missing values, duplicates, outliers, standardization
    WHY: Core requirement - make dirty data usable
    """
    
    def __init__(self):
        self.quality_reports = {}
    
    def quality_metrics(self, df, name, stage):
        """Calculate quality metrics (GOVERNANCE requirement)"""
        metrics = {
            'stage': stage,
            'timestamp': datetime.now().isoformat(),
            'rows': len(df),
            'columns': len(df.columns),
            'missing_cells': df.isnull().sum().sum(),
            'missing_pct': (df.isnull().sum().sum() / (df.shape[0] * df.shape[1]) * 100),
            'duplicate_rows': df.duplicated().sum(),
            'duplicate_pct': (df.duplicated().sum() / len(df) * 100) if len(df) > 0 else 0
        }
        
        if name not in self.quality_reports:
            self.quality_reports[name] = []
        self.quality_reports[name].append(metrics)
        
        return metrics
    
    def clean_weather(self, df):
        """Clean weather data - SPECIFIC TO EXPECTED SCHEMA"""
        logger.info("\nCleaning: Weather Data")
        
        # Track quality before
        before = self.quality_metrics(df, 'weather', 'before')
        
        df_clean = df.copy()
        
        # 1. Remove exact duplicates
        initial = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        logger.info(f"  Removed {initial - len(df_clean)} duplicates")
        
        # 2. Standardize column names (handle different formats)
        df_clean.columns = df_clean.columns.str.lower().str.strip()
        
        # 3. Handle station_id (CRITICAL for merging)
        if 'station_id' in df_clean.columns:
            df_clean['station_id'] = df_clean['station_id'].astype(str).str.strip().str.upper()
            # Drop rows with missing station_id
            df_clean = df_clean.dropna(subset=['station_id'])
        
        # 4. Clean date/time columns
        date_cols = [col for col in df_clean.columns if 'date' in col or 'time' in col]
        for col in date_cols:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
            df_clean = df_clean.dropna(subset=[col])
        
        # 5. Handle numeric columns (temperature, rainfall, etc.)
        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            # Remove impossible values (domain knowledge)
            if 'temp' in col.lower():
                df_clean = df_clean[(df_clean[col] >= -50) & (df_clean[col] <= 60)]
            elif 'rain' in col.lower() or 'precip' in col.lower():
                df_clean.loc[df_clean[col] < 0, col] = 0  # Can't have negative rain
            
            # Fill missing with median (robust to outliers)
            if df_clean[col].isnull().sum() > 0:
                median_val = df_clean[col].median()
                df_clean[col].fillna(median_val, inplace=True)
        
        # Track quality after
        after = self.quality_metrics(df_clean, 'weather', 'after')
        
        logger.info(f"  Quality: {before['missing_pct']:.1f}% → {after['missing_pct']:.1f}% missing")
        
        return df_clean
    
    def clean_stations(self, df):
        """Clean station region mapping"""
        logger.info("\nCleaning: Station Region Data")
        
        before = self.quality_metrics(df, 'stations', 'before')
        
        df_clean = df.copy()
        df_clean.columns = df_clean.columns.str.lower().str.strip()
        
        # Remove duplicates
        df_clean = df_clean.drop_duplicates()
        
        # Standardize station_id
        if 'station_id' in df_clean.columns:
            df_clean['station_id'] = df_clean['station_id'].astype(str).str.strip().str.upper()
        
        # Standardize location names (Title Case)
        text_cols = df_clean.select_dtypes(include=['object']).columns
        for col in text_cols:
            if col != 'station_id':
                df_clean[col] = df_clean[col].astype(str).str.strip().str.title()
        
        after = self.quality_metrics(df_clean, 'stations', 'after')
        
        logger.info(f"  Quality: {before['missing_pct']:.1f}% → {after['missing_pct']:.1f}% missing")
        
        return df_clean
    
    def clean_activity(self, df):
        df.columns = df.columns.str.strip().str.lower()
        df = df.copy()

    # -------- NUMERIC COLUMNS --------
        df["irrigationhours"] = df["irrigationhours"].fillna(0)
        df["fertilizer_amount"] = df["fertilizer_amount"].fillna(
        df["fertilizer_amount"].median()
    )
       

    # -------- CATEGORICAL COLUMNS --------
        df["region"] = df["region"].fillna("UNKNOWN")
        df["croptype"] = df["croptype"].fillna("UNKNOWN")

    # -------- DATE COLUMN --------
        df["activitydate"] = pd.to_datetime(
        df["activitydate"], errors="coerce"
    )

    # Drop rows where date is invalid
        df = df.dropna(subset=["activitydate"])

        return df

    
    def clean_units(self, df):
        """Clean reference units"""
        logger.info("\nCleaning: Reference Units")
        
        before = self.quality_metrics(df, 'units', 'before')
        
        df_clean = df.copy()
        df_clean.columns = df_clean.columns.str.lower().str.strip()
        
        # Remove duplicates
        df_clean = df_clean.drop_duplicates()
        
        # Standardize unit names
        text_cols = df_clean.select_dtypes(include=['object']).columns
        for col in text_cols:
            df_clean[col] = df_clean[col].astype(str).str.strip().str.lower()
        
        after = self.quality_metrics(df_clean, 'units', 'after')
        
        logger.info(f"  Quality: {before['missing_pct']:.1f}% → {after['missing_pct']:.1f}% missing")
        
        return df_clean
    
    def clean_all(self, datasets):
        """Clean all datasets"""
        logger.info("="*60)
        logger.info("STAGE 2: DATA CLEANING")
        logger.info("="*60)
        
        cleaned = {}
        cleaned['weather'] = self.clean_weather(datasets['weather'])
        cleaned['stations'] = self.clean_stations(datasets['stations'])
        cleaned['activity'] = self.clean_activity(datasets['activity'])
        cleaned['units'] = self.clean_units(datasets['units'])
        
        return cleaned


# ============================================================================
# STAGE 3: DATA MERGING & RECONCILIATION
# ============================================================================

class DataMerging:
    """
    FOCUS: Reconcile datasets, track lineage, validate merge quality
    WHY: Core requirement - create unified analytical dataset
    """
    
    def __init__(self):
        self.lineage = []
    
    def merge_datasets(self, cleaned):
        """Merge all datasets intelligently"""
        logger.info("="*60)
        logger.info("STAGE 3: DATA MERGING")
        logger.info("="*60)
        
        # Start with weather as base (usually most records)
        base = cleaned['weather'].copy()
        logger.info(f"\nBase dataset: Weather ({len(base)} records)")
        
        # Merge with stations (LEFT JOIN to keep all weather records)
        if 'station_id' in base.columns and 'station_id' in cleaned['stations'].columns:
            base = base.merge(
                cleaned['stations'],
                on='station_id',
                how='left',
                suffixes=('', '_station')
            )
            logger.info(f"After station merge: {len(base)} records")
            self.lineage.append({
                'timestamp': datetime.now().isoformat(),
                'operation': 'merge_weather_stations',
                'left_rows': len(cleaned['weather']),
                'right_rows': len(cleaned['stations']),
                'result_rows': len(base)
            })
        
        # Add activity data if possible (might need date-based join)
        # This is dataset-specific - adjust based on actual schema
        
        # Store units as reference (not directly merged)
        base.attrs['unit_conversions'] = cleaned['units'].to_dict('records')
        
        logger.info(f"\nFinal unified dataset: {base.shape[0]} rows × {base.shape[1]} columns")
        
        return base


# ============================================================================
# STAGE 4: FEATURE ENGINEERING (V1 and V2)
# ============================================================================

class FeatureEngineering:
    """
    Handles feature creation for Enterprise AI Pipeline
    Generates V1 and V2 feature sets
    """

 
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.feature_catalog = {}
    def engineer_features(self, unified_df: pd.DataFrame):
        """
        Main entry point for feature engineering
        """
        self.logger.info("Starting feature engineering")

        df_v1 = self.create_v1_features(unified_df.copy())
        df_v2 = self.create_v2_features(df_v1.copy())

        return df_v1, df_v2

    # -------------------------------
    # V1 FEATURES (BASIC + SAFE)
    # -------------------------------
    def create_v1_features(self, df: pd.DataFrame):
        self.logger.info("Creating V1 features")

        # ---- Identify temperature column safely ----
        possible_temp_cols = ['temperature', 'temp', 'temp_c', 'avg_temp']

        temp_col = None
        for col in possible_temp_cols:
            if col in df.columns:
                temp_col = col
                break

        if temp_col is None:
            raise ValueError("No temperature column found in dataset")

        # ---- Force numeric temperature ----
        df[temp_col] = pd.to_numeric(df[temp_col], errors='coerce')

        # ---- Remove invalid rows ----
        df = df.dropna(subset=[temp_col])

        # ---- Temperature Category ----
        df['temp_category'] = pd.cut(
            df[temp_col],
            bins=[-np.inf, 0, 15, 25, np.inf],
            labels=['Cold', 'Cool', 'Warm', 'Hot']
        )

        # ---- Basic flags ----
        df['is_hot'] = df[temp_col] > 25
        df['is_cold'] = df[temp_col] < 5

        self.logger.info("V1 feature creation completed")
        self.feature_catalog.update({
    "temp_category": "Temperature bucket: Cold/Cool/Warm/Hot",
    "is_hot": "True if temperature > 25C",
    "is_cold": "True if temperature < 5C"
})
        return df

    # -------------------------------
    # V2 FEATURES (DERIVED + AGGREGATE)
    # -------------------------------
    def create_v2_features(self, df: pd.DataFrame):
        self.logger.info("Creating V2 features")

        # ---- Rolling temperature trend (if date exists) ----
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.sort_values('date')

            df['temp_rolling_avg_3'] = (
                df.select_dtypes(include=[np.number])
                  .iloc[:, 0]
                  .rolling(window=3, min_periods=1)
                  .mean()
            )

        # ---- Encoded category ----
        if 'temp_category' in df.columns:
            df['temp_category_code'] = df['temp_category'].astype('category').cat.codes

        self.logger.info("V2 feature creation completed")
        self.feature_catalog.update({
    "temp_rolling_avg_3": "3-point rolling average of temperature",
    "temp_category_code": "Encoded numerical form of temperature category"
})
        return df


# ============================================================================
# PIPELINE ORCHESTRATOR
# ============================================================================

class EnterprisePipeline:
    """
    Main pipeline orchestrator with failure recovery
    FOCUS: Reliability, governance, auditability
    """
    
    def __init__(self):
        self.ingestion = DataIngestion()
        self.cleaning = DataCleaning()
        self.merging = DataMerging()
        self.features = FeatureEngineering()
        
        # Create output directories
        Path(PipelineConfig.OUTPUT_DIR).mkdir(exist_ok=True)
        Path(f"{PipelineConfig.OUTPUT_DIR}/cleaned").mkdir(exist_ok=True)
        Path(f"{PipelineConfig.OUTPUT_DIR}/features").mkdir(exist_ok=True)
        Path(f"{PipelineConfig.OUTPUT_DIR}/metadata").mkdir(exist_ok=True)
    
    def run(self):
        """Execute full pipeline"""
        try:
            logger.info("\n" + "="*60)
            logger.info("ENTERPRISE AI FOUNDATION PIPELINE")
            logger.info("="*60)
            
            # STAGE 1: Ingest
            raw_datasets = self.ingestion.ingest_all()
            
            # STAGE 2: Clean
            cleaned_datasets = self.cleaning.clean_all(raw_datasets)
            
            # Save cleaned datasets
            for name, df in cleaned_datasets.items():
                output_path = f"{PipelineConfig.OUTPUT_DIR}/cleaned/{name}_cleaned.csv"
                df.to_csv(output_path, index=False)
                logger.info(f"Saved: {output_path}")
            
            # STAGE 3: Merge
            unified_df = self.merging.merge_datasets(cleaned_datasets)
            
            # Save unified dataset
            unified_path = f"{PipelineConfig.OUTPUT_DIR}/unified_dataset.csv"
            unified_df.to_csv(unified_path, index=False)
            logger.info(f"Saved: {unified_path}")
            
            # STAGE 4: Feature Engineering
            df_v1, df_v2 = self.features.engineer_features(unified_df)
            
            # Save feature datasets
            v1_path = f"{PipelineConfig.OUTPUT_DIR}/features/features_v1.csv"
            v2_path = f"{PipelineConfig.OUTPUT_DIR}/features/features_v2.csv"
            df_v1.to_csv(v1_path, index=False)
            df_v2.to_csv(v2_path, index=False)
            logger.info(f"Saved: {v1_path}")
            logger.info(f"Saved: {v2_path}")
            # GOVERNANCE: Save metadata
            self.save_metadata()
            
            logger.info("\n" + "="*60)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("="*60)
            logger.info(f"V1 Features: {df_v1.shape}")
            logger.info(f"V2 Features: {df_v2.shape}")
            logger.info(f"Check outputs in: {PipelineConfig.OUTPUT_DIR}/")
            
            return True
            
        except Exception as e:
            logger.error(f"\nPIPELINE FAILED: {str(e)}")
            logger.error("Check pipeline_execution.log for details")
            raise
    
    def save_metadata(self):
        """Save governance metadata (REQUIRED for auditability)"""
        metadata = {
            'pipeline_run': datetime.now().isoformat(),
            'ingestion_metadata': self.ingestion.metadata,
            'quality_reports': self.cleaning.quality_reports,
            'merge_lineage': self.merging.lineage,
            'feature_catalog': self.features.feature_catalog
        }
        
        # Convert to JSON-safe types (numpy / pandas) before writing
        safe_metadata = self.make_json_safe(metadata)

        metadata_path = f"{PipelineConfig.OUTPUT_DIR}/metadata/pipeline_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(safe_metadata, f, indent=2)
        
        logger.info(f"Saved: {metadata_path}")

    @staticmethod
    def make_json_safe(obj):
        """Convert numpy / pandas types to JSON-safe Python types."""
        if isinstance(obj, dict):
            return {k: EnterprisePipeline.make_json_safe(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [EnterprisePipeline.make_json_safe(i) for i in obj]
        # numpy scalar types have 'item' method
        elif hasattr(obj, 'item') and not isinstance(obj, (str, bytes, bytearray)):
            try:
                return obj.item()
            except Exception:
                return obj
        else:
            return obj


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    """
    HOW TO RUN:
    1. Place 4 CSV files in 'raw_data/' directory
    2. Run: python enterprise_pipeline.py
    3. Check 'output/' directory for results
    
    FAILURE RECOVERY:
    - All steps logged in pipeline_execution.log
    - Idempotent - safe to re-run
    - Metadata tracks every operation
    """
    
    pipeline = EnterprisePipeline()
    success = pipeline.run()
    
    sys.exit(0 if success else 1)