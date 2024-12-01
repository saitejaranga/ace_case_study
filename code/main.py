from pyspark.sql import SparkSession
import logging
import findspark
from code.process_order import process_order as process_order
from code.local_material import local_material as local_material
from code.my_utils import harmonize

import sys
import os

# Add the current directory to PYTHONPATH
sys.path.insert(0, os.path.abspath("."))

"""
Exercise Overview:
You will work with data from two SAP systems (PRE and PRD) to process and integrate data, 
providing a unified view for supply chain insights.

Goals:
1. Process Local Material and Process Order data for both systems.
2. Ensure consistent schema for harmonization across systems.
3. Write modular, reusable, and maintainable code.
"""

# Initialize findspark to locate Spark installation
findspark.init()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    filename="app.log",  # Optional: log to a file
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
LOGGER = logging.getLogger(__name__)
LOGGER.info("Application started and logging configured.")

# Initialize Spark session
spark = SparkSession.builder.appName("SAP_Data_Processing").master("local[2]").getOrCreate()
LOGGER.info(f"Spark session initialized: {spark}")

# Configuration for the two systems
SYSTEM_CONFIGS = [
    {"path": "data/system_1/", "name": "PRE"},
    {"path": "data/system_2/", "name": "PRD"},
]

# Parameters specific to each system
PARAMS = {
    "PRE": {
        "local_material": {
            "col_mara_global_material_number": ["ZZMDGM"],  
            "check_old_material_number_is_valid": True,
            "check_material_is_not_deleted": True,
            "check_deletion_flag_is_null": True,
            "drop_duplicate_records": False,
        },
        "process_order": {
            "col_global_material": "ZZMDGM",
        },
    },
    "PRD": {
        "local_material": {
            "col_mara_global_material_number": ["ZZMDGM"],
            "check_old_material_number_is_valid": True,
            "check_material_is_not_deleted": True,
            "check_deletion_flag_is_null": True,
            "drop_duplicate_records": False, 
        },
        "process_order": {
            "col_global_material": "ZZMDGM",
        },
    },
}

def process_system_data(spark, logger, config):
    """
    Process Local Material and Process Order data for a single system.

    :param spark: Spark session object
    :param logger: Logger instance for logging
    :param config: Dictionary with system-specific configurations
    """
    system_name = config["name"]
    logger.info(f"Starting processing for system: {system_name}")
    
    try:
        # Process Local Material
        local_data = local_material(spark, logger, config, PARAMS.get(system_name).get("local_material"))
        logger.info(f"Local Material processing complete for system: {system_name}")

        # Process Order Data
        process_data = process_order(spark, logger, config, PARAMS.get(system_name).get("process_order"))
        logger.info(f"Process Order processing complete for system: {system_name}")

        return {
            "local_material": local_data,
            "process_order": process_data,
        }
    except Exception as e:
        logger.error(f"Error processing data for system {system_name}: {e}")
        raise

def main():
    """
    Main function to process and integrate data from both systems.
    """
    # Dictionaries to hold processed data for harmonization
    local_material_data = {}
    process_order_data = {}

    for config in SYSTEM_CONFIGS:
        # Process system data with parameters
        system_name = config["name"]
        processed_data = process_system_data(spark, LOGGER, config)
        
        # Collect processed local material and process order data
        local_material_data[system_name] = processed_data["local_material"]
        process_order_data[system_name] = processed_data["process_order"]

    # Harmonize Local Material data between systems
    if "PRE" in local_material_data and "PRD" in local_material_data:
        LOGGER.info("Harmonizing Local Material data between PRE and PRD systems.")
        try:
            success = harmonize(
                df_pre=local_material_data["PRE"],
                df_prd=local_material_data["PRD"],
                table_name="local_material",
                log=LOGGER,
            )
            if success:
                LOGGER.info("Harmonization of Local Material data was successful.")
            else:
                LOGGER.warning("Harmonization of Local Material data failed.")
        except Exception as e:
            LOGGER.error(f"Error during harmonization of Local Material data: {e}")
    else:
        LOGGER.warning("Insufficient Local Material data for harmonization. Ensure both PRE and PRD data are processed.")

    # Harmonize Process Order data between systems
    if "PRE" in process_order_data and "PRD" in process_order_data:
        LOGGER.info("Harmonizing Process Order data between PRE and PRD systems.")
        try:
            success = harmonize(
                df_pre=process_order_data["PRE"],
                df_prd=process_order_data["PRD"],
                table_name="process_order",
                log=LOGGER,
            )
            if success:
                LOGGER.info("Harmonization of Process Order data was successful.")
            else:
                LOGGER.warning("Harmonization of Process Order data failed.")
        except Exception as e:
            LOGGER.error(f"Error during harmonization of Process Order data: {e}")
    else:
        LOGGER.warning("Insufficient Process Order data for harmonization. Ensure both PRE and PRD data are processed.")

if __name__ == "__main__":
    main()
