from pyspark.sql import SparkSession
import logging
import findspark

# from code.PRD.local_material import local_material
from code.PRE.local_material import local_material
from my_utils import create_required_dataframes
from schema.pre_schema import PRE_Schema_data
from testfunctions import compare_schemas

"""
Exercise Overview:
You will work with data from two SAP systems that have similar data sources. 
Your task is to process and integrate this data to provide a unified view for supply chain insights. 

The exercise involves:
* Processing Local Material data.Processing Process Order data.
* Ensuring both datasets have the same schema for harmonization across systems. 
* Writing modular, reusable code with proper documentation.
* Following best-in-class principles for flexibility and maintainability.

Note: You will create two Python scripts (local_material.py and process_order.py) 
for each system, i.e. in total of four scripts (2 systems per modeled entity/event).

General Instructions
Work on both SAP systems: 
* Perform all the steps for both systems to ensure consistency
* Enable and accomplish data harmonization through a common data model

Focus on data fields and transformations: 
* Pay attention to the required fields and the transformations applied to them.
* Document your code: Include comments explaining why certain modules and functions are used.
* Follow best practices: Write modular code, handle exceptions, and ensure reusability.


Detailed instructions see attached PDF

"""

# Initialize findspark to locate Spark installation
findspark.init()

logging.basicConfig(
    level=logging.INFO,
    filename="app.log",  # Optional: log to a file
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
LOGGER = logging.getLogger(__name__)
LOGGER.info("################ log message ##############")

spark = SparkSession.builder.appName("Local_Material").master("local[2]").getOrCreate()
print(spark)

prd_config = {
    "path": "data/system_2/",
    "name": "PRD",
}

pre_config = {
    "path": "data/system_1/",
    "name": "PRE",
}


def get_dataframes(spark, config, required_dataframes, log):
    """
    Create required dataframes for local material processing.

    Args:
        spark: Spark session object.
        config: Dictionary containing configuration details.
        required_dataframes: List of required dataframe names.
        log: Logger instance.

    Returns:
        Dictionary of created dataframes.
    """
    return create_required_dataframes(
        spark=spark,
        config=config,
        required_dataframes=required_dataframes,
        source_system=config["name"],
        log=log,
    )

# Required dataframes
req_dfs = ["MARA", "MARC", "MBEW", "T001W", "T001K", "T001"]

# Load required dataframes
created_dfs = get_dataframes(spark, pre_config, req_dfs, LOGGER)

mara_df = created_dfs.get("MARA")
marc_df = created_dfs.get("MARC")
mbew_df = created_dfs.get("MBEW")
t001w_df = created_dfs.get("T001W")
t001k_df = created_dfs.get("T001K")
t001_df = created_dfs.get("T001")

# Compare the schemas
diff1, diff2, type_mismatches = compare_schemas(t001_df.schema, PRE_Schema_data.get("T001"))

# Print the differences
print("Fields in schema1 not in schema2:", diff1)
print("Fields in schema2 not in schema1:", diff2)
print("Fields with type mismatches:", type_mismatches)

# Stop the Spark session
spark.stop()