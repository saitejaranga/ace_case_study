from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Dict, List, AnyStr

from code.my_utils import (
    create_required_dataframes,
    prep_sap_order_header_data,
    prep_sap_order_item,
    prep_sap_order_master_data,
    prep_sap_general_material_data,
    integrations,
    post_prep_process_order,
    validate_and_write_dataframe,
)


def get_dataframes(spark: SparkSession, config: Dict, required_dataframes: List[AnyStr], log: Logger) -> Dict[AnyStr, DataFrame]:
    """
    Create required dataframes for processing.

    Args:
        spark: Spark session object.
        config: Configuration dictionary.
        required_dataframes: List of required dataframe names.
        log: Logger instance.

    Returns:
        Dictionary of created dataframes.
    """
    log.info("Creating required dataframes for process order.")
    try:
        return create_required_dataframes(
            spark=spark,
            config=config,
            required_dataframes=required_dataframes,
            source_system=config["name"],
            log=log,
        )
    except Exception as e:
        log.error(f"Error while creating dataframes: {e}")
        raise

def process_order(
    spark: SparkSession,
    log: Logger,
    config: Dict,
    process_order_params: Dict
) -> bool:
    """
    Main function to process SAP order data.

    Args:
        spark: Spark session object.
        log: Logger instance.
        config: Configuration dictionary.
        check_material_is_not_deleted: Flag to check material status.
        check_old_material_number_is_valid: Flag to validate old material numbers.

    Returns:
        Boolean flag indicating success or failure.
    """
    log.info("Starting process order processing.")
    try:
        # Define required dataframes
        req_dfs = ["AFKO", "AFPO", "AUFK", "MARA"]

        # Load required dataframes
        log.info("Loading required dataframes.")
        created_dfs = get_dataframes(spark, config, req_dfs, log)
        afko_df = created_dfs.get("AFKO")
        afpo_df = created_dfs.get("AFPO")
        aufk_df = created_dfs.get("AUFK")
        mara_df = created_dfs.get("MARA")

        # Preparation steps
        log.info("Preparing SAP order header data.")
        prep_sap_order_header_data_df = prep_sap_order_header_data(afko_df, log)

        log.info("Preparing SAP order item data.")
        prep_sap_order_item_df = prep_sap_order_item(afpo_df, log)

        log.info("Preparing SAP order master data.")
        prep_sap_order_master_data_df = prep_sap_order_master_data(aufk_df, log)

        log.info("Preparing SAP general material data.")
        log.info("Fetching the global material column.")
        col_global_material = process_order_params.get("col_global_material")
        prep_sap_general_material_data_df = prep_sap_general_material_data(
            mara_df, col_global_material=col_global_material, log=log
        )

        # Integration step
        log.info("Integrating prepared data.")
        integration_df = integrations(
            prep_sap_order_header_data_df,
            prep_sap_order_item_df,
            prep_sap_order_master_data_df,
            prep_sap_general_material_data_df,
            log=log,
        )

        # Post-preparation process
        log.info("Post-processing process order.")
        post_prep_process_order_df = post_prep_process_order(integration_df, log)

        # Validation and writing to the target
        log.info("Validating and writing final dataframe.")
        flag = validate_and_write_dataframe(
            df=post_prep_process_order_df,
            source_system=config["name"],
            table_name="PROCESS_ORDER_SCHEMA",
            log=log,
        )
        log.info(f"status of validate and write: {flag}")
        return post_prep_process_order_df
    except Exception as e:
        log.error(f"Error during process order processing: {e}")
        raise
