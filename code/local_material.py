from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Dict, List, AnyStr

from code.my_utils import (
    create_required_dataframes,
    prep_general_material_data,
    prep_material_valuation,
    prep_plant_data_for_material,
    prep_plant_and_branches,
    prep_valuation_area,
    prep_company_codes,
    integration,
    post_prep_local_material,
    validate_and_write_dataframe,
)

def get_dataframes(spark: SparkSession, config: Dict, required_dataframes: List[AnyStr], log: Logger) -> Dict[AnyStr, DataFrame]:
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
    log.info("Creating required dataframes for local material processing.")
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

def local_material(
    spark: SparkSession,
    log: Logger,
    config: Dict,
    local_material_params: Dict
) -> bool:
    """
    Main function to process material data.

    Args:
        spark: Spark session object.
        log: Logger instance.
        config: Configuration dictionary.
        local_material_params: Dictionary containing local material processing parameters.

    Returns:
        Boolean flag indicating success or failure.
    """
    log.info("Starting local material processing.")
    try:
        # Extract parameters from the local_material_params dictionary
        col_mara_global_material_number = local_material_params.get("col_mara_global_material_number")
        check_material_is_not_deleted = local_material_params.get("check_material_is_not_deleted", True)
        check_old_material_number_is_valid = local_material_params.get("check_old_material_number_is_valid", True)
        check_deletion_flag_is_null = local_material_params.get("check_deletion_flag_is_null", True)
        drop_duplicate_records = local_material_params.get("drop_duplicate_records", False)

        # Define required dataframes
        req_dfs = ["MARA", "MARC", "MBEW", "T001W", "T001K", "T001"]

        # Create dataframes
        log.info("Creating dataframes.")
        created_dfs = get_dataframes(spark, config, req_dfs, log)
        mara_df = created_dfs.get("MARA")
        marc_df = created_dfs.get("MARC")
        mbew_df = created_dfs.get("MBEW")
        t001w_df = created_dfs.get("T001W")
        t001k_df = created_dfs.get("T001K")
        t001_df = created_dfs.get("T001")

        # Perform data preparation
        log.info("Preparing general material data.")
        prep_general_material_data_df = prep_general_material_data(
            sap_mara=mara_df,
            check_material_is_not_deleted=check_material_is_not_deleted,
            check_old_material_number_is_valid=check_old_material_number_is_valid,
            log=log,
            col_mara_global_material_number_list=col_mara_global_material_number,
        )

        log.info("Preparing material valuation data.")
        prep_material_valuation_df = prep_material_valuation(sap_mbew=mbew_df, log=log)

        log.info("Preparing plant data for material.")
        prep_plant_data_for_material_df = prep_plant_data_for_material(
            sap_marc=marc_df,
            log=log,
            additional_fields=[],
            check_deletion_flag_is_null=check_deletion_flag_is_null,
            drop_duplicate_records=drop_duplicate_records,
        )

        log.info("Preparing plant and branches data.")
        prep_plant_and_branches_df = prep_plant_and_branches(sap_t001w=t001w_df, log=log)

        log.info("Preparing valuation area data.")
        prep_valuation_area_df = prep_valuation_area(sap_t001k=t001k_df, log=log)

        log.info("Preparing company codes data.")
        prep_company_codes_df = prep_company_codes(sap_t001=t001_df, log=log)

        # Integration and post-processing
        log.info("Integrating prepared data.")
        integration_df = integration(
            prep_general_material_data_df,
            prep_material_valuation_df,
            prep_plant_data_for_material_df,
            prep_valuation_area_df,
            prep_plant_and_branches_df,
            prep_company_codes_df,
            log=log,
        )

        log.info("Post-processing local material.")
        post_prep_local_material_df = post_prep_local_material(integration_df, log)

        # Validation and write
        log.info("Validating and writing final dataframe.")
        flag =  validate_and_write_dataframe(
            df=post_prep_local_material_df,
            source_system=config["name"],
            table_name="LOCAL_MATERIAL",
            log=log,
        )
        log.info(f"Status of the validate and write: {flag}")

        return post_prep_local_material_df
    except Exception as e:
        log.error(f"Error during local material processing: {e}")
        raise
