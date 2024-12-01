from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import types as T
from typing import AnyStr, Dict, List, Optional
from pyspark.sql import Window
from pyspark.sql import functions as F
from logging import Logger
from code.schema.prd_schema import PRD_Schema_data
from code.schema.pre_schema import PRE_Schema_data

OUTPUT_PATH = "./output/"


def create_df(
    spark: SparkSession,
    path: AnyStr,
    source_system: AnyStr,
    table_name: AnyStr,
    log: Logger,
) -> DataFrame:
    """
    This function creates dataframe out of the CSV file.
    Args:
        spark (SparkSession): The active Spark session.
        path (AnyStr): Path to the CSV file.
        source_system (AnyStr): Source system identifier (PRE or PRD).
        table_name (AnyStr): Name of the table.
        log (Logger): Logger object for logging messages.

    Returns:
        DataFrame: The created Spark DataFrame.

    Raises:
        KeyError: If the schema for the given table_name is not found.
        Exception: If an error occurs while creating the DataFrame.
    """
    log.info(f"Creating DataFrame for table: {table_name}")

    try:
        if source_system == "PRE":
            schema = PRE_Schema_data[table_name]
        elif source_system == "PRD":
            schema = PRD_Schema_data[table_name]
        else:
            raise KeyError(f"Invalid source system: {source_system}")

        log.info(f"Schema retrieved for table: {table_name}")
    except KeyError as e:
        log.error(f"Error fetching schema for table {table_name}: {str(e)}")
        raise

    try:
        log.info(f"provided schema, {schema}")
        df = spark.read.schema(schema).csv(path, header=True)
        log.info(f"Successfully created DataFrame for table: {table_name}")
        return df
    except Exception as e:
        log.error(f"Error creating DataFrame for table {table_name}: {str(e)}")
        raise


def create_required_dataframes(
    spark: SparkSession,
    config: Dict,
    required_dataframes: List[str],
    source_system: AnyStr,
    log: Logger,
) -> Dict[AnyStr, DataFrame]:
    """
    This function reads CSV files for each required DataFrame and stores them in a dictionary.

    Args:
        spark (SparkSession): The active Spark session.
        config (Dict): Configuration dictionary containing the base path for CSV files.
        required_dataframes (List[str]): List of DataFrame names to be created.
        source_system (AnyStr): Source system identifier (PRE or PRD).
        log (Logger): Logger object for logging messages.

    Returns:
        Dict[str, DataFrame]: A dictionary where keys are DataFrame names and
                              values are the corresponding Spark DataFrames.

    Raises:
        KeyError: If the 'path' key is not found in the config dictionary.
        FileNotFoundError: If a required CSV file is not found.
    """
    log.info("Starting creation of required DataFrames")
    dataframe_dict = {}
    base_path = config.get("path")

    if not base_path:
        error_msg = "'path' key not found in config dictionary"
        log.error(error_msg)
        raise KeyError(error_msg)

    for df_name in required_dataframes:
        file_path = f"{base_path}{source_system}_{df_name}.csv"

        try:
            dataframe_dict[df_name] = create_df(
                spark, file_path, source_system, df_name, log
            )
            log.info(f"Successfully created DataFrame: {df_name}")
        except FileNotFoundError:
            log.error(f"CSV file not found for DataFrame: {df_name}")
            raise

    log.info(f"Completed creation of {len(dataframe_dict)} required DataFrames")
    return dataframe_dict


def prep_general_material_data(
    sap_mara: DataFrame,
    check_old_material_number_is_valid: bool,
    check_material_is_not_deleted: bool,
    log: Logger,
    col_mara_global_material_number_list: List[str] = ["ZZMDGM"],
) -> DataFrame:
    """
    Prepare General Material Data frame from SAP MARA table.
    Args:
        sap_mara (DataFrame): Input DataFrame of the SAP MARA table.
        col_mara_global_material_number_list (List[str]): List of possible column names for the global material number.
        check_old_material_number_is_valid (bool): If True, filters out materials with invalid old material numbers.
        check_material_is_not_deleted (bool): If True, excludes materials flagged for deletion.
        log (Logger): Logger object for logging messages.

    Returns:
        DataFrame: Processed DataFrame with selected columns and applied transformations.
    Raises:
        ValueError: If none of the specified global material number columns are found in the DataFrame.
    """
    try:
        log.info("Starting preparation of general material data")
        
        # Required columns to select
        required_columns = ["MANDT", "MATNR", "MEINS", "BISMT", "LVORM"]
        
        # Identify the actual global material number column present in the DataFrame
        global_material_col = next(
            (col for col in col_mara_global_material_number_list if col in sap_mara.columns),
            None
        )
        
        if not global_material_col:
            error_msg = f"None of the specified global material number columns {col_mara_global_material_number_list} were found in the DataFrame."
            log.error(error_msg)
            raise ValueError(error_msg)
        
        log.info(f"Using '{global_material_col}' as the global material number column")
        
        # Select necessary columns, including the global material number column
        sap_mara = sap_mara.select(
            *required_columns, F.col(global_material_col).alias("global_material_number")
        )
        
        # Apply filters based on parameters
        if check_old_material_number_is_valid:
            invalid_bismt_values = ["ARCHIVE", "DUPLICATE", "RENUMBERED"]
            sap_mara = sap_mara.filter(
                (F.col("BISMT").isNull()) | (~F.col("BISMT").isin(invalid_bismt_values))
            )
            log.info("Filtered out materials with invalid old material numbers")
        
        if check_material_is_not_deleted:
            sap_mara = sap_mara.filter((F.col("LVORM").isNull()) | (F.col("LVORM") == ""))
            log.info("Excluded materials flagged for deletion")
        
        # Select the final required columns
        sap_mara = sap_mara.select("MANDT", "MATNR", "MEINS", "global_material_number")
        
        log.info("Completed preparation of general material data")
        return sap_mara
    
    except Exception as e:
        log.error(f"Error in prep_general_material_data: {str(e)}")
        raise


def prep_material_valuation(sap_mbew: DataFrame, log: Logger) -> DataFrame:
    
    """
    Prepare Material Valuation Data from SAP MBEW table.
    Args:
        sap_mbew (DataFrame): Input DataFrame of the SAP MBEW table (Material Valuation).
        log (Logger): Logger object for logging messages.
    Returns:
        DataFrame: Processed DataFrame with selected columns and applied transformations.
    """
    try:
        log.info("Starting preparation of material valuation data")
        
        # Select the required columns, including additional columns for processing
        required_columns = [
            "MANDT", "MATNR", "BWKEY", "VPRSV", "VERPR", "STPRS", "PEINH", 
            "BKLAS", "LAEPR", "LVORM", "BWTAR"
        ]
        sap_mbew = sap_mbew.select(*required_columns)
        
        # Filter out materials that are flagged for deletion (LVORM is null or empty)
        sap_mbew = sap_mbew.filter((F.col("LVORM").isNull()) | (F.col("LVORM") == ""))
        
        # Filter for entries with BWTAR (Valuation Type) as null to exclude split valuation materials
        sap_mbew = sap_mbew.filter(F.col("BWTAR").isNull())
        
        # Deduplicate records:
        # Take the record having the highest evaluated price LAEPR at MATNR and BWKEY level
        window_spec = Window.partitionBy("MATNR", "BWKEY").orderBy(
            F.col("LAEPR").desc_nulls_last()
        )
        sap_mbew = sap_mbew.withColumn("row_number", F.row_number().over(window_spec))
        sap_mbew = sap_mbew.filter(F.col("row_number") == 1).drop("row_number")
        
        # Select the final required columns
        final_columns = [
            "MANDT", "MATNR", "BWKEY", "VPRSV", "VERPR", "STPRS", 
            "PEINH", "BKLAS"
        ]
        sap_mbew = sap_mbew.select(*final_columns)
        
        log.info("Completed preparation of material valuation data")
        return sap_mbew
    
    except Exception as e:
        log.error(f"Error in prep_material_valuation: {str(e)}")
        raise

def prep_plant_data_for_material(
    sap_marc: DataFrame,
    log: Logger,
    check_deletion_flag_is_null: bool = True,
    drop_duplicate_records: bool = False,
    additional_fields: Optional[List[AnyStr]] = None,
) -> DataFrame:
    """
    Prepare Plant Data for Material from SAP MARC table.
    Args:
        sap_marc (DataFrame): Input DataFrame of the SAP MARC table (Plant Data for Material).
        check_deletion_flag_is_null (bool): If True, exclude records where LVORM is not null.
        drop_duplicate_records (bool): If True, drop duplicate records.
        additional_fields (Optional[List[str]]): List of additional field names to include.
        log (Logger): Logger object for logging messages.
    Returns:
        DataFrame: Processed DataFrame with selected columns and applied transformations.
    """
    try:
        log.info("Starting preparation of plant data for material")
        
        # Default additional fields if None
        if additional_fields is None:
            additional_fields = []
        
        # Base required fields
        required_fields = [
            "SOURCE_SYSTEM_ERP",  # Source ERP system identifier
            "MATNR",  # Material Number
            "WERKS",  # Plant
            "LVORM",  # Deletion Flag
        ]
        
        # Include additional fields as required
        all_fields = required_fields + additional_fields
        
        # Ensure all specified fields exist in the DataFrame
        missing_fields = [field for field in all_fields if field not in sap_marc.columns]
        if missing_fields:
            error_msg = f"The following required fields are missing in the input DataFrame: {missing_fields}"
            log.error(error_msg)
            raise ValueError(error_msg)
        
        # Select the required columns
        sap_marc = sap_marc.select(*all_fields)
        
        # Filter records where the deletion flag (LVORM) is null
        if check_deletion_flag_is_null:
            sap_marc = sap_marc.filter((F.col("LVORM").isNull()) | (F.col("LVORM") == ""))
        
        # Drop Duplicates if drop_duplicate_records is True
        if drop_duplicate_records:
            sap_marc = sap_marc.dropDuplicates()
        
        # Remove the deletion flag column as it's no longer needed
        sap_marc = sap_marc.drop("LVORM")
        
        log.info("Completed preparation of plant data for material")
        return sap_marc
    
    except Exception as e:
        log.error(f"Error in prep_plant_data_for_material: {str(e)}")
        raise


def prep_plant_and_branches(sap_t001w: DataFrame, log: Logger) -> DataFrame:
    """
    Prepare Plant and Branches Data from SAP T001W table.
    Args:
        sap_t001w (DataFrame): Input DataFrame of the SAP T001W table (Plant/Branch).
        log (Logger): Logger object for logging messages.
    Returns:
        DataFrame: Processed DataFrame with selected columns.
    """
    try:
        log.info("Starting preparation of plant and branches data")
        
        # Select the required columns
        required_columns = [
            "MANDT",  # Client
            "WERKS",  # Plant
            "BWKEY",  # Valuation Area
            "NAME1",  # Name of Plant/Branch
        ]
        
        # Check if all required columns are present in the input DataFrame
        missing_columns = [col for col in required_columns if col not in sap_t001w.columns]
        if missing_columns:
            error_msg = f"The following required columns are missing in the input DataFrame: {missing_columns}"
            log.error(error_msg)
            raise ValueError(error_msg)
        
        sap_t001w = sap_t001w.select(*required_columns)
        
        log.info("Completed preparation of plant and branches data")
        return sap_t001w
    
    except Exception as e:
        log.error(f"Error in prep_plant_and_branches: {str(e)}")
        raise


def prep_valuation_area(sap_t001k: DataFrame, log: Logger) -> DataFrame:
    """
    Prepare Valuation Area Data from SAP T001K table.
    Args:
        sap_t001k (DataFrame): Input DataFrame of the SAP T001K table (Valuation Area).
        log (Logger): Logger object for logging messages.
    Returns:
        DataFrame: Processed DataFrame with selected columns and duplicates removed.
    """
    try:
        log.info("Starting preparation of valuation area data")
        
        # Define the required columns
        required_columns = [
            "MANDT",  # Client
            "BWKEY",  # Valuation Area
            "BUKRS",  # Company Code
        ]
        
        # Check if all required columns are present in the input DataFrame
        missing_columns = [col for col in required_columns if col not in sap_t001k.columns]
        if missing_columns:
            error_msg = f"The following required columns are missing in the input DataFrame: {missing_columns}"
            log.error(error_msg)
            raise ValueError(error_msg)
        
        # Select the required columns
        try:
            sap_t001k = sap_t001k.select(*required_columns)
            log.info(f"Selected {len(required_columns)} required columns")
        except Exception as select_error:
            log.error(f"Error selecting columns: {select_error}")
            raise
        
        # Drop duplicates to ensure uniqueness
        try:
            sap_t001k = sap_t001k.dropDuplicates()
        except Exception as duplicate_error:
            log.error(f"Error removing duplicates: {duplicate_error}")
            raise
        
        log.info("Completed preparation of valuation area data")
        return sap_t001k
    
    except Exception as e:
        log.error(f"Unexpected error in prep_valuation_area: {e}")
        raise

def prep_company_codes(sap_t001: DataFrame, log: Logger) -> DataFrame:
    """
    Prepare Company Codes Data from SAP T001 table.

    Args:
        sap_t001 (DataFrame): Input DataFrame of the SAP T001 table (Company Codes).
        log (Logger): Logger object for logging messages.

    Returns:
        DataFrame: Processed DataFrame with selected columns.

    """
    try:
        log.info("Starting preparation of company codes data")

        # Define the required columns
        required_columns = [
            "MANDT",  # Client
            "BUKRS",  # Company Code
            "WAERS",  # Currency Key
        ]

        # Check if all required columns are present in the input DataFrame
        missing_columns = [col for col in required_columns if col not in sap_t001.columns]
        if missing_columns:
            error_msg = f"The following required columns are missing in the input DataFrame: {missing_columns}"
            log.error(error_msg)
            raise ValueError(error_msg)

        # Select the required columns
        sap_t001 = sap_t001.select(*required_columns)
        log.info(f"Selected {len(required_columns)} required columns")
        log.info("Completed preparation of company codes data")

        return sap_t001

    except Exception as e:
        log.error(f"Unexpected error in prep_company_codes: {e}")
        raise


def prefix_columns(df: DataFrame, prefix: AnyStr, log: Logger, exclude_cols: List = None) -> DataFrame:
    """
    Prefix column names of the DataFrame except for columns in exclude_cols.

    Parameters:
    - df: DataFrame
        Input DataFrame.
    - prefix: str
        Prefix to add to column names.
    - exclude_cols: list of str, optional
        List of column names to exclude from prefixing.
    - log: Logger
        Logger object for logging messages.

    Returns:
    - DataFrame:
        DataFrame with prefixed column names.
    """
    try:
        log.info("Starting prefixing columns")

        # Validate input parameters
        if not isinstance(prefix, str):
            error_msg = "The 'prefix' parameter must be a string."
            log.error(error_msg)
            raise ValueError(error_msg)
        
        if exclude_cols is None:
            exclude_cols = []
        elif not isinstance(exclude_cols, list):
            error_msg = "The 'exclude_cols' parameter must be a list."
            log.error(error_msg)
            raise ValueError(error_msg)
        else:
            # Ensure all items in exclude_cols are strings
            if not all(isinstance(col, str) for col in exclude_cols):
                error_msg = "All items in 'exclude_cols' must be strings."
                log.error(error_msg)
                raise ValueError(error_msg)

        log.info(f"Prefixing columns with prefix '{prefix}' excluding columns: {exclude_cols}")

        # Create a mapping from old column names to new column names
        new_columns = [
            F.col(c).alias(prefix + "_" + c) if c not in exclude_cols else F.col(c)
            for c in df.columns
        ]
        prefixed_df = df.select(*new_columns)

        log.info("Successfully prefixed columns")
        return prefixed_df

    except Exception as e:
        log.error(f"Unexpected error in prefix_columns: {e}")
        raise

def remove_prefixes(df: DataFrame, log: Logger) -> DataFrame:
    """
    Remove prefixes from column names in the final DataFrame.

    Parameters
    ----------
    df : DataFrame
        DataFrame with prefixed column names
    log : Logger
        Logger object for logging messages

    Returns
    -------
    DataFrame
        DataFrame with prefixes removed from column names
    """
    try:
        log.info("Starting to remove prefixes from column names")

        # Collect original column names
        original_columns = df.columns
        renamed_columns = {}

        # Remove prefixes from column names
        for col in original_columns:
            if "_" in col:
                new_col_name = col.split("_", 1)[1]
                df = df.withColumnRenamed(col, new_col_name)
                renamed_columns[col] = new_col_name

        if renamed_columns:
            log.info(f"Removed prefixes from columns: {renamed_columns}")
        else:
            log.info("No prefixes found to remove in column names")

        log.info("Successfully removed prefixes from column names")
        return df

    except Exception as e:
        log.error(f"Unexpected error in remove_prefixes: {e}")
        raise

def integration(
    sap_mara: DataFrame,
    sap_mbew: DataFrame,
    sap_marc: DataFrame,
    sap_t001k: DataFrame,
    sap_t001w: DataFrame,
    sap_t001: DataFrame,
    log: Logger,
) -> DataFrame:
    """
    Integrate multiple SAP tables into a unified DataFrame through a series of left joins
    with explicit join conditions using prefixed columns.

    Args:
        sap_mara (DataFrame): General Material Data
        sap_mbew (DataFrame): Material Valuation Data
        sap_marc (DataFrame): Plant Data for Material (base table)
        sap_t001k (DataFrame): Valuation Area Data
        sap_t001w (DataFrame): Plant and Branches Data
        sap_t001 (DataFrame): Company Codes Data
        log (Logger): Logger object for logging messages

    Returns:
        DataFrame: Integrated DataFrame with unified column names (prefixes removed)
    """
    try:
        log.info("Starting integration of SAP tables")

        # Add prefixes to all DataFrames
        log.info("Adding prefixes to DataFrames")
        marc_df = prefix_columns(sap_marc, "marc", log=log)
        mara_df = prefix_columns(sap_mara, "mara", log=log)
        mbew_df = prefix_columns(sap_mbew, "mbew", log=log)
        t001k_df = prefix_columns(sap_t001k, "t001k", log=log)
        t001w_df = prefix_columns(sap_t001w, "t001w", log=log)
        t001_df = prefix_columns(sap_t001, "t001", log=log)

        # Log the columns of each prefixed DataFrame
        log.info("Logging columns of each prefixed DataFrame")
        for name, df in [
            ("MARC", marc_df),
            ("MARA", mara_df),
            ("MBEW", mbew_df),
            ("T001K", t001k_df),
            ("T001W", t001w_df),
            ("T001", t001_df),
        ]:
            log.debug(f"{name} DataFrame columns: {df.columns}")

        # Start with marc_df as the base
        result_df = marc_df

        # Join with mara_df
        log.info("Joining MARC with MARA")
        join_condition_mara = F.col("marc_MATNR") == F.col("mara_MATNR")
        result_df = result_df.join(mara_df, join_condition_mara, "left")
        log.info("Successfully joined MARC with MARA")

        # Join with t001w_df
        log.info("Joining result with T001W")
        join_condition_t001w = (F.col("mara_MANDT") == F.col("t001w_MANDT")) & (
            F.col("marc_WERKS") == F.col("t001w_WERKS")
        )
        result_df = result_df.join(t001w_df, join_condition_t001w, "left")
        log.info("Successfully joined result with T001W")

        # Join with mbew_df
        log.info("Joining result with MBEW")
        join_condition_mbew = (
            (F.col("mara_MANDT") == F.col("mbew_MANDT"))
            & (F.col("mara_MATNR") == F.col("mbew_MATNR"))
            & (F.col("t001w_BWKEY") == F.col("mbew_BWKEY"))
        )
        result_df = result_df.join(mbew_df, join_condition_mbew, "left")
        log.info("Successfully joined result with MBEW")

        # Join with t001k_df
        log.info("Joining result with T001K")
        join_condition_t001k = (F.col("mara_MANDT") == F.col("t001k_MANDT")) & (
            F.col("mbew_BWKEY") == F.col("t001k_BWKEY")
        )
        result_df = result_df.join(t001k_df, join_condition_t001k, "left")
        log.info("Successfully joined result with T001K")

        # Join with t001_df
        log.info("Joining result with T001")
        join_condition_t001 = (F.col("mara_MANDT") == F.col("t001_MANDT")) & (
            F.col("t001k_BUKRS") == F.col("t001_BUKRS")
        )
        result_df = result_df.join(t001_df, join_condition_t001, "left")
        log.info("Successfully joined result with T001")

        # Remove duplicate key columns, keeping marc_ prefixed ones and ensuring BWKEY is fetched only from t001w_
        log.info("Removing duplicate key columns")
        key_columns = {"MATNR", "WERKS", "BUKRS", "MANDT"}
        prefixes_to_check = ["mara_", "t001w_", "mbew_", "t001k_", "t001_"]

        columns_to_drop = [
            col
            for col in result_df.columns
            if (
                any(col.startswith(prefix) for prefix in prefixes_to_check)
                and col.split("_")[1] in key_columns
            )
            or (col.split("_")[1] == "BWKEY" and not col.startswith("t001w_"))
        ]

        result_df = result_df.drop(*columns_to_drop)
        log.info(f"Dropped duplicate columns: {columns_to_drop}")

        # Remove all prefixes from column names
        log.info("Removing prefixes from column names")
        final_df = remove_prefixes(result_df, log=log)

        # Log the final columns of the DataFrame
        log.debug(f"Final columns of the integrated DataFrame: {final_df.columns}")

        # Log the number of rows in the final DataFrame
        row_count = final_df.count()
        log.info(f"Number of rows in the final integrated DataFrame: {row_count}")

        log.info("Completed integration of SAP tables")
        return final_df

    except Exception as e:
        log.error(f"Unexpected error in integration: {e}")
        raise

def derive_intra_and_inter_primary_key(
    df: DataFrame, source_system_col_name: AnyStr, unique_ids_cols: List[AnyStr], log: Logger
) -> DataFrame:
    """
    Utility function to derive intra and inter primary keys.

    Args:
        df (DataFrame): Input DataFrame.
        source_system_col_name (str): The column name representing the source system identifier.
        unique_ids_cols (List[str]): List of column names to use as unique identifiers.
        log (Logger): Logger object for logging messages.

    Returns:
        DataFrame: DataFrame with new primary key columns added.
    """
    try:
        log.info("Starting derivation of intra and inter primary keys")

        # Validate input parameters
        if not isinstance(source_system_col_name, str):
            error_msg = "The 'source_system_col_name' parameter must be a string."
            log.error(error_msg)
            raise ValueError(error_msg)

        if not isinstance(unique_ids_cols, list) or not all(isinstance(col, str) for col in unique_ids_cols):
            error_msg = "The 'unique_ids_cols' parameter must be a list of strings."
            log.error(error_msg)
            raise ValueError(error_msg)

        log.info(f"Using columns {unique_ids_cols} for primary key generation")
        log.info(f"Source system column: {source_system_col_name}")

        # Check if all required columns are present in the DataFrame
        missing_columns = [
            col
            for col in unique_ids_cols + [source_system_col_name]
            if col not in df.columns
        ]
        if missing_columns:
            error_msg = f"The following required columns are missing in the input DataFrame: {missing_columns}"
            log.error(error_msg)
            raise ValueError(error_msg)

        # Derive primary_key_intra by concatenating unique_ids_cols
        df = df.withColumn(
            "primary_key_intra", F.concat_ws("_", *[F.col(col) for col in unique_ids_cols])
        )
        log.info("Generated intra primary key: primary_key_intra")

        # Derive primary_key_inter by concatenating source_system_col_name and unique_ids_cols
        df = df.withColumn(
            "primary_key_inter",
            F.concat_ws(
                "_", F.col(source_system_col_name), *[F.col(col) for col in unique_ids_cols]
            ),
        )
        log.info("Generated inter primary key: primary_key_inter")
        log.info("Completed derivation of intra and inter primary keys")
        return df

    except Exception as e:
        log.error(f"Unexpected error in derive_intra_and_inter_primary_key: {e}")
        raise

def post_prep_local_material(df: DataFrame, log: Logger) -> DataFrame:
    """
    Post-process the integrated local material DataFrame.

    Args:
        df (DataFrame): The DataFrame resulting from the integration step.
        log (Logger): Logger object for logging messages.

    Returns:
        DataFrame: The post-processed DataFrame.

    Transformations:
    - Create 'mtl_plant_emd' by concatenating 'WERKS' and 'NAME1' with a hyphen.
    - Assign 'global_mtl_id' from 'global_material_number' if available; otherwise, use 'MATNR'.
    - Derive primary keys 'primary_key_intra' and 'primary_key_inter'.
    - Handle duplicates by adding 'no_of_duplicates' and dropping duplicate records.
    """
    try:
        log.info("Starting post-processing of local material data")

        # Validate input parameters
        if not isinstance(df, DataFrame):
            error_msg = "The 'df' parameter must be a Spark DataFrame."
            log.error(error_msg)
            raise TypeError(error_msg)

        # Required columns for processing
        required_columns = ["WERKS", "NAME1", "global_material_number", "MATNR", "SOURCE_SYSTEM_ERP"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            error_msg = f"The following required columns are missing in the input DataFrame: {missing_columns}"
            log.error(error_msg)
            raise ValueError(error_msg)

        # Log the initial columns of the DataFrame
        log.debug(f"Initial columns of the DataFrame: {df.columns}")

        # -------------------------------
        # Create mtl_plant_emd: Concatenate WERKS and NAME1 with a hyphen
        # -------------------------------
        df = df.withColumn(
            "mtl_plant_emd", F.concat_ws("-", F.col("WERKS"), F.col("NAME1"))
        )
        log.info("Added column: mtl_plant_emd")

        # -------------------------------
        # Assign global_mtl_id from global material number if available, otherwise use MATNR
        # -------------------------------
        df = df.withColumn(
            "global_mtl_id",
            F.when(
                F.col("global_material_number").isNotNull()
                & (F.col("global_material_number") != ""),
                F.col("global_material_number"),
            ).otherwise(F.col("MATNR")),
        )
        log.info("Added column: global_mtl_id")

        # -------------------------------
        # Derive primary keys
        # -------------------------------
        unique_ids_cols = ["MATNR", "WERKS"]
        df = derive_intra_and_inter_primary_key(
            df=df,
            source_system_col_name="SOURCE_SYSTEM_ERP",
            unique_ids_cols=unique_ids_cols,
            log=log,
        )
        log.info("Added columns: primary_key_intra, primary_key_inter")

        # -------------------------------
        # Handle Duplicates
        # -------------------------------
        log.info("Handling duplicates based on 'SOURCE_SYSTEM_ERP', 'MATNR', 'WERKS'")
        # Add a temporary column no_of_duplicates indicating duplicate counts
        window_dup = Window.partitionBy("SOURCE_SYSTEM_ERP", "MATNR", "WERKS")
        df = df.withColumn("no_of_duplicates", F.count("*").over(window_dup))
        log.info("Added column: no_of_duplicates")

        # Get initial count before dropping duplicates
        initial_count = df.count()
        log.info(f"Initial row count before dropping duplicates: {initial_count}")

        # Drop duplicates
        df = df.dropDuplicates(["SOURCE_SYSTEM_ERP", "MATNR", "WERKS"])
        final_count = df.count()
        duplicates_removed = initial_count - final_count
        log.info(f"Removed {duplicates_removed} duplicate rows")

        # Log the final columns of the DataFrame
        log.debug(f"Final columns of the DataFrame: {df.columns}")
        log.info("Completed post-processing of local material data")

        return df

    except Exception as e:
        log.error(f"Unexpected error in post_prep_local_material: {e}")
        raise

def validate_and_write_dataframe(
    df: DataFrame, source_system: AnyStr, table_name: AnyStr, log: Logger
) -> bool:
    """
    Validates the schema of a DataFrame and writes it to a given path in Parquet format.

    Parameters:
        df (DataFrame): The input DataFrame to validate and write.
        expected_schema (StructType): The expected schema for the DataFrame.
        path (str): The path where the DataFrame should be written as Parquet.
        logger (logging.Logger): The logger instance for logging information.

    Returns:
        None
    """
    try:
        # Log the schema of the DataFrame
        log.info(f"Input DataFrame schema:\n{df.schema}")
        log.info(f"Writing the data for system: {source_system} and table_name: {table_name}")

        if source_system == None:

            # Write the DataFrame to the specified path in Parquet format
            path = OUTPUT_PATH + "/" + table_name + "/"
            df.write.mode("overwrite").parquet(path)
            log.info(f"DataFrame successfully written to path: {path}")
            return True
        else:
            # Get the schema and validate the schema
            if source_system == "PRE":
                expected_schema = PRE_Schema_data[table_name]
            elif source_system == "PRD":
                expected_schema = PRD_Schema_data[table_name]
            else:
                raise KeyError(f"Invalid source system: {source_system}")

            if df.schema != expected_schema:
                log.error("DataFrame schema does not match the expected schema.")
                log.error(f"Expected schema:\n{expected_schema}")
                return False
            else:
                log.info("DataFrame schema matches the expected schema.")

            # Write the DataFrame to the specified path in Parquet format
            path = OUTPUT_PATH + source_system + "/" + table_name + "/"
            df.write.mode("overwrite").parquet(path)
            log.info(f"DataFrame successfully written to path: {path}")
            return True

    except Exception as e:
        log.error(f"An error occurred while processing the DataFrame: {e}")
        raise


"""
########## Task -2 ##########
"""


def prep_sap_order_header_data(df_afko: DataFrame, log: Logger) -> DataFrame:
    """
    Prepare SAP Order Header Data from AFKO table.

    Args:
        df_afko (DataFrame): Input DataFrame of the SAP AFKO table.
        log (Logger): Logger object for logging messages.

    Returns:
        DataFrame: Processed DataFrame with selected columns and new date fields.
    """
    try:
        log.info("Starting prep_sap_order_header_data function")

        # Validate input parameters
        if not isinstance(df_afko, DataFrame):
            error_msg = "The 'df_afko' parameter must be a Spark DataFrame."
            log.error(error_msg)
            raise TypeError(error_msg)

        # Required columns for processing
        cols = [
            "SOURCE_SYSTEM_ERP",
            "MANDT",
            "AUFNR",
            "GLTRP",
            "GSTRP",
            "FTRMS",
            "GLTRS",
            "GSTRS",
            "GSTRI",
            "GETRI",
            "GLTRI",
            "FTRMI",
            "FTRMP",
            "DISPO",
            "FEVOR",
            "PLGRP",
            "FHORI",
            "AUFPL",
        ]

        # Check for missing columns
        missing_cols = [col for col in cols if col not in df_afko.columns]
        if missing_cols:
            error_msg = f"The following required columns are missing in the input DataFrame: {missing_cols}"
            log.error(error_msg)
            raise ValueError(error_msg)
        log.info(f"All required columns are present in df_afko")

        # Select required columns
        data = df_afko.select(*cols)
        log.info(f"Selected {len(cols)} columns from df_afko")

        # -------------------------------
        # Create start_date
        # If 'GSTRP' is null, use current date; else use 'GSTRP' date.
        # Convert to 'yyyy-MM-dd' format with day set to '01'.
        # -------------------------------
        log.info("Creating 'start_date' column")
        data = data.withColumn(
            "start_date",
            F.date_format(
                F.when(
                    F.col("GSTRP").isNull(),
                    F.current_date()
                ).otherwise(F.col("GSTRP")),
                "yyyy-MM"
            )
        )
        data = data.withColumn(
            "start_date", F.to_date(F.concat_ws("-", F.col("start_date"), F.lit("01")), "yyyy-MM-dd")
        )
        log.info("Created 'start_date' column")

        # -------------------------------
        # Create finish_date
        # If 'GLTRP' is null, use current date; else use 'GLTRP' date.
        # Convert to 'yyyy-MM-dd' format with day set to '01'.
        # -------------------------------
        log.info("Creating 'finish_date' column")
        data = data.withColumn(
            "finish_date",
            F.date_format(
                F.when(
                    F.col("GLTRP").isNull(),
                    F.current_date()
                ).otherwise(F.col("GLTRP")),
                "yyyy-MM"
            )
        )
        data = data.withColumn(
            "finish_date", F.to_date(F.concat_ws("-", F.col("finish_date"), F.lit("01")), "yyyy-MM-dd")
        )
        log.info("Created 'finish_date' column")

        # Log the final columns of the DataFrame
        log.debug(f"Final columns of the DataFrame: {data.columns}")

        log.info("Successfully completed prep_sap_order_header_data function")
        return data

    except Exception as e:
        log.error(f"Unexpected error in prep_sap_order_header_data: {str(e)}")
        raise

def prep_sap_order_item(df_afpo: DataFrame, log: Logger) -> DataFrame:
    """
    Prepare SAP Order Item Data from AFPO table.

    Args:
        df_afpo (DataFrame): Input DataFrame of the SAP AFPO table.
        log (Logger): Logger object for logging messages.

    Returns:
        DataFrame: Processed DataFrame with selected columns.
    """
    try:
        log.info("Starting prep_sap_order_item function")

        # Validate input parameters
        if not isinstance(df_afpo, DataFrame):
            error_msg = "The 'df_afpo' parameter must be a Spark DataFrame."
            log.error(error_msg)
            raise TypeError(error_msg)

        # Required columns for processing
        required_columns = ["AUFNR", "POSNR", "DWERK", "MATNR", "MEINS", "KDAUF", "KDPOS"]

        # Check for missing columns
        missing_columns = [col for col in required_columns if col not in df_afpo.columns]
        if missing_columns:
            error_msg = f"The following required columns are missing in df_afpo: {missing_columns}"
            log.error(error_msg)
            raise ValueError(error_msg)
        log.info("All required columns are present in df_afpo")

        # Select required columns
        data = df_afpo.select(*required_columns)
        log.info(f"Selected {len(required_columns)} columns from df_afpo")

        log.debug(f"Final columns of the DataFrame: {data.columns}")
        log.info("Successfully completed prep_sap_order_item function")
        return data

    except Exception as e:
        log.error(f"Unexpected error in prep_sap_order_item: {str(e)}")
        raise

def prep_sap_order_master_data(df_aufk: DataFrame, log: Logger) -> DataFrame:
    """
    Prepare SAP Order Master Data from AUFK table.

    Args:
        df_aufk (DataFrame): Input DataFrame of the SAP AUFK table.
        log (Logger): Logger object for logging messages.

    Returns:
        DataFrame: Processed DataFrame with selected columns.
    """
    try:
        log.info("Starting prep_sap_order_master_data function")

        # Validate input parameters
        if not isinstance(df_aufk, DataFrame):
            error_msg = "The 'df_aufk' parameter must be a Spark DataFrame."
            log.error(error_msg)
            raise TypeError(error_msg)

        # List of potential columns to select
        cols = ["AUFNR", "OBJNR", "ERDAT", "ERNAM", "AUART", "ZZGLTRP_ORIG", "ZZPRO_TEXT"]

        # Check which columns are available in df_aufk
        available_cols = df_aufk.columns
        selected_cols = [col_name for col_name in cols if col_name in available_cols]

        if not selected_cols:
            error_msg = "None of the required columns are present in df_aufk."
            log.error(error_msg)
            raise ValueError(error_msg)

        missing_cols = [col for col in cols if col not in selected_cols]
        if missing_cols:
            log.warning(f"The following columns are missing and will be skipped: {missing_cols}")

        # Select available columns
        data = df_aufk.select(*selected_cols)
        log.info(f"Selected {len(selected_cols)} columns from df_aufk")

        log.debug(f"Final columns of the DataFrame: {data.columns}")
        log.info("Successfully completed prep_sap_order_master_data function")
        return data

    except Exception as e:
        log.error(f"Unexpected error in prep_sap_order_master_data: {str(e)}")
        raise


def prep_sap_general_material_data(
    df_mara: DataFrame, col_global_material: str, log: Logger
) -> DataFrame:
    """
    Prepare SAP General Material Data from MARA table.

    Args:
        df_mara (DataFrame): Input DataFrame of the SAP MARA table.
        col_global_material (str): Column name for the global material number.
        log (Logger): Logger object for logging messages.

    Returns:
        DataFrame: Processed DataFrame after filtering and selecting required columns.
    """
    try:
        log.info("Starting prep_sap_general_material_data function")

        # Validate input parameters
        if not isinstance(df_mara, DataFrame):
            error_msg = "The 'df_mara' parameter must be a Spark DataFrame."
            log.error(error_msg)
            raise TypeError(error_msg)

        if not isinstance(col_global_material, str):
            error_msg = "The 'col_global_material' parameter must be a string."
            log.error(error_msg)
            raise TypeError(error_msg)

        # Required columns for filtering
        filter_columns = ["BISMT", "LVORM"]
        # Required columns for selection
        required_columns = ["MATNR", col_global_material, "NTGEW", "MTART"]

        # Check for missing columns
        missing_filter_cols = [col for col in filter_columns if col not in df_mara.columns]
        missing_required_cols = [col for col in required_columns if col not in df_mara.columns]
        if missing_filter_cols:
            error_msg = f"The following required columns for filtering are missing in df_mara: {missing_filter_cols}"
            log.error(error_msg)
            raise ValueError(error_msg)
        if missing_required_cols:
            error_msg = f"The following required columns for selection are missing in df_mara: {missing_required_cols}"
            log.error(error_msg)
            raise ValueError(error_msg)

        # Filter materials
        log.info("Filtering materials based on BISMT and LVORM")
        data = df_mara.filter(
            (
                (F.col("BISMT").isNull())
                | (~F.col("BISMT").isin(["ARCHIVE", "DUPLICATE", "RENUMBERED"]))
            )
            & (F.col("LVORM").isNull() | (F.col("LVORM") == ""))
        )
        log.info("Filtering completed")

        # Select required columns
        data = data.select(*required_columns)
        log.info(f"Selected {len(required_columns)} columns from df_mara")

        # Rename global material number column if necessary
        if col_global_material != "global_material_number":
            data = data.withColumnRenamed(col_global_material, "global_material_number")
            log.info(f"Renamed column '{col_global_material}' to 'global_material_number'")

        log.debug(f"Final columns of the DataFrame: {data.columns}")
        log.info("Successfully completed prep_sap_general_material_data function")
        return data

    except Exception as e:
        log.error(f"Unexpected error in prep_sap_general_material_data: {str(e)}")
        raise

def integrations(
    afko: DataFrame,
    afpo: DataFrame,
    aufk: DataFrame,
    mara: DataFrame,
    log: Logger,
    sap_cdpos: DataFrame = None,
) -> DataFrame:
    """
    Integrate multiple DataFrames by joining them on specified conditions.

    Args:
        afko (DataFrame): Input DataFrame for AFKO.
        afpo (DataFrame): Input DataFrame for AFPO.
        aufk (DataFrame): Input DataFrame for AUFK.
        mara (DataFrame): Input DataFrame for MARA.
        log (Logger): Logger object for logging messages.
        sap_cdpos (DataFrame, optional): Optional DataFrame for additional data.

    Returns:
        DataFrame: Resulting integrated DataFrame.
    """
    try:
        log.info("Starting the integration of DataFrames.")

        # Add prefixes to all DataFrames
        afko_df = prefix_columns(afko, "afko", log=log)
        afpo_df = prefix_columns(afpo, "afpo", log=log)
        aufk_df = prefix_columns(aufk, "aufk", log=log)
        mara_df = prefix_columns(mara, "mara", log=log)

        log.info(f"AFKO Dataframe: {afko_df.columns}")
        log.info(f"AFPO Dataframe: {afpo_df.columns}")
        log.info(f"AUFK Dataframe: {aufk_df.columns}")
        log.info(f"MARA Dataframe: {mara_df.columns}")

        # Start with afko_df as the base
        result_df = afko_df

        # Join with afpo_df
        join_condition_afpo = F.col("afko_AUFNR") == F.col("afpo_AUFNR")
        result_df = result_df.join(afpo_df, join_condition_afpo, "left")
        log.info("Join with AFPO is completed.")

        # Join with aufk_df
        join_condition_aufk = F.col("afko_AUFNR") == F.col("aufk_AUFNR")
        result_df = result_df.join(aufk_df, join_condition_aufk, "left")
        log.info("Join with AUFK is completed.")

        # Join with mara_df
        join_condition_mara = F.col("afpo_MATNR") == F.col("mara_MATNR")
        result_df = result_df.join(mara_df, join_condition_mara, "left")
        log.info("Join with MARA is completed.")

        
        # If sap_cdpos is provided, join with it
        if sap_cdpos is not None:
            sap_cdpos_df = prefix_columns(sap_cdpos, "cdpos", log=log)
            join_condition_cdpos = F.col("aufk_OBJNR") == F.col("cdpos_OBJNR")
            result_df = result_df.join(sap_cdpos_df, join_condition_cdpos, "left")
            log.info("Join with CDPOS is completed.")
    
        # Handle missing values for ZZGLTRP_ORIG
        result_df = result_df.withColumn(
            "aufk_ZZGLTRP_ORIG", F.coalesce(F.col("aufk_ZZGLTRP_ORIG"), F.col("afko_GLTRP"))
        )
        log.info("Created column ZZGLTRP_ORIG.")

        # Remove duplicate key columns (keep afko_ prefixed ones)
        columns_to_drop = [
            col
            for col in result_df.columns
            if any(col.startswith(prefix) for prefix in ["mara_", "afko_", "afpo_", "cdpos_"])
            and col.split("_")[1] in ["AUFNR", "MATNR"]
        ]
        result_df = result_df.drop(*columns_to_drop)
        log.info(f"Dropped duplicate key columns: {columns_to_drop}")

        # Remove all prefixes from column names
        final_df = remove_prefixes(result_df, log=log)
        log.info("Removed prefixes from column names.")

        log.info("Integration of DataFrames completed successfully.")
        return final_df

    except Exception as e:
        log.error(f"Unexpected error in integrations function: {e}")
        raise

def post_prep_process_order(df: DataFrame, log) -> DataFrame:
    """
    Post-processing function for order data.

    Args:
        df (DataFrame): Input DataFrame containing order data.
        log (Logger): Logger object for logging messages.

    Returns:
        DataFrame: Processed DataFrame with additional columns.
    """
    try:
        log.info("Starting post-processing of order data.")

        # Derive primary keys using the defined function
        df = derive_intra_and_inter_primary_key(df, "SOURCE_SYSTEM_ERP", ["AUFNR", "POSNR", "DWERK"], log)

        # Ensure date columns are in date format
        df = df.withColumn("ZZGLTRP_ORIG", F.to_date("ZZGLTRP_ORIG"))
        df = df.withColumn("FTRMI", F.to_date("FTRMI"))
        df = df.withColumn("GSTRI", F.to_date("GSTRI"))
        log.info("Date columns are converted to date format.")

        # Calculate On-Time Flag
        df = df.withColumn(
            "on_time_flag",
            F.when(
                (~F.col("ZZGLTRP_ORIG").isNull()) & (~F.col("FTRMI").isNull()),
                F.when(F.col("ZZGLTRP_ORIG") >= F.col("FTRMI"), F.lit(1)).otherwise(F.lit(0)),
            ),
        )
        log.info("On-time flag is created.")

        # Calculate On-Time Deviation
        df = df.withColumn(
            "actual_on_time_deviation",
            F.when(
                (~F.col("ZZGLTRP_ORIG").isNull()) & (~F.col("FTRMI").isNull()),
                F.datediff(F.col("ZZGLTRP_ORIG"), F.col("FTRMI")),
            ),
        )
        log.info("Actual on-time deviation column is created.")

        # Categorize late_delivery_bucket
        df = df.withColumn(
            "late_delivery_bucket",
            F.when(F.col("actual_on_time_deviation") <= 0, "On time or early")
            .when(
                (F.col("actual_on_time_deviation") > 0) & (F.col("actual_on_time_deviation") <= 5),
                "1-5 days late",
            )
            .when(
                (F.col("actual_on_time_deviation") > 5) & (F.col("actual_on_time_deviation") <= 10),
                "6-10 days late",
            )
            .when(F.col("actual_on_time_deviation") > 10, ">10 days late"),
        )
        log.info("Late delivery bucket is created.")

        # Derive MTO vs. MTS Flag
        df = df.withColumn(
            "mto_vs_mts_flag", F.when(F.col("KDAUF").isNotNull(), "MTO").otherwise("MTS")
        )
        log.info("MTO vs MTS flag is created.")

        # Convert dates to timestamps
        df = df.withColumn("order_finish_timestamp", F.col("FTRMI").cast("timestamp"))
        df = df.withColumn("order_start_timestamp", F.col("GSTRI").cast("timestamp"))
        log.info("Order timestamp columns are created.")

        log.info("Post-processing of order data completed successfully.")
        return df

    except Exception as e:
        log.error(f"Unexpected error in post_prep_process_order function: {e}")
        raise

def harmonize(df_pre: DataFrame, df_prd: DataFrame, table_name: str, log) -> bool:
    """
    Harmonize two DataFrames by checking their schemas and performing a union operation.

    This function compares the schemas of the provided DataFrames. If the schemas match,
    it logs a message indicating a match. If they do not match, it logs a message indicating
    that the schemas do not match. It then performs a union of the two DataFrames, allowing
    for missing values, and writes the resulting DataFrame using the `validate_and_write_dataframe` function.

    Args:
        df_pre (DataFrame): The DataFrame representing the pre-production dataset.
        df_prd (DataFrame): The DataFrame representing the production dataset.
        table_name (str): The name of the table associated with the DataFrames, used for logging and validation.
        log: Logger object for logging messages.

    Returns:
        bool: True if the DataFrame was successfully validated and written, False otherwise.
    """
    # Retrieve schemas of the DataFrames
    pre_schema = df_pre.schema
    prd_schema = df_prd.schema

    # Compare schemas and log the result
    if pre_schema == prd_schema:
        log.info("Schema matched.")
    else:
        log.info("Schema didn't match.")

    # Perform union of the DataFrames, allowing for missing values
    out_df = df_prd.unionByName(df_pre, allowMissingColumns=True)

    # Validate and write the resulting DataFrame
    flag = validate_and_write_dataframe(out_df, source_system=None, table_name=table_name, log=log)

    return flag

