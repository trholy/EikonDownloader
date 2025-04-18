from eikondownloader import EikonDownloader, DataProcessor

import pandas as pd

import logging
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)

fh = logging.FileHandler('download_constituents_stats.log')
fh.setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.addHandler(fh)

# API key
api_key = sys.argv[1]

# Indices to download
indices_list = [
    'HSI',
    'FTSE',
]

# RIC replacement dict for not available sources
name_mapping = False
name_mapping_dict = {
    'SP500': 'SPX',
    'SP400': 'IDX',
    #  'N225': 'N225E',
    'SP600': 'SPCY',
    'SP1000': 'SPMIDSM',
    'SPCOMP': 'SPSUP'
}

# Data fields to download
data_fields = [
    'TR.TRBCEconomicSector',
    'TR.TRBCBusinessSector',
    'TR.TRBCIndustryGroup',
    'TR.TRBCIndustry',
    'TR.TRESGScore',
    'TR.EnvironmentPillarScore',
    'TR.SocialPillarScore',
    'TR.GovernancePillarScore',
    'TR.ISIN',
    'TR.CompanyMarketCap(ShType=FFL)'
]

# Initialize downloader
downloader = EikonDownloader(
    api_key=api_key,
    request_delay=2,
    request_limit_delay=3600,
    error_delay=10
)

# Generate target dates
target_dates = downloader.generate_target_dates(
    end_date="2024-12-31",
    num_years=2,
    frequency="quarters"  # Options: 'months', 'quarters', 'years'
)
logger.info(f"Target dates:\n{target_dates}\n")

# Loop through indices
for index_name in indices_list:

    # Replace index_name, because source is may not available
    if name_mapping:
        index_name = name_mapping_dict.get(index_name, index_name)

    # Initialize data_processor
    data_processor = DataProcessor(
        path=f"download/{index_name}/",
        mode="index"
    )

    # Get .csv-files in the specified directory
    df_name_list = data_processor.get_csv_files()

    # Get unique rics
    uni_rics = data_processor.get_unique_rics(
        files=df_name_list,
        sep=","
    )

    # Loop through target dates
    for target_date in target_dates:
        logger.info(f"\nActual target date for {index_name}: {target_date}")

        chunked_uni_rics = data_processor.split_list(uni_rics, 2000)

        additional_stock_data_df_list = []
        for part, rics in enumerate(chunked_uni_rics):
            logger.info(
                f"Downloading chunk {part + 1}/{len(chunked_uni_rics)}"
                f" for {index_name}"
            )

            # Download index additional data at target date
            additional_stock_data_df_, err = downloader.get_additional_data(
                rics=rics,
                fields=data_fields,
                target_date=target_date,
                max_retries=5
            )
            if additional_stock_data_df_ is not None:
                additional_stock_data_df_list.append(additional_stock_data_df_)

            if err is not None:
                logger.error(f"An error occurred:\n{err}")

        # Merge chunked dataframes
        if additional_stock_data_df_list:
            merged_df = pd.concat(additional_stock_data_df_list, axis=0)
            merged_df.reset_index(inplace=True)
        else:
            logger.warning(
                f"No data collected for {index_name} at {target_date}"
            )
            continue

        # Create directory if not existing
        additional_stock_data_path = (
            f"download/{index_name}/additional_stock_data/"
        )
        if not os.path.isdir(additional_stock_data_path):
            os.makedirs(additional_stock_data_path)

        # Save downloaded dataframe
        if merged_df is not None and not merged_df.empty:

            merged_df.to_csv(
                path_or_buf=f"{additional_stock_data_path}"
                            f"{index_name}_additional_stock_data_{target_date}.csv",
                sep=",",
                index=False
            )
            logger.info(
                f"Saved {index_name} at {target_date}"
                f" to {additional_stock_data_path}"
            )
