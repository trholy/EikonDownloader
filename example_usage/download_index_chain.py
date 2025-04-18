from eikondownloader.download import EikonDownloader

import logging
import sys
import os


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)

fh = logging.FileHandler('index_chain_download.log')
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
    'TR.ISIN',
    'TR.TRBCEconomicSector',
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
logger.info(f"Target dates:\n{target_dates}")

# Loop through indices and target dates
for index_name in indices_list:

    # Replace index_name, because source is may not available
    if name_mapping:
        index_name = name_mapping_dict.get(index_name, index_name)

    for target_date in target_dates:
        logger.info(f"\nActual target date for {index_name}: {target_date}")

        # Download index chain at target date
        index_chain_df, err = downloader.get_index_chain(
            index_ric=index_name,
            target_date=target_date,
            fields=data_fields,
            parameters=None,
            max_retries=5,
            pre_fix="0#."
        )
        if err is not None:
            logger.error(f"An error occurred:\n{err}")

        # Create directory if not existing
        index_chain_df_path = f"download/{index_name}/index_chain/"
        if not os.path.isdir(index_chain_df_path):
            os.makedirs(index_chain_df_path, exist_ok=True)

        # Save downloaded dataframe
        if index_chain_df is not None and not index_chain_df.empty:
            index_chain_df.to_csv(
                path_or_buf=f"{index_chain_df_path}"
                            f"{index_name}_chain_{target_date}.csv",
                sep=",",
                index=False
            )
            logger.info(
                f"Saved {index_name} at {target_date} to {index_chain_df_path}"
            )
