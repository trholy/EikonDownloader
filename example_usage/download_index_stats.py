from eikondownloader import EikonDownloader, DataProcessor

import logging
import sys
import os


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)

fh = logging.FileHandler('download_index_stats.log')
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
name_mapping = True
name_mapping_dict = {
    'SP500': 'SPX',
    'SP400': 'IDX',
    # 'N225': 'N225E',
    'SP600': 'SPCY',
    'SP1000': 'SPMIDSM',
    'SPCOMP': 'SPSUP'
}

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

target_type = "index"
# Choose additional index data fields according target_type
if target_type == 'etp':
    additional_index_data_fields = [
        'TR.ETPConstituentRIC',
        'TR.ETPConstituentWeightPercent',
    ]
else:
    additional_index_data_fields = [
        'TR.IndexConstituentRIC',
        'TR.IndexConstituentWeightPercent',
    ]
logger.info(
    f"Using fields: {additional_index_data_fields} for type: {target_type}"
)

# Loop through indices
for index_name in indices_list:

    # Replace index_name, because source is may not available
    if name_mapping:
        index_name = name_mapping_dict.get(index_name, index_name)

    # Initialize data_processor
    data_processor = DataProcessor(
        path=f"download/{index_name}/",
        mode=target_type
    )

    # Get .csv-files in the specified directory
    df_name_list = data_processor.get_csv_files()

    # Loop through target dates
    for target_date in target_dates:
        logger.info(f"\nActual target date for {index_name}: {target_date}")

        # Download additional index data at target date
        additional_index_data_df, err = downloader.get_additional_data(
            rics=index_name,
            fields=additional_index_data_fields,
            pre_fix=".",
            target_date=target_date,
            max_retries=5
        )

        if err is not None:
            logger.error(f"An error occurred:\n{err}")

        # Create directory if not existing
        additional_index_data_path = (
            f"download/{index_name}/additional_index_data/"
        )
        if not os.path.isdir(additional_index_data_path):
            os.makedirs(additional_index_data_path)

        # Save downloaded dataframe
        if (additional_index_data_df is not None
                and not additional_index_data_df.empty):
            additional_index_data_df.to_csv(
                path_or_buf=f"{additional_index_data_path}"
                            f"{index_name}_additional_index_data_{target_date}.csv",
                sep=",",
                index=False
            )
            logger.info(
                f"\nSaved {index_name} at {target_date}"
                f" to {additional_index_data_path}"
            )
