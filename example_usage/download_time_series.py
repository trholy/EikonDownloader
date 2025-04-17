from eikondownloader import EikonDownloader, DataProcessor

import logging
import sys
import os


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)

fh = logging.FileHandler('time_series_download.log')
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
    'SPX': 'SP500',
    'IDX': 'SP400',
    'N225': 'N225E',
    'SPCY': 'SP600',
    'SPMIDSM': 'SP1000',
    'SPSUP': 'SPCOMP'
}

start_date = "2000-01-01"
end_date = "2024-12-31"

# Loop through indices
for index_name in indices_list:

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

    # Initialize downloader
    downloader = EikonDownloader(
        api_key=api_key,
        request_delay=1,
        request_limit_delay=3600,
        error_delay=5
    )

    # Create directory if not existing
    index_df_path = f"download/{index_name}/time_series_data/"
    if not os.path.isdir(index_df_path):
        os.makedirs(index_df_path)

    # Get index time series
    index_df = downloader.get_index_timeseries(
        index_ric=name_mapping_dict.get(index_name, index_name),
        end_date=end_date,
        num_years=None,
        start_date=start_date,
        max_retries=10,
        fields='CLOSE',
        interval="daily",
        corax='adjusted',
        calendar=None,
        count=None
    )

    if index_df is not None and not index_df.empty:
        # Start with download of index stocks
        logger.info(
            f"index_df for {index_name} is not None or empty,"
            f" Starting download of\n{uni_rics}\n")

        for counter, ric in enumerate(uni_rics):
            index_df = downloader.get_stock_timeseries(
                index_df=index_df,
                ric=ric,
                end_date=end_date,
                num_years=None,
                start_date=start_date,
                max_retries=10,
                fields='CLOSE',
                interval="daily",
                corax='adjusted',
                calendar=None,
                count=None
            )

            percent_done = round(((counter + 1) / len(uni_rics)) * 100, 2)
            logger.info(f"[{percent_done}%] Download {ric} completed.\n")

            # Temporally save of index time series dataframe
            if (counter % 10 == 0
                    and index_df is not None
                    and not index_df.empty):
                index_df.to_csv(
                    path_or_buf=f"{index_df_path}"
                                f"{index_name}_time_series_"
                                f"{start_date}_{end_date}.csv",
                    sep=",",
                    index=True
                )
                logger.info(
                    f"\nindex_df for {index_name}"
                    f" is saved temporally to {index_df_path}\n"
                )

        # Final save of index time series dataframe
        if index_df is not None and not index_df.empty:
            index_df.to_csv(
                path_or_buf=f"{index_df_path}"
                            f"{index_name}_time_series_"
                            f"{start_date}_{end_date}.csv",
                sep=",",
                index=True
            )
            logger.info(
                f"\nindex_df for {index_name}"
                f"is saved finally to {index_df_path}\n"
            )

    # SKIP IF INDEX_DF IS NONE
    else:
        logger.error(
            f"index_df for {index_name} is None,"
            f" not downloading symbols..."
        )
