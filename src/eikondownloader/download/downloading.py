import pandas as pd
import numpy as np
import eikon as ek
from minio import Minio
from minio.error import S3Error

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple, Union
from datetime import datetime, timedelta
import hashlib
import logging
import time
import os
import re


class EikonDownloader:
    def __init__(
            self,
            api_key: Optional[str] = None,
            request_delay: Optional[Union[int, float]] = 3,
            general_error_delay: Optional[Union[int, float]] = 5,
            gateway_delay: Optional[Union[int, float]] = 5,
            request_limit_delay: Optional[Union[int, float]] = 6,
            proxy_error_delay: Optional[Union[int, float]] = 6,
            network_error_delay: Optional[Union[int, float]] = 1
    ):
        """
        Initializes the EikonDownloader class with optional parameters for API
         key and various delay settings.

        param: api_key; Optional[str]; An optional Eikon API key to
         authenticate requests. Defaults to None.
        param: request_delay; Optional[Union[int, float]]; The delay in
         seconds between requests to avoid hitting rate limits. Defaults to 3.
        param: general_error_delay; Optional[Union[int, float]]; The delay in
         minutes to wait when a general error occurs. Defaults to 5.
        param: gateway_delay; Optional[Union[int, float]]; The delay in
         minutes to wait when a Gateway Time-out. Defaults to 5.
        param: request_limit_delay; Optional[Union[int, float]]; The delay in
         hours to wait when a request limit is reached. Defaults to 6.
        param: proxy_error_delay; Optional[Union[int, float]]; The delay in
         hours to wait when a proxy error occurs. Defaults to 6.
        param: network_error_delay; Optional[Union[int, float]]; The delay in
         hours to wait when a general error occurs. Defaults to 1.
        """
        self.request_delay = request_delay
        self.request_limit_delay = request_limit_delay
        self.proxy_error_delay = proxy_error_delay
        self.general_error_delay = general_error_delay
        self.network_error_delay = network_error_delay
        self.gateway_delay = gateway_delay

        # Create formatters: simpler for console, detailed for file
        console_formatter = logging.Formatter('%(levelname)s: %(message)s')
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Set up console handler and assign the console formatter
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)

        # Set up file handler and assign the file formatter
        file_handler = logging.FileHandler(
            f"{EikonDownloader.__name__}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        file_handler.setFormatter(file_formatter)

        # Create a logger and set its level to INFO
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Add both handlers to the logger
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

        if api_key:
            try:
                ek.set_app_key(api_key)
            except Exception as e:
                self.logger.error(f"Failed to set Eikon API key: {e}")
                raise

    @staticmethod
    def generate_target_dates(
            end_date: Union[str, datetime],
            num_years: int,
            frequency: str,
            reverse: bool = True
    ) -> List[str]:
        """
        Generates a list of target dates based on the specified end date,
         number of years, and frequency.

        param: end_date; Union[str, datetime]; The end date for the date range.
         Can be a string in a format recognized by pandas or a datetime object.
        param: num_years; int; The number of years to generate dates for.
         Must be between 1 and 99.
        param: frequency; str; The frequency of the dates to generate.
         Must be one of 'months' ('m'), 'quarters' ('q'), or 'years' ('y').
        param: reverse; bool; If True, the list of dates will be reversed so
         that the most recent date comes first. Defaults to True.
        :return: List[str]; A list of date strings in the format 'YYYY-MM-DD'.
        """
        freq_map = {
            'months': 'M',
            'quarters': 'Q',
            'years': 'Y',
            'm': 'M',
            'q': 'Q',
            'y': 'Y'
        }

        if num_years <= 0 or num_years >= 100:
            raise ValueError(
                "Invalid number of years! Choose a value in (0, 100)."
            )

        frequency = frequency.lower()
        if frequency not in freq_map:
            raise ValueError(
                "Invalid frequency! Choose from: 'months' ('m'),"
                " 'quarters' ('q'), 'years' ('y')."
            )

        periods = num_years * {
            'months': 12, 'm': 12,
            'quarters': 4, 'q': 4,
            'years': 1, 'y': 1
        }[frequency]

        try:
            end_date = pd.to_datetime(end_date)
        except pd.errors.ParserError as e:
            raise ValueError(f"Invalid end_date format: {e}")
        end_date = end_date.strftime("%Y-%m-%d")

        target_dates = pd.date_range(
            end=end_date,
            periods=periods,
            freq=freq_map[frequency]
        ).strftime("%Y-%m-%d").tolist()

        return target_dates if not reverse else target_dates[::-1]

    @staticmethod
    def generate_decade_dates(
            end_date: Union[str, datetime, np.datetime64],
            num_years: Optional[int] = None,
            start_date: Optional[Union[str, datetime, np.datetime64]] = None
    ) -> Tuple[List[str], List[str]]:
        """
        Generates lists of start and end dates for each decade within the
         specified range.

        param: end_date; Union[str, datetime, np.datetime64]; The end date
         for the date range. Can be a string in 'YYYY-MM-DD' format, a datetime
         object, or a numpy datetime64 object.
        param: num_years; Optional[int]; The number of years to generate dates
         for, starting from the end_date. If provided, start_date is calculated
         as end_date minus num_years.
        param: start_date; Optional[Union[str, datetime, np.datetime64]]; The
         start date for the date range. Can be a string in 'YYYY-MM-DD' format,
          a datetime object, or a numpy datetime64 object. If not provided,
          it is calculated based on num_years.
        :return: Tuple[List[str], List[str]]; A tuple containing two lists of
         date strings in the format 'YYYY-MM-DD'. The first list contains the
          start dates of each decade, and the second list contains the
          end dates of each decade.
        """
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        elif isinstance(end_date, np.datetime64):
            end_date = pd.Timestamp(end_date).to_pydatetime()

        if start_date:
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            elif isinstance(start_date, np.datetime64):
                start_date = pd.Timestamp(start_date).to_pydatetime()
        elif num_years is not None:
            start_date = end_date.replace(year=end_date.year - num_years)
            start_date = start_date + timedelta(days=1)
        else:
            raise ValueError(
                "Either 'num_years' or 'start_date' must be provided."
            )

        if start_date > end_date:
            raise ValueError("'start_date' cannot be later than 'end_date'.")

        start_dates, end_dates = [], []
        current_year = end_date.year

        while current_year >= start_date.year:
            end_of_decade = datetime(current_year, 12, 31)
            start_of_decade = datetime(current_year - 9, 1, 1)

            if end_of_decade < start_date:
                break

            if start_of_decade < start_date:
                start_of_decade = start_date

            if end_of_decade > end_date:
                end_of_decade = end_date

            start_dates.append(start_of_decade.strftime("%Y-%m-%d"))
            end_dates.append(end_of_decade.strftime("%Y-%m-%d"))

            current_year -= 10

        return start_dates, end_dates

    def get_index_chain(
            self,
            index_ric: str,
            target_date: Union[str, datetime],
            fields: Union[str, List[str]],
            parameters: Optional[dict] = None,
            max_retries: int = 5,
            pre_fix: Optional[str] = "0#."
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Retrieves the index chain data for a specified index RIC at a given
         target date.

        param: index_ric; str; The RIC (Reuters Instrument Code) of the index.
        param: target_date; Union[str, datetime]; The target date for which
         to retrieve the index chain data. Can be a string in 'YYYY-MM-DD'
         format or a datetime object.
        param: fields; Union[str, List[str]]; The fields to retrieve.
         Can be a single field as a string or a list of fields.
        param: parameters; Optional[dict]; Additional parameters to pass
         to the Eikon API. Defaults to None.
        param: max_retries; int; The maximum number of retries to attempt
         if the request fails. Defaults to 5.
        param: pre_fix; Optional[str]; A prefix to prepend to the index RIC.
         Defaults to "0#.".
        :return: Tuple[pd.DataFrame, Optional[str]]; A tuple containing
         a DataFrame with the index chain data and an optional error message.
          If the data retrieval is successful, the error message will be None.
        """
        if not isinstance(index_ric, str):
            raise TypeError("'index_ric' must be of type 'str'!")

        if isinstance(pre_fix, str):
            index_ric = pre_fix + index_ric

        if isinstance(fields, str):
            fields = [fields]

        if isinstance(target_date, datetime):
            target_date = target_date.strftime('%Y-%m-%d')

        retry_count = 0
        while retry_count < max_retries:
            try:
                self._apply_request_delay()
                self.logger.info(f"Downloading {index_ric} at {target_date}.")

                index_chain_df, err = ek.get_data(
                    instruments=[f"{index_ric}({target_date})"],
                    fields=fields,
                    parameters=parameters,
                    debug=False,
                    raw_output=False,
                    field_name=False
                )

                if (isinstance(index_chain_df, pd.DataFrame)
                        and not index_chain_df.empty):
                    self.logger.info(
                        f"Successfully downloaded {index_ric} at {target_date}."
                    )
                    return index_chain_df, None
                else:
                    self.logger.warning(
                        f"No data returned for {index_ric} at {target_date}."
                        f" Sleeping for {self.general_error_delay} minutes."
                    )
                    self._apply_general_error_delay()
                    retry_count += 1

            except ek.EikonError as err:
                if err.code == -1:
                    self.logger.warning(
                        f"Skipping download of {index_ric} at {target_date}."
                    )
                    return self._empty_df_chain(index_ric), f"Error: {err.code}"
                elif err.code == 401:
                    self.logger.warning(
                        f"Eikon Proxy not running or cannot be reached."
                        f" Sleeping for {self.proxy_error_delay} hours."
                    )
                    self._apply_proxy_error_delay()
                    retry_count += 1
                elif err.code == 429:
                    self.logger.warning(
                        f"Request limit reached. Sleeping for"
                        f" {self.request_limit_delay} hours.")
                    self._apply_request_limit_delay()
                    retry_count += 1
                elif err.code == 500:
                    self.logger.warning(
                        f"Network Error. Sleeping for"
                        f" {self.network_error_delay} hours.")
                    self._apply_network_error_delay()
                    retry_count += 1
                elif err.code == 2504:
                    self.logger.warning(
                        f"Gateway Time-out. Sleeping for"
                        f" {self.network_error_delay} minutes.")
                    self._apply_gateway_delay()
                    retry_count += 1
                else:
                    self.logger.warning(
                        f"Unhandled Eikon error code: {err.code}"
                        f" Sleeping for {self.general_error_delay} minutes."
                    )
                    self._apply_general_error_delay()
                    retry_count += 1
            except Exception as e:
                self.logger.error(
                    f"Unexpected error for {index_ric}: {str(e)}"
                    f" Sleeping for {self.general_error_delay} minutes."
                )
                self._apply_general_error_delay()
                retry_count += 1

        self.logger.critical(
            f"Data retrieval for {index_ric} at {target_date}"
            f" failed after {max_retries} retries."
        )
        return self._empty_df_chain(index_ric), "Max retries exceeded."

    def get_etp_chain(
            self,
            etp_ric: str,
            target_date: Union[str, datetime],
            fields: Union[str, List[str]],
            parameters: Optional[dict] = None,
            max_retries: int = 5,
            pre_fix: Optional[str] = None,
    ) -> Union[Tuple[None, str], Tuple[pd.DataFrame, None]]:
        """
        Retrieves the ETP chain data for a given ETP RIC on
         a specified target date.

        param: etp_ric; The RIC (Reuters Instrument Code) of the ETP; str
        param: target_date; The date for which to retrieve the ETP chain data;
         Union[str, datetime]
        param: fields; The fields to retrieve; Union[str, List[str]]
        param: parameters; Additional parameters to pass to the
         data retrieval function; Optional[dict]; default: None
        param: max_retries; Maximum number of retries in case
         of failure; int; default: 5
        param: pre_fix; Prefix to be used in logging; Optional[str]; default: None
        :return: A tuple containing the ETP chain data as a pandas DataFrame
         and an error message; Union[Tuple[None, str], Tuple[pd.DataFrame, None]]
        """
        self.logger.info(
            f"Attempting to download data for ETP chain"
            f" {etp_ric} on {target_date}."
        )

        # Retrieve ETP chain data by calling get_index_chain
        etp_chain_df, err = self.get_index_chain(
            etp_ric,
            target_date=target_date,
            fields=fields,
            parameters=parameters,
            max_retries=max_retries,
            pre_fix=pre_fix
        )

        # Handle errors gracefully
        if (err or etp_chain_df is None
                or not isinstance(etp_chain_df, pd.DataFrame)
                or etp_chain_df.empty):
            self.logger.error(
                f"Failed to download ETP chain data"
                f" for {etp_ric} at {target_date}. Error: {err}"
            )
            return None, err

        # Log successful data retrieval
        self.logger.info(
            f"Successfully downloaded ETP chain data"
            f" for {etp_ric} at {target_date}."
        )
        return etp_chain_df, None

    def get_index_timeseries(
            self,
            index_ric: str,
            end_date: Union[str, datetime],
            num_years: Optional[int],
            start_date: Optional[Union[str, datetime]],
            pre_fix: Union[str, None] = ".",
            max_retries: int = 5,
            fields: Union[str, List[str]] = 'CLOSE',
            interval: str = "daily",
            corax: str = 'adjusted',
            calendar: Optional[str] = None,
            count: Optional[int] = None,
    ) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Retrieves the timeseries data for a given index RIC within
         a specified date range.

        param: index_ric; The RIC (Reuters Instrument Code) of the index; str
        param: end_date; The end date for the timeseries data;
         Union[str, datetime]
        param: num_years; The number of years of data to retrieve;
         Optional[int]; default: None
        param: start_date; The start date for the timeseries data;
         Optional[Union[str, datetime]]; default: None
        param: pre_fix; Prefix to be added to the index RIC;
         Union[str, None]; default: "."
        param: max_retries; Maximum number of retries in case of failure;
         int; default: 5
        param: fields; The fields to retrieve; Union[str, List[str]];
         default: 'CLOSE'
        param: interval; The interval of the timeseries data
         (e.g., 'daily', 'weekly'); str; default: "daily"
        param: corax; The type of price adjustment ('adjusted', 'unadjusted');
         str; default: 'adjusted'
        param: calendar; The calendar to use for the timeseries data;
         Optional[str]; default: None
        param: count; The number of data points to retrieve; Optional[int];
         default: None
        :return: A tuple containing the timeseries data as a pandas DataFrame
         and an error message; Tuple[Optional[pd.DataFrame], Optional[str]]
        """
        if isinstance(index_ric, list):
            raise TypeError("'index_ric' has to be of type 'str', not 'list'!")

        if isinstance(pre_fix, str) and isinstance(index_ric, str):
            index_ric = pre_fix + index_ric

        # Call get_stock_timeseries to fetch the data
        index_timeseries_df, err = self.get_stock_timeseries(
            index_df=None,
            ric=index_ric,
            end_date=end_date,
            num_years=num_years,
            start_date=start_date,
            max_retries=max_retries,
            fields=fields,
            interval=interval,
            corax=corax,
            calendar=calendar,
            count=count
        )

        # Check if data was retrieved successfully
        if (err or index_timeseries_df is None or
                not isinstance(index_timeseries_df, pd.DataFrame)
                or index_timeseries_df.empty):
            self.logger.warning(
                f"No data retrieved for {index_ric}"
                f" from {start_date} to {end_date}."
            )
            return None, err

        # Rename the 'CLOSE' column to match the index RIC
        if isinstance(index_timeseries_df, pd.DataFrame):
            df_columns = [
                column.lower() for column in index_timeseries_df.columns
            ]
            if 'CLOSE'.lower() in df_columns:
                self.logger.debug(f'Renaming column "CLOSE" to {index_ric}.')
                index_timeseries_df.rename(
                    columns={'CLOSE': index_ric}, inplace=True
                )

        return index_timeseries_df, None

    def get_stock_timeseries(
            self,
            ric: str,
            end_date: Union[str, datetime],
            index_df: Optional[pd.DataFrame] = None,
            num_years: Optional[int] = None,
            start_date: Optional[Union[str, datetime]] = None,
            max_retries: int = 5,
            fields: Union[str, List[str]] = 'CLOSE',
            interval: str = "daily",
            corax: str = 'adjusted',
            calendar: Optional[str] = None,
            count: Optional[int] = None
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Retrieves the timeseries data for a given stock RIC within
         a specified date range.

        param: ric; The RIC (Reuters Instrument Code) of the stock; str
        param: end_date; The end date for the timeseries data;
         Union[str, datetime]
        param: index_df; An optional DataFrame to join the retrieved data with;
         Optional[pd.DataFrame]; default: None
        param: num_years; The number of years of data to retrieve;
         Optional[int]; default: None
        param: start_date; The start date for the timeseries data;
         Optional[Union[str, datetime]]; default: None
        param: max_retries; Maximum number of retries in case of failure;
         int; default: 5
        param: fields; The fields to retrieve; Union[str, List[str]];
         default: 'CLOSE'
        param: interval; The interval of the timeseries data
         (e.g., 'daily', 'weekly'); str; default: "daily"
        param: corax; The type of price adjustment ('adjusted', 'unadjusted');
         str; default: 'adjusted'
        param: calendar; The calendar to use for the timeseries data;
         Optional[str]; default: None
        param: count; The number of data points to retrieve; Optional[int];
         default: None
        :return: A tuple containing the timeseries data as a pandas DataFrame
         and an error message; Tuple[pd.DataFrame, Optional[str]]
        """
        if isinstance(fields, str):
            fields = [fields]

        # Generate decade-based date ranges
        start_dates, end_dates = self.generate_decade_dates(
            end_date=end_date,
            num_years=num_years,
            start_date=start_date,
        )

        ric_timeseries_df_list = []  # List to store dataframes

        for start_date, end_date in zip(start_dates, end_dates):

            retry_count = 0
            data_retrieved = False

            while retry_count < max_retries:
                try:
                    self._apply_request_delay()
                    self.logger.info(
                        f"Downloading {ric} from {start_date} to {end_date}."
                    )

                    ric_timeseries_df = ek.get_timeseries(
                        rics=ric,
                        fields=fields,
                        start_date=start_date,
                        end_date=end_date,
                        interval=interval,
                        count=count,
                        calendar=calendar,
                        corax=corax,
                        normalize=False,
                        raw_output=False,
                        debug=False
                    )

                    if isinstance(ric_timeseries_df, pd.DataFrame):
                        if not ric_timeseries_df.empty:
                            ric_timeseries_df.sort_index(
                                ascending=False, inplace=True
                            )
                            ric_timeseries_df.columns = [ric]
                            ric_timeseries_df_list.append(ric_timeseries_df)
                            data_retrieved = True
                            self.logger.info(
                                f"Data retrieved for {ric}"
                                f" from {start_date} to {end_date}."
                            )
                            break
                        else:
                            self.logger.warning(
                                f"No data retrieved for {ric}"
                                f" from {start_date} to {end_date}."
                            )
                    else:
                        self.logger.warning(
                            f"Unexpected data type for {ric}"
                            f" from {start_date} to {end_date}."
                        )
                    retry_count += 1

                except ek.EikonError as err:
                    if err.code == -1:
                        self.logger.warning(
                            f"Skipping download of {ric} with fields: {fields}"
                            f" from {start_date} to {end_date}"
                            f" due to error {err.code}."
                        )
                        break
                    elif err.code == 401:
                        self.logger.warning(
                            f"Eikon Proxy not running or cannot be reached."
                            f" Sleeping for {self.proxy_error_delay} hours."
                        )
                        self._apply_proxy_error_delay()
                        retry_count += 1
                    elif err.code == 429:
                        self.logger.warning(
                            f"Request limit reached. Sleeping for"
                            f" {self.request_limit_delay} hours.")
                        self._apply_request_limit_delay()
                        retry_count += 1
                    elif err.code == 500:
                        self.logger.warning(
                            f"Network Error. Sleeping for"
                            f" {self.network_error_delay} hours.")
                        self._apply_network_error_delay()
                        retry_count += 1
                    elif err.code == 2504:
                        self.logger.warning(
                            f"Gateway Time-out. Sleeping for"
                            f" {self.network_error_delay} minutes.")
                        self._apply_gateway_delay()
                        retry_count += 1
                    else:
                        self.logger.warning(
                            f"Unhandled Eikon error code: {err.code}"
                            f" Sleeping for {self.general_error_delay} minutes."
                        )
                        self._apply_general_error_delay()
                        retry_count += 1
                except Exception as e:
                    self.logger.error(
                        f"Unexpected error for {ric}"
                        f" with fields: {fields}"
                        f" from {start_date} to {end_date}: {str(e)}"
                        f" Sleeping for {self.general_error_delay} minutes."
                    )
                    self._apply_general_error_delay()
                    retry_count += 1

            # If no data was retrieved, append a NaN DataFrame
            if not data_retrieved:
                self.logger.warning(
                    f"No data retrieved for {ric}"
                    f" from {start_date} to {end_date}, adding NaNs."
                )
                ric_timeseries_df = pd.DataFrame(
                    index=index_df.index
                    if isinstance(index_df, pd.DataFrame) #and len(index_df.index) > 2
                    else pd.date_range(start=start_date, end=end_date, freq='D'),
                    columns=[ric],
                    data=np.nan
                )
                ric_timeseries_df_list.append(ric_timeseries_df)

        # Merge all retrieved dataframes
        if ric_timeseries_df_list:
            merged_ric_timeseries_df = pd.concat(ric_timeseries_df_list, axis=0)

            # Drop duplicate dates, keeping only the first occurrence
            merged_ric_timeseries_df = merged_ric_timeseries_df[
                ~merged_ric_timeseries_df.index.duplicated(keep='first')
            ]

            # Join merged data with index_df
            if isinstance(index_df, pd.DataFrame):
                index_df = index_df.join(merged_ric_timeseries_df, how='outer')
            else:
                index_df = merged_ric_timeseries_df.copy(deep=True)

        self.logger.info(
            f"Finished downloading {ric}"
            f" from {start_dates[-1]} to {end_dates[0]}."
        )

        return index_df, None

    def get_constituents_data(
            self,
            rics: Union[str, List[str]],
            fields: Union[str, List[str]],
            target_date: Union[str, datetime],
            pre_fix: Optional[str] = None,
            parameters: Optional[dict] = None,
            max_retries: int = 5
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Retrieves the constituents data for given RICs
         on a specified target date.

        param: rics; The RICs (Reuters Instrument Codes) for which to
         retrieve data; Union[str, List[str]]
        param: fields; The fields to retrieve; Union[str, List[str]]
        param: target_date; The date for which to retrieve the data;
         Union[str, datetime]
        param: pre_fix; Prefix to be added to each RIC; Optional[str];
         default: None
        param: parameters; Additional parameters to pass to the
         data retrieval function; Optional[dict]; default: None
        param: max_retries; Maximum number of retries in case of failure;
         int; default: 5
        :return: A tuple containing the constituents data as a pandas DataFrame
         and an error message; Tuple[pd.DataFrame, Optional[str]]
        """
        if isinstance(rics, str):
            rics = [rics]

        if isinstance(fields, str):
            fields = [fields]

        if isinstance(target_date, datetime):
            target_date = target_date.strftime('%Y-%m-%d')

        if isinstance(pre_fix, str):
            rics = [pre_fix + ric for ric in rics]

        retry_count = 0

        while retry_count < max_retries:
            try:
                self._apply_request_delay()
                self.logger.info(
                    f"Downloading additional data for {rics}"
                    f" with fields: {fields} at {target_date}."
                )

                meta_data_df, err = ek.get_data(
                    instruments=rics,
                    fields=fields,
                    parameters=parameters or {
                        'SDate': target_date,
                        'EDate': target_date
                    },
                    debug=False,
                    raw_output=False,
                    field_name=False
                )

                if (isinstance(meta_data_df, pd.DataFrame)
                        and not meta_data_df.empty
                        and meta_data_df.shape[0] > int(len(rics) * 0.1)):
                    self.logger.info(
                        f"Successfully downloaded {rics} with fields: {fields}"
                        f" at {target_date}."
                    )
                    return meta_data_df, None
                elif err:
                    if (isinstance(err, list)
                            and any(e['code'] == 412 for e in err)):
                        for e in err:
                            self.logger.warning(
                                f"Unable to resolve all requested identifiers:"
                                f" {e['message']}. Returning received data"
                                f" of {rics} with fields: {fields} at {target_date}."
                            )
                        return meta_data_df, None
                    else:
                        self.logger.error(
                            f"Error downloading {rics} with fields: {fields}"
                            f" at {target_date}: {err}. Retrying..."
                        )
                        retry_count += 1
                else:
                    self.logger.error(
                        f"No data received for {rics} with fields: {fields}"
                        f" at {target_date}. Retrying..."
                    )
                    retry_count += 1

            except ek.EikonError as err:
                if err.code == -1:
                    self.logger.warning(
                        f"Skipping download of {rics} with fields: {fields}"
                        f" at {target_date} due to error {err.code}."
                    )
                    return self._empty_df_data(rics, fields), f"Error: {err.code}"
                elif err.code == 401:
                    self.logger.warning(
                        f"Eikon Proxy not running or cannot be reached."
                        f" Sleeping for {self.proxy_error_delay} hours."
                    )
                    self._apply_proxy_error_delay()
                    retry_count += 1
                elif err.code == 429:
                    self.logger.warning(
                        f"Request limit reached. Sleeping for"
                        f" {self.request_limit_delay} hours.")
                    self._apply_request_limit_delay()
                    retry_count += 1
                elif err.code == 500:
                    self.logger.warning(
                        f"Network Error. Sleeping for"
                        f" {self.network_error_delay} hours.")
                    self._apply_network_error_delay()
                    retry_count += 1
                elif err.code == 2504:
                    self.logger.warning(
                        f"Gateway Time-out. Sleeping for"
                        f" {self.network_error_delay} minutes.")
                    self._apply_gateway_delay()
                    retry_count += 1
                else:
                    self.logger.warning(
                        f"Unhandled Eikon error code: {err.code}"
                        f" Sleeping for {self.general_error_delay} minutes."
                    )
                    self._apply_general_error_delay()
                    retry_count += 1
            except Exception as e:
                self.logger.error(
                    f"Unexpected error for {rics}"
                    f" with fields: {fields} at {target_date}: {str(e)}"
                    f" Sleeping for {self.general_error_delay} minutes."
                )
                self._apply_general_error_delay()
                retry_count += 1

        self.logger.critical(
            f"Data retrieval  for {rics} with fields: {fields} at {target_date}"
            f" failed after {max_retries} retries."
        )
        return self._empty_df_data(rics, fields), "Max retries exceeded."

    def get_index_data(
            self,
            ric: str,
            fields: Union[str, List[str]],
            target_date: Union[str, datetime],
            pre_fix: Optional[str] = None,
            parameters: Optional[dict] = None,
            max_retries: int = 5,
            nan_ratio: float = 0.25
    ) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Retrieves the index data for a given RIC on a specified target date.

        param: ric; The RIC (Reuters Instrument Code) of the index; str
        param: fields; The fields to retrieve; Union[str, List[str]]
        param: target_date; The date for which to retrieve the data;
         Union[str, datetime]
        param: pre_fix; Prefix to be added to the RIC; Optional[str];
         default: None
        param: parameters; Additional parameters to pass to the data retrieval
         function; Optional[dict]; default: None
        param: max_retries; Maximum number of retries in case of failure;
         int; default: 5
        param: nan_ratio; The maximum allowed ratio of NaN values in the data;
         float; default: 0.25
        :return: A tuple containing the index data as a pandas DataFrame and
         an error message; Tuple[Optional[pd.DataFrame], Optional[str]]
        """
        if not isinstance(ric, str):
            raise TypeError("'ric' must be of type 'str'!")

        try:
            index_stats_df, err = self.get_constituents_data(
                rics=ric,
                fields=fields,
                target_date=target_date,
                pre_fix=pre_fix,
                parameters=parameters,
                max_retries=max_retries
            )
        except Exception as e:
            err = str(e)
            index_stats_df = None

        if (isinstance(index_stats_df, pd.DataFrame)
                and not index_stats_df.empty):
            nan_ratio_index_stats_df = index_stats_df.isnull().mean()
            has_high_nan_ratio = (nan_ratio_index_stats_df >= nan_ratio).any()
        else:
            has_high_nan_ratio = True

        if (err or not isinstance(index_stats_df, pd.DataFrame)
                or has_high_nan_ratio or index_stats_df.empty):
            self.logger.error(
                f"Finally failed to download additional data"
                f" for {ric} with fields {fields} at {target_date}."
                f" Error: {err}"
            )
            return None, err

        else:
            self.logger.info(
                f"Successfully downloaded additional data"
                f" for {ric} with fields {fields} at {target_date}."
            )
            return index_stats_df, None

    @staticmethod
    def _empty_df_chain(
            ric: str
    ) -> pd.DataFrame:
        """
        Creates an empty DataFrame with a single column named after the
         given RIC and a single NaN value.

        param: ric; The RIC (Reuters Instrument Code) to be used
         as the column name; str
        :return: A pandas DataFrame with one column named after
         the RIC and one NaN value; pd.DataFrame
        :raises ValueError: If the RIC is None or an empty string.
        :raises TypeError: If the RIC is not a string.
        """
        try:
            if ric is None:
                raise ValueError("RIC cannot be None.")
            if not isinstance(ric, str):
                raise TypeError(
                    f"RIC must be a string, got {type(ric).__name__}.")
            if ric == "":
                raise ValueError("RIC cannot be an empty string.")

            return pd.DataFrame(columns=[ric], data=[[np.nan]])
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise

    @staticmethod
    def _empty_df_data(
            self,
            rics: List[str],
            fields: List[str]
    ) -> pd.DataFrame:
        """
        Creates an empty DataFrame with NaN values for the specified RICs and fields.

        :param rics: A list of RICs (Reuters Instrument Codes) to be used as
         columns in the DataFrame.
        :param fields: A list of fields to be used as columns in the DataFrame,
         alongside the RICs.

        :return: An empty DataFrame with columns for the RICs and fields,
         and a single row filled with NaN values.
        """
        return pd.DataFrame(
            columns=["RIC"] + fields,
            data=[[np.nan] * (len(fields) + 1)])

    def _apply_request_delay(self) -> None:
        """
        Applies a delay to the request by sleeping
         for a specified number of seconds.

        :return: None
        """
        if (isinstance(self.request_delay, (int, float))
                and self.request_delay > 0):
            logging.debug(
                f"Applying request delay of {self.request_delay} seconds."
            )
            time.sleep(self.request_delay)

    def _apply_general_error_delay(self) -> None:
        """
        Applies a delay due to general errors by
         sleeping for a specified number of minutes.

        :return: None
        """
        if (isinstance(self.general_error_delay, (int, float))
                and self.general_error_delay > 0):
            logging.debug(
                f"Applying error delay of {self.general_error_delay} minutes."
            )
            time.sleep(self.general_error_delay * 60)

    def _apply_gateway_delay(self) -> None:
        """
        Applies a delay due to Gateway Time-out errors by
         sleeping for a specified number of minutes.

        :return: None
        """
        if (isinstance(self.gateway_delay, (int, float))
                and self.gateway_delay > 0):
            logging.debug(
                f"Applying error delay of {self.gateway_delay} minutes."
            )
            time.sleep(self.gateway_delay * 60)

    def _apply_request_limit_delay(self) -> None:
        """
        Applies a delay due to request limits by sleeping
         for a specified number of hours.

        :return: None
        """
        if (isinstance(self.request_limit_delay, (int, float))
                and self.request_limit_delay > 0):
            logging.debug(
                f"Applying request limit delay"
                f" of {self.request_limit_delay} hours."
            )
            time.sleep(self.request_limit_delay * 3600)

    def _apply_proxy_error_delay(self) -> None:
        """
        Applies a delay due to proxy errors by sleeping
         for a specified number of hours.

        :return: None
        """
        if (isinstance(self.proxy_error_delay, (int, float))
                and self.proxy_error_delay > 0):
            logging.debug(
                f"Applying proxy error delay"
                f" of {self.proxy_error_delay} hours."
            )
            time.sleep(self.proxy_error_delay * 3600)

    def _apply_network_error_delay(self) -> None:
        """
        Applies a delay due to networks errors by sleeping
         for a specified number of hours.

        :return: None
        """
        if (isinstance(self.network_error_delay, (int, float))
                and self.network_error_delay > 0):
            logging.debug(
                f"Applying proxy error delay"
                f" of {self.network_error_delay} hours."
            )
            time.sleep(self.network_error_delay * 3600)

    @staticmethod
    def _unpack_tuple(
            object_: Union[tuple, pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Unpacks a tuple to extract the first element if it is a pandas
         DataFrame, or returns the DataFrame directly if the input is a DataFrame.

        param: object_; The input object to unpack; Union[tuple, pd.DataFrame]
        :return: A pandas DataFrame extracted from the tuple
         or the input DataFrame; pd.DataFrame
        :raises ValueError: If the input tuple is empty.
        :raises TypeError: If the first element of the tuple is not a DataFrame
         or if the input is neither a tuple nor a DataFrame.
        """
        try:
            if isinstance(object_, tuple):
                if len(object_) == 0:
                    raise ValueError("Received an empty tuple. Cannot unpack.")
                if not isinstance(object_[0], pd.DataFrame):
                    raise TypeError(
                        "Expected a DataFrame as the first tuple element."
                    )
                return object_[0]
            elif isinstance(object_, pd.DataFrame):
                return object_
            else:
                raise TypeError(
                    f"Expected tuple or DataFrame, got {type(object_).__name__}"
                )
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise


class OSDownloader:
    def __init__(
            self,
            endpoint: str,
            access_key: str,
            secret_key: str,
            files_path: str,
            secure: bool = False,
            log_downloads: bool = True,
            log_files_path: str = "log_files_OSDownloader",
            workers: int = 1,
    ):
        """
        Initializes the OSDownloader.
        """
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.workers = workers
        self.files_path = files_path
        self.log_downloads = log_downloads
        self.log_files_path = log_files_path

        self.remote_files = []
        self.downloaded_files = []
        self.corrupted_files = []

        # Create directories if they don't exist
        self._ensure_directory_exists(self.log_files_path)

        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _ensure_directory_exists(
            path: str
    ) -> None:
        """
        Helper function to ensure a directory exists, creates if not.
        """
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

    def _ensure_bucket(
            self,
            bucket_name
    ) -> None:
        """
        Ensure the bucket exists.
        """
        if self.client.bucket_exists(bucket_name):
            self.logger.info(f"Bucket {bucket_name} exists")
        else:
            self.logger.error(f"Bucket {bucket_name} does not exists")

    @staticmethod
    def calculate_md5(
            file_path: str
    ) -> str:
        """Computes the MD5 hash of a file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    @staticmethod
    def _get_current_date() -> str:
        """
        Get the current system date, formatted as "DD-MMM-YYYY-HH-MM".

        :return: Current date in formatted datetime format.
        """
        # Get current datetime
        now = datetime.now()

        # Format as string and parse back to enforce "DD-MMM-YYYY-HH:MM" format
        formatted_str = now.strftime("%d-%b-%Y-%H-%M")

        return formatted_str

    def _get_remote_files(
            self,
            bucket_name: str,
            folder_prefix: str,
    ) -> list:
        """
        Retrieve a list of remote files from MinIO.
        """
        remote_files = list(self.client.list_objects(
            bucket_name,
            prefix=folder_prefix,
            recursive=True
        ))

        if not remote_files:
            self.logger.warning(
                f"No files found in bucket '{bucket_name}'"
                f" with prefix '{folder_prefix}'.")

        return remote_files

    def download_bucket(
            self,
            bucket_name: str,
            folder_prefix: Optional[str] = None
    ) -> None:
        """
        Recursively downloads a bucket or folder from MinIO.
        """
        self._ensure_bucket(bucket_name)
        remote_files = self._get_remote_files(bucket_name, folder_prefix)

        if not remote_files:
            self.logger.info(
                f"No files to download from bucket '{bucket_name}'"
                f" with prefix '{folder_prefix}'.")
            return  # Exit early if no files

        files_to_download = []
        for obj in remote_files:
            remote_path = obj.object_name
            self.remote_files.append(remote_path)
            local_file_path = os.path.join(self.files_path, remote_path)

            # Ensure the directory exists
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            # Skip if the file already exists and matches hash
            if os.path.exists(local_file_path) and self._verify_file_integrity(
                    bucket_name, local_file_path, remote_path):
                self.logger.info(
                    f"Skipping already downloaded file: {remote_path}"
                )
                continue

            files_to_download.append((local_file_path, remote_path))

        if not files_to_download:
            self.logger.info("All files are already downloaded and verified.")
            return  # Exit early

        self.logger.info(
            f"Starting download of {len(files_to_download)} files...")

        # Parallel Download with Real-time Logging
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(self._download_file, bucket_name, local,
                                remote): (local, remote)
                for local, remote in files_to_download
            }

            for future in as_completed(futures):
                local_file_path, remote_path = futures[future]
                try:
                    if future.result():
                        self.downloaded_files.append(remote_path)
                        self.logger.info(
                            f"Successfully downloaded: {remote_path}")
                    else:
                        self.corrupted_files.append(remote_path)
                        self.logger.warning(
                            f"Hash mismatch: {remote_path}"
                            f"(Possible corruption)")

                except Exception as e:
                    self.logger.error(f"Error downloading {remote_path}: {e}")

        # Final summary logs
        self.logger.info(
            f"Downloaded {len(self.downloaded_files)} files successfully.")
        if self.corrupted_files:
            self.logger.warning(
                f"{len(self.corrupted_files)} files may be corrupted.")

        # Log downloads if enabled
        if self.log_downloads:
            time_stamp = self._get_current_date()
            formatted_time_stamp = re.sub(r"[-:\s]", "_", time_stamp)

            self._write_log_file(
                f"{self.log_files_path}/"
                f"OSDownloader_downloaded_files_{formatted_time_stamp}.log",
                self.downloaded_files)
            self._write_log_file(
                f"{self.log_files_path}/"
                f"OSDownloader_corrupted_files_{formatted_time_stamp}.log",
                self.corrupted_files)

    def _download_file(
            self,
            bucket_name: str,
            local_file_path: str,
            remote_path: str
    ) -> bool:
        """
        Downloads a single file with immediate logging and integrity check.
        """
        try:
            self.client.fget_object(bucket_name, remote_path, local_file_path)

            # Verify integrity after download
            return self._verify_file_integrity(
                bucket_name, local_file_path, remote_path
            )

        except S3Error as e:
            self.logger.error(
                f"Failed to download {remote_path} due to S3 error: {e}")
        except Exception as e:
            self.logger.error(
                f"Unexpected error downloading {remote_path}: {e}")

        return False

    def _verify_file_integrity(
            self,
            bucket_name: str,
            local_file_path: str,
            remote_path: str
    ) -> bool:
        """
        Verifies if the local file matches the remote file's checksum.
        """
        try:
            local_md5 = self.calculate_md5(local_file_path)
            obj_stat = self.client.stat_object(bucket_name, remote_path)
            return obj_stat.etag == local_md5

        except Exception as e:
            self.logger.error(
                f"Error verifying file integrity for {remote_path}: {e}")
            return False

    def _write_log_file(
            self,
            filename: str,
            data: list
    ) -> None:
        """
        Writes a list of file links to a log file, ensuring each entry
         is on a new line.
        """
        try:
            with open(filename, "w", encoding="utf-8") as file:
                file.write("\n".join(data) + "\n")
            self.logger.info(f"Saved log: {filename} ({len(data)} entries)")
        except Exception as e:
            self.logger.error(f"Error writing log file {filename}: {e}")
