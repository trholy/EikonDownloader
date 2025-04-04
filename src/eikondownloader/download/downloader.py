import pandas as pd
import numpy as np
import eikon as ek

from typing import List, Optional, Tuple, Union
from datetime import datetime
import logging
import time


class EikonDownloader:
    def __init__(
            self,
            api_key: str,
            request_delay: Optional[Union[int, float]] = 1,
            request_limit_delay: Optional[Union[int, float]] = 3600,
            error_delay: Optional[Union[int, float]] = 5,
    ):
        """
        Initializes the object with necessary configuration settings and sets
         up the Eikon API key. This constructor sets the default delays
         for request, request limit, and error handling. It also configures
         the Eikon API by setting the provided `api_key`. Additionally,
         a logger is initialized for tracking the operations.

        :param api_key: The Eikon API key to authenticate requests.
        :param request_delay: The delay (in seconds) between each request
         to avoid overwhelming the server. Default is 1 second.
        :param request_limit_delay: The delay (in seconds) when the request
         limit is reached. Default is 3600 seconds (1 hour).
        :param error_delay: The delay (in seconds) to wait after encountering
         an error. Default is 5 seconds.

        :return: None
        """
        self.request_delay = request_delay
        self.request_limit_delay = request_limit_delay
        self.error_delay = error_delay

        ek.set_app_key(api_key)

        self.logger = logging.getLogger(__name__)

    @staticmethod
    def generate_target_dates(
            end_date: Union[str, datetime],
            num_years: int,
            frequency: str,
            reserve: bool = True
    ) -> List[str]:
        """
        Generate a list of target dates based on the provided parameters.

        This method generates a list of target dates starting from the
         `end_date` and going backward for a specified number of years, based
          on the given frequency (either months, quarters, or years). The dates
          are formatted as strings in the "YYYY-MM-DD" format. The list can be
           returned in reverse order if the `reserve` parameter is set to `True`.

        :param end_date: The end date to generate target dates from. Can be
         a string (e.g. '2025-12-31') or a datetime object.
        :param num_years: The number of years to go back from the `end_date`.
        :param frequency: The frequency of target dates, can be one of 'months',
         'quarters', or 'years'.
        :param reserve: If `True`, the returned list of dates will be in reverse
         order. Defaults to `True`.
        :return: A list of strings representing the target dates in
         "YYYY-MM-DD" format.
        :raises ValueError: If an invalid frequency is provided (anything
         other than 'months', 'quarters', or 'years').
        """
        freq_map = {
            'months': 'M',
            'quarters': 'Q',
            'years': 'Y'
        }

        if frequency not in freq_map:
            raise ValueError(
                "Invalid frequency! Choose from: 'months', 'quarters', 'years'."
            )

        periods = num_years * {
            'months': 12, 'quarters': 4, 'years': 1
        }[frequency]

        target_dates = pd.date_range(
            end=end_date,
            periods=periods,
            freq=freq_map[frequency]
        ).strftime("%Y-%m-%d").tolist()

        return target_dates if not reserve else target_dates[::-1]

    @staticmethod
    def generate_decade_dates(
            end_date: Union[str, datetime, np.datetime64],
            num_years: Optional[int] = None,
            start_date: Optional[Union[str, datetime, np.datetime64]] = None
    ) -> Tuple[List[str], List[str]]:
        """
        Generate start and end dates for each decade within the specified
         date range.

        This method generates a list of start and end dates for each decade,
         with the start date being the first day of the decade (January 1st
         of a year ending in 0) and the end date being the last day of the
         decade. The date range is determined by either providing a specific
          `start_date` or  by calculating it using the `num_years` parameter
          (which represents how many years before the `end_date` the start
           date should be).

        :param end_date: The end date of the date range. Can be a string
         (e.g. '2025-12-31'), datetime object, or np.datetime64.
        :param num_years: The number of years before the `end_date` to
         calculate the start date. Used only if `start_date` is not provided.
        :param start_date: The start date of the date range. Can be a string
         (e.g. '2010-01-01'), datetime object, or np.datetime64. If not
          provided, `num_years` must be provided to determine the start date.
        :return: A tuple containing two lists:
         - A list of start dates (the first day of each decade in
          "YYYY-MM-DD" format).
         - A list of end dates (the last day of each decade in
          "YYYY-MM-DD" format).
        :raises ValueError: If `start_date` is later than `end_date`,
         or if neither `start_date` nor `num_years` is provided.
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
        else:
            raise ValueError(
                "Either 'num_years' or 'start_date' must be provided.")

        if start_date > end_date:
            raise ValueError("'start_date' cannot be later than 'end_date'.")

        start_dates, end_dates = [], []
        current_date = end_date

        while current_date >= start_date:
            end_dates.append(current_date.strftime("%Y-%m-%d"))
            start_of_decade = datetime(current_date.year - 9, 1, 1)
            start_dates.append(start_of_decade.strftime("%Y-%m-%d"))
            current_date = start_of_decade.replace(year=current_date.year - 10)

        return start_dates, end_dates

    def get_index_chain(
            self,
            index_ric: str,
            target_date: Union[str, datetime],
            fields: Union[str, List[str]],
            parameters: Optional[dict] = None,
            max_retries: int = 10,
            pre_fix: Optional[str] = "0#."
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Retrieve the index chain for a given index RIC and target date.

        This method downloads the index chain data for a specified index RIC
         at a given target date, with optional retries and error handling.
         The index RIC is prefixed with a default or provided string, and the
         method can request one or more fields. If the download fails after
         the specified number of retries, an empty DataFrame is returned
         along with an error message.

        :param index_ric: The Reuters Instrument Code (RIC) for the index.
         Must be a string.
        :param target_date: The target date for the index data. Can be a string
         (e.g. '2025-12-31') or a datetime object.
        :param fields: The fields to retrieve for the index. Can be a single
         field as a string or a list of fields.
        :param parameters: Optional dictionary of additional parameters for
         the request (default is None).
        :param max_retries: The maximum number of retries to attempt in case
         of failure. Default is 10.
        :param pre_fix: A prefix to be added to the `index_ric` before making
         the request. Default is "0#.".
        :return: A tuple containing:
         - A pandas DataFrame with the requested index chain data.
         - An optional error message if an error occurs, otherwise None.
        :raises TypeError: If `index_ric` is not a string.
        """
        if not isinstance(index_ric, str):
            raise TypeError("'index_ric' must be of type 'str'!")

        if pre_fix:
            index_ric = pre_fix + index_ric

        if isinstance(fields, str):
            fields = [fields]

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

                if index_chain_df is not None and not index_chain_df.empty:
                    self.logger.info(
                        f"Successfully downloaded {index_ric} at {target_date}."
                    )
                    return index_chain_df, None

                self.logger.warning(
                    f"No data returned for {index_ric} at {target_date}.")
                retry_count += 1

            except ek.EikonError as err:
                self.logger.error(
                    f"Error downloading {index_ric} at {target_date}."
                    f" Error code: {err.code}")

                if err.code == -1:
                    self.logger.warning(
                        f"Skipping download of {index_ric} at {target_date}."
                    )
                    return self._empty_df_chain(index_ric), f"Error: {err.code}"
                elif err.code == 429:
                    self.logger.warning(
                        f"Request limit reached. Sleeping for"
                        f" {self.request_limit_delay} seconds.")
                    self._apply_request_limit_delay()

                retry_count += 1

            except Exception as e:
                self.logger.error(f"Unexpected error for {index_ric}: {str(e)}")
                return self._empty_df_chain(
                    index_ric), f"Unexpected error: {str(e)}"

        self.logger.error(
            f"Failed to download {index_ric} "
            f"at {target_date} after {max_retries} attempts.")
        return self._empty_df_chain(index_ric), "Max retries exceeded."

    def get_etp_chain(
            self,
            etp_ric: str,
            target_date: Union[str, datetime],
            fields: Union[str, List[str]],
            parameters: Optional[dict] = None,
            max_retries: int = 10,
            pre_fix: Optional[str] = None,
    ) -> Union[Tuple[None, str], Tuple[pd.DataFrame, None]]:
        """
        Download data for a given Exchange Traded Product (ETP) chain
         at a specific target date. This method retrieves the data for an
         ETP chain by calling the `get_index_chain` method internally, and
         handles retries and error logging. It returns the ETP chain data
         as a DataFrame if successful, or an error message if the download
         fails after the specified retries.

        :param etp_ric: The Reuters Instrument Code (RIC) for the ETP chain
         to download data for. Must be a string.
        :param target_date: The target date for which to retrieve data. Can be
         a string (e.g., '2025-12-31') or a datetime object.
        :param fields: The fields to retrieve for the ETP chain. Can be a
         single field as a string or a list of fields.
        :param parameters: Optional dictionary of additional parameters for
         the request (default is None).
        :param max_retries: The maximum number of retries to attempt in case
         of failure. Default is 10.
        :param pre_fix: Optional prefix to be added to the `etp_ric` before
         making the request. Default is None.
        :return: A tuple containing:
         - A pandas DataFrame with the requested ETP chain data if successful.
         - An error message (string) if an error occurs, otherwise None.
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
        if err:
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
            max_retries: int = 10,
            fields: Union[str, List[str]] = 'CLOSE',
            interval: str = "daily",
            corax: str = 'adjusted',
            calendar: Optional[str] = None,
            count: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Download index data for a given index within a specified date range and
         merge multiple requests. This method retrieves historical index data
         based on the specified start and end dates, and other optional
         parameters like fields, interval, and calendar. It then merges the
         data and returns it as a DataFrame. The method handles retries in case
         of failures and logs the success or failure of each download attempt.

        :param index_ric: The Reuters Instrument Code (RIC) for the index to
         download data for. Must be a string.
        :param fields: The fields to retrieve for the index, default is 'CLOSE'.
         Can be a single field as a string or a list of fields.
        :param end_date: The reference date to end the data range. Can be a
         string (e.g., '2025-12-31') or a datetime object.
        :param num_years: The number of years to go back from the `end_date`
         to generate past target dates.
        :param start_date: The reference date to start the data range. Can be
         a string (e.g., '2015-01-01') or a datetime object.
        :param pre_fix: Optional prefix to be added to the `index_ric` before
         making the request. Default is '.'.
        :param max_retries: The maximum number of retries in case of failure.
         Default is 10.
        :param interval: The data interval to retrieve. Possible values:
         'tick', 'minute', 'hour', 'daily', 'weekly', 'monthly', 'quarterly',
          'yearly'.
        :param corax: The adjustment type for data. Possible values:
         'adjusted', 'unadjusted'.
        :param calendar: The calendar type to use. Possible values: 'native',
         'tradingdays', 'calendardays'.
        :param count: The maximum number of data points to retrieve.

        :return: A pandas DataFrame with the requested index data, or None
         if no data is retrieved.
        :raises TypeError: If `index_ric` is provided as a list rather than
         a string.
        """
        if isinstance(index_ric, list):
            raise TypeError("'index_ric' has to be of type 'str', not 'list'!")

        if isinstance(pre_fix, str) and isinstance(index_ric, str):
            index_ric = pre_fix + index_ric

        self.logger.info(
            f"Starting download for index {index_ric}"
            f" from {start_date} to {end_date}."
        )

        # Call get_stock_timeseries to fetch the data
        index_timeseries_df = self.get_stock_timeseries(
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
        if index_timeseries_df is None or index_timeseries_df.empty:
            self.logger.warning(
                f"No data retrieved for {index_ric}"
                f" from {start_date} to {end_date}."
            )
            return None

        # Rename the 'CLOSE' column to match the index RIC
        if 'CLOSE' in index_timeseries_df.columns:
            index_timeseries_df.rename(
                columns={'CLOSE': index_ric},
                inplace=True
            )

        self.logger.info(f"Successfully downloaded index data for {index_ric}.")

        return index_timeseries_df

    def get_stock_timeseries(
            self,
            ric: str,
            end_date: Union[str, datetime],
            index_df: Optional[pd.DataFrame] = None,
            num_years: Optional[int] = None,
            start_date: Optional[Union[str, datetime]] = None,
            max_retries: int = 10,
            fields: Union[str, List[str]] = 'CLOSE',
            interval: str = "daily",
            corax: str = 'adjusted',
            calendar: Optional[str] = None,
            count: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Download stock price data for a given symbol within a specified date
         range and merge multiple requests. This method retrieves stock price
         data for the given RIC (Reuters Instrument Code) over a series of
         decades, handling retries and error logging. It returns a DataFrame
         with the stock price data for the specified fields, interval, and
         other parameters. If no data is retrieved, NaN values are appended for
         the corresponding date range.

        :param ric: The stock RIC to download data for. Must be a string.
        :param fields: The fields to retrieve for the stock. Can be a single
         field as a string or a list of fields.
        :param end_date: The reference end date (format: 'YYYY-MM-DD') or a
         datetime object.
        :param index_df: An optional DataFrame to append the retrieved stock
         data to (default is None).
        :param num_years: The number of years to generate past target dates.
         Optional.
        :param start_date: The reference start date (format: 'YYYY-MM-DD') or
         a datetime object. Optional.
        :param max_retries: The maximum number of retries to attempt in case
         of failure. Default is 10.
        :param interval: The data interval to retrieve. Possible values:
         'tick', 'minute', 'hour', 'daily', 'weekly', 'monthly', 'quarterly',
          'yearly'. Default is 'daily'.
        :param corax: The type of data adjustment. Possible values: 'adjusted',
         'unadjusted'. Default is 'adjusted'.
        :param calendar: The calendar type to use. Possible values: 'native',
         'tradingdays', 'calendardays'. Optional.
        :param count: The maximum number of data points to retrieve. Optional.

        :return: A pandas DataFrame with the retrieved stock data merged with
         the provided `index_df`, or a new DataFrame if `index_df` is None.
        :raises TypeError: If `fields` is not a string or a list of strings.
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
            retries = 0
            data_retrieved = False

            while retries < max_retries:
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

                    if (ric_timeseries_df is not None
                            and not ric_timeseries_df.empty):
                        ric_timeseries_df.sort_index(ascending=False,
                                                     inplace=True)
                        """
                        ric_timeseries_df.rename(columns={'CLOSE': ric},
                                                 inplace=True)
                        """
                        ric_timeseries_df.columns = [ric]
                        data_retrieved = True
                        ric_timeseries_df_list.append(ric_timeseries_df)
                        break  # Break out of retry loop if successful

                    else:
                        data_retrieved = False

                except ek.EikonError as err:
                    self.logger.error(
                        f"Error downloading {ric} from"
                        f" {start_date} to {end_date}: {err.code}"
                    )

                    if err.code == -1:  # Critical error, cannot recover
                        self.logger.warning(
                            f"Skipping {ric} due to critical error {err.code}.")
                        break

                    elif err.code == 429:  # Rate limit exceeded
                        self.logger.warning(
                            f"Rate limit hit. Sleeping for"
                            f" {self.request_limit_delay} minutes."
                        )
                        self._apply_request_limit_delay()
                        retries += 1  # Increment retry count

                    else:
                        self.logger.info(
                            f"Retrying download ({retries + 1}/{max_retries})."
                        )
                        self._apply_request_delay()
                        retries += 1

                except Exception as e:
                    self.logger.error(f"Unexpected error for {ric}: {str(e)}")
                    return index_df

            # If no data was retrieved, append a NaN DataFrame
            if not data_retrieved:
                self.logger.warning(
                    f"No data retrieved for {ric}"
                    f" from {start_date} to {end_date}, adding NaNs."
                )
                ric_timeseries_df = pd.DataFrame(
                    index=pd.date_range(start=start_date, end=end_date,
                                        freq='D'),
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

            # Prevent duplicate column names by adding prefix
            """
            merged_ric_timeseries_df = merged_ric_timeseries_df.add_prefix(
                f"{ric}_")
            """
            # Join merged data with index_df
            if index_df is not None:
                index_df = index_df.join(merged_ric_timeseries_df, how='outer')
            else:
                index_df = merged_ric_timeseries_df.copy(deep=True)

        self.logger.info(
            f"Finished downloading {ric}"
            f" from {start_dates[-1]} to {end_dates[0]}."
        )

        return index_df

    def get_additional_data(
            self,
            rics: Union[str, List[str]],
            fields: Union[str, List[str]],
            target_date: Union[str, datetime],
            pre_fix: Optional[str] = None,
            parameters: Optional[dict] = None,
            max_retries: int = 10
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, str]]:
        """
        Downloads additional stock data for a given list of RICs and fields,
         with support for retries and error handling. This method retrieves
         extra data (such as ESG scores, market cap, etc.) for one or more
         RICs (Reuters Instrument Codes) based on the specified fields.
         It supports retries in case of errors and logs the process.

        :param rics: The RIC(s) (string or list of strings) for which additional
         stock data is to be retrieved.
        :param fields: The fields (string or list of strings) to retrieve for
         the given RIC(s).
        :param target_date: The target date for the data in 'YYYY-MM-DD' format
         or as a datetime object.
        :param pre_fix: An optional prefix to be added to the RIC(s).
        :param parameters: Optional global parameters to include in the request.
        :param max_retries: The maximum number of retries in case of failure
         (default is 10).

        :return: A tuple with a DataFrame containing the downloaded data
         (or an empty DataFrame) and an error message (if any). If no data is
         available after retries, it returns an empty DataFrame with a
         corresponding error message.
        :raises: Returns a tuple of (empty DataFrame, error message) in case
         of failure after maximum retries.
        """
        if isinstance(pre_fix, str) and isinstance(rics, str):
            rics = pre_fix + rics

        if isinstance(rics, str):
            rics = [rics]

        if isinstance(fields, str):
            fields = [fields]

        retry_count = 0

        while retry_count < max_retries:
            try:
                self._apply_request_delay()
                self.logger.info(
                    f"Downloading additional stock data"
                    f" for {rics} with fields: {fields}."
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
                """
                # Replace <NA> with np.nan
                meta_data_df = meta_data_df.replace({pd.NA: np.nan})

                cols_to_convert_to_float = [
                    'ESG Score',
                    'Environmental Pillar Score',
                    'Social Pillar Score',
                    'Governance Pillar Score',
                    'Company Market Cap'
                ]
                cols_to_convert_to_string = [
                    'Instrument',
                    'TRBC Economic Sector Name',
                    'TRBC Business Sector Name',
                    'TRBC Industry Group Name',
                    'TRBC Industry Name',
                    'ISIN'
                ]
                meta_data_df[cols_to_convert_to_float] = meta_data_df[cols_to_convert_to_float].astype(float)
                meta_data_df[cols_to_convert_to_string] = meta_data_df[cols_to_convert_to_string].astype('string')
                """

                if err:
                    self.logger.error(
                        f"Error downloading additional stock data: {err}")
                    return self._empty_df_data(rics, fields), f"Error: {err}"

                if (meta_data_df is not None
                        and not meta_data_df.empty
                        and meta_data_df.shape[0] > 1):
                    self.logger.info(
                        "Successfully downloaded additional stock data.")
                    return meta_data_df, None

                else:
                    self.logger.warning(
                        f"No additional stock data available for {rics}.")
                    retry_count += 1
                    continue

            except ek.EikonError as err:
                self.logger.error(
                    f"Downloading additional stock data failed!"
                    f" Error code: {err.code}"
                )

                if err.code == -1:
                    self.logger.warning(
                        f"Skipping download of {rics} due to error {err.code}.")
                    return self._empty_df_data(rics,
                                               fields), f"Error: {err.code}"

                elif err.code == 429:
                    self.logger.warning(
                        f"Rate limit hit. Sleeping"
                        f" for {self.request_limit_delay} minutes."
                    )
                    self._apply_request_limit_delay()

                else:
                    self.logger.info(
                        f"Retrying download ({retry_count + 1}/{max_retries})."
                    )
                    self._apply_request_delay()

                retry_count += 1

            except Exception as e:
                self.logger.error(
                    f"Unexpected error while downloading additional"
                    f" stock data: {str(e)}"
                )
                return (self._empty_df_data(rics, fields),
                        f"Unexpected error: {str(e)}")

        self.logger.error(
            f"Failed to download additional stock data"
            f" for {rics} after {max_retries} attempts."
        )
        return self._empty_df_data(rics, fields), "Max retries exceeded."

    def _empty_df_chain(self, ric: str) -> pd.DataFrame:
        """
        Creates an empty DataFrame with a single column corresponding to the
         given RIC, filled with NaN values.

        :param ric: The RIC (Reuters Instrument Code) to be used as the
         column name for the DataFrame.

        :return: An empty DataFrame with a column named after the provided
         RIC and a row of NaN values.
        """
        return pd.DataFrame(columns=[ric], data=[np.nan])

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
        Applies a delay between requests if the delay time is specified
         and greater than zero.

        :return: None
        """
        if isinstance(self.request_delay,
                      (int, float)) and self.request_delay > 0:
            time.sleep(self.request_delay)

    def _apply_request_limit_delay(self) -> None:
        """
        Applies a delay when the request limit is reached, based on
         the specified delay time.

        :return: None
        """
        if isinstance(self.request_limit_delay,
                      (int, float)) and self.request_limit_delay > 0:
            time.sleep(self.request_limit_delay)

    @staticmethod
    def _unpack_tuple(
            object_: Union[tuple, pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Unpacks a tuple returned by `ek.get_data` to extract the DataFrame.

        This static method checks if the input is a tuple. If it is, it
         attempts to extract the first element and verifies it is a pandas
         DataFrame. If the input is already a DataFrame, it is returned
         directly. If the input doesn't match the expected types, an appropriate
         error is raised.

        :param object_: The input object, which can either be a tuple
         or a DataFrame.

        :return: The extracted DataFrame if the input is a tuple containing
         one, or the DataFrame itself if the input is already a DataFrame.

        :raises ValueError: If the tuple is empty.
        :raises TypeError: If the first element of the tuple is not a DataFrame,
         or if the input is neither a tuple nor a DataFrame.
        """
        if isinstance(object_, tuple):
            if len(object_) == 0:
                raise ValueError("Received an empty tuple. Cannot unpack.")
            if not isinstance(object_[0], pd.DataFrame):
                raise TypeError(
                    "Expected a DataFrame as the first tuple element.")
            return object_[0]
        elif isinstance(object_, pd.DataFrame):
            return object_
        else:
            raise TypeError(
                f"Expected tuple or DataFrame, got {type(object_).__name__}")
