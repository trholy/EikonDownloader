# EikonDownloader

The `EikonDownloader` class is designed to facilitate the downloading of financial data from the Eikon Data API. It includes methods to generate date ranges, retrieve index chains, ETP chains, timeseries data, and constituents data, with built-in error handling and retry mechanisms.

## Constructor (__init__ method)

The `__init__` method initializes the `EikonDownloader` class with optional parameters for API key and various delay settings.

### Parameters

- `api_key` (Optional[str]): An optional Eikon API key to authenticate requests. Defaults to `None`.
- `request_delay` (Optional[Union[int, float]]): The delay in seconds between requests to avoid hitting rate limits. Defaults to `3`.
- `general_error_delay` (Optional[Union[int, float]]): The delay in minutes to wait when a general error occurs. Defaults to `5`.
- `gateway_delay` (Optional[Union[int, float]]): The delay in minutes to wait when a Gateway Time-out occurs. Defaults to `5`.
- `request_limit_delay` (Optional[Union[int, float]]): The delay in hours to wait when a request limit is reached. Defaults to `6`.
- `proxy_error_delay` (Optional[Union[int, float]]): The delay in hours to wait when a proxy error occurs. Defaults to `6`.
- `network_error_delay` (Optional[Union[int, float]]): The delay in hours to wait when a network error occurs. Defaults to `1`.

## Methods

### Public Methods

#### generate_target_dates

Generates a list of target dates based on the specified end date, number of years, and frequency.

##### Parameters

- `end_date` (Union[str, datetime]): The end date for the date range. Can be a string in a format recognized by pandas or a datetime object.
- `num_years` (int): The number of years to generate dates for. Must be between 1 and 99.
- `frequency` (str): The frequency of the dates to generate. Must be one of 'months' ('m'), 'quarters' ('q'), or 'years' ('y').
- `reverse` (bool): If `True`, the list of dates will be reversed so that the most recent date comes first. Defaults to `True`.

##### Returns

- List[str]: A list of date strings in the format 'YYYY-MM-DD'.

#### generate_decade_dates

Generates lists of start and end dates for each decade within the specified range.

##### Parameters

- `end_date` (Union[str, datetime, np.datetime64]): The end date for the date range. Can be a string in 'YYYY-MM-DD' format, a datetime object, or a numpy datetime64 object.
- `num_years` (Optional[int]): The number of years to generate dates for, starting from the `end_date`. If provided, `start_date` is calculated as `end_date` minus `num_years`.
- `start_date` (Optional[Union[str, datetime, np.datetime64]]): The start date for the date range. Can be a string in 'YYYY-MM-DD' format, a datetime object, or a numpy datetime64 object. If not provided, it is calculated based on `num_years`.

##### Returns

- Tuple[List[str], List[str]]: A tuple containing two lists of date strings in the format 'YYYY-MM-DD'. The first list contains the start dates of each decade, and the second list contains the end dates of each decade.

#### get_index_chain

Retrieves the index chain data for a specified index RIC at a given target date.

##### Parameters

- `index_ric` (str): The RIC (Reuters Instrument Code) of the index.
- `target_date` (Union[str, datetime]): The target date for which to retrieve the index chain data. Can be a string in 'YYYY-MM-DD' format or a datetime object.
- `fields` (Union[str, List[str]]): The fields to retrieve. Can be a single field as a string or a list of fields.
- `parameters` (Optional[dict]): Additional parameters to pass to the Eikon API. Defaults to `None`.
- `max_retries` (int): The maximum number of retries to attempt if the request fails. Defaults to `5`.
- `pre_fix` (Optional[str]): A prefix to prepend to the index RIC. Defaults to `"0#."`.

##### Returns

- Tuple[pd.DataFrame, Optional[str]]: A tuple containing a DataFrame with the index chain data and an optional error message. If the data retrieval is successful, the error message will be `None`.

#### get_etp_chain

Retrieves the ETP chain data for a given ETP RIC on a specified target date.

##### Parameters

- `etp_ric` (str): The RIC (Reuters Instrument Code) of the ETP.
- `target_date` (Union[str, datetime]): The date for which to retrieve the ETP chain data.
- `fields` (Union[str, List[str]]): The fields to retrieve.
- `parameters` (Optional[dict]): Additional parameters to pass to the data retrieval function. Defaults to `None`.
- `max_retries` (int): Maximum number of retries in case of failure. Defaults to `5`.
- `pre_fix` (Optional[str]): Prefix to be used in logging. Defaults to `None`.

##### Returns

- Union[Tuple[None, str], Tuple[pd.DataFrame, None]]: A tuple containing the ETP chain data as a pandas DataFrame and an error message.

#### get_index_timeseries

Retrieves the timeseries data for a given index RIC within a specified date range.

##### Parameters

- `index_ric` (str): The RIC (Reuters Instrument Code) of the index.
- `end_date` (Union[str, datetime]): The end date for the timeseries data.
- `num_years` (Optional[int]): The number of years of data to retrieve.
- `start_date` (Optional[Union[str, datetime]]): The start date for the timeseries data.
- `pre_fix` (Union[str, None]): Prefix to be added to the index RIC. Defaults to `"."`.
- `max_retries` (int): Maximum number of retries in case of failure. Defaults to `5`.
- `fields` (Union[str, List[str]]): The fields to retrieve. Defaults to `'CLOSE'`.
- `interval` (str): The interval of the timeseries data (e.g., 'daily', 'weekly'). Defaults to `"daily"`.
- `corax` (str): The type of price adjustment ('adjusted', 'unadjusted'). Defaults to `'adjusted'`.
- `calendar` (Optional[str]): The calendar to use for the timeseries data. Defaults to `None`.
- `count` (Optional[int]): The number of data points to retrieve. Defaults to `None`.

##### Returns

- Tuple[Optional[pd.DataFrame], Optional[str]]: A tuple containing the timeseries data as a pandas DataFrame and an error message.

#### get_stock_timeseries

Retrieves the timeseries data for a given stock RIC within a specified date range.

##### Parameters

- `ric` (str): The RIC (Reuters Instrument Code) of the stock.
- `end_date` (Union[str, datetime]): The end date for the timeseries data.
- `index_df` (Optional[pd.DataFrame]): An optional DataFrame to join the retrieved data with. Defaults to `None`.
- `num_years` (Optional[int]): The number of years of data to retrieve.
- `start_date` (Optional[Union[str, datetime]]): The start date for the timeseries data.
- `max_retries` (int): Maximum number of retries in case of failure. Defaults to `5`.
- `fields` (Union[str, List[str]]): The fields to retrieve. Defaults to `'CLOSE'`.
- `interval` (str): The interval of the timeseries data (e.g., 'daily', 'weekly'). Defaults to `"daily"`.
- `corax` (str): The type of price adjustment ('adjusted', 'unadjusted'). Defaults to `'adjusted'`.
- `calendar` (Optional[str]): The calendar to use for the timeseries data. Defaults to `None`.
- `count` (Optional[int]): The number of data points to retrieve. Defaults to `None`.

##### Returns

- Tuple[pd.DataFrame, Optional[str]]: A tuple containing the timeseries data as a pandas DataFrame and an error message.

#### get_constituents_data

Retrieves the constituents data for given RICs on a specified target date.

##### Parameters

- `rics` (Union[str, List[str]]): The RICs (Reuters Instrument Codes) for which to retrieve data.
- `fields` (Union[str, List[str]]): The fields to retrieve.
- `target_date` (Union[str, datetime]): The date for which to retrieve the data.
- `pre_fix` (Optional[str]): Prefix to be added to each RIC. Defaults to `None`.
- `parameters` (Optional[dict]): Additional parameters to pass to the data retrieval function. Defaults to `None`.
- `max_retries` (int): Maximum number of retries in case of failure. Defaults to `5`.

##### Returns

- Tuple[pd.DataFrame, Optional[str]]: A tuple containing the constituents data as a pandas DataFrame and an error message.

#### get_index_data

Retrieves the index data for a given RIC on a specified target date.

##### Parameters

- `ric` (str): The RIC (Reuters Instrument Code) of the index.
- `fields` (Union[str, List[str]]): The fields to retrieve.
- `target_date` (Union[str, datetime]): The date for which to retrieve the data.
- `pre_fix` (Optional[str]): Prefix to be added to the RIC. Defaults to `None`.
- `parameters` (Optional[dict]): Additional parameters to pass to the data retrieval function. Defaults to `None`.
- `max_retries` (int): Maximum number of retries in case of failure. Defaults to `5`.
- `nan_ratio` (float): The maximum allowed ratio of NaN values in the data. Defaults to `0.25`.

##### Returns

- Tuple[Optional[pd.DataFrame], Optional[str]]: A tuple containing the index data as a pandas DataFrame and an error message.

### Hidden/Protected Methods

#### _empty_df_chain

Creates an empty DataFrame with a single column named after the given RIC and a single NaN value.

##### Parameters

- `ric` (str): The RIC (Reuters Instrument Code) to be used as the column name.

##### Returns

- pd.DataFrame: A pandas DataFrame with one column named after the RIC and one NaN value.

##### Raises

- ValueError: If the RIC is `None` or an empty string.
- TypeError: If the RIC is not a string.

#### _empty_df_data

Creates an empty DataFrame with RICs as the index and fields as columns, filled with NaN values.

##### Parameters

- `rics` (Union[str, List[str]]): The RICs (Reuters Instrument Codes) to be used as the index.
- `fields` (Union[str, List[str]]): The fields to be used as the columns.

##### Returns

- pd.DataFrame: A pandas DataFrame with RICs as the index and fields as columns, filled with NaN values.

##### Raises

- ValueError: If RICs or fields are an empty list or string.
- TypeError: If RICs or fields are not a list or string.

#### _apply_request_delay

Applies a delay to the request by sleeping for a specified number of seconds.

##### Returns

- None

#### _apply_request_limit_delay

Applies a delay due to request limits by sleeping for a specified number of seconds.

##### Returns

- None

#### _apply_proxy_error_delay

Applies a delay due to proxy errors by sleeping for a specified number of seconds.

##### Returns

- None

#### _apply_error_delay

Applies a delay due to general errors by sleeping for a specified number of seconds.

##### Returns

- None

#### _unpack_tuple

Unpacks a tuple to extract the first element if it is a pandas DataFrame, or returns the DataFrame directly if the input is a DataFrame.

##### Parameters

- `object_` (Union[tuple, pd.DataFrame]): The input object to unpack.

##### Returns

- pd.DataFrame: A pandas DataFrame extracted from the tuple or the input DataFrame.

##### Raises

- ValueError: If the input tuple is empty.
- TypeError: If the first element of the tuple is not a DataFrame or if the input is neither a tuple nor a DataFrame.