# `EikonDownloader`

The `EikonDownloader` class is designed to facilitate the downloading of financial data from the Eikon API. It handles API key configuration, request delays, and error handling to ensure robust data retrieval. The class provides methods to generate target dates, download index chains, ETP chains, index timeseries, stock timeseries, and additional stock data.

## Constructor

```python
EikonDownloader(api_key, request_delay=1, request_limit_delay=3600, error_delay=5)
```

**Parameters:**

- `api_key` : `str`
  - The Eikon API key to authenticate requests.
  
- `request_delay` : `int` or `float`, default=1
  - The delay (in seconds) between each request to avoid overwhelming the server.
  
- `request_limit_delay` : `int` or `float`, default=3600
  - The delay (in seconds) when the request limit is reached.
  
- `error_delay` : `int` or `float`, default=5
  - The delay (in seconds) to wait after encountering an error.

## Methods

### `generate_target_dates`

```python
EikonDownloader.generate_target_dates(end_date, num_years, frequency, reserve=True)
```

Generate a list of target dates based on the provided parameters.

**Parameters:**

- `end_date` : `str` or `datetime`
  - The end date to generate target dates from.
  
- `num_years` : `int`
  - The number of years to go back from the `end_date`.
  
- `frequency` : `str`
  - The frequency of target dates, can be one of 'months', 'quarters', or 'years'.
  
- `reserve` : `bool`, default=True
  - If `True`, the returned list of dates will be in reverse order.

**Returns:**

- `List[str]`
  - A list of strings representing the target dates in "YYYY-MM-DD" format.

**Raises:**

- `ValueError`
  - If an invalid frequency is provided.

### `generate_decade_dates`

```python
EikonDownloader.generate_decade_dates(end_date, num_years=None, start_date=None)
```

Generate start and end dates for each decade within the specified date range.

**Parameters:**

- `end_date` : `str`, `datetime`, or `np.datetime64`
  - The end date of the date range.
  
- `num_years` : `int`, optional
  - The number of years before the `end_date` to calculate the start date.
  
- `start_date` : `str`, `datetime`, or `np.datetime64`, optional
  - The start date of the date range.

**Returns:**

- `Tuple[List[str], List[str]]`
  - A tuple containing two lists: start dates and end dates in "YYYY-MM-DD" format.

**Raises:**

- `ValueError`
  - If `start_date` is later than `end_date`, or if neither `start_date` nor `num_years` is provided.

### `get_index_chain`

```python
EikonDownloader.get_index_chain(index_ric, target_date, fields, parameters=None, max_retries=10, pre_fix="0#.")
```

Retrieve the index chain for a given index RIC and target date.

**Parameters:**

- `index_ric` : `str`
  - The Reuters Instrument Code (RIC) for the index.
  
- `target_date` : `str` or `datetime`
  - The target date for the index data.
  
- `fields` : `str` or `List[str]`
  - The fields to retrieve for the index.
  
- `parameters` : `dict`, optional
  - Optional dictionary of additional parameters for the request.
  
- `max_retries` : `int`, default=10
  - The maximum number of retries to attempt in case of failure.
  
- `pre_fix` : `str`, default="0#."
  - A prefix to be added to the `index_ric` before making the request.

**Returns:**

- `Tuple[pd.DataFrame, Optional[str]]`
  - A tuple containing a pandas DataFrame with the requested index chain data and an optional error message.

**Raises:**

- `TypeError`
  - If `index_ric` is not a string.

### `get_etp_chain`

```python
EikonDownloader.get_etp_chain(etp_ric, target_date, fields, parameters=None, max_retries=10, pre_fix=None)
```

Download data for a given Exchange Traded Product (ETP) chain at a specific target date.

**Parameters:**

- `etp_ric` : `str`
  - The Reuters Instrument Code (RIC) for the ETP chain to download data for.
  
- `target_date` : `str` or `datetime`
  - The target date for which to retrieve data.
  
- `fields` : `str` or `List[str]`
  - The fields to retrieve for the ETP chain.
  
- `parameters` : `dict`, optional
  - Optional dictionary of additional parameters for the request.
  
- `max_retries` : `int`, default=10
  - The maximum number of retries to attempt in case of failure.
  
- `pre_fix` : `str`, optional
  - Optional prefix to be added to the `etp_ric` before making the request.

**Returns:**

- `Union[Tuple[None, str], Tuple[pd.DataFrame, None]]`
  - A tuple containing a pandas DataFrame with the requested ETP chain data if successful, or an error message if an error occurs.

### `get_index_timeseries`

```python
EikonDownloader.get_index_timeseries(index_ric, end_date, num_years, start_date=None, pre_fix=".", max_retries=10, fields="CLOSE", interval="daily", corax="adjusted", calendar=None, count=None)
```

Download index data for a given index within a specified date range and merge multiple requests.

**Parameters:**

- `index_ric` : `str`
  - The Reuters Instrument Code (RIC) for the index to download data for.
  
- `end_date` : `str` or `datetime`
  - The reference date to end the data range.
  
- `num_years` : `int`
  - The number of years to go back from the `end_date` to generate past target dates.
  
- `start_date` : `str` or `datetime`, optional
  - The reference date to start the data range.
  
- `pre_fix` : `str`, default="."
  - Optional prefix to be added to the `index_ric` before making the request.
  
- `max_retries` : `int`, default=10
  - The maximum number of retries in case of failure.
  
- `fields` : `str` or `List[str]`, default="CLOSE"
  - The fields to retrieve for the index.
  
- `interval` : `str`, default="daily"
  - The data interval to retrieve.
  
- `corax` : `str`, default="adjusted"
  - The adjustment type for data.
  
- `calendar` : `str`, optional
  - The calendar type to use.
  
- `count` : `int`, optional
  - The maximum number of data points to retrieve.

**Returns:**

- `Optional[pd.DataFrame]`
  - A pandas DataFrame with the requested index data, or `None` if no data is retrieved.

**Raises:**

- `TypeError`
  - If `index_ric` is provided as a list rather than a string.

### `get_stock_timeseries`

```python
EikonDownloader.get_stock_timeseries(ric, end_date, index_df=None, num_years=None, start_date=None, max_retries=10, fields="CLOSE", interval="daily", corax="adjusted", calendar=None, count=None)
```

Download stock price data for a given symbol within a specified date range and merge multiple requests.

**Parameters:**

- `ric` : `str`
  - The stock RIC to download data for.
  
- `end_date` : `str` or `datetime`
  - The reference end date.
  
- `index_df` : `pd.DataFrame`, optional
  - An optional DataFrame to append the retrieved stock data to.
  
- `num_years` : `int`, optional
  - The number of years to generate past target dates.
  
- `start_date` : `str` or `datetime`, optional
  - The reference start date.
  
- `max_retries` : `int`, default=10
  - The maximum number of retries to attempt in case of failure.
  
- `fields` : `str` or `List[str]`, default="CLOSE"
  - The fields to retrieve for the stock.
  
- `interval` : `str`, default="daily"
  - The data interval to retrieve.
  
- `corax` : `str`, default="adjusted"
  - The type of data adjustment.
  
- `calendar` : `str`, optional
  - The calendar type to use.
  
- `count` : `int`, optional
  - The maximum number of data points to retrieve.

**Returns:**

- `pd.DataFrame`
  - A pandas DataFrame with the retrieved stock data merged with the provided `index_df`, or a new DataFrame if `index_df` is `None`.

**Raises:**

- `TypeError`
  - If `fields` is not a string or a list of strings.

### `get_additional_data`

```python
EikonDownloader.get_additional_data(rics, fields, target_date, pre_fix=None, parameters=None, max_retries=10)
```

Downloads additional stock data for a given list of RICs and fields, with support for retries and error handling.

**Parameters:**

- `rics` : `str` or `List[str]`
  - The RIC(s) for which additional stock data is to be retrieved.
  
- `fields` : `str` or `List[str]`
  - The fields to retrieve for the given RIC(s).
  
- `target_date` : `str` or `datetime`
  - The target date for the data.
  
- `pre_fix` : `str`, optional
  - An optional prefix to be added to the RIC(s).
  
- `parameters` : `dict`, optional
  - Optional global parameters to include in the request.
  
- `max_retries` : `int`, default=10
  - The maximum number of retries in case of failure.

**Returns:**

- `Union[pd.DataFrame, Tuple[pd.DataFrame, str]]`
  - A tuple with a DataFrame containing the downloaded data and an error message (if any).

**Raises:**

- Returns a tuple of (empty DataFrame, error message) in case of failure after maximum retries.

### `_empty_df_chain`

```python
EikonDownloader._empty_df_chain(ric)
```

Creates an empty DataFrame with a single column corresponding to the given RIC, filled with NaN values.

**Parameters:**

- `ric` : `str`
  - The RIC to be used as the column name for the DataFrame.

**Returns:**

- `pd.DataFrame`
  - An empty DataFrame with a column named after the provided RIC and a row of NaN values.

### `_empty_df_data`

```python
EikonDownloader._empty_df_data(rics, fields)
```

Creates an empty DataFrame with NaN values for the specified RICs and fields.

**Parameters:**

- `rics` : `List[str]`
  - A list of RICs to be used as columns in the DataFrame.
  
- `fields` : `List[str]`
  - A list of fields to be used as columns in the DataFrame, alongside the RICs.

**Returns:**

- `pd.DataFrame`
  - An empty DataFrame with columns for the RICs and fields, and a single row filled with NaN values.

### `_apply_request_delay`

```python
EikonDownloader._apply_request_delay()
```

Applies a delay between requests if the delay time is specified and greater than zero.

**Returns:**

- `None`

### `_apply_request_limit_delay`

```python
EikonDownloader._apply_request_limit_delay()
```

Applies a delay when the request limit is reached, based on the specified delay time.

**Returns:**

- `None`

### `_unpack_tuple`

```python
EikonDownloader._unpack_tuple(object_)
```

Unpacks a tuple returned by `ek.get_data` to extract the DataFrame.

**Parameters:**

- `object_` : `tuple` or `pd.DataFrame`
  - The input object, which can either be a tuple or a DataFrame.

**Returns:**

- `pd.DataFrame`
  - The extracted DataFrame if the input is a tuple containing one, or the DataFrame itself if the input is already a DataFrame.

**Raises:**

- `ValueError`
  - If the tuple is empty.
  
- `TypeError`
  - If the first element of the tuple is not a DataFrame, or if the input is neither a tuple nor a DataFrame.
