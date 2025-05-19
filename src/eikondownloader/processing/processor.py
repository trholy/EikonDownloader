import pandas as pd
import numpy as np

from typing import Union
import logging
import glob
import os


# Configure logging to remove the default prefix
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
)


class DataProcessor:
    def __init__(
            self,
            path: str,
            mode: str = "index"
    ):
        """
        Initializes the DataProcessor with a specified path and mode.

        param: path; The directory path where the data files are located. (str)
        param: mode; The mode of operation, either 'index' or 'etp'.
         Default is 'index'. (str)
        """
        self.path = path
        self.mode = mode.lower()

        # Validate mode
        if self.mode not in {'index', 'etp'}:
            raise ValueError("Invalid mode! Choose either 'index' or 'etp'.")

        self.mode_mapping = {'index': 'index_chain', 'etp': 'etp_chain'}
        self.column_mapping = {'index': 'Instrument', 'etp': 'Constituent RIC'}

        self.logger = logging.getLogger(__name__)

    def get_csv_files(
            self
    ) -> list:
        """
        Retrieves a sorted list of CSV files from the directory
         corresponding to the specified mode.

        :return: A sorted list of CSV file paths found in the directory
         corresponding to the specified mode. (list)
        """
        try:
            mode_dir = self.mode_mapping[self.mode]
        except KeyError:
            self.logger.error(f"Mode '{self.mode}' not found in mode_mapping.")
            raise KeyError(f"Mode '{self.mode}' not found in mode_mapping.")

        try:
            search_path = os.path.join(self.path, mode_dir, "*.csv")
            files = sorted(glob.glob(search_path))
            files = [os.path.normpath(file) for file in files]
        except OSError as e:
            self.logger.error(f"OS error occurred: {e}")
            raise OSError(f"OS error occurred: {e}")

        if not files:
            self.logger.warning(
                f"No CSV files found in {search_path}. Check your directory."
            )
            raise FileNotFoundError(f"No CSV files found in {search_path}.")

        return files

    def get_index_names(
            self,
            search_hidden: bool = False
    ) -> list:
        """
        Retrieve all index names from the directory.

        This method constructs a search path using the provided `path` and a
         flag (`search_hidden`) to determine whether to include hidden
         directories. It returns a sorted list of index names (i.e.,
         folder names) from the directory. If no index names are found, a
         warning is logged.

        :param search_hidden: Whether to include hidden directories
         (default is False).

        :return: Sorted list of index names (folder names).

        :raises: None
        """
        search_path = os.path.join(self.path, ".*" if search_hidden else "*")
        index_names = sorted(
            [os.path.basename(folder) for folder in glob.glob(search_path)
             if os.path.isdir(folder)]
        )

        if not index_names:
            self.logger.warning(
                f"No index names found in {search_path}."
                f" Check your directory."
            )

        return index_names

    def get_unique_rics(
            self,
            files: list,
            sep: str = ","
    ) -> np.ndarray:
        """
        Extracts and returns a sorted array of unique RICs from
         a list of CSV files.

        param: files; A list of file paths to the CSV files containing RICs.
         (list)
        param: sep; The delimiter to use when reading the CSV files.
         Default is ",". (str)
        :return: A sorted NumPy array of unique RICs extracted from
         the provided CSV files. (np.ndarray)
        """
        if not files:
            self.logger.warning("No CSV files provided. Returning empty list.")
            return np.array([])

        try:
            column_name = self.column_mapping[self.mode]
        except KeyError:
            self.logger.error(f"Mode '{self.mode}' not found in column_mapping.")
            raise KeyError(f"Mode '{self.mode}' not found in column_mapping.")

        rics_list, shapes = [], []

        for file in files:
            try:
                df_temp = pd.read_csv(file, sep=sep, usecols=[column_name])
                rics = df_temp[column_name].dropna().astype(str)
                rics_list.append(rics)
                shapes.append(df_temp.shape[0])

                self.logger.info(
                    f"Processed file: {file} | Shape: {df_temp.shape}"
                )

            except FileNotFoundError as fnf_e:
                self.logger.error(f"FileNotFoundError: {fnf_e}")
                continue
            except KeyError as key_e:
                self.logger.error(f"KeyError: {key_e} in file {file}")
                continue
            except pd.errors.EmptyDataError as empty_e:
                self.logger.error(f"EmptyDataError: {empty_e} in file {file}")
                continue
            except Exception as e:
                self.logger.error(f"Unexpected error reading file {file}: {e}")
                continue

        if not rics_list:
            self.logger.warning("No valid RICs found. Returning empty list.")
            return np.array([])

        # Concatenate lists efficiently
        all_rics = pd.concat(rics_list, ignore_index=True).unique()
        sorted_rics = np.sort(all_rics)

        self.logger.info(
            f"Min RIC count: {min(shapes)} |"
            f" Max RIC count: {max(shapes)} |"
            f" Total unique RICs: {len(sorted_rics)}."
        )

        return sorted_rics

    @staticmethod
    def split_list(
            lst: Union[list, np.ndarray],
            chunk_size: int = 2000
    ) -> list:
        """
        Splits the input list or NumPy array into smaller chunks of a
         specified size.

        :param lst: The input list or NumPy array to be split.
        :param chunk_size: The size of each chunk. Default is 2000.

        :return: A list containing the chunks.
        """
        if isinstance(lst, np.ndarray):
            lst = list(lst)

        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
