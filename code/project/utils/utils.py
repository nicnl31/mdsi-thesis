import binance
import pandas as pd
from binance.client import Client
from typing import Tuple
import time
import calendar
from datetime import datetime, timezone



def get_local_credentials(
		path: str='/Users/nicholasle/.binance_api/creds.txt'
) -> Tuple[str, str]:
	"""
	Gets the Binance API credentials from the local machine.

	:param path:
		The local path, which is a .txt file containing Binance API key and
		secret. The file is structured as follows:

		-------- FILE: creds.txt --------
		"YOUR_API_KEY"
		"YOUR_API_SECRET"
		---------------------------------
	:return:
		A tuple (key, secret).
	"""
	with open(path) as f:
		creds = [line.strip() for line in f.readlines()]
	return creds[0], creds[1]


def get_time_millisecond(dt: datetime) -> int:
	"""
	Get a fixed datetime in terms of milliseconds since Epoch.

	:param dt:
		The datetime object.
	:return:
		time_millisec, the number of milliseconds since Epoch.
	"""
	time_tuple = dt.timetuple()
	time_millisec = int(calendar.timegm(time_tuple) * 1000)
	return time_millisec


def download_data(symbol_list: list, get_earliest_date: bool, main_timeframe: str='1h',
				  detailed_timeframe: str='1m', save_to_path: str='../datasets',
				  requested_date: datetime=None, verbose: bool=True):
	"""
	Downloads data and saves the data to the project folder.

	:param symbol_list:
		The list of symbols in Binance-friendly format "BASEQUOTE", for example
		"BTCUSDT".
	:param main_timeframe:
		The main timeframe used in trading.
	:param detailed_timeframe:
		The smallest timeframe used for analysing intracandle movements in the
		main timeframe.
	:param get_earliest_date:
		Whether to download the data from the earliest date available.
	:return:
	"""
	api_key, api_secret = get_local_credentials()
	client = binance.Client(api_key=api_key, api_secret=api_secret)

	# Get the current datetime for the data download, so that the ending bar
	# date is consistent for all datasets
	time_now = get_time_millisecond(datetime.utcnow())

	# Retrieve fixed column names for the data, as well as columns to remove and
	# rename
	columns = [
		'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
		'qav', 'num_trades', 'taker_based_vol', 'taker_quote_vol', 'ignore'
	]
	columns_to_drop = [
		'close_time', 'volume', 'num_trades', 'taker_based_vol', 'taker_quote_vol',
		'ignore'
	]
	columns_to_rename = {
		'open_time': 'date',
		'qav': 'volume'
	}

	# all_data = {}
	for symbol in symbol_list:
		if verbose:
			print(f"Downloading data for {symbol}...")
		start_timestamp = None
		# If get_earliest date: get the earliest available timestamp for each symbol
		if get_earliest_date:
			start_timestamp = client._get_earliest_valid_timestamp(
				symbol=symbol, interval='1m'
			)
		elif not get_earliest_date:
			if not requested_date:
				requested_date = datetime(2024, 2, 14, 0, 0, 0, 0, tzinfo=timezone.utc)
			start_timestamp = get_time_millisecond(requested_date)

		# All data for the main timeframe
		# bars_data_main_timeframe = client.get_historical_klines(
		# 	symbol=symbol,
		# 	interval=main_timeframe,
		# 	start_str=start_timestamp,
		# 	end_str=time_now
		# )
		# dataframe_main_timeframe = pd.DataFrame(bars_data_main_timeframe)
		# dataframe_main_timeframe.columns = columns
		# dataframe_main_timeframe.drop(columns_to_drop, axis=1, inplace=True)
		# dataframe_main_timeframe.rename(columns=columns_to_rename, inplace=True)
		# dataframe_main_timeframe['date'] = pd.to_datetime(
		# 	dataframe_main_timeframe['date'],
		# 	unit='ms', utc=True)

		# All data for the detailed timeframe
		bars_data_detailed_timeframe = client.get_historical_klines(
			symbol=symbol,
			interval=detailed_timeframe,
			start_str=start_timestamp,
			end_str=time_now,
			limit=800
		)
		dataframe_detailed_timeframe = pd.DataFrame(bars_data_detailed_timeframe)
		dataframe_detailed_timeframe.columns = columns
		dataframe_detailed_timeframe.drop(columns_to_drop, axis=1, inplace=True)
		dataframe_detailed_timeframe.rename(columns=columns_to_rename, inplace=True)
		dataframe_detailed_timeframe['date'] = pd.to_datetime(
			dataframe_detailed_timeframe['date'],
			unit='ms', utc=True)

		if verbose:
			print(f"Saving data for {symbol}...")
		dataframe_detailed_timeframe.to_csv(f'{save_to_path}/{symbol}_{detailed_timeframe}.csv')

		# Append data to the all_data dict
		# all_data[f'{symbol}_{main_timeframe}'] = dataframe_main_timeframe
		# all_data[f'{symbol}_{detailed_timeframe}'] = dataframe_detailed_timeframe

	if verbose:
		print("Finished downloading and saving all data.")

	# return all_data


# def save_data(symbol_list: list, all_data: dict, main_timeframe: str='1h',
# 				  detailed_timeframe: str='1m', save_to_path: str='../datasets',
# 			  verbose: bool=True) -> None:
# 	for symbol in symbol_list:
# 		if verbose:
# 			print(f"Saving data for {symbol}")
# 		all_data[f'{symbol}_{main_timeframe}'].to_csv(f'{save_to_path}/{symbol}_{main_timeframe}.csv')
# 		all_data[f'{symbol}_{detailed_timeframe}'].to_csv(f'{save_to_path}/{symbol}_{detailed_timeframe}.csv')
# 	if verbose:
# 		print(f"Finished saving all data.")

def parse_timeframe(timeframe):
	"""
	Helper function copied from Freqtrade, to parse a human-readable timeframe
	to machine-digestible numbers. The base scale is a second.

	:param timeframe:
		The input timeframe.
	:return:
	"""
	amount = int(timeframe[:-1])
	unit = timeframe[-1]
	if 'y' == unit:
		scale = 60 * 60 * 24 * 365
	elif 'M' == unit:
		scale = 60 * 60 * 24 * 30
	elif 'w' == unit:
		scale = 60 * 60 * 24 * 7
	elif 'd' == unit:
		scale = 60 * 60 * 24
	elif 'h' == unit:
		scale = 60 * 60
	elif 'm' == unit:
		scale = 60
	elif 's' == unit:
		scale = 1
	else:
		raise Exception(f'Timeframe unit "{unit}" is not supported.')
	return amount * scale


def timeframe_to_seconds(timeframe: str) -> int:
	"""
	Translates the timeframe interval value written in the human readable
	form ('1m', '5m', '1h', '1d', '1w', etc.) to the number
	of seconds for one timeframe interval.
	"""
	return parse_timeframe(timeframe)


def timeframe_to_minutes(timeframe: str) -> int:
	"""
	Same as timeframe_to_seconds, but returns minutes.
	"""
	return parse_timeframe(timeframe) // 60


def merge_dataframe(main_dataframe: pd.DataFrame, detailed_dataframe: pd.DataFrame,
					main_timeframe: str, detailed_timeframe: str, ffill: bool=True,
					date_column: str='date') -> pd.DataFrame:
	"""
	Merge the dataframe of the main timeframe with the dataframe of the detailed
	timeframe, in a way that avoids lookahead bias.

	:param main_dataframe:
	:param detailed_dataframe:
	:return:
	"""

	# Convert the main and detailed timeframes to minutes
	minutes_main_tf = timeframe_to_minutes(main_timeframe)
	minutes_detailed_tf = timeframe_to_minutes(detailed_timeframe)

	if minutes_main_tf == minutes_detailed_tf:
		# No need to forwardshift if the timeframes are identical
		main_dataframe['date_merge'] = main_dataframe[date_column]
