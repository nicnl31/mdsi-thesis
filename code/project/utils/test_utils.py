import utils
import binance
import pandas as pd
from binance.client import Client
from typing import Tuple
import time
from datetime import datetime


def test_download_data(symbol_list: list=['BTCUSDT'], main_timeframe='1h', detailed_timeframe='1m'):
	data = utils.download_data(symbol_list=symbol_list, get_earliest_date=False,
							   main_timeframe=main_timeframe,
							   detailed_timeframe=detailed_timeframe)
	print(data[f'{symbol_list[0]}_{main_timeframe}'].head())
	print(data[f'{symbol_list[0]}_{detailed_timeframe}'].head())


def test_save_data(symbol_list: list=['BTCUSDT'], main_timeframe='1h', detailed_timeframe='1m'):
	data = utils.download_data(symbol_list=symbol_list, get_earliest_date=False,
							   main_timeframe=main_timeframe,
							   detailed_timeframe=detailed_timeframe)
	utils.save_data(symbol_list=symbol_list, all_data=data)


def test_parse_timeframe():
	hour = utils.parse_timeframe('3h')
	print(hour)
	error = utils.parse_timeframe('1g')


test_save_data()
