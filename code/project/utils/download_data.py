import utils


if __name__ == '__main__':
	pairs = [
		"BTCUSDT",
		"ETHUSDT",
		"BNBUSDT",
		"SOLUSDT",
		"XRPUSDT",
		"ADAUSDT",
		"AVAXUSDT",
		"TRXUSDT",
		"LINKUSDT",
		"MATICUSDT"
	]
	utils.download_data(symbol_list=pairs, get_earliest_date=True)
