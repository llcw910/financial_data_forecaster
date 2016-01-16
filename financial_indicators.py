from pprint import pprint
import json
import sched, time
import datetime
import sys, traceback
import os
import time
'''
Function list:
	cal_EMA(closing_today, window_length, EMA_yesterday)
	cal_RSI(rs)
	load_data(f_15mins, f_daily)

	init_daily(stock, data_daily)
	ibd_daily(stock, date, insert_name, insert_value)
	exist_daily(stock, date, value)
	gen_EMA_daily(stock)
	gen_RSI_daily(stock)
	gen_MACD(stock, long_EMA, short_EMA, signal_EMA)
	gen_KD(stock, period, d_period)
	gen_change(stock)
'''
#init
EMA_array = [12, 14, 26] #for EMA
EMA_ud_array = [14] #for RSI
file_15mins_name = "stock_data.json"
file_daily_name = "daily_stock_data.json"

#22 attr + epoch
attr_15mins = ['date', 'price', 'change', 'volume', 'open', 'avg_daily_volume', 'stock_exchange', 'market_cap', 'book_value', 'ebitda', 'dividend_share', 'dividend_yield', 'earnings_share', 'days_high', 'days_low', '50day_moving_avg', '200day_moving_avg', 'price_earnings_ratio', 'price_earnings_growth_ratio', 'price_sales', 'price_book', 'short_ratio']
attr_daily = ['adj_close', 'close', 'date', 'high', 'low', 'open', 'symbol', 'volume']
indicators = ['date', 'ema', 'upday_ema', 'downday_ema', 'rs', 'rsi']

#HSI-Commerce & Industry x 23
stock_CI = ['0001.HK', '0019.HK', '0027.HK', '0066.HK', '0135.HK', '0144.HK', '0151.HK', '0267.HK', '0291.HK', '0293.HK', '0322.HK', '0386.HK', '0494.HK', '0700.HK', '0762.HK', '0857.HK', '0883.HK', '0941.HK', '0992.HK', '1044.HK', '1088.HK', '1880.HK', '1928.HK', '2319.HK']
#HSI-Utilities x 4
stock_U = ['0002.HK', '0003.HK', '0006.HK', '0836.HK']
#HSI-Properties x 10
stock_P = ['0004.HK', '0012.HK', '0016.HK', '0017.HK', '0083.HK', '0101.HK', '0688.HK', '0823.HK', '1109.HK', '1113.HK']
#HSI-Finance x 12
stock_F = ['0005.HK', '0011.HK', '0023.HK', '0388.HK', '0939.HK', '1299.HK', '1398.HK', '2318.HK', '2388.HK', '2628.HK', '3328.HK', '3988.HK']

#Global Variables
data_15mins = {}
data_daily = {}

output_daily = {}

def cal_EMA(closing_today, window_length, EMA_yesterday):
	k = 2/float((window_length + 1))
	EMA = closing_today * k + EMA_yesterday * (1 - k)
	return EMA;

def cal_RSI(rs):
	RSI = (100 - (100 / (1 + rs)))
	return RSI;

def load_data(f_15mins, f_daily):
	global data_15mins, data_daily
	with open(f_15mins, 'r') as fp:
		if os.stat(f_15mins).st_size == 0:
			print("Empty file: " + f_15mins)
		else:
			data_15mins = json.load(fp)
			print("================================= 15 mins data loaded =================================")

	with open(f_daily, 'r') as fp:
		if os.stat(f_daily).st_size == 0:
			print("Empty file: " + f_daily)
		else:
			data_daily = json.load(fp)
			print("================================== daily data loaded ==================================")

def init_daily(stock, data_daily):
	output_daily[stock] = []
	for data in data_daily[stock]:
		#fetch useful data
		temp = {}
		temp["date"] = data["date"]
		temp["high"] = data["high"]
		temp["low"] = data["low"]
		temp["close"] = data["close"]
		output_daily[stock].append(temp)

def ibd_daily(stock, date, insert_name, insert_value):
	for data in output_daily[stock]:
		if data['date'] == date:
			data[insert_name] = insert_value

def exist_daily(stock, date, value):
	for data in output_daily[stock]:
		if data['date'] == date:
			if value not in data:
				return -1
			else:
				return data[value]

def gen_EMA_daily(stock):
	count = 0
	temp = 0 #sum of close
	gain = 0 #average gain
	lost = 0 #average lost
	temp_data = {}
	SMA_hash = {}
	UD_hash = {}
	for size in EMA_array:
		SMA_hash[size] = -1
	for number in EMA_ud_array:
		UD_hash[str(number) + 'up'] = -1
		UD_hash[str(number) + 'down'] = -1
	for data in output_daily[stock]:
		count += 1
		#adding EMA
		temp += float(data['close'])
		#adding UP & DOWN
		if 'date' in temp_data:
			if float(data['close']) > float(temp_data['close']):
				gain += float(data['close']) - float(temp_data['close'])
			elif float(temp_data['close']) > float(data['close']):
				lost += float(temp_data['close']) - float(data['close'])
		#insert SMA
		for size in EMA_array:
			if SMA_hash[size] == -1 and size == count:
				SMA_hash[size] = temp
				ibd_daily(stock, data['date'], 'SMA_' + str(size), temp/size)
		#insert first UP & DOWN
		for number in EMA_ud_array:
			if UD_hash[str(number) + 'up'] == -1 and UD_hash[str(number) + 'down'] == -1 and number+1 == count:
				UD_hash[str(number) + 'up'] = gain/number
				UD_hash[str(number) + 'down'] = lost/number
				ibd_daily(stock, data['date'], 'UP_' + str(number), gain/number)
				ibd_daily(stock, data['date'], 'DOWN_' + str(number), lost/number)

		if 'date' in temp_data:
			#cal EMA
			for size in EMA_array:
				SMA_temp = exist_daily(stock, temp_data['date'], 'SMA_' + str(size))
				EMA_temp = exist_daily(stock, temp_data['date'], 'EMA_' + str(size))
				if SMA_temp != -1:
					ibd_daily(stock, data['date'], 'EMA_' + str(size), cal_EMA(float(data['close']), size, SMA_temp))
				if EMA_temp != -1:
					ibd_daily(stock, data['date'], 'EMA_' + str(size), cal_EMA(float(data['close']), size, EMA_temp))
			#cal UP & DOWN
			for number in EMA_ud_array:
				UP_temp = exist_daily(stock, temp_data['date'], 'UP_' + str(number))
				DOWN_temp = exist_daily(stock, temp_data['date'], 'DOWN_' + str(number))
				if UP_temp != -1:
					if float(data['close']) > float(temp_data['close']):
						ibd_daily(stock, data['date'], 'UP_' + str(number), ((UP_temp*(number - 1) + (float(data['close']) - float(temp_data['close'])))/number))
					else:
						ibd_daily(stock, data['date'], 'UP_' + str(number), ((UP_temp*(number - 1))/number))
				if DOWN_temp != -1:
					if float(temp_data['close']) > float(data['close']):
						ibd_daily(stock, data['date'], 'DOWN_' + str(number), ((DOWN_temp*(number - 1) + (float(temp_data['close']) - float(data['close'])))/number))
					else:
						ibd_daily(stock, data['date'], 'DOWN_' + str(number), ((DOWN_temp*(number - 1))/number))
		
		temp_data = data
	print("================================= EMA daily generated =================================")

def gen_RSI_daily(stock):
	count = 0
	for data in output_daily[stock]:
		count += 1
		for number in EMA_ud_array:
			UP_temp = exist_daily(stock, data['date'], 'UP_' + str(number))
			DOWN_temp = exist_daily(stock, data['date'], 'DOWN_' + str(number))
			if UP_temp != -1 and DOWN_temp != -1:
				ibd_daily(stock, data['date'], 'RSI_' + str(number), cal_RSI((UP_temp/DOWN_temp)))
	
	print("================================= RSI daily generated =================================")

def gen_MACD(stock, long_EMA, short_EMA, signal_EMA):
	count = 0
	temp = 0 #sum of MACD
	temp_data = {}
	for data in output_daily[stock]:
		LONG_temp = exist_daily(stock, data['date'], 'EMA_' + str(long_EMA))
		SHORT_temp = exist_daily(stock, data['date'], 'EMA_' + str(short_EMA))
		#MACD Line
		if LONG_temp != -1 and SHORT_temp != -1:
			count += 1
			ibd_daily(stock, data['date'], 'MACD_' + str(long_EMA) + '_' + str(short_EMA), SHORT_temp - LONG_temp)
			temp += (SHORT_temp - LONG_temp)
			#Signal Line + Histogram
			if signal_EMA == count:
				ibd_daily(stock, data['date'], 'MACD_signal_' + str(long_EMA) + '_' + str(short_EMA) + '_' + str(signal_EMA), temp/signal_EMA)
				ibd_daily(stock, data['date'], 'MACD_histogram_' + str(long_EMA) + '_' + str(short_EMA) + '_' + str(signal_EMA), data['MACD_' + str(long_EMA) + '_' + str(short_EMA)] - data['MACD_signal_' + str(long_EMA) + '_' + str(short_EMA) + '_' + str(signal_EMA)])
			elif signal_EMA < count:
				ibd_daily(stock, data['date'], 'MACD_signal_' + str(long_EMA) + '_' + str(short_EMA) + '_' + str(signal_EMA), cal_EMA(data['MACD_' + str(long_EMA) + '_' + str(short_EMA)], signal_EMA, temp_data['MACD_signal_' + str(long_EMA) + '_' + str(short_EMA) + '_' + str(signal_EMA)]))
				ibd_daily(stock, data['date'], 'MACD_histogram_' + str(long_EMA) + '_' + str(short_EMA) + '_' + str(signal_EMA), data['MACD_' + str(long_EMA) + '_' + str(short_EMA)] - data['MACD_signal_' + str(long_EMA) + '_' + str(short_EMA) + '_' + str(signal_EMA)])
		
		temp_data = data

	print("================================= MACD daily generated ================================")

def gen_KD(stock, period, d_period):
	count = 0
	temp = 0 #for D
	ptr = 1 #hash pointer for K period
	D_ptr = 1 #hash pointer for D period
	temp_hash = {}
	D_hash = {}
	for data in output_daily[stock]:
		count += 1
		temp_hash[str(ptr) + 'high'] = data['high']
		temp_hash[str(ptr) + 'low'] = data['low']
		if ptr < period:
			ptr += 1
		elif ptr >= period:
			ptr = 1
		if period <= count:
			temp_high = -1
			temp_low = -1
			for x in range(1, period + 1):
				if temp_high == -1:
					temp_high = float(temp_hash[str(x) + 'high'])
				elif float(temp_hash[str(x) + 'high']) > temp_high:
					temp_high = float(temp_hash[str(x) + 'high'])
				if temp_low == -1:
					temp_low = float(temp_hash[str(x) + 'low'])
				elif float(temp_hash[str(x) + 'low']) < temp_low:
					temp_low = float(temp_hash[str(x) + 'low'])
			ibd_daily(stock, data['date'], 'K_' + str(period), (float(data['close']) - temp_low)/(temp_high - temp_low)*100)
			D_hash[str(D_ptr)] = (float(data['close']) - temp_low)/(temp_high - temp_low)*100
			if period + d_period - 1 <= count:
				for x in range(1, d_period + 1):
					temp += D_hash[str(x)]
				ibd_daily(stock, data['date'], 'D_' + str(period), temp/d_period)
				temp = 0
			if D_ptr < d_period:
				D_ptr += 1
			elif D_ptr >= d_period:
				D_ptr = 1
	print("================================== KD daily generated =================================")

def gen_change(stock):
	count = 0
	temp_close = 0
	for data in output_daily[stock]:
		count += 1
		ibd_daily(stock, data['date'], 'change', float(data['close'])-temp_close)
		temp_close = float(data['close'])
	print("=================================== Change generated ==================================")

#================================== body ==================================
load_data(file_15mins_name, file_daily_name)

init_daily('00001', data_daily)
print("***************************************************************************************\n*                                PROCESSING DAILY DATA                                *\n***************************************************************************************")
gen_EMA_daily('00001')
gen_RSI_daily('00001')
gen_MACD('00001', 26, 12, 9)
gen_KD('00001', 14, 3)
gen_change('00001')

'''

MFI money flow index
BB bollinger bands


for stock in stock_CI:
	init_daily(stock, data_daily)
	gen_EMA_daily(stock)
	gen_RSI_daily(stock)
for stock in stock_U:
	init_daily(stock, data_daily)
for stock in stock_P:
	init_daily(stock, data_daily)
for stock in stock_F:
	init_daily(stock, data_daily)
'''

with open('hi.json', 'w') as fp:
	json.dump(output_daily, fp, indent=4)
