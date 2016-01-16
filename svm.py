from pprint import pprint
from sklearn import svm
from sklearn import datasets
import numpy as np
#import matplotlib.pyplot as plt
import json
import sched, time
import datetime
import sys, traceback
import os
import time
'''
Function list:
	load_data(f_indicators)
'''
#init
file_indicators = "hi.json"
train = 2000
test = 1000

#22 attr + epoch
attr_15mins = ['date', 'price', 'change', 'volume', 'open', 'avg_daily_volume', 'stock_exchange', 'market_cap', 'book_value', 'ebitda', 'dividend_share', 'dividend_yield', 'earnings_share', 'days_high', 'days_low', '50day_moving_avg', '200day_moving_avg', 'price_earnings_ratio', 'price_earnings_growth_ratio', 'price_sales', 'price_book', 'short_ratio']
attr_daily = ['adj_close', 'close', 'date', 'high', 'low', 'open', 'symbol', 'volume']
indicators = ['RSI_14', 'MACD_signal_26_12_9', 'MACD_histogram_26_12_9', 'K_14', 'D_14', 'EMA_14']

#HSI-Commerce & Industry x 23
stock_CI = ['0001.HK', '0019.HK', '0027.HK', '0066.HK', '0135.HK', '0144.HK', '0151.HK', '0267.HK', '0291.HK', '0293.HK', '0322.HK', '0386.HK', '0494.HK', '0700.HK', '0762.HK', '0857.HK', '0883.HK', '0941.HK', '0992.HK', '1044.HK', '1088.HK', '1880.HK', '1928.HK', '2319.HK']
#HSI-Utilities x 4
stock_U = ['0002.HK', '0003.HK', '0006.HK', '0836.HK']
#HSI-Properties x 10
stock_P = ['0004.HK', '0012.HK', '0016.HK', '0017.HK', '0083.HK', '0101.HK', '0688.HK', '0823.HK', '1109.HK', '1113.HK']
#HSI-Finance x 12
stock_F = ['0005.HK', '0011.HK', '0023.HK', '0388.HK', '0939.HK', '1299.HK', '1398.HK', '2318.HK', '2388.HK', '2628.HK', '3328.HK', '3988.HK']

#Global Variables
data_indicators = {}

output_daily = {}


def load_data(f_indicators):
	global data_indicators
	with open(f_indicators, 'r') as fp:
		if os.stat(f_indicators).st_size == 0:
			print("Empty file: " + f_indicators)
		else:
			data_indicators = json.load(fp)
			print("================================== Indicators loaded ==================================")

def my_kernel(X, Y):
	return np.dot(X, Y.T)

#================================== body ==================================

load_data(file_indicators)


X = []
y = []
y_change = []

test_X = []
test_y = []
test_change = []
count = 0

for data in data_indicators['00001']:
	temp = []
	check = 1
	for attr in indicators:
		if attr in data:
			temp.append(data[attr])
		else:
			check = 0
	if check == 1:
		if count < train:
			count += 1
			X.append(temp)
			y.append(int(float(data["close"])))
			if data["change"] > 0:
				y_change.append(1)
			else:
				y_change.append(0)
		else:
			count += 1
			test_X.append(temp)
			test_y.append(int(float(data["close"])))
			if data["change"] > 0:
				test_change.append(1)
			else:
				test_change.append(0)

clf = svm.SVC()
clf.fit(X, y_change)
result = clf.predict(test_X)


count = 0
correct = 0
for data in result:
	if data == test_change[count]:
		correct += 1
	count += 1

print(str(correct) + " correct out of " + str(count) + ": " + str(correct/count))


clf.fit(X, y)
result = clf.predict(test_X)

count = 0
correct = 0
for data in result:
	if data == test_y[count]:
		correct += 1
	count += 1

print(str(correct) + " correct out of " + str(count) + ": " + str(correct/count))



