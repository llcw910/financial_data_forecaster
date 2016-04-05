from pprint import pprint
from qpython import qconnection
from qpython.qtemporal import qtemporal, from_raw_qtemporal, array_from_raw_qtemporal
from qpython.qtype import *
import qpython.qtype
import numpy
import sched, time
import datetime
import sys, traceback
import os
import time

#init
database_name = "daily_data"

#HSI-Commerce & Industry x 24
stock_CI = ['0001.HK', '0019.HK', '0027.HK', '0066.HK', '0135.HK', '0144.HK', '0151.HK', '0267.HK', '0291.HK', '0293.HK', '0322.HK', '0386.HK', '0494.HK', '0700.HK', '0762.HK', '0857.HK', '0883.HK', '0941.HK', '0992.HK', '1044.HK', '1088.HK', '1880.HK', '1928.HK', '2319.HK']
#HSI-Utilities x 4
stock_U = ['0002.HK', '0003.HK', '0006.HK', '0836.HK']
#HSI-Properties x 10
stock_P = ['0004.HK', '0012.HK', '0016.HK', '0017.HK', '0083.HK', '0101.HK', '0688.HK', '0823.HK', '1109.HK', '1113.HK']
#HSI-Finance x 12
stock_F = ['0005.HK', '0011.HK', '0023.HK', '0388.HK', '0939.HK', '1299.HK', '1398.HK', '2318.HK', '2388.HK', '2628.HK', '3328.HK', '3988.HK']
#Indicators Databse
indicators_db = ['resource', 'volume', 'trending', 'momentum']

def connect_db():
	q = qconnection.QConnection(host='localhost', port=5000, pandas = True)
	q.open()
	print("================================= Connection Details ==================================")
	print(q)
	print('IPC version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))
	return q;

def fetch_daily(stock_id, q):
	print("================================ Fetch Data '" + str(stock_id) + "' =================================")
	query = 's)select close, high, low, date, volume from daily_data where stock_id = \'' + str(stock_id) + '\' and volume <> 0'
	df = q(query)
	return df;

def clear_kdb(db_name, q):
	query = 'delete ' + str(db_name) + ' from`.'
	df = q(query)

def create_kdb(db_name, q):
	if db_name == 'volume':
		query = 'volume:([]stock_id:`symbol$(); date:`date$(); CMF:`float$(); ADL:`float$(); EMV:`float$(); MFI:`float$(); OBV:`float$(); PVO:`float$())'
	elif db_name == 'trending':
		query = 'trending:([]stock_id:`symbol$(); date:`date$(); AROON:`float$(); CCI:`float$(); FORCE:`float$(); VTXP:`float$(); VTXN:`float$())'
	elif db_name == 'momentum':
		query = 'momentum:([]stock_id:`symbol$(); date:`date$(); ADX:`float$(); COPPOCK:`float$(); CHAIKIN:`float$())'
	elif db_name == 'resource':
		query = 'resource:([]stock_id:`symbol$(); date:`date$(); EMA_12:`float$(); EMA_14:`float$())'
	df = q(query)

def init_kdb(stock_id, source, db_name, q):
	if db_name =='volume':
		for data in source:
			query = '`volume insert(`' + str(stock_id) + '; ' + str(from_raw_qtemporal(data[3], qtype=QDATE)).replace('-','.') + '; 0n; 0n; 0n; 0n; 0n; 0n)'
			df = q(query)
	elif db_name == 'trending':
		for data in source:
			query = '`trending insert(`' + str(stock_id) + '; ' + str(from_raw_qtemporal(data[3], qtype=QDATE)).replace('-','.') + '; 0n; 0n; 0n; 0n; 0n)'
			df = q(query)
	elif db_name == 'momentum':
		for data in source:
			query = '`momentum insert(`' + str(stock_id) + '; ' + str(from_raw_qtemporal(data[3], qtype=QDATE)).replace('-','.') + '; 0n; 0n; 0n)'
			df = q(query)
	elif db_name == 'resource':
		for data in source:
			query = '`resource insert(`' + str(stock_id) + '; ' + str(from_raw_qtemporal(data[3], qtype=QDATE)).replace('-','.') + '; 0n; 0n)'
			df = q(query)

def cal_EMA(closing_today, window_length, EMA_yesterday):
	k = 2/float((window_length + 1))
	EMA = closing_today * k + EMA_yesterday * (1 - k)
	return EMA;

def gen_EMA(stock_id, period, source, q, column_name, table):
	count = 0
	temp = 0
	for data in source:
		if count == period-1:
			temp += data[0]
			sma = temp/period
			temp = sma
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(sma)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		elif count < period:
			temp += data[0]
		else:
			ema = cal_EMA(data[0], period, temp)
			temp = ema
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(ema)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		count += 1

	print("============================== EMA generated '" + str(stock_id) + "' ================================")

# Volume-based Indicators [6]
def gen_CMF(stock_id, period, source, q, column_name, table):
	count = 0
	temp_mfvol = 0
	temp_vol = 0
	temp = {}
	temp['ptr'] = 0
	for data in source:
		if (data[1]-data[2] == 0):
			mul = 0
		else:
			mul = ((data[0]-data[2])-(data[1]-data[0]))/(data[1]-data[2])
		vol = mul*data[4]
		if count == period-1:
			temp_mfvol += vol
			temp_vol += data[4]
			temp['mfvol_' + str(count)] = vol
			temp['vol_' + str(count)] = data[4]
			cmf = temp_mfvol/temp_vol
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(cmf)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		elif count < period:
			temp_mfvol += vol
			temp_vol += data[4]
			temp['mfvol_' + str(count)] = vol
			temp['vol_' + str(count)] = data[4]
		else:
			temp_mfvol = temp_mfvol + vol - temp['mfvol_' + str(temp['ptr'])]
			temp_vol = temp_vol + data[4] - temp['vol_' + str(temp['ptr'])]
			temp['mfvol_' + str(count%period)] = vol
			temp['vol_' + str(count%period)] = data[4]
			temp['ptr'] = (temp['ptr'] + 1)%period
			cmf = temp_mfvol/temp_vol
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(cmf)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		count += 1

	print("============================== CMF generated '" + str(stock_id) + "' ================================")

def gen_ADL(stock_id, source, q, column_name, table):
	count = 0
	adl = 0
	for data in source:
		if (data[1]-data[2] == 0):
			mul = 0
		else:
			mul = ((data[0]-data[2])-(data[1]-data[0]))/(data[1]-data[2])
		vol = mul*data[4]
		adl += vol
		query = str(table) + ':update ' + str(column_name) + ':' + str(float(adl)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
		df = q(query)
		
		count += 1

	print("============================== ADL generated '" + str(stock_id) + "' ================================")

def gen_EMV(stock_id, period, source, q, column_name, table):
	count = 0
	temp_high = 0
	temp_low = 0
	temp_emv = 0
	temp = {}
	temp['ptr'] = 0
	for data in source:
		if count == 0:
			temp_high = data[1]
			temp_low = data[2]
		elif count == period:
			if data[1]-data[2] == 0:
				emv = 0
			else:
				emv = ((data[1]+data[2])/2-(temp_high+temp_low)/2)/((data[4]/100000000)/(data[1]-data[2]))
			temp_emv += emv
			temp[str(count-1)] = emv
			temp_high = data[1]
			temp_low = data[2]
			emv_period = temp_emv/period
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(emv_period)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		elif count < period:
			if data[1]-data[2] == 0:
				emv = 0
			else:
				emv = ((data[1]+data[2])/2-(temp_high+temp_low)/2)/((data[4]/100000000)/(data[1]-data[2]))
			temp_emv += emv
			temp[str(count-1)] = emv
			temp_high = data[1]
			temp_low = data[2]
		else:
			if data[1]-data[2] == 0:
				emv = 0
			else:
				emv = ((data[1]+data[2])/2-(temp_high+temp_low)/2)/((data[4]/100000000)/(data[1]-data[2]))
			temp_emv = temp_emv + emv - temp[str(temp['ptr'])]
			temp['ptr'] = (temp['ptr'] + 1)%period
			temp[str((count-1)%period)] = emv
			temp_high = data[1]
			temp_low = data[2]
			emv_period = temp_emv/period
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(emv_period)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		
		count += 1

	print("============================== EMV generated '" + str(stock_id) + "' ================================")

def gen_MFI(stock_id, period, source, q, column_name, table):
	count = 0
	up = 0
	down = 0
	temp_up = {}
	temp_down = {}
	temp = {}
	temp['ptr'] = 0
	for data in source:
		tp = (data[0]+data[1]+data[2])/3
		rmf = tp*data[4]
		if count == 0:
			temp_tp = tp
		elif count == period:
			if tp > temp_tp:
				up += rmf
				temp_up[str(count-1)] = rmf
				temp_down[str(count-1)] = 0
				temp_tp = tp
			else:
				down += rmf
				temp_up[str(count-1)] = 0
				temp_down[str(count-1)] = rmf
				temp_tp = tp
			mfi = 100-100/(1+up/down)
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(mfi)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		elif count < period:
			if tp > temp_tp:
				up += rmf
				temp_up[str(count-1)] = rmf
				temp_down[str(count-1)] = 0
				temp_tp = tp
			else:
				down += rmf
				temp_up[str(count-1)] = 0
				temp_down[str(count-1)] = rmf
				temp_tp = tp
		else:
			if tp > temp_tp:
				up = up + rmf - temp_up[str(temp['ptr'])]
				down = down - temp_down[str(temp['ptr'])]
				temp_up[str((count-1)%period)] = rmf
				temp_down[str((count-1)%period)] = 0
				temp_tp = tp
			else:
				up = up - temp_up[str(temp['ptr'])]
				down = down + rmf - temp_down[str(temp['ptr'])]
				temp_up[str((count-1)%period)] = 0
				temp_down[str((count-1)%period)] = rmf
				temp_tp = tp
			temp['ptr'] = (temp['ptr'] + 1)%period
			mfi = 100-100/(1+up/down)
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(mfi)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)

		count += 1

	print("============================== MFI generated '" + str(stock_id) + "' ================================")

def gen_OBV(stock_id, source, q, column_name, table):
	count = 0
	temp_close = 0
	obv = 0
	for data in source:
		if count == 0:
			temp_close = data[0]
		else:
			if data[0] > temp_close:
				obv += data[4]
				temp_close = data[0]
			elif data[0] < temp_close:
				obv -= data[4]
				temp_close = data[0]
			else:
				temp_close = data[0]
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(obv)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		
		count += 1

	print("============================== OBV generated '" + str(stock_id) + "' ================================")

def gen_PVO(stock_id, period_1, period_2, source, q, column_name, table):
	count = 0
	temp_1 = 0
	temp_2 = 0
	for data in source:
		if count == period_1-1:
			temp_1 += data[4]
			sma_1 = temp_1/period_1
			temp_1 = sma_1
		elif count < period_1:
			temp_1 += data[4]
		else:
			ema_1 = cal_EMA(data[4], period_1, temp_1)
			temp_1 = ema_1

		if count == period_2-1:
			temp_2 += data[4]
			sma_2 = temp_2/period_2
			temp_2 = sma_2
			pvo = ((ema_1-sma_2)/sma_2)*100
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(pvo)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		elif count < period_2:
			temp_2 += data[4]
		else:
			ema_2 = cal_EMA(data[4], period_2, temp_2)
			temp_2 = ema_2
			pvo = ((ema_1-ema_2)/ema_2)*100
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(pvo)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		count += 1

	print("============================== PVO generated '" + str(stock_id) + "' ================================")

# Trending Indicators [4]
def gen_AROON(stock_id, period, source, q, column_name, table):
	count = 0
	temp = {}
	temp['ptr'] = 0
	temp['up'] = 0
	temp['down'] = 9999
	for data in source:
		if count < period:
			temp['date_' + str(temp['ptr'])] = count
			temp['close_' + str(temp['ptr'])] = data[0]
			for num in range(0,count+1):
				if temp['close_' + str(num)] > temp['up']:
					temp['up'] = temp['close_' + str(num)]
					temp['up_date'] = temp['date_' + str(num)]
				if temp['close_' + str(num)] < temp['down']:
					temp['down'] = temp['close_' + str(num)]
					temp['down_date'] = temp['date_' + str(num)]
			temp['ptr'] = (temp['ptr'] + 1)%period
			a_up = 100*(period-(count-temp['up_date']))/period
			a_down = 100*(period-(count-temp['down_date']))/period
			aroon = a_up-a_down
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(aroon)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		else:
			temp['up'] = 0
			temp['down'] = 9999
			temp['date_' + str(temp['ptr'])] = count
			temp['close_' + str(temp['ptr'])] = data[0]
			for num in range(0,period):
				if temp['close_' + str(num)] > temp['up']:
					temp['up'] = temp['close_' + str(num)]
					temp['up_date'] = temp['date_' + str(num)]
				if temp['close_' + str(num)] < temp['down']:
					temp['down'] = temp['close_' + str(num)]
					temp['down_date'] = temp['date_' + str(num)]
			temp['ptr'] = (temp['ptr'] + 1)%period
			a_up = 100*(period-(count-temp['up_date']))/period
			a_down = 100*(period-(count-temp['down_date']))/period
			aroon = a_up-a_down
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(aroon)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		count += 1

	print("============================= AROON generated '" + str(stock_id) + "' ===============================")

def gen_CCI(stock_id, period, source, q, column_name, table):
	count = 0
	temp = {}
	temp['ptr'] = 0
	temp_sum = 0
	for data in source:
		tp = (data[0]+data[1]+data[2])/3
		md = 0
		if count == period-1:
			temp[str(count)] = tp
			temp_sum += tp
			sma = temp_sum/period
			for num in range(0, period):
				md += abs(sma-temp[str(num)])
			md = md/period
			cci = (tp-sma)/(0.015*md)
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(cci)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		elif count < period:
			temp[str(count)] = tp
			temp_sum += tp
		else:
			temp_sum = temp_sum + tp - temp[str(temp['ptr'])]
			temp[str(count%period)] = tp
			sma = temp_sum/period
			for num in range(0, period):
				md += abs(sma-temp[str(num)])
			md = md/period
			cci = (tp-sma)/(0.015*md)
			temp['ptr'] = (temp['ptr']+1)%period
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(cci)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		count += 1

	print("============================== CCI generated '" + str(stock_id) + "' ================================")

def gen_FORCE(stock_id, period, source, q, column_name, table):
	count = 0
	temp = 0
	temp_close = 0
	for data in source:
		if count == 0:
			temp_close = data[0]
		elif count == period:
			fi = (data[0] - temp_close)*data[4]
			temp += fi
			sma = temp/period
			temp = sma
			temp_close = data[0]
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(sma)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		elif count < period:
			fi = (data[0] - temp_close)*data[4]
			temp += fi
			temp_close = data[0]
		else:
			fi = (data[0] - temp_close)*data[4]
			ema = cal_EMA(fi, period, temp)
			temp = ema
			temp_close = data[0]
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(ema)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		count += 1

	print("============================= FORCE generated '" + str(stock_id) + "' ===============================")

def gen_VTX(stock_id, period, source, q, column_name, table):
	count = 0
	temp = {}
	temp['ptr'] = 0
	temp['high'] = 0
	temp['low'] = 0
	pvm_sum = 0
	nvm_sum = 0
	tr_sum = 0
	for data in source:
		if count == 0:
			temp['close'] = data[0]
			temp['high'] = data[1]
			temp['low'] = data[2]
		elif count == period:
			temp['pvm_' + str(count-1)] = abs(data[1]-temp['low'])
			temp['nvm_' + str(count-1)] = abs(data[2]-temp['high'])
			pvm_sum += abs(data[1]-temp['low'])
			nvm_sum += abs(data[2]-temp['high'])
			tr = max(data[1]-data[2], abs(data[1]-temp['close']), abs(data[2]-temp['close']))
			temp['tr_' + str(count-1)] = tr
			tr_sum += tr
			pvi = pvm_sum/tr_sum
			nvi = nvm_sum/tr_sum
			query = str(table) + ':update VTXP:' + str(float(pvi)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
			query = str(table) + ':update VTXN:' + str(float(nvi)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
			temp['close'] = data[0]
			temp['high'] = data[1]
			temp['low'] = data[2]
		elif count < period:
			temp['pvm_' + str(count-1)] = abs(data[1]-temp['low'])
			temp['nvm_' + str(count-1)] = abs(data[2]-temp['high'])
			pvm_sum += abs(data[1]-temp['low'])
			nvm_sum += abs(data[2]-temp['high'])
			tr = max(data[1]-data[2], abs(data[1]-temp['close']), abs(data[2]-temp['close']))
			temp['tr_' + str(count-1)] = tr
			tr_sum += tr
			temp['close'] = data[0]
			temp['high'] = data[1]
			temp['low'] = data[2]
		else:
			pvm_sum = pvm_sum + abs(data[1]-temp['low']) - temp['pvm_' + str(temp['ptr'])]
			nvm_sum = nvm_sum + abs(data[2]-temp['high']) - temp['nvm_' + str(temp['ptr'])]
			temp['pvm_' + str((count-1)%period)] = abs(data[1]-temp['low'])
			temp['nvm_' + str((count-1)%period)] = abs(data[2]-temp['high'])
			tr = max(data[1]-data[2], abs(data[1]-temp['close']), abs(data[2]-temp['close']))
			tr_sum = tr_sum + tr - temp['tr_' + str(temp['ptr'])]
			temp['tr_' + str((count-1)%period)] = tr
			temp['ptr'] = (temp['ptr'] + 1)%period
			pvi = pvm_sum/tr_sum
			nvi = nvm_sum/tr_sum
			query = str(table) + ':update VTXP:' + str(float(pvi)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
			query = str(table) + ':update VTXN:' + str(float(nvi)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
			temp['close'] = data[0]
			temp['high'] = data[1]
			temp['low'] = data[2]
		count += 1

	print("============================== VTX generated '" + str(stock_id) + "' ================================")

# Momentum Indicators [11]
def gen_ADX(stock_id, period, source, q, column_name, table):
	count = 0
	temp = {}
	tr_sum = 0
	pdm_sum = 0
	ndm_sum = 0
	adx_sum = 0
	for data in source:
		if count == 0:
			temp['close'] = data[0]
			temp['high'] = data[1]
			temp['low'] = data[2]
		elif count == period:
			tr = max(data[1]-data[2], abs(data[1]-temp['close']), abs(data[2]-temp['close']))
			tr_sum += tr
			if data[1]-temp['high'] > temp['low']-data[2]:
				pdm_sum += max(data[1]-temp['high'], 0)
			elif temp['low']-data[2] > data[1]-temp['high']:
				ndm_sum += max(temp['low']-data[2], 0)
			temp['tr'] = tr_sum
			temp['pdm'] = pdm_sum
			temp['ndm'] = ndm_sum
			temp['pdi'] = 100*(temp['pdm']/temp['tr'])
			temp['ndi'] = 100*(temp['ndm']/temp['tr'])
			temp['didiff'] = abs(temp['pdi']-temp['ndi'])
			temp['disum'] = temp['pdi']+temp['ndi']
			temp['dx'] = 100*(temp['didiff']/temp['disum'])
			adx_sum += temp['dx']
			temp['close'] = data[0]
			temp['high'] = data[1]
			temp['low'] = data[2]
		elif count < period:
			tr = max(data[1]-data[2], abs(data[1]-temp['close']), abs(data[2]-temp['close']))
			tr_sum += tr
			if data[1]-temp['high'] > temp['low']-data[2]:
				pdm_sum += max(data[1]-temp['high'], 0)
			elif temp['low']-data[2] > data[1]-temp['high']:
				ndm_sum += max(temp['low']-data[2], 0)
			temp['close'] = data[0]
			temp['high'] = data[1]
			temp['low'] = data[2]
		else:
			tr = max(data[1]-data[2], abs(data[1]-temp['close']), abs(data[2]-temp['close']))
			temp['tr'] = temp['tr']-(temp['tr']/period)+tr
			if data[1]-temp['high'] > temp['low']-data[2]:
				temp['pdm'] = temp['pdm']-(temp['pdm']/period) + max(data[1]-temp['high'], 0)
				temp['ndm'] = temp['ndm']-(temp['ndm']/period)
			elif temp['low']-data[2] > data[1]-temp['high']:
				temp['pdm'] = temp['pdm']-(temp['pdm']/period)
				temp['ndm'] = temp['ndm']-(temp['ndm']/period) + max(temp['low']-data[2], 0)
			temp['pdi'] = 100*(temp['pdm']/temp['tr'])
			temp['ndi'] = 100*(temp['ndm']/temp['tr'])
			temp['didiff'] = abs(temp['pdi']-temp['ndi'])
			temp['disum'] = temp['pdi']+temp['ndi']
			temp['dx'] = 100*(temp['didiff']/temp['disum'])
			if count == period*2-1:
				temp['adx'] = (adx_sum + temp['dx'])/period
				query = str(table) + ':update ' + str(column_name) + ':' + str(float(temp['adx'])) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
				df = q(query)
			elif count > period*2-1:
				temp['adx'] = (temp['adx']*13 + temp['dx'])/period
				query = str(table) + ':update ' + str(column_name) + ':' + str(float(temp['adx'])) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
				df = q(query)
			else:
				adx_sum += temp['dx']
			temp['close'] = data[0]
			temp['high'] = data[1]
			temp['low'] = data[2]
		count += 1

	print("============================== ADX generated '" + str(stock_id) + "' ================================")

def gen_COPPOCK(stock_id, period_1, period_2, period_3, source, q, column_name, table):
	count = 0
	temp = {}
	temp['ptr_1'] = 0
	temp['ptr_2'] = 0
	temp['ptr_3'] = 0
	coppock = 0
	period_3_sum = 0
	roc_1 = 0
	roc_2 = 0
	for data in source:
		if count < period_1:
			temp[str(count) + '_1'] = data[0]
		else:
			roc_1 = (data[0]-temp[str(temp['ptr_1']) + '_1'])/temp[str(temp['ptr_1']) + '_1']*100
			temp['ptr_1'] = (temp['ptr_1'] + 1)%(period_1+1)
			temp[str(count%(period_1+1)) + '_1'] = data[0]

		if count < period_2:
			temp[str(count) + '_2'] = data[0]
		else:
			roc_2 = (data[0]-temp[str(temp['ptr_2']) + '_2'])/temp[str(temp['ptr_2']) + '_2']*100
			temp['ptr_2'] = (temp['ptr_2'] + 1)%(period_2+1)
			temp[str(count%(period_2+1)) + '_2'] = data[0]
			roc_sum = roc_1 + roc_2
			temp['roc_sum_' + str((count-period_2)%period_3)] = roc_sum
			if count >= period_2+period_3-1:
				for num in range(0, period_3):
					coppock += temp['roc_sum_' + str((temp['ptr_3'] + num)%period_3)]*(num+1)
					period_3_sum += num+1
				coppock = coppock/period_3_sum
				query = str(table) + ':update ' + str(column_name) + ':' + str(float(coppock)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
				df = q(query)
				temp['ptr_3'] = (temp['ptr_3'] + 1)%(period_3)
			coppock = 0
			period_3_sum = 0
		count += 1

	print("============================ COPPOCK generated '" + str(stock_id) + "' ==============================")

def gen_CHAIKIN(stock_id, period_1, period_2, source, q, column_name, table):
	count = 0
	adl = 0
	p1 = 0
	p2 = 0
	for data in source:
		if (data[1]-data[2] == 0):
			mul = 0
		else:
			mul = ((data[0]-data[2])-(data[1]-data[0]))/(data[1]-data[2])
		vol = mul*data[4]
		adl += vol

		if count == period_1-1:
			p1 += adl
			p1 = p1/period_1
		elif count < period_1:
			p1 += adl
		else:
			p1 = cal_EMA(adl, period_1, p1)

		if count == period_2-1:
			p2 += adl
			p2 = p2/period_2
			chaikin = p1-p2
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(chaikin)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		elif count < period_2:
			p2 += adl
		else:
			p2 = cal_EMA(adl, period_2, p2)
			chaikin = p1-p2
			if count < 20:
				print(p2)
			query = str(table) + ':update ' + str(column_name) + ':' + str(float(chaikin)) + ' from ' + str(table) + ' where stock_id=`' + str(stock_id) + ',date=' + str(data[3])
			df = q(query)
		count += 1

	print("============================ CHAIKIN generated '" + str(stock_id) + "' ==============================")


#================================== body ==================================
q = connect_db()

'''
for db_name in indicators_db:
	clear_kdb(db_name, q)
	create_kdb(db_name, q)

print("***************************************************************************************\n*                                PROCESSING DAILY DATA                                *\n***************************************************************************************")

for stock in stock_CI:
	data = fetch_daily(stock, q)
	init_kdb(stock, data, 'resource', q)
	gen_EMA(stock, 12, data, q, 'EMA_12', 'resource')

	init_kdb(stock, data, 'volume', q)
	gen_CMF(stock, 20, data, q, 'CMF', 'volume')
	gen_ADL(stock, data, q, 'ADL', 'volume')
	gen_EMV(stock, 14, data, q, 'EMV', 'volume')
	gen_MFI(stock, 14, data, q, 'MFI', 'volume')
	gen_OBV(stock, data, q, 'OBV', 'volume')
	gen_PVO(stock, 12, 26, data, q, 'PVO', 'volume')

	init_kdb(stock, data, 'trending', q)
	gen_AROON(stock, 25, data, q, 'AROON', 'trending')
	gen_CCI(stock, 20, data, q, 'CCI', 'trending')
	gen_FORCE(stock, 13, data, q, 'FORCE', 'trending')
	gen_VTX(stock, 14, data, q, 'VTX', 'trending')
'''
	

data = fetch_daily('0001.HK', q)
clear_kdb('resource', q)
create_kdb('resource', q)
init_kdb('0001.HK', data, 'resource', q)
gen_EMA('0001.HK', 12, data, q, 'EMA_12', 'resource')

clear_kdb('momentum', q)
create_kdb('momentum', q)
init_kdb('0001.HK', data, 'momentum', q)
gen_ADX('0001.HK', 14, data, q, 'ADX', 'momentum')
gen_COPPOCK('0001.HK', 11, 14, 10, data, q, 'COPPOCK', 'momentum')
gen_CHAIKIN('0001.HK', 3, 10, data, q, 'CHAIKIN', 'momentum')



'''
query = 's)select COPPOCK from momentum'
df = q(query)
count = 0
for data in df:
	print(data)
	count += 1
	if count == 40:
		break
'''

