import quandl
import csv
import pandas as pd
import datetime
import os
import place_order, time

if not os.path.isdir("Data/"):
	os.mkdir("Data")
	print("Created Data Directory ...")
if not os.path.isdir("MA_Data/"):
	os.mkdir("MA_Data")
	print("Created Moving Average Data Directory ...")
if not os.path.isdir("BuySell/"):
	os.mkdir("BuySell")
	print("Created BuySell Directory ...")


quandl.ApiConfig.api_key = "Ufux_HxUXZKAgFjxWhGi"		# API key
start_date="2018-10-01"		# Start Date
end_date=datetime.datetime.today().strftime('%Y-%m-%d')		# End Date

sma_period = 20		# Slow moving average period
fma_period = 5		# Fast moving average period
order_id = 0
balance = 10000

# Function to get the list of instruments 
def get_instruments_list(filename):
	with open(filename, 'r') as f:
		ins_list = list(csv.reader(f))
	return ins_list[1:]

def get_valid_order_id(msg):  
	global  order_id
	order_id = msg.orderId
	

# Function to use Quandl API for getting data
def instrument_data(ins_list):
	for token_data in ins_list:

		# Quandl Code
		quandl_code = "EOD/"+token_data[0]
		new_data = quandl.get(quandl_code, start_date=start_date, end_date=end_date)

		# stores the point where we left off last time
		last_index = -1

		# If there is no old data
		if os.path.exists("Data/"+token_data[0]+".csv"):
			old_data = pd.read_csv(r"Data/"+token_data[0]+".csv", index_col="Date")
			
			index_list = new_data.index
			last_index = old_data.index[-1]
			if last_index in index_list:
				drop_data_index_list = index_list[:index_list.get_loc(last_index)+1]
				new_data = new_data.drop(drop_data_index_list)
				
			new_data = old_data.append(new_data)
	
		new_data.index = pd.DatetimeIndex(new_data.index).normalize()

		new_data.to_csv(r"Data/"+token_data[0]+".csv")
	return last_index
		
		

# Function to extract fast and slow moving averages
def get_moving_avg(ins_list):

	for token_data in ins_list:
		data = pd.read_csv(r"Data/"+token_data[0]+".csv", index_col="Date")
		FMA = pd.Series(data[data.columns[3]].rolling(window=fma_period).mean(), name="FMA")
		SMA = pd.Series(data[data.columns[3]].rolling(window=sma_period).mean(), name="SMA")
		data = data.join(FMA)
		data = data.join(SMA)
		data.to_csv(r"MA_Data/"+token_data[0]+".csv")

# Function to check for crossovers and give sell or buy signal
def ma_crossover(ins_list, last_index, connection):
	
	global order_id
	buy_sell_flags = {}
	# Initialize the buy sell flags
	if len(os.listdir("BuySell/")) == 0:
		for token_data in ins_list:
			buy_sell_flags[token_data[0]] = '1'
		
		with open("BuySell/buy_sell_flags.csv", "w", newline="") as f:
			filewriter = csv.writer(f, delimiter=',')
			for key, value in buy_sell_flags.items():
				filewriter.writerow([key, int(value)])
			f.close()

	with open("BuySell/buy_sell_flags.csv", 'r') as f:
		reader = csv.reader(f)
		buy_sell_flags = dict(reader)
			
	for token_data in ins_list:
		print(token_data[0])
		data = pd.read_csv(r"MA_Data/"+token_data[0]+".csv", index_col="Date") 

		# Check whtther the data is enough
		if len(data.index) < sma_period:
			print("Not enough data")
			return 0
		
		FMA = data["FMA"]
		SMA = data["SMA"]
		CLOSE = data["Close"]
		# Previous Index : Since the index of data is date-time format
		if last_index == -1:
			prev_index=data.index[sma_period]
		else:
			prev_index=last_index

		# Loop for checking crossover for the slow and fast moving averages
		for index in data.index[data.index.get_loc(prev_index):]:
			if FMA[index] > SMA[index] and FMA[prev_index] < SMA[prev_index]:
				print(fma_period, "day ma crosses above", sma_period, "day ma on ", str(index)[:10], " BUY SIGN")
				

				# Check the buy sell flag
				
				if buy_sell_flags[token_data[0]] == '1':

					print("BUY, Placing Order")
					
					place_order.place_order(connection, order_id, token_data, "BUY", "LMT", int(balance//CLOSE[index]), CLOSE[index])
				
					buy_sell_flags[token_data[0]] = '0'	
					order_id = order_id + 1
				else:
					print("CANNOT BUY")	
				print("BUY-SELL Flag", buy_sell_flags)
				print("------------------------------")	

			elif SMA[index] > FMA[index] and SMA[prev_index] < FMA[prev_index]:	
				print(fma_period, "day ma crosses below", sma_period, "day ma on ", str(index)[:10], " SELL SIGN")
				

				# Check the buy sell flag
				if buy_sell_flags[token_data[0]] == '0':

					print("SELL, Placing Order")	

					place_order.place_order(connection, order_id, token_data, "SELL", "LMT", int(balance//CLOSE[index]), CLOSE[index])
					
					buy_sell_flags[token_data[0]] = '1'
					order_id = order_id + 1
				else:
					print("CANNOT SELL")
				print("BUY-SELL Flag", buy_sell_flags)
				print("------------------------------")
			prev_index = index	

	with open("BuySell/buy_sell_flags.csv", "w", newline="") as f:
			filewriter = csv.writer(f, delimiter=',')
			for key, value in buy_sell_flags.items():
				filewriter.writerow([key, value])
			f.close()
		
def main():

	filename="contracts.csv"
	ins_list=get_instruments_list(filename)
	last_index = instrument_data(ins_list)
	get_moving_avg(ins_list)
	connection = place_order.connection_est(7497, 1234)	
	connection.register(get_valid_order_id, 'NextValidId')
	time.sleep(2)		
	ma_crossover(ins_list, last_index, connection)
	place_order.connection_dis()

main()