import quandl
import csv
import pandas as pd

quandl.ApiConfig.api_key = "Ufux_HxUXZKAgFjxWhGi"		# API key
start_date="2018-10-01"		# Start Date
end_date="2018-12-01"		# End Date

sma_period=20		# Slow moving average period
fma_period=5		# Fast moving average period

# Function to get the list of instruments 
def get_instruments_list(filename):
	with open(filename, 'r') as f:
		ins_list = list(csv.reader(f))
	return ins_list[1:]

# Function to get quandl codes
def get_quandl_codes(ins_list):
	q_list=[]
	for instrument in ins_list:
		q_list.append("EOD/"+instrument[1])
	return q_list

# Function to use Quandl API for getting data
def instrument_data(q_list):

	# As of now it gets only the adjusted Close for each instrument
	data = quandl.get(q_list, column_index=11, start_date=start_date, end_date=end_date)
	return data

# Function to extract fast and slow moving averages
def get_moving_avg(data):
	FMA = pd.Series(data[data.columns[0]].rolling(window=fma_period).mean(), name="FMA")
	SMA = pd.Series(data[data.columns[0]].rolling(window=sma_period).mean(), name="SMA")
	data = data.join(FMA)
	data = data.join(SMA)
	return data

# Function to check for crossovers and give sell or buy signal
def ma_crossover(data):
	
	# Order list contains the sell or buy signal
	order_list=[]
	for i in range(sma_period+1):
		order_list.append(0)

	# Check whtther the data is enough
	if len(data.index) < sma_period:
		print("Not enough data")
		return 0
	
	FMA = data["FMA"]
	SMA = data["SMA"]
	
	# Previous Index : Since the index of data is date-time format
	prev_index=data.index[sma_period]

	# Loop for checking crossover for the slow and fast moving averages
	for index in data.index[sma_period+1:]:
		if FMA[index] > SMA[index] and FMA[prev_index] < SMA[prev_index]:
			print(fma_period, "day ma crosses above", sma_period, "day ma on ", str(index)[:10], " BUY SIGN")
			print(index, FMA[index], FMA[prev_index])
			
			# Assigning order as 1 for buy condition
			order_list.append(1)

		elif SMA[index] > FMA[index] and SMA[prev_index] < FMA[prev_index]:	
			print(fma_period, "day ma crosses below", sma_period, "day ma on ", str(index)[:10], " SELL SIGN")
			print(index, FMA[index], FMA[prev_index])

			# Assigning order as 2 for sell condition 
			order_list.append(2)
		else:

			# Assigning order as 0 for neither buy nor sell
			order_list.append(0)
		prev_index = index	
	ORDER = pd.Series(order_list, index=data.index, name="ORDER")
	
	# Adding the list of orders in the main dataframe (labelled data)
	data = data.join(ORDER)
	print(data)

def main():

	filename="contracts.csv"
	ins_list=get_instruments_list(filename)
	q_list=get_quandl_codes(ins_list)
	print(q_list)

	data=instrument_data(q_list)

	# Loop that runs through each instrument in the list
	for instrument in data.columns:
		print(instrument)
		print("-------------------------------------")
		
		# Calculates moving average for each column in the dataframe containing data from quandl
		mov_avg = get_moving_avg(pd.DataFrame(data, columns=[instrument]))

		# Check for crossovers
		ma_crossover(mov_avg)
		print("-------------------------------------")
		print("-------------------------------------")


main()