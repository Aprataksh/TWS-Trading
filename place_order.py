import csv, time
from ib.opt import Connection, message

from ib.ext.Contract import Contract
from ib.ext.Order import Order

# Establishing connection with the TWS 
def connection_est(portid, clientId):
	global connection
	connection = Connection.create(port=portid, clientId=clientId)
	connection.register(get_valid_order_id, 'NextValidId')
	connection.connect()
	return connection

def connection_dis():
	global connection
	connection.disconnect()

# Function for getting instrument list from contracts.csv
def get_instruments_list(filename):
	with open(filename, 'r') as f:
		ins_list = list(csv.reader(f))
	return ins_list[1:]

# Function to get the next valid order id
def get_valid_order_id(msg):  
	global  order_id
	order_id = msg.orderId
	
# Function for generating a contract 
def make_contracts(symbol, sec_Type, exch, prim_exch, curr):
	Contract.m_symbol=symbol
	Contract.m_secType=sec_Type
	Contract.m_exchange=exch
	Contract.m_primaryExchange=prim_exch
	Contract.m_currency=curr
	print("----CONTRACT----")
	print("Symbol = ", symbol)
	print("Exchnage = ", exch)
	print("Currency = ", curr)
	print()
	return Contract

# Function for generating an order
def make_orders(order_type, action, quantity, price):
	order=Order()
	order.m_orderType=order_type
	order.m_totalQuantity=quantity
	order.m_action=action
	order.m_lmtPrice=price
	print("----ORDER----")
	print("Order Type = ", order_type)
	print("Action = ", action)
	print("Quantity = ", quantity)
	print("Price = ", price)
	return order

# Function for placing an order given an order id, instrument and components of order
def place_order(connection, oid, instrument, buy_sell, order_type, quantity, price):
	contract=make_contracts(instrument[1], instrument[2], instrument[4], instrument[4], instrument[5])
	order=make_orders("LMT", buy_sell, quantity, price)
	print("Order Id = ", oid)
	print()
	connection.placeOrder(oid, contract, order)
	
def main():
	filename="contracts.csv"
	order_list=get_instruments_list(filename)
	connection.register(get_valid_order_id, 'NextValidId')
	time.sleep(2)

	# Loop for renewing the order id for each order placement (essential thing in TWS)
	for i in range(7, 10):	
		
		instrument=order_list[i-7]
		print(instrument)
		place_order(i, instrument, "BUY", "LMT", 1, 100)

"""connection_est(7497, 1234)
main()
connection_dis()"""