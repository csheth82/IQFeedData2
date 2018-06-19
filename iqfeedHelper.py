# iqfeed.py

import sys
import socket
import pandas as pd
from io import StringIO
from datetime import datetime

def fetchData(message):
    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.sendall(message.encode())
    buffer = ""
    data = ""
    while True:
        data = sock.recv(4096).decode('utf-8')
        buffer += data

        # Check if the end message string arrives
        if "!ENDMSG!" in buffer:
            break
    # Remove the end message string
    data = buffer[:-12]
    sock.close
    data = "".join(data.split("\r"))
    data = data.replace(",\n", "\n")[:-1]
    return  data


# Retrieves interval(seconds) data between bdatetime and edatetime(optional) for specified symbol. Filter time(optional)
def getHistoricalTimeBars(sym,interval,bdatetime,edatetime="",bfiltertime="",efiltertime=""):
    message = "HIT,%s,%s,%s,%s,,%s,%s,1\n" % (sym,interval,bdatetime,edatetime,bfiltertime,efiltertime)
    data = fetchData(message)
    # Convert to Pandas data frame and set datetime as index
    data = pd.read_csv(StringIO(data), index_col=0, header=None,names=['High','Low','Open','Close','totVolume','intVolume'])
    data.index = pd.to_datetime(data.index)
    return data

# Retrieves numBars number of interval data for specified symbol
def getNumberHistoricalTimeBars(sym,interval,numBars):
    message = "HIX,%s,%s,%s,1\n" % (sym,interval,numBars)
    data = fetchData(message)
    # Convert to Pandas data frame and set datetime as index
    data = pd.read_csv(StringIO(data), index_col=0, header=None,names=['High','Low','Open','Close','totVolume','intVolume'])
    data.index = pd.to_datetime(data.index)
    return data

# Retrieves numBars number of volume interval data for specified symbol
def getNumberHistoricalVolumeBars(sym,interval,numBars):
    message = "HIX,%s,%s,%s,1,,,v\n" % (sym,interval,numBars)
    data = fetchData(message)
    # Convert to Pandas data frame and set datetime as index
    data = pd.read_csv(StringIO(data), index_col=0, header=None,names=['High','Low','Open','Close','totVolume','intVolume'])
    data.index = pd.to_datetime(data.index)
    return data

def getHistoricalVolumeBars(sym,interval,bdatetime,edatetime="",bfiltertime="",efiltertime=""):
    message = "HIT,%s,%s,%s,%s,,%s,%s,1,,,v\n" % (sym,interval,bdatetime,edatetime,bfiltertime,efiltertime)
    data = fetchData(message)
    # Convert to Pandas data frame and set datetime as index
    data = pd.read_csv(StringIO(data), index_col=0, header=None,names=['High','Low','Open','Close','totVolume','intVolume'])
    data.index = pd.to_datetime(data.index)
    return data

# Retrieves numDays days of interval data for specified symbol
def getHistoricalTimeBarsForDays(sym,interval,numDays,bfiltertime="",efiltertime=""):
    message = "HID,%s,%s,%s,,%s,%s,1\n" % (sym,interval,numDays,bfiltertime,efiltertime)
    data = fetchData(message)
    # Convert to Pandas data frame and set datetime as index
    data = pd.read_csv(StringIO(data), index_col=0, header=None,names=['High','Low','Open','Close','totVolume','intVolume'])
    data.index = pd.to_datetime(data.index)
    return data

# Retrieves daily HLOC, Volume, Open Interest data between bdatetime and edateime(optional) for specified symbol
def getDailyData(sym,bdatetime,edatetime=""):
    message = "HDT,%s,%s,%s,,1\n" % (sym,bdatetime,edatetime)
    data = fetchData(message)
    # Convert to Pandas data frame and set datetime as index
    data = pd.read_csv(StringIO(data), index_col=0, header=None,names=['High','Low','Open','Close','Volume','OpenInterest'])
    data.index = pd.to_datetime(data.index)
    data.index = data.index.date
    return data

# Retrieves numDays days of daily data between bdatetime and edateime(optional) for specified symbol
def getNumberDailyData(sym,numDays):
    message = "HDX,%s,%s,1\n" % (sym,numDays)
    data = fetchData(message)
    # Convert to Pandas data frame and set datetime as index
    data = pd.read_csv(StringIO(data), index_col=0, header=None,names=['High','Low','Open','Close','Volume','OpenInterest'])
    data.index = pd.to_datetime(data.index)
    data.index = data.index.date
    return data

# Retrieves tick data between bdatetime and edatetime(optional) with optional filter time for specified symbol
def getHistoricalTicks(sym,bdatetime,edatetime="",bfiltertime="",efiltertime=""):
    message = "HTT,%s,%s,%s,,%s,%s,1\n" % (sym,bdatetime,edatetime,bfiltertime,efiltertime)
    data = fetchData(message)
    # Convert to Pandas data frame and set datetime as index
    data = pd.read_csv(StringIO(data), index_col=0, header=None,names=['Last','LastSize','totVolume','Bid','Ask','TickID','TradeReason','TradeMC','TradeCondition'])
    data.index = pd.to_datetime(data.index)
    return data

# Retrieves numTicks number of tick data for specified symbol
def getNumberTicks(sym,numTicks):
    message = "HTX,%s,%s,1\n" % (sym,numTicks)
    data = fetchData(message)
    # Convert to Pandas data frame and set datetime as index
    data = pd.read_csv(StringIO(data), index_col=0, header=None,names=['Last','LastSize','totVolume','Bid','Ask','TickID','TradeReason','TradeMC','TradeCondition'])
    data.index = pd.to_datetime(data.index)
    return data

# Retrieves numDays days of tick data for specified symbol with optional filter time
def getTicksForDays(sym,numDays,bfiltertime="",efiltertime=""):
    message = "HTD,%s,%s,,%s,%s,1\n" % (sym,numDays,bfiltertime,efiltertime)
    data = fetchData(message)
    # Convert to Pandas data frame and set datetime as index
    data = pd.read_csv(StringIO(data), index_col=0, header=None,names=['Last','LastSize','totVolume','Bid','Ask','TickID','TradeReason','TradeMC','TradeCondition'])
    data.index = pd.to_datetime(data.index)
    return data

def getSeasonalOpenInterestForProducts(productArray,numDays):
    for product in productArray:
        df = getNumberDailyData(product,30)
        print(df)

# Build iqFeed symbols for specified symbol, months and years
def buildFuturesChain(sym,months,years):
    fCode =[]
    for y in years:
        for m in months:
            fC = sym+m+y
            fCode.append(fC)
    return fCode

# Get active contracts for specified symbol
def getActiveFuturesForProduct(sym):
    monthcodes = "FGHJKMNQUVXZ"
    currYear = datetime.today().year
    years = str(currYear)[-1] + str(currYear+1)[-1]
    message = "CFU,%s,%s,%s\n" % (sym,monthcodes,years)
    data = fetchData(message)
    data = data.split(",")
    return data

# Get CFTC Commitment of Traders Data for specified product and field
def getCOT(prodCode,fieldCode,beginDate,endDate=""):
    requestCode = "CF_" + prodCode + "_" + fieldCode
    data = getDailyData(requestCode,beginDate,endDate)
    return data.Close

# Testing the functions
if __name__ == "__main__":
    df = getHistoricalTimeBars("@SMZ19",60,"20180611 000000")
    df2 = getDailyData("@SX18","20180101")
    df3 = getHistoricalTicks("@SMZ19","20180611 120000")
    df4 = getNumberTicks("@SX18",10)
    df5 = getTicksForDays("@SX18",1,"131400","131500")
    df6 = getNumberDailyData("@SX18",10)
    df7 = getNumberHistoricalTimeBars("@SX18",1800,200)
    df8 = getHistoricalTimeBarsForDays("@SX18",60,20)
    df9 = getNumberHistoricalVolumeBars("@SX18",1000,200)