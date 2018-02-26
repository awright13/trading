import datetime;
import pandas as pd;
import quandl;
import matplotlib.pyplot as plt;
import math;
import numpy as np;
import time;
from quandl import QuandlError;
plt.style.use('alex');

class Market:
    ''' The Market class has two primary roles: to pull data from the Quandl API, and to store that data in a
    structured dataframe, self.History.  The one required parameter is exchange -- which will correspond to one
    of the available exchange codes listed below.  User can also pass a datestart parameter, which specifies from
    which date onwards the request for data will be made. The passed exchange code will be formatted correctly for the
    Quandl API and passed for a request.  
    
    The file parameter should be the location of the file that stores your market data in .csv format, and the data 
    should be structured in the format of the class self.History pandas DataFrame. In the event that a file
    parameter is passed, the initialization of the class will identify the latest date in the dataframe, and set
    datestart to this date.
    
    In all cases the class pulls in data up to the current date.
    
    Market class has access to the following exchanges via the Quandl API:
    
    Wiki stock Prices (High Volumne)	WIKIP2000
     Wiki stock Prices (Lower Volumne)	WIKIP_over2000
    BraveNewCoin	BNC3
    London Metal Exchange	LME
    Chicago Mercantile Exchange	CME
    Intercontinental Exchange Futures	ICE
    London Stock Exchange	LSE
    Wiki Commodity Price	COM
    Hong Kong Exchange	HKEX
    Warsaw Stock Exchange	WSE
    Global Petroleum Prices (Gasoline)	GPPG
    Global Petroleum Prices (Diesel)	GPPD
    London Platinum and Palladium Market	LPPM
    LIFFE Futures Data	LIFFE
    Eurex Futures	EUREX
    Johnson Matthey	JOHNMATT
'''

		
    def __init__(self,
                 Market_file = None,
                 exclude_file = None,
                 mavg_df_file = None,
                 acf_df_file = None,
                 dir_file = "C:/Users/awright/Desktop/January 2018/Other/trading/Asset listings/MarketandListingCodes.csv"
                 ):

        
        quandl.ApiConfig.api_key = 'Npr_WVEspvDGSfRd9Dt_'          
        self.colors = [prop['color'] for prop in plt.rcParams['axes.prop_cycle']]
        
        if exclude_file:
            self.excludes = pd.read_csv(exclude_file, header=None).iloc[:,0].values
        else:
            self.excludes = None
            
        if mavg_df_file:
            self.mavg_df = pd.read_csv(mavg_df_file, 
                                       index_col = 0,
                                       encoding = "ISO-8859-1", dtype = {'Date':str}, parse_dates=['Date'])
        else:
            self.mavg_df = None
            
        if acf_df_file:
            self.acf_df = pd.read_csv(acf_df_file)
        else:
            self.acf_df = None
            
        if dir_file:
            self.Dir = pd.read_csv(dir_file)
        else:
            self.Dir = None
        
        if Market_file:
        ## Append an existing History from the latest date to today ##

            self.History = pd.read_csv(Market_file, index_col = 0, encoding = "ISO-8859-1", dtype = {'Symbol':object,
                                                                                    'Description':object,
                                                                                    'Date': str,
                                                                                    'Open':'float64',
                                                                                    'Close':'float64',
                                                                                    'High':'float64',
                                                                                    'Low':'float64',
                                                                                    'Volume':'float64',
                                                                                    'DailyPctChg':'float64'},
                                                                                     parse_dates=['Date'])  
            self.dateStart = max(self.History.Date)
        
        else:

            self.History = pd.DataFrame([],columns = ['Symbol',
                                                      'Description',
                                                      'Date',
                                                      'Open',
                                                      'Close',
                                                      'High',
                                                      'Low',
                                                      'Volume',
                                                      'DailyPctChg']).astype({'Symbol':object,
                                                                         'Description':object,
                                                                         'Date': datetime.date,
                                                                         'Open':'float64',
                                                                         'Close':'float64',
                                                                         'High':'float64',
                                                                         'Low':'float64',
                                                                         'Volume':int,
                                                                         'DailyPctChg':'float64'})
    
        self.symbols = self.History.Symbol.unique()
    
    def getHistory(self,
                  exchange,
                  period = 'daily',
                  datestart = (str((datetime.datetime.now().date() + datetime.timedelta(days  =-1)).year) 
                      + '-' + str((datetime.datetime.now().date() + datetime.timedelta(days  =-1)).month) + '-' 
                      + str((datetime.datetime.now().date() + datetime.timedelta(days  =-1)).day)),
                  dateend = (str((datetime.datetime.now().date().year))
                      + '-' + str((datetime.datetime.now().date().month))
                      + '-' + str((datetime.datetime.now().date().day)))):
        
        self.dateStart = datestart
        self.dateEnd = dateend
        
        if len(self.History) != 0:
            self.dateStart = max(self.History.Date) + datetime.timedelta(days=1)

        if exchange == 'WIKIP2000':
            syms = pd.read_csv("C:/Users/awright/Desktop/January 2018/Other/trading/Asset listings/StockSymbols2000.csv"
                              , encoding = "ISO-8859-1")
            
            for i in range(len(syms)):  
                sym = syms['Symbol'][i]
                description = syms['Description'][i]
                try:
                    code = 'WIKI/' + sym
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','Adj. Open','Adj. High','Adj. Low','Adj. Close','Adj. Volume']]
                    assetHistory['Symbol'] = sym
                    assetHistory['Description'] = description
                    assetHistory.columns = ['Date','Open','High','Low','Close', 'Volume','Symbol','Description']

                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue

        elif exchange == 'WIKIP_over2000':
            syms = pd.read_csv("C:/Users/awright/Desktop/January 2018/Other/trading/Asset listings/StockSymbols_over2000.csv"
                              , encoding = "ISO-8859-1")

            for i in range(len(syms)):  
                sym = syms['Symbol'][i]
                description = syms['Description'][i]
                try:
                    code = 'WIKI/' + sym
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','Adj. Open','Adj. High','Adj. Low','Adj. Close','Adj. Volume']]
                    assetHistory['Symbol'] = sym
                    assetHistory['Description'] = description
                    assetHistory.columns = ['Date','Open','High','Low','Close', 'Volume','Symbol','Description']

                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue


        elif exchange == 'BNC3':
            for row in self.Dir[self.Dir['Exchange'] == exchange].index.tolist():
                try:
                    code = self.Dir.loc[row,'Listing']
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','Open','High','Low','Close', 'Volume']]
                    assetHistory['Symbol'] = self.Dir.loc[row,'Symbol']
                    assetHistory['Description'] = self.Dir.loc[row,'Description']

                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(self.Dir.loc[row,'Description'],self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(self.Dir.loc[row,'Description']))
                    continue

        elif exchange == 'LME':
            for row in self.Dir[self.Dir['Exchange'] == exchange].index.tolist():
                code = self.Dir.loc[row,'Listing']
                try:
                    self.History = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    print('London Metal Exchange Data Loaded for dates from {} to {} '.format(code,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(code))
                    continue

        elif exchange == 'CME':
            listings = []
            for l in self.Dir[self.Dir['Exchange'] == 'CME'].loc[:,'Listing'].values.tolist():
                if l[-4:] in ['2017','2018','2019','2020','2021']:
                    listings.append(l)
                    if len(listings) > 2000:
                        listings = listings[:2000]

            for code in listings:
                description = self.Dir[self.Dir['Listing'] == code]['Description']
                try:
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','Settle']]
                    assetHistory['Symbol'] = code[4:]
                    assetHisory['Description'] = description
                    assetHistory.columns = ['Date','Close','Symbol','Description']
                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue           

        elif exchange == 'ICE':
            listings = []
            for l in self.Dir[self.Dir['Exchange'] == 'ICE'].loc[:,'Listing'].values.tolist():
                if l[-4:] in ['2017','2018','2019','2020','2021']:
                    listings.append(l)
                    if len(listings) > 2000:
                        listings = listings[:2000]

            for code in listings:
                description = self.Dir[self.Dir['Listing'] == code]['Description']
                try:
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','Settle']]
                    assetHistory['Symbol'] = code[4:]
                    assetHistory['Description'] = description
                    assetHistory.columns = ['Date','Close','Symbol', 'Description']
                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue

        elif exchange == 'LSE':
            syms = pd.read_csv("C:/Users/awright/Desktop/January 2018/Other/trading/Asset listings/LSE_Subset.csv")
            for i in rang(len(syms)):
                sym = syms['Symbol'][i]
                description = syms['Description'][i]
                try:
                    code = 'LSE/' + sym
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','High','Low','Last Close', 'Volume']]
                    assetHistory['Symbol'] = sym
                    assetHistory['Description'] = description
                    assetHistory.columns = ['Date','High','Low','Close', 'Volume','Symbol','Description']

                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue

        elif exchange == 'COM':

            for row in self.Dir[self.Dir['Exchange'] == exchange].index.tolist():
                code = self.Dir.loc[row,'Listing']
                description = self.Dir.loc[row,'Description']
                try:
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','Value']]
                    assetHistory['Symbol'] = self.Dir.loc[row,'Symbol']
                    assetHistory['Description'] = description
                    assetHistory.columns = ['Date','Close','Symbol','Description']
                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue

        elif exchange == 'HKEX':
            Symbols = []
            Codes = []
            for i in range(1,2001):
                digits = len(str(i))
                Symbol = '00000'
                Symbol = Symbol[:5-digits] + str(i)
                Code = 'HKEX/' + Symbol
                Symbols.append(Symbol)
                Codes.append(Code)

            for i in range(len(Codes)):
                description = self.Dir[self.Dir['Listing'] == Codes[i]]['Description']
                try:
                    assetHistory = quandl.get(Codes[i], start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','Nominal Price','High','Low','Share Volume (000)']]
                    assetHistory['Symbol'] = Symbols[i]
                    assetHistory['Description'] = description
                    assetHistory.columns = ['Date','Close','High','Low','Volume','Symbol','Description']
                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(Description,self.dateStart,self.dateEnd))

                except (QuandlError, KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue

        elif exchange == 'WSE':
            listings = []
            for l in self.Dir[self.Dir['Exchange'] == 'WSE'].loc[:,'Listing'].values.tolist():
                    listings.append(l)

            for code in listings:
                description = self.Dir[self.Dir['Listing'] == code]['Description']
                try:
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','Open','High','Low','Close','Volume']]
                    assetHistory['Symbol'] = code[4:]
                    assetHistory['Description'] = description
                    assetHistory.columns = ['Date','Open','High','Low','Close','Volume','Symbol','Description']
                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue

        elif exchange == 'GPPG':
            listings = []
            for l in self.Dir[self.Dir['Exchange'] == 'GPP'].loc[:,'Listing'].values.tolist():
                    listings.append(l)

            for code in listings:
                description = self.Dir[self.Dir['Listing'] == code]['Description']
                try:
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','Gasoline Price']]
                    assetHistory['Symbol'] = code[4:]
                    assetHistory['Description'] = description
                    assetHistory.columns = ['Date','Close','Symbol','Description']
                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue

        elif exchange == 'GPPD':
            listings = []
            for l in self.Dir[self.Dir['Exchange'] == 'GPP'].loc[:,'Listing'].values.tolist():
                    listings.append(l)
            for code in listings:
                description = self.Dir[self.Dir['Listing'] == code]['Description']
                try:
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','Diesel Price']]
                    assetHistory['Symbol'] = code[4:]
                    assetHistory['Description'] = description
                    assetHistory.columns = ['Date','Close','Symbol','Description']
                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue

        elif exchange == 'LPPM':
            listings = []
            for l in self.Dir[self.Dir['Exchange'] == 'LPPM'].loc[:,'Listing'].values.tolist():
                    listings.append(l)

            for code in listings:
                description = self.Dir[self.Dir['Listing'] == code]['Description']
                try:
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','USD PM']]
                    assetHistory['Symbol'] = code[5:]
                    assetHistry['Description'] = description
                    assetHistory.columns = ['Date','Close','Symbol','Description']
                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue

        elif exchange == 'LIFFE':
            listings = []
            for l in self.Dir[self.Dir['Exchange'] == 'LIFFE'].loc[:,'Listing'].values.tolist():
                if l[-4:] in ['2017','2018','2019','2020','2021','2022','2023']:
                    listings.append(l)

            for code in listings:
                description = self.Dir[self.Dir['Listing'] == code]['Description']
                try:
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','Open','High','Low','Settle','Volume']]
                    assetHistory['Symbol'] = code[6:]
                    assetHistory['Description'] = description
                    assetHistory.columns = ['Date','Open','High','Low','Close','Volume','Symbol','Description']
                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue

        elif exchange == 'EUREX':
            listings = []
            for l in self.Dir[self.Dir['Exchange'] == 'EUREX'].loc[:,'Listing'].values.tolist():
                if l[-4:] in ['2017','2018','2019','2020','2021','2022','2023']:
                    listings.append(l)

            for code in listings:
                description = self.Dir[self.Dir['Listing'] == code]['Description']
                try:
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','Open','High','Low','Settle','Volume']]
                    assetHistory['Symbol'] = code[6:]
                    assetHistory['Description'] = description
                    assetHistory.columns = ['Date','Open','High','Low','Close','Volume','Symbol','Description']
                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue

        elif exchange == 'JOHNMATT':
            listings = []
            for l in self.Dir[self.Dir['Exchange'] == 'JOHNMATT'].loc[:,'Listing'].values.tolist():
                    listings.append(l)

            for code in listings:
                description = self.Dir[self.Dir['Listing'] == code]['Description']
                try:
                    assetHistory = quandl.get(code, start_date=self.dateStart, end_date=self.dateEnd,collapse = period).reset_index()
                    assetHistory = assetHistory[['Date','New York 9:30']]
                    assetHistory['Symbol'] = code[9:]
                    assetHistory['Description'] = description
                    assetHistory.columns = ['Date','Close','Symbol','Description']
                    self.History = pd.concat([self.History,assetHistory], ignore_index=True)
                    print('{} data acquired for dates from {} to {}'.format(description,self.dateStart,self.dateEnd))

                except (KeyError, NameError):
                    print('{} data FAILED TO LOAD'.format(description))
                    continue
                    
        self.History = self.History[['Symbol','Description','Date','Volume','Open','Low','High','Close']]
        self.History['DailyPctChg'] = (self.History['Close'] / (self.History.groupby('Symbol')['Close'].shift(1))) - 1
        self.symbols = self.History.Symbol.unique()
   

 ################################# Plotting Methods #################################

    def tsPlotTogether(self,
                 symbols,
                  metric = 'price',
                 datestart = (str((datetime.datetime.now().date() + datetime.timedelta(weeks  =-4)).year) 
                      + '-' + str((datetime.datetime.now().date() + datetime.timedelta(weeks  =-4)).month) + '-' 
                      + str((datetime.datetime.now().date() + datetime.timedelta(weeks  =-4)).day)),
                  dateend = (str((datetime.datetime.now().date().year))
                      + '-' + str((datetime.datetime.now().date().month))
                      + '-' + str((datetime.datetime.now().date().day)))):
        ''' available metrics: price, maxnorm, pctchg'''
        

        dates = self.History['Date'][(self.History['Symbol'] == symbols[0]) 
                             &(self.History['Date'] >= datestart)
                             & (self.History['Date'] <= dateend)]
        
        fig, ax = plt.subplots(figsize=(16,8))
                               
        if metric == 'price':

            for i in range(len(symbols)):

                sym = symbols[i]
                prices = self.History[(self.History['Symbol'] == sym)
                           & (self.History['Date'] >= datestart)
                           & (self.History['Date'] <= dateend)].Close.values
                
                ax.set_title('Time Series from {} to {} \n Metric = {}'.format(datestart,dateend,metric), fontsize = 16)
                ax.plot(dates,prices, label = sym)

            plt.legend()
            plt.show()
            
        if metric == 'maxnorm':

            for i in range(len(symbols)):

                sym = symbols[i]
                prices = (self.History[(self.History['Symbol'] == sym)
                           & (self.History['Date'] >= datestart)
                           & (self.History['Date'] <= dateend)].Close.values 
                          / max(self.History[(self.History['Symbol'] == sym)
                           & (self.History['Date'] >= datestart)
                           & (self.History['Date'] <= dateend)].Close))
                          
                ax.set_title('Time Series from {} to {} \n Metric = {}'.format(datestart,dateend,metric), fontsize = 16)
                ax.plot(dates,prices, label = sym)

            plt.legend()
            plt.show()
            
        if metric == 'pctchg':

            for i in range(len(symbols)):

                sym = symbols[i]
                pctChgs = self.History[(self.History['Symbol'] == sym)
                           & (self.History['Date'] >= datestart)
                           & (self.History['Date'] <= dateend)].DailyPctChg.values
                
                ax.set_title('Time Series from {} to {} \n Metric = {}'.format(datestart,dateend,metric), fontsize = 16)
                ax.plot(dates,pctChgs, label = sym)

            plt.legend()
            plt.show()
            
    def tsPlotAlone(self,
                 symbols,
                  metric = 'price',
                 datestart = (str((datetime.datetime.now().date() + datetime.timedelta(weeks  =-4)).year) 
                      + '-' + str((datetime.datetime.now().date() + datetime.timedelta(weeks  =-4)).month) + '-' 
                      + str((datetime.datetime.now().date() + datetime.timedelta(weeks  =-4)).day)),
                  dateend = (str((datetime.datetime.now().date().year))
                      + '-' + str((datetime.datetime.now().date().month))
                      + '-' + str((datetime.datetime.now().date().day)))):
        ''' available metrics: price, pctchg'''
        


        
        ncols = 1
        nrows = max([2,len(symbols)])
        rows = len(symbols)
        fig, ax = plt.subplots(nrows = nrows, ncols = ncols,figsize=(16,8*nrows))
        
        if metric == 'price':

            for i in range(rows):
                    sym = symbols[i]

                    dates = self.History['Date'][(self.History['Symbol'] == sym) 
                     &(self.History['Date'] >= datestart)
                     & (self.History['Date'] <= dateend)]

                    prices = self.History[(self.History['Symbol'] == sym)
                               & (self.History['Date'] >= datestart)
                               & (self.History['Date'] <= dateend)].Close.values
                    
                    volumes = self.History[(self.History['Symbol'] == sym)
                               & (self.History['Date'] >= datestart)
                               & (self.History['Date'] <= dateend)].Volume.values
                    
                    scale =  4 * ( max(self.History[(self.History['Symbol'] == sym)
                           & (self.History['Date'] >= datestart)
                           & (self.History['Date'] <= dateend)].Volume)
                            / max(self.History[(self.History['Symbol'] == sym)
                           & (self.History['Date'] >= datestart)
                           & (self.History['Date'] <= dateend)].Close))
                    
                    scaled_volumes = volumes / scale


                    ax[i].plot(dates,prices)
                    ax[i].bar(dates.tolist(),scaled_volumes, alpha = 0.7)
                    #ax[i].grid(which='both')
                    ax[i].set_title('{} from {} to {}\n Metric = {}'.format(sym,datestart,dateend,metric), fontsize=16)
                    

            plt.show()
            
        if metric == 'pctchg':

            for i in range(rows):
                    sym = symbols[i]

                    dates = self.History['Date'][(self.History['Symbol'] == sym) 
                     &(self.History['Date'] >= datestart)
                     & (self.History['Date'] <= dateend)]

                    pctChgs = self.History[(self.History['Symbol'] == sym)
                               & (self.History['Date'] >= datestart)
                               & (self.History['Date'] <= dateend)].DailyPctChg.values
                    
                    volumes = self.History[(self.History['Symbol'] == sym)
                               & (self.History['Date'] >= datestart)
                               & (self.History['Date'] <= dateend)].Volume.values
                    
                    scale =  4 * ( max(self.History[(self.History['Symbol'] == sym)
                           & (self.History['Date'] >= datestart)
                           & (self.History['Date'] <= dateend)].Volume)
                            / max(self.History[(self.History['Symbol'] == sym)
                           & (self.History['Date'] >= datestart)
                           & (self.History['Date'] <= dateend)].DailyPctChg))
                    
                    scaled_volumes = volumes / scale


                    ax[i].plot(dates,pctChgs)
                    ax[i].bar(dates.tolist(),scaled_volumes, alpha = 0.7)
                    #ax[i].grid(which='both')
                    ax[i].set_title('{} from {} to {}\n Metric = {}'.format(sym,datestart,dateend,metric), fontsize=16)
                    
            plt.show()
            
         
    def tsPlotMavg(self,
                     symbols = [],
                     windows = [1,5,30],
                      metric = 'price',
                        mavg_df = False,
                     datestart = (str((datetime.datetime.now().date() + datetime.timedelta(weeks  =-4)).year) 
                          + '-' + str((datetime.datetime.now().date() + datetime.timedelta(weeks  =-4)).month) + '-' 
                          + str((datetime.datetime.now().date() + datetime.timedelta(weeks  =-4)).day)),
                      dateend = (str((datetime.datetime.now().date().year))
                          + '-' + str((datetime.datetime.now().date().month))
                          + '-' + str((datetime.datetime.now().date().day)))):
        
        
        
        
        ncols = 1
        nrows = max([2,len(symbols)])
        rows = len(symbols)
        fig, ax = plt.subplots(nrows = nrows, ncols = ncols, figsize=(16,8*nrows))
        
        
        if metric == 'price':
            
            
            
            for i in range(rows):
                sym = symbols[i]
                
                
                dates = self.History['Date'][(self.History['Symbol'] == sym) 
                     &(self.History['Date'] >= datestart)
                     & (self.History['Date'] <= dateend)]

                prices = self.History[(self.History['Symbol'] == sym)
                           & (self.History['Date'] >= datestart)
                           & (self.History['Date'] <= dateend)].Close
                
                
                for window in windows:
                    
                    label = str(window) + ' day mavg'
                    mavgs = prices.rolling(window=window).mean().fillna(0)
                    
                    ax[i].plot(dates,mavgs, label = label)
                    ax[i].set_title('{} Rolling Averages from {} to {}\n Metric = {}'.format(sym,datestart,dateend,metric))
                    ax[i].legend()
                    
                    
            plt.legend()
            plt.show()
            
    
    def plotACF(self, symbols = [], periods = 30, calc_only = False):

        ax = self.acf_df[self.acf_df.Symbol in symbols].plot(kind='bar'
                    , figsize=(16,8)
                    , grid=True
                    , position = 0
                    , title = 'Autocorrelation Corelleogram\n{}'.format(symbols)
                    )

        ax.set_xlabel('Lag (days)')
        ax.set_ylabel('r')
        plt.show()
        
                    
################################# Calculation Methods ####################################        
      
    
    def calcACF(self, periods = 30):
        
        symbols = self.History.Symbol.unique()
        acfs = []
        lags = range(1,periods)
        for sym in symbols:
            sym_pctchg = self.History[self.History['Symbol'] == sym].DailyPctChg
            sym_acf = [sym_pctchg.autocorr(lag = i) for i in lags]
            acfs.append(sym_acf)
            
        self.acf_df = pd.DataFrame(acfs).T
        self.acf_df.columns = symbols
        self.acf_df.index += 1
    
    
    def calcMavg(self, windows = [1,5,30]):
        
        if self.mavg_df is None:
            datestart = min(self.History.Date)
        else:
            datestart = max(self.mavg_df.Date)
            
        dateend = (str((datetime.datetime.now().date().year))
                  + '-' + str((datetime.datetime.now().date().month))
                  + '-' + str((datetime.datetime.now().date().day)))
        

        symbols = self.History.Symbol.unique()

        mavg_dict = {'Date':self.History[(self.History['Date'] >= datestart) 
                                              & (self.History['Date'] <= dateend)].Date.unique()}

        for symbol in symbols:

            for window in windows:

                label = symbol + '-' + str(window) + 'mavg'

                mavg_dict[label] = self.History[(self.History['Symbol'] == symbol) &
                                             (self.History['Date'] >= datestart) 
                                              & (self.History['Date'] <= dateend)].Close.rolling(window=window).mean().values

        self.mavg_df = pd.DataFrame(mavg_dict)




################################# Maintenance Methods ################################

    def saveHistory(self, path):
        '''C:/Users/awright/Desktop/January 2018/Other/trading/Market Histories'''
        self.History.to_csv(path)
        print('History saved\nFile located at {}'.format(path))
        
    def saveAcfDf(self, path):
        
        self.acf_df.to_csv(path)
        print('Autocorrelation DataFrame saved\nFile located at {}'.format(path))
        
    def saveMavgDf(self, path):
        
        self.mavg_df.to_csv(path)
        print('Moving Average DataFrame saved\nFile located at {}'.format(path))
        
    def saveExceptions(self, path):
        
        pd.Series(self.exceptions).to_csv(path)
        print('Exceptions DataFrame saved\nFile located at {}'.format(path))
    
    
    def trimHistory(self,
                    threshold = 10,
                    save_to_file = False,
                    path = None):
        ''' C:/Users/awright/Desktop/January 2018/Other/trading/Market Histories/WIKIP2000_Excludes.csv '''
        
        expected_count = max(self.History.groupby('Symbol').count().Description)
        min_count = expected_count - threshold
        
        self.excludes = self.History.groupby('Symbol').count()[self.History.groupby('Symbol').count().Description < min_count].index.tolist()
        self.History = self.History[~self.History.Symbol.isin(self.excludes)]
        print('The following assets were removed from History:\n{}'.format(str(self.excludes)))
        
        if save_to_file == True:
            pd.Series(self.excludes).to_csv('C:/Users/awright/Desktop/January 2018/Other/trading/Market Histories/WIKIP2000_Excludes.csv'
                           , index = False)
            
            print('Your excludes file has been saved to {}'.format(path))
            
    
    def fillMissingData(self):
        
        pivot = self.History.pivot(index='Date',columns='Symbol',values='Close').isnull()
        pivot = pivot.reset_index()
        unpivot = pd.melt(pivot, id_vars = 'Date', value_vars=pivot.columns[1:])

        missing_data = unpivot[unpivot.value == True]

        subdatas = []

        # For each identified missing date/symbol pair in missing_data, identify the most recent previous existing record and
        # substitute that value in its place.
        for i in range(len(missing_data)):
            breaker = 0
            symbol = missing_data.iloc[i].Symbol
            date = missing_data.iloc[i].Date.to_pydatetime()

            if date.weekday() == 0:
                back = 3
            elif date.weekday() == 6:
                back = 2
            else:
                back = 1

            prevdate = date + datetime.timedelta(days=-back)

            while self.History[(self.History.Date == prevdate) & (self.History.Symbol == symbol)].shape[0] == 0:

                prevdate = prevdate + datetime.timedelta(days=-1)

                if prevdate <= min(self.History.Date):

                    self.History = self.History[~(self.History.Symbol == symbol)]
                    breaker = 1
                    break

            if breaker == 1:
                print('Data Substitution Failed. All {} data removed from History'.format(symbol))
                pass
            else:
                ind = self.History[(self.History.Date == prevdate) & (self.History.Symbol == symbol)].index.values[0]
                subdata = self.History[self.History.index == (ind)].to_dict('records')[0]
                subdata['Date'] = date
                subdata['DailyPctChg'] = 0
                subdatas.append(subdata)

        # Lastly, append all the new (substitution) records onto the old History DataFrame, and then reorganize so we're back in order.
        self.History = pd.concat([self.History, pd.DataFrame(subdatas)], ignore_index = True)
        self.History = self.History.sort_values(by=['Symbol','Date'])
        print('The following data points were filled with the most recent existing data point:')
        print(missing_data)
            
        
            
    