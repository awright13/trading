class Portfolio:
    '''Creates a portfolio linked to a given market.  If no parameters are passed, the Portfolio is empty and
    no market is associated with the Portfolio.  
    Alternatively, you can pass into the data parameters a pre-existing Portfolio in .csv format, as well connect
    to the Quandl API to pull in the desired market data.
    '''
    
    def __init__(self, data = pd.DataFrame(), marketData = pd.DataFrame()):
        
        self.Portfolio = pd.DataFrame(data, columns = ['Symbol',
                                                       'Quantity',
                                                       'BuyDate',
                                                       'BuyPrice',
                                                       'SellDate',
                                                       'SellPrice',
                                                       'CurrentPrice',
                                                       'CurrentReturn',
                                                       'CurrentPctReturn',
                                                       'TotalReturn',
                                                       'TotalPctReturn']).astype({'Symbol':object,
                                                                                  'Quantity':int,
                                                                          'BuyDate':datetime.date,
                                                                          'BuyPrice':'float64',
                                                                          'SellDate':datetime.date,
                                                                          'SellPrice':'float64',
                                                                          'CurrentPrice':'float64',
                                                                          'CurrentReturn':'float64',
                                                                          'CurrentPctReturn':'float64',
                                                                          'TotalReturn':'float64',
                                                                          'TotalPctReturn':'float64'})
        
    
        self.yesterday = datetime.datetime.now().date() + datetime.timedelta(days  =-1)
        self.Assets = self.Portfolio.Symbol.values
        self.marketData = marketData
        self.openPositions = self.Portfolio[self.Portfolio['SellDate'].isnull()]
        
        for i in range(self.Portfolio.shape[0]):
            
            
            
            currentPrice = (self.marketData[(self.marketData['Symbol'] == self.Portfolio.loc[i,'Symbol']) 
                                            & (self.marketData['Date'] == self.yesterday)].loc[:,'Close'])
            
            update_df = pd.DataFrame({'CurrentReturn': currentPrice - self.Portfolio.loc[i,'BuyPrice']
                                     ,'CurrentPctReturn': ((currentPrice/self.Portfolio.loc[i,'BuyPrice']) - 1) * 100
                                     ,'TotalReturn': self.Portfolio.loc[i,'SellPrice'] - self.Portfolio.loc[i,'BuyPrice']
                                     ,'TotalPctReturn': ((self.Portfolio.loc[i,'SellPrice']/self.Portfolio.loc[i,'BuyPrice']) - 1)*100}
                                     , index = [i])     
            
            self.Portfolio.update(update_df)
            
         
        
        
    def buy(self,
            symbol,
            quantity = 1,
            buyDate =  datetime.datetime.now().date() + datetime.timedelta(days  =-1),
            sellDate = datetime.date(2050,1,1) ):

        
        
        a_quantity = quantity
        a_BuyDate = buyDate
        a_BuyPrice =  self.marketData[(self.marketData['Symbol'] == symbol) & (self.marketData['Date'] == buyDate)].loc[0,'Close']
        a_SellDate = sellDate
        a_SellPrice = None
        a_CurrentReturn = 0
        a_CurrentPctReturn = 0
        a_TotalReturn = 0
        a_TotalPctReturn = 0
        
        self.Portfolio = self.Portfolio.append({'Symbol':symbol,
                                                'Quantity':a_quantity,
                                               'BuyDate':a_BuyDate,
                                               'BuyPrice':a_BuyPrice,
                                               'SellDate':a_SellDate,
                                               'SellPrice':a_SellPrice,
                                               'CurrentPrice':a_BuyPrice,
                                               'CurrentReturn':a_CurrentReturn,
                                               'CurrentPctReturn':a_CurrentPctReturn,
                                               'TotalReturn':a_TotalReturn,
                                               'TotalPctReturn':a_TotalPctReturn},
                                               ignore_index=True)
        

    def sell(self,
            symbol,
            buyDate,
            quantity = 1,
            sellDate = datetime.datetime.now().date() + datetime.timedelta(days  =-1)):

        asset_index = self.Portfolio[(self.Portfolio['Symbol'] == symbol) & (self.Portfolio['BuyDate'] == buyDate)].index[0]
        sellPrice = self.marketData[(self.marketData['Symbol'] == symbol) & (self.marketData['Date'] == sellDate)].loc[0,'Close']

        sell_df = pd.DataFrame({'SellDate':sellDate
                                , 'SellPrice':sellPrice
                                , 'CurrentPrice':sellPrice
                                ,'CurrentReturn': sellPrice - self.Portfolio.loc[asset_index,'BuyPrice']
                                ,'CurrentPctReturn': ((sellPrice/self.Portfolio.loc[asset_index,'BuyPrice']) - 1) * 100
                                ,'TotalReturn': sellPrice - self.Portfolio.loc[asset_index,'BuyPrice']
                                ,'TotalPctReturn': ((sellPrice/self.Portfolio.loc[asset_index,'BuyPrice']) - 1)*100}
                                , index = [asset_index])
        
        self.Portfolio.update(sell_df)