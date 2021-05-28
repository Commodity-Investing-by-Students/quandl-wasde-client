# Created by Maximo Xavier DeLeon
import pandas as pd
import math, numpy as np
from usdadata import WasdeClient




def main():
    report = WasdeClient(quandl_api_key=None) # create an instance of the wasde client class

    soybean_data_list = report.query(commodity='Soybeans', region='United States', world_summary=False,cleaned=True) # query our soybean data into a tuple
    beans_current_year, beans_last_year, beans_two_years_ago = soybean_data_list # assign each nested dataframe in the list to an actual variable

    beans_current_year = beans_current_year.rename(columns={'Use, Total (Crushings+Exports+Seed+Residual)':'Total Use', 'Supply, Total (Beginning Stocks+Production+Imports)':'Total Supply'})

    beans_current_year['Ending Stocks'] = beans_current_year['Total Supply'] - beans_current_year['Total Use']
    beans_current_year['Stocks to Use Ratio %'] = ((beans_current_year['Total Supply'] - beans_current_year['Total Use']) / beans_current_year['Total Supply'])*100
    if 'None' in beans_current_year.columns:
        beans_current_year.drop('None',axis=1)
    else: pass
    #beans_current_year[['Total Use','Total Supply']].plot()
    #beans_current_year[['Ending Stocks']].plot()
    print(beans_current_year)

if __name__ == '__main__':
    main()




