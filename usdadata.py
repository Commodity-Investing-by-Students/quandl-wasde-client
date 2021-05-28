import pandas as pd # dataframes
import quandl # data feed
from os import path # check version capability
from datetime import date # check version capability


# Created by Maximo Xavier DeLeon
class WasdeClient:

  def __init__(self,quandl_api_key=None):
    self.today = date.today() # get the current date
    if type(quandl_api_key) is not None: # if the user has a quandl api key then use it
      quandl.ApiConfig.api_key = quandl_api_key # set the api key
    else: pass # ignore if there's no api key
    self.check_pull() # check if the wasde csv file exists and if it has been updated today
    self.commodity_list = [commodity for commodity in  self.wasde_data.commodity.unique()] # create a list of commodities in the wasde from the unique values in the commmodity column
    self.region_list = [region for region in  self.wasde_data.region.unique()] # create a list of regions in the wasde from the unique values in the region column

  # checks if the wasde csv file has been downloaded today by seeing if the csv exists and if the text file for the previous update date exists
  def check_pull(self,date_file='datefile.txt'):
    check_line = 'LAST UPDATED ON: ' + str(self.today) # this is what we are looking for in the text file to ensure our wasde csv file is updated
    if path.exists(date_file) and path.exists('WASDE_DATA.csv'):
      with open(date_file) as file: # read the text file - credit to vt cs1064 for teaching me this
        content = file.read().splitlines()

      if check_line not in content: # check if the file's update check matches the one generated
        file_to_change = open(date_file, 'w') #open/create a file
        file_to_change.write(check_line) # write the current date
        file_to_change.close() # close the file
        wasde_data = quandl.get_table("WASDE/DATA", paginate=True) # pull the data from quandl
        self.wasde_data = wasde_data.drop(['min_value',	'max_value'], axis=1) # drop the empty columns
        self.wasde_data.to_csv('WASDE_DATA.csv') # write the data to a csv file and save it

      else: self.wasde_data = pd.read_csv('WASDE_DATA.csv') # if the file has been updated already and or the wasde csv file is in the directory then open the csv and make df

    else:
      file_to_change = open(date_file, 'w') # open/create a file
      file_to_change.write(check_line) # write the line into the file
      file_to_change.close() # close the file
      wasde_data = quandl.get_table("WASDE/DATA", paginate=True) # pull the data from quandl
      self.wasde_data = wasde_data.drop(['min_value',	'max_value'], axis=1) # drop the empty columns
      self.wasde_data.to_csv('WASDE_DATA.csv') # write the data to a csv file and save it


  # returns wasde report data for a user defined agriculture commodity and region
  def query(self,commodity, region, world_summary=False,cleaned=True):
    if commodity in self.commodity_list and region in self.region_list:
      world_summary = True if region != 'United States' else world_summary # is the country is not the united states then set the ~world~ report option to true
      query_df = self.wasde_data.loc[(self.wasde_data.commodity == commodity) & (self.wasde_data.region == region)] # query the specific commodity and region
      query_df['is_world'] = self.wasde_data.code.str.contains(pat = 'WORLD') # create a tag for the report id to index for united states condition

      if region == 'United States' and world_summary is True:
        query_df = query_df.loc[query_df.is_world == True] # index rows that have the world summary tag
        query_df = query_df.drop(['is_world'], axis=1) # drop the indexing column
        return self.format(df=query_df,fancy=cleaned) # return the commodity wasde data

      elif region == 'United States' and world_summary is False:
        query_df = query_df.loc[query_df.is_world == False] # index rows that do not have the world summary tag
        query_df = query_df.drop(['is_world'], axis=1) # drop the indexing column
        return self.format(df=query_df,fancy=cleaned) # return the commodity wasde data

      else:
        query_df = query_df.drop(['is_world'], axis=1) # drop the indexing column
        return self.format(df=query_df,fancy=cleaned) # return the commodity wasde data

  # method to split up the returned dataframes into something less confusing to work with
  def format(self,df,fancy=True):
    # dictionary to store the 3 types of values the WASDE will return for each report
    wasde_frame_configs = {'current_year': (df.year.str.contains(pat= 'Proj') == True) & (df.year.str.contains(pat= 'Est') == False),
                        'last_year': (df.year.str.contains(pat= 'Proj') == False) & (df.year.str.contains(pat= 'Est') == True),
                        'two_years_ago': ( df.year.str.contains(pat= 'Proj') == False) & (df.year.str.contains(pat= 'Est') == False)}
    returned_dataframes = [] # store the dataframes to be split in here
    month_dict = {'May':5, 'Mar':3, 'Apr':4, 'Feb':2, 'Jan':1, 'Dec':12, 'Nov':11, 'Oct':10, 'Sep':9, 'Aug':8, 'Jul':7, 'Jun':6, 'Annual': 'Annual' } # dictionary coverting text month into numerical
    for wasde_config in wasde_frame_configs: # iterate through the dictionary
      df.loc[wasde_frame_configs[wasde_config] == True, 'estimate_type'] = wasde_config # conditional formatting to label the values
      if wasde_config == 'current_year':
        df.period = df.period.map(month_dict)  # conditional formatting to label the values
        df.drop(df[(pd.DatetimeIndex(df.report_month).month != df.period) & (df.period != 'Annual')].index, inplace = True) # remove the rows for the previous month estimate
      else: pass

      returned_dataframes.append(df.loc[df.estimate_type == wasde_config]) # append the dataframe to a list

    if fancy:
        df_to_clean_list = [dirty_df.drop(['region','commodity','estimate_type','code','year'],axis=1) for dirty_df in returned_dataframes] # remove the information we don't need
        returned_dataframes = [] # clear the list of "dirty" dataframes so we can put our clean ones in
        for current_df in df_to_clean_list: # iterate thorugh the list of dataframes
          report_date_list = current_df.report_month.unique() # creates a list of report releases
          items = current_df.item.unique() # create a list of items that are unique in describing crop data
          report_list = [] # this is where we will store our new dataframe rows
          for report in report_date_list: # iterate trhough each monthly report
            value_list = current_df.value.loc[current_df.report_month == report].to_list() # grab the values being used for each monthly report
            item_list = current_df.item.loc[current_df.report_month == report].to_list() # grab the items to pair with the corresponding values
            zip_object = zip(item_list,value_list) # zip the two values
            current_row = dict(zip_object) # create a dictionary for the current row
            report_list.append(current_row)  # append the dictionary as an element in our list
          cleaned_df = pd.DataFrame(data = report_list, columns=items, index=report_date_list) # build our dataframe with our nested dictionaries for each report
          cleaned_df = cleaned_df.drop('Residual 5/',axis=1)
          returned_dataframes.append(cleaned_df.iloc[::-1]) # append the dataframe to our return list then
    else: pass

    return (returned_dataframes[0], returned_dataframes[1], returned_dataframes[2]) # return the different dataframes as tuples

