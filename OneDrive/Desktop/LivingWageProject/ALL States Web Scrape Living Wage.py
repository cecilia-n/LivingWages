""""
ONLY WORKS IF FILE ALREADY THERE, PROBLEMS WITH APPENDING STILL
"""


import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

#FIX: ACCOUNT BY STATE (Get dataframe for Each State)
def ProcessLivingWagesState(counties_id_info):
    """
    Process a State and Manage CSV Data File
    """
    print(counties_id_info)
    # Base URL for the counties
    base_url = 'https://livingwage.mit.edu/counties/'
    # Send a request to the server
    # List to store the dfs
    all_dfs = []
    # Loop through the list of counties
    # Loop through the counties in the DataFrame using zip
    for county_name, county_id in zip(counties_id_info['county_name'], counties_id_info['county_id']):
        # Construct the full URL for each county
        url = f'{base_url}{county_id}'
        # Send an HTTP request to the URL
        response = requests.get(url)
        # Check if the request was successful
        if response.status_code == 200:
            soup = BeautifulSoup(response.content,'html.parser')
            # Find the div containing the table by its class
            table_div = soup.find('div', class_='table-responsive table_wrap')
            # Within this div, find the table
            table = table_div.find('table', class_='results_table table-striped')

        #headers = extractHeaders(table)
        one_county_rows = []
        for tr in table.find_all('tbody')[0].find_all('tr'):
            row = [tr.find('td', {'class':'wage_title'}).get_text(strip=True)]
            for td in tr.find_all('td')[1:]:
                row.append(td.get_text(strip=True))
            row.extend([county_name, county_id])
            one_county_rows.append(row)

            county_rows = pd.DataFrame(one_county_rows)
            ##Each individual county dataframe
            #print(one_county_rows)
        all_dfs.append(county_rows)
        # Concatenate all DataFrames into a single DataFrame
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print('COMBINED DF: ',combined_df)
        return combined_df
        
    #combined_df.to_csv('C:\Users\CeciliaNguyen\OneDrive - Futuro Health O365\Desktop\AllStates DF LivingWageWebScrape (Cleaned).csv')
    #combined_df.to_csv(f'C:\Users\cecil\OneDrive\Desktop\LivingWageProject\{state_name}_{state_id}')

################################ HELPER FUNCTIONS ##################################################################################################
url = 'https://livingwage.mit.edu/'
def getStateID(url):
    """
    Returns Dictionary of State and ID --> {'CA': '06'}
    """
    # Send a request to the server
    response = requests.get(url)
    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.text,'html.parser')

        # Find all <li> elements within the <ul> that contains the states
        state_list_items = soup.find_all('li')
        state_list_items = state_list_items[4:]
        #print(state_list_items)

        # Create a dict of {state_name:state_id}
        states = {}
        # Loop through the list items and extract state names and IDs
        for li in state_list_items:
            a_tag = li.find('a')  # Find the <a> tag within each <li>
            if a_tag:
                state_name = a_tag.text.strip()  # Get the state name
                state_url = a_tag['href']  # Get the href link
                # Extract the state ID from the URL (e.g., /states/01/locations)
                state_id = state_url.split('/')[2]  #The ID is the second part of the split URL
                # Append the dict (state,id) to the list
                states[state_name]= state_id
        # Print the list of state tuples
        print(states)
        return states

##################################
state_dict = {'California': '06'} #Will include Full state list
county_dict = {}

def getCountyID(state_id):
    """""""""
    Returns Dictionary of Counties by State -- > {Santa Clara County: 0629, ...}
    """""""""
    #for state, id in state_id_info.items():
        # URL of the page with the list of counties (CA)
    url = f'https://livingwage.mit.edu/states/{state_id}/locations'

    # Send a request to the server
    response = requests.get(url)
    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.text,'html.parser')
        # Find the container with the list of counties
        counties_dict= soup.find('div',class_='counties list-unstyled')
        # Extract the links to counties
        counties = []
        for li in counties_dict.find_all('li'):
            a_tag = li.find('a')
            if a_tag:
                county_name = a_tag.text.strip()
                county_url = a_tag['href'].strip()
                county_url = county_url.replace('/counties/','')
                county_dict[county_name] =  county_url

        #print(county_dict)
        
        # Convert dictionary to DataFrame
        county_df = pd.DataFrame.from_dict(county_dict, orient='index', columns=['county_id']).reset_index()

        # Rename the index column
        county_df.rename(columns={'index': 'county_name'}, inplace=True)
        #changed to DataFrame
        return county_df



#COMPLETE SAVE CSV
def saveStateCountyLivingWage(state_name, state_id, county_df):
    """Save Dataframe to CSV, appending if file exists"""
    # Define the full file path
    save_dir = "C:/Users/cecil/OneDrive/Desktop/LivingWageProject/"
    file_name = os.path.join(save_dir, f'{state_name}_{state_id}_LivingWage.csv')
    
    # Check if the file exists; write or append accordingly
    if os.path.exists(file_name):
        with open(file_name, 'r') as filename:
            # Load existing CSV file
            curr_df = pd.read_csv(file_name)
            print('CURR DF: \n', curr_df)
            # Calculate expected rows based on the number of counties
            expected_rows = len(county_dict) * 3 # 3 rows per county
            
            # Check if row count is as expected
            if len(curr_df) == expected_rows:
                print(f"{state_name} CSV file already has the correct number of rows.")
                return  # No further action needed if data is complete
            elif len(curr_df) < expected_rows:
                # Handle incomplete data
                print(f"{state_name} CSV file has incomplete data. Expected {expected_rows} rows, found {len(curr_df)} rows.")
                
                # Identify the last processed county
                last_county= curr_df[['county_name']].iloc[-1]

                #If last row of county isn't fully 3
                print(f"Continuing from last county: {last_county}")
                #FIX THIS LOGIC!
                # Filter the new DataFrame to avoid duplicates
                # Identify the set of processed county IDs
                #processed_county_ids = set(curr_df['county_id'].unique())
                
                #ERROR NOT DATAFRAME
                print ('COUNTY DF :\n', county_df)
                # Filter out processed counties from county_dict
                #ACTUALLY WANT DATAFRAME OF COUNTY NAME AND ID, TO PROCESS
                remaining_counties_df = county_df[~county_df['county_name'].isin(curr_df['county_name'])].dropna()
                remaining_counties_df = pd.DataFrame(remaining_counties_df)
                print(f'Remaining Counties: \n {remaining_counties_df}')
                #Create Dataframe of remaining counties, including headers
                remaining_counties_df = ProcessLivingWagesState(remaining_counties_df)
                remaining_counties_df.columns = ['Wage Type', '1 ADULT - 0 Children', '1 ADULT - 1 Child', '1 ADULT - 2 Children', '1 ADULT - 3 Children', '2 ADULTS(1 WORKING) - 0 Children',
                                                 	'2 ADULTS(1 WORKING) - 1 Child', '2 ADULTS(1 WORKING) - 2 Children', '2 ADULTS(1 WORKING) - 3 Children', '2 ADULTS(BOTH WORKING) - 0 Children',
                                                    '2 ADULTS(BOTH WORKING) - 1 Child', '2 ADULTS(BOTH WORKING) - 2 Children', '2 ADULTS(BOTH WORKING) - 3 Children','county_name', 'county_id']
                # Concatenate the original DataFrame with the remaining counties DataFrame
                combined_df = pd.concat([curr_df, remaining_counties_df],axis = 0)
                
                #Include Headers for Correct Appending
                
                # Save the combined DataFrame to CSV, overwriting the original file
                combined_df.to_csv(file_name, columns = remaining_counties_df.columns, index=False)
                print(f"Appended data for remaining counties in {state_name} and saved to {file_name}.")
                return combined_df

            elif len(curr_df) < expected_rows:
                print("There's Extra Rows, Please Look Over File")
    else:
        print(f"No existing file for {state_name}. Creating a new CSV file.")
        new_state_df = ProcessLivingWagesState(county_df)
        new_state_df.to_csv(file_name, index=False)

#Make sure tom check if last three rows is county, if not, then replace that county's all 3 rows.
##TO-DO: Make sure ids ascending
#state_id_info = getStateID(url)
#counties_id_info = getCountyID(state_dict)
""""print()
#Iterate through States
for state_name, state_id in state_id_info.items():
    final_county_df_state = ProcessLivingWagesState(state_name,state_id)
    final_county_df_state
"""

#Count County from site, if greater than n amount counties, skip (Compute later)
def count_counties(state_id):
    # Construct the URL for the state's counties
    url = f'https://livingwage.mit.edu/states/{state_id}/locations' 
    # Send a request to the server
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        # Find the container with the list of counties
        counties_list = soup.find('div', class_='counties list-unstyled')
        
        # Count the number of counties
        county_count = len(counties_list.find_all('li'))  # Count <li> elements for counties
        print(county_count)
        return county_count
    else:
        print(f"Failed to retrieve counties for state ID {state_id}. Status code: {response.status_code}")
        return 0  # Return 0 if the request fails
count_counties(state_id = '06')



"""
#Iterate through States
for state_name, state_id in state_dict.items():
      # Skip states with more than 500 counties
    counties = getCountyID(state_id)
    if len(counties) >= 500:
        print(f"Skipping {state_name} ({state_id}) - too many counties: {len(counties)}.")
    else:
        ProcessLivingWagesState(state_name,state_id)

        ---INCLUDE SAVE TO CSV, CHECKING IF FILES ARE THERE

        saveStateCountyLivingWage('CA', '06')
FIX ISSUE: Append the right dataframe/county Living Wage Entry
"""


  
for state_name, state_id in state_dict.items():
      # Skip states with more than 500 counties
    county_df = getCountyID(state_id)
    if len(county_df) >= 300:
        print(f"Skipping {state_name} ({state_id}) - too many counties: {len(county_dict)}.")
    else:
        print(f'Making DataFrame for {state_name}')
        saveStateCountyLivingWage(state_name, state_id, county_df)
