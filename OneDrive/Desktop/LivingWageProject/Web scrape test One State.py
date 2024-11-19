import requests
from bs4 import BeautifulSoup
import pandas as pd

 #FIX: ACCOUNT BY STATE (Get dataframe for Each State)
def ProcessLivingWagesState(state_name,state_id):
    """
    Process a State and Manage CSV Data File
    """
    # Base URL for the counties
    base_url = 'https://livingwage.mit.edu/counties/'
    # Send a request to the server
    # List to store the dfs
    all_dfs = []
    # Loop through the list of counties
    for county_name, county_id in counties_id_info.items():
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

        top_headers = []
        for th in table.find_all('thead')[0].find_all('th'):
            # Extend each top header across its colspan
            colspan = int(th.get('colspan',1))
            top_headers.extend([th.get_text(strip=True).replace('\xa0',' ')]* colspan)

        # Extract subheaders (0 Children, 1 Child, etc.)
        sub_headers = []
        for th in table.find_all('thead')[1].find_all('td'):
            sub_headers.append(th.get_text(strip=True).replace('\xa0',' '))
        # Combine the two header levels
        headers = ['Wage Type']  # Start with Wage Type as the first column

        for top, sub in zip(top_headers,sub_headers):
            headers.append(f'{top} - {sub}')
        del headers[1]
        headers.extend(["county_name","county_id"])

        one_county_rows = []
        for tr in table.find_all('tbody')[0].find_all('tr'):
            row = [tr.find('td', {'class':'wage_title'}).get_text(strip=True)]
            for td in tr.find_all('td')[1:]:
                row.append(td.get_text(strip=True))
            row.extend([county_name, county_id])
            one_county_rows.append(row)

        df = pd.DataFrame(one_county_rows, columns=headers)
        ##Each individual county dataframe
        #print(one_county_rows)
        all_dfs.append(df)
        # Concatenate all DataFrames into a single DataFrame
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(combined_df)
        combined_df.to_csv(r"C:\Users\cecil\OneDrive\Desktop\LivingWageProject\California_06_LivingWage.csv", index = False)
           
    #combined_df.to_csv('C:\Users\CeciliaNguyen\OneDrive - Futuro Health O365\Desktop\AllStates DF LivingWageWebScrape (Cleaned).csv')
    #combined_df.to_csv('C:\Users\cecil\OneDrive\Desktop\CA_test_county_df.csv\{}_{}'.format(county_id[:2]))
####################################HELPER FUNCTION

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
state_id_info = getStateID(url)

##################################
state_dict = {'California': '06'} #Will include Full state list
county_dict = {}

def getCountyID(state_dict):
    """""""""
    Returns Dictionary of Counties by State -- > {Santa Clara County: 0629, ...}
    """""""""
    for state, id in state_dict.items():
        # URL of the page with the list of counties (CA)
        url = f'https://livingwage.mit.edu/states/{state_dict[state]}/locations'

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

        print(county_dict)
        return county_dict

####################
counties_id_info = getCountyID(state_dict)
counties_id_info


  

#Make sure id ascending

#Iterate through States
for state_name, state_id in state_dict.items():
    #final_county_df_state = ProcessLivingWagesState(state_name,state_id)
    final_county_df_state = ProcessLivingWagesState(state_name,state_id)
    final_county_df_state
