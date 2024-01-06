from dagster import asset, op
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
import pandas_gbq
import pandas as pd
import re
import datetime 
import db_dtypes

project_id = 'ams-ticket-tracker'

@asset
def scrape_country_links(context) -> None:
    
    home_url = 'https://www.livefootballtickets.com/euro-cup-tickets.html'
    response = requests.get(home_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    country_links = soup.select('div.team-list a.team-list-item')

    country_pages_content = []
    
    all_match_links = []
    
    # Iterate through the list of country links
    for link in country_links:
        country_url = link['href']
        country_name = link.get_text(strip=True)
        
        # Send a GET request to the country page
        country_response = requests.get(country_url)
        country_page_content = country_response.text
    
        # Store the URL and content in a dictionary, then add to the list
        country_pages_content.append({
            'country_name': country_name,
            'url': country_url,
            'html_content': country_page_content
        })
    
    country_df = pd.DataFrame(country_pages_content)
    pandas_gbq.to_gbq(country_df, 'raw.country_links', project_id=project_id, if_exists='replace')

@asset()
def scrape_match_links() -> None:
    # Configure BigQuery client
    client = bigquery.Client()

    query = """
    SELECT DISTINCT url FROM `raw.country_links`
    """
    # Execute the query
    query_job = client.query(query)
    urls = [row.url for row in query_job]

    all_match_links = []
    matches_data = []

    # Iterate through URLs to find match URLs
    for country_url in urls:

        country_soup = BeautifulSoup(requests.get(country_url).text, 'html.parser')   

        match_links = country_soup.select('.event-list-item-inner .event-info-name a')

    # Extract the 'href' attribute (the URL) from each link
        match_urls = [a['href'] for a in match_links]

        for match_url in match_urls:
            print(match_url)
            if match_url in all_match_links:
                pass
            else:
                all_match_links.append(match_url)

                match_soup = BeautifulSoup(requests.get(match_url).text, 'html.parser')
                venue = match_soup.find(attrs={"data-qa": "event-venue"}).get_text(strip=True)
                # Find the element with the data-qa attribute set to 'event-location'
                location = match_soup.find(attrs={"data-qa": "event-location"}).get_text(strip=True)
                tickets_available = match_soup.find(class_="tickets-available").span.get_text(strip=True)
                tickets = match_soup.select('li.ticket-row')

                # Find the element with the class 'box-header--title'
                title_element = match_soup.find(class_="box-header--title").get_text(strip=True)

                title_pattern = r'MATCH (\d+) - (.+?)v(.+) Tickets'
                title_match = re.search(title_pattern, title_element)

                # Define a regex pattern to extract the team codes from the URL
                url_pattern = r'match-\d+-(\w+)-v-(\w+)-tickets'
                url_matched = re.search(url_pattern, match_url)

                if title_match and url_matched:
                    match_number = title_match.group(1)
                    team_one = title_match.group(2).strip()
                    team_two = title_match.group(3).strip()
                    team_one_code = url_matched.group(1).upper()
                    team_two_code = url_matched.group(2).upper()
                    if team_one_code == 'GERMANY':
                        team_one_code = 'A1'
                    # print(match_number, team_one, team_two, team_one_code, team_two_code)

                    match_dict = {
                        'match_url': match_url,
                        'match_id': match_number,
                        'venue': venue,
                        'partition': 1,
                        'home_country': team_one,
                        'away_country': team_two,
                        'home_country_id': team_one_code,
                        'away_country_id': team_two_code
                    }

                    matches_data.append(match_dict)
    matches_df = pd.DataFrame(matches_data)
    pandas_gbq.to_gbq(matches_df, 'raw.matches_main', project_id=project_id, if_exists='replace')


@op
def scrape_ticket_prices_op(context):
    # Configure BigQuery client
    client = bigquery.Client()

    query = """
    SELECT DISTINCT match_id, match_url FROM `raw.matches_main`
    """
    # Execute the query
    query_job = client.query(query)
    matches = [{'match_id': row.match_id, 'match_url': row.match_url} for row in query_job]

    ticket_data = []
    # Iterate through match URLs to scrape ticket information
    for match in matches:
        match_id = match['match_id']
        match_url = match['match_url']
        # print(match_id, match_url)
        response = requests.get(match_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        tickets_available = soup.find(class_="tickets-available").span.get_text(strip=True)

        # Your existing logic to find and iterate over tickets
        tickets = soup.select('li.ticket-row')  # Update the selector as needed
        for ticket in tickets:
            
            stadium_id = ticket['data-stadium-id']
            ticket_heading = ticket.find('h5', class_='ticket-heading').text
            ticket_price = ticket.find('div', class_='ticket-price').h4.text

            ticket_quantity_div = ticket.find('div', class_='ticket-quantity')
            select_element = ticket_quantity_div.find('select')
            option_values = [int(option['value']) for option in select_element.find_all('option')]
            ticket_quantities = max(option_values) if option_values else None

            ticket_category = soup.find('h5', class_='ticket-heading').text
            
            ticket_dict = {
                # Populate with actual match details
                'match_id': match_id,  # You need to define how to extract or set match_id
                'stadium_id': stadium_id,
                'tickets_available': tickets_available,  # Define how to extract tickets available
                'ticket_heading': ticket_heading,
                'category': ticket_category,
                'ticket_price': ticket_price,
                'ticket_quantities': ticket_quantities,
                'home_country_id': 'null',
                'away_country_id': 'null'
                # Define these or ensure they are extracted correctly
            }
            ticket_data.append(ticket_dict)

    tickets_df = pd.DataFrame(ticket_data)
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    current_time = datetime.datetime.now()
    table_name_suffix = current_time.strftime('%Y%m%d%H')
    tickets_table_name = f"raw.tickets_main_{table_name_suffix}"
    pandas_gbq.to_gbq(tickets_df, tickets_table_name, project_id=project_id, if_exists='append')