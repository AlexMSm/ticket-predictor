from dagster import asset
import requests
from bs4 import BeautifulSoup

@asset
def scrape_country_links(context):
    project_id = 'ams-ticket-tracker'
    home_url = 'https://www.livefootballtickets.com/euro-cup-tickets.html'
    response = requests.get(home_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    country_links = soup.select('div.team-list a.team-list-item')

    country_pages_content = []
    
    all_match_links = []
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    current_time = datetime.datetime.now()
    table_name_suffix = current_time.strftime('%Y%m%d%H')
    
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
    
    country_table_name = f"raw.country_links_{table_name_suffix}"
    pandas_gbq.to_gbq(country_df, 'raw.country_links', project_id=project_id, if_exists='replace')


