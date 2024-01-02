from dagster import Definitions, load_assets_from_modules

from .assets import scrape_ticket_prices, scrape_country_links, scrape_match_links

defs = Definitions(
    assets=[scrape_ticket_prices, scrape_match_links, scrape_country_links],
)
