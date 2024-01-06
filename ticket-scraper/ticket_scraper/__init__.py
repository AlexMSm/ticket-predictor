from dagster import Definitions, load_assets_from_modules

from .assets import scrape_ticket_prices_op, scrape_country_links, scrape_match_links
from .jobs import scrape_job

defs = Definitions(
    assets=[scrape_match_links, scrape_country_links],
    jobs=[scrape_job]
)
