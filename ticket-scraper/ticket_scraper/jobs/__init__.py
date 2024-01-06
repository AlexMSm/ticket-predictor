from dagster import Definitions, asset, define_asset_job
from .scrape_job import scrape_job


# ticket_scrape_job = define_asset_job(name="ticket_scrape_job", selection="scrape_ticket_prices")