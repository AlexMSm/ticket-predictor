from dagster import JobDefinition, job 

from ..assets import scrape_ticket_prices_op



@job
def scrape_job():
    scrape_ticket_prices_op()
