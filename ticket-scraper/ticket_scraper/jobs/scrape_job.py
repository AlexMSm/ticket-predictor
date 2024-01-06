from dagster import JobDefinition, job, ScheduleDefinition

from ..assets import scrape_ticket_prices_op



@job
def scrape_job():
    scrape_ticket_prices_op()

basic_schedule = ScheduleDefinition(job=scrape_job, cron_schedule="0 10,22 * * *", execution_timezone="UTC")

