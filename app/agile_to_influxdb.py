#!/usr/bin/env python

from configparser import ConfigParser
from urllib import parse

import click
import maya
import requests
from influxdb import InfluxDBClient


def retrieve_paginated_data(
        api_key, url, from_date, to_date, page=None
):
    args = {
        'period_from': from_date,
        'period_to': to_date,
    }
    if page:
        args['page'] = page
    response = requests.get(url, params=args, auth=(api_key, ''))
    response.raise_for_status()
    data = response.json()
    results = data.get('results', [])
    if data['next']:
        url_query = parse.urlparse(data['next']).query
        next_page = parse.parse_qs(url_query)['page'][0]
        results += retrieve_paginated_data(
            api_key, url, from_date, to_date, next_page
        )
    return results


def store_series(connection, series, agile_rates):
    if not agile_rates:
        raise click.ClickException('No Agile rate data to store')

    # Note that this is using rates' "from" timestamps as keys, rather than the "to" timestamps as used in the main
    #  "consumption" code.
    agile_rates_with_from_dates = {
        point['valid_from']: point['value_inc_vat']
        for point in agile_rates
    }
    # agile_rates_with_from_dates = {
    #     maya.parse(point['valid_from']): point['value_inc_vat']
    #     for point in agile_rates
    # }

    def fields_for_measurement(measurement):
        # e.g. for `valid_from`=16:00 and `valid_to`=16:30:
        # InfluxDB timestamp = 16:30 (`valid_to`), to be consistent with `interval_end` used in the consumption code.
        # Store two fields:
        #  `agile_rate_prev`: the Agile rate from 16:00 to 16:30 - "the previous period" (relative to the timestamp)
        #  `agile_rate_next`: the Agile rate from 16:30 to 17:00 - "the next period" (relative to the timestamp)
        # This allows easy retrieval, from InfluxDB, of the "prev" rate that matches the stored consumption data, for
        #  easier calculations, and of the "next" rate which is more useful for displaying the Agile rate in a graph.
        # This is less necessary when using Flux to retrieve data, rather than InfluxQL, as Flux has an experimental
        #  `subDuration()` function, but Flux support in Grafana is not yet stable (and indeed is barely usable), so
        #  a workaround is required, at least until Flux is more widely-supported and widely-used.
        valid_from_iso = maya.parse(measurement['valid_from']).iso8601()  # e.g. 16:00
        valid_to_iso = maya.parse(measurement['valid_to']).iso8601()  # e.g. 16:30
        # valid_from = maya.parse(measurement['valid_from'])  # e.g. 16:00
        # valid_to = maya.parse(measurement['valid_to'])  # e.g. 16:30

        agile_rate_prev = agile_rates_with_from_dates.get(valid_from_iso, None)  # e.g. rate for 16:00-16:30
        agile_rate_next = agile_rates_with_from_dates.get(valid_to_iso, None)  # e.g. rate for 16:30-17:00
        # agile_rate_prev = agile_rates_with_from_dates.get(valid_from, None)  # e.g. rate for 16:00-16:30
        # agile_rate_next = agile_rates_with_from_dates.get(valid_to, None)  # e.g. rate for 16:30-17:00

        fields = {
            'agile_rate_prev': agile_rate_prev,
            'agile_rate_next': agile_rate_next,
        }
        return fields

    measurements = [
        {
            'measurement': series,
            'tags': {},
            'time': measurement['valid_to'],
            'fields': fields_for_measurement(measurement),
        }
        for measurement in agile_rates
    ]
    connection.write_points(measurements)


@click.command()
@click.option(
    '--config-file',
    default="octograph.ini",
    type=click.Path(exists=True, dir_okay=False, readable=True),
)
# @TODO Fetch Agile data for following day if this is run after whatever ~4-6pm-ish time the Agile data is made
#        available. (The Octopus API docs say that "day-ahead [Agile] prices are normally created by 4pm in the
#        Europe/London timezone" [https://developer.octopus.energy/docs/api/#agile-octopus], but the IFTTT process
#        consistently notifies me at 17:14 each day...)
# @TODO Can we get rid of that "cludge" with DST changeover? Storing dates in agile_rates dict as unix timestamps or
#        similar might help here?
# @TODO Use a new config file option to determine whether to store `agile_rate` and/or `agile_rate_{prev,next}`, to ease
#        migration and avoid breaking things.
# @TODO Check the Click docs to see about combining this into the main app file as a separate command.
@click.option('--from-date', default='yesterday midnight', type=click.STRING)
@click.option('--to-date', default='midnight in 2 days', type=click.STRING)
def cmd(config_file, from_date, to_date):

    config = ConfigParser()
    config.read(config_file)

    influx = InfluxDBClient(
        host=config.get('influxdb', 'host', fallback="localhost"),
        port=config.getint('influxdb', 'port', fallback=8086),
        username=config.get('influxdb', 'user', fallback=""),
        password=config.get('influxdb', 'password', fallback=""),
        database=config.get('influxdb', 'database', fallback="energy"),
    )

    api_key = config.get('octopus', 'api_key')
    if not api_key:
        raise click.ClickException('No Octopus API key set')

    agile_url = config.get('electricity', 'agile_rate_url', fallback=None)
    timezone = config.get('electricity', 'unit_rate_low_zone', fallback=None)

    from_iso = maya.when(from_date, timezone=timezone).iso8601()
    to_iso = maya.when(to_date, timezone=timezone).iso8601()

    click.echo(
        f'Retrieving Agile rates for {from_iso} until {to_iso}...',
        nl=False
    )
    agile_rates = retrieve_paginated_data(
        api_key, agile_url, from_iso, to_iso
    )
    click.echo(f' {len(agile_rates)} rates.')
    store_series(influx, 'electricity', agile_rates)


if __name__ == '__main__':
    cmd()
