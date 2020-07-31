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


def store_series(connection, series, metrics, rate_data):

    agile_data = rate_data.get('agile_unit_rates', [])
    agile_rates_to = {
        point['valid_to']: point['value_inc_vat']
        for point in agile_data
    }
    agile_rates_from = {
        point['valid_from']: point['value_inc_vat']
        for point in agile_data
    }

    def active_rate_field(measurement):
        if series == 'gas':
            return 'unit_rate'
        elif not rate_data['unit_rate_low_zone']:  # no low rate
            # TODO This way of determining the tariff type could do with improvement
            return 'unit_rate_high'

        low_start_str = rate_data['unit_rate_low_start']
        low_end_str = rate_data['unit_rate_low_end']
        low_zone = rate_data['unit_rate_low_zone']

        measurement_at = maya.parse(measurement['interval_start'])

        low_start = maya.when(
            measurement_at.datetime(to_timezone=low_zone).strftime(
                f'%Y-%m-%dT{low_start_str}'
            ),
            timezone=low_zone
        )
        low_end = maya.when(
            measurement_at.datetime(to_timezone=low_zone).strftime(
                f'%Y-%m-%dT{low_end_str}'
            ),
            timezone=low_zone
        )
        low_period = maya.MayaInterval(low_start, low_end)

        return \
            'unit_rate_low' if measurement_at in low_period \
            else 'unit_rate_high'

    def fields_for_measurement(measurement):
        consumption = measurement['consumption']
        rate = active_rate_field(measurement)
        rate_cost = rate_data[rate]
        cost = consumption * rate_cost
        standing_charge = rate_data['standing_charge'] / 48  # 30 minute reads
        fields = {
            'consumption': consumption,
            'cost': cost,
            'total_cost': cost + standing_charge,
        }
        if agile_data:
            agile_standing_charge = rate_data['agile_standing_charge'] / 48
            agile_unit_rate = agile_rates_to.get(
                maya.parse(measurement['interval_end']).iso8601(),
                rate_data[rate]  # cludge, use Go rate during DST changeover
            )
            agile_cost = agile_unit_rate * consumption
            fields.update({
                'agile_rate': agile_unit_rate,
                'agile_cost': agile_cost,
                'agile_total_cost': agile_cost + agile_standing_charge,
            })
        return fields

    def tags_for_measurement(measurement):
        period = maya.parse(measurement['interval_end'])
        time = period.datetime().strftime('%H:%M')
        return {
            'active_rate': active_rate_field(measurement),
            'time_of_day': time,
        }

    measurements = [
        {
            'measurement': series,
            'tags': tags_for_measurement(measurement),
            'time': measurement['interval_end'],
            'fields': fields_for_measurement(measurement),
        }
        for measurement in metrics
    ]
    connection.write_points(measurements)


@click.command()
@click.option(
    '--config-file',
    default="octograph.ini",
    type=click.Path(exists=True, dir_okay=True, readable=True),
)
# @TODO This whole script could do with a lot more comments to explain what's going on...
# @TODO "yesterday midnight" will only work if this is run prior to midnight (give or take some DST stuff); I had a
#        cron job running this at 1am and that caused Problems!
# @TODO Add an option for JUST fetching Agile data. Will need to tweak the script so that Agile data can be inserted to
#        Influx without any corresponding consumption data.
# @TODO Fetch Agile data for next day if run after whatever ~4-5pm UTC time at which Agile data is made available.
# @TODO All data is being stored with timestamps of the *end* of the 30-min period, which might make sense for
#        consumption data, but makes it extremely difficult to plot Agile rates in Grafana (without Flux)!
# @TODO Fetch more than one day of data by default so that we automatically fill in any gaps in the dataset caused by
#        minor, temporary issues (e.g. with Octopus' API; with the smart meters; with this script or the hardware &
#        Internet connection on which it relies; etc.). InfluxDB should simply update any existing records rather
#        than duplicating data.
# @TODO Would be nice to avoid adding 'cost' and 'total_cost' fields (which are for Go) when using Agile.
# @TODO I'm pretty sure the active_rate_field() and tags_for_measurement() functions are inserting meaningless
#        `active_rate`="unit_rate_high" tags into our Agile data.
# @TODO Not sure what the point of the `time_of_day` tag is, either, since it essentially duplicates the record's
#        standard timestamp:
#            measurement-point = {
#                'tags': [ 'time_of_day': maya.parse(measurement['interval_end']).datetime().strftime('%H:%M') ],
#                'time': measurement['interval_end']
#            } # (Abridged)
#            Unless a time_of_day tag makes some queries easier?
# @TODO Can we get rid of that "cludge" with DST changeover? Storing dates in agile_rates dict as unix timestamps or
#        similar might help here?
# @TODO Gas data is returned by the API in either kWh or m³ depending on the type of smart meter:
#        "SMETS1 Secure" = kWh; "SMETS2" = m³.
#        Reference: https://developer.octopus.energy/docs/api/#consumption
#        The tariff charges for gas per kWh, so if data is returned as m³ (can this even be detected‽) it needs to be
#         converted to kWh using:
#            kWh = m³ * [calorific value] * 1.02264 / 3.6
#        1.02264 is the "volume correction" to account for variations in temperature and pressure.
#         It's a fixed value and is set by [the Gas (Calculation of Thermal Energy) Regulations 1996](https://www.legislation.gov.uk/uksi/1996/439/made/data.pdf),
#         so it applies equally to everyone in the UK, as far as I can tell.
#        "x / 3.6" is the standard conversion from MJ (megajoules) to kWh.
#        The calorific value (in MJ/m³) varies over time and between customers, though "gas transporters are required
#         to maintain this figure within 38 MJ/m³ to 41 MJ/m³" [https://www.gov.uk/guidance/gas-meter-readings-and-bill-calculation#step-3].
#        This is a fairly narrow range, so simply using the midpoint of 39.5 would result in calculations having an
#         error within 4%, which is probably sufficient for our purposes.
#        My own bills between 2015 and 2020 have only had calorific values between 39.0 and 39.6, so 39.5 would
#         always be within 1.5%.
#        Alternatively a new configuration option could be added for it.
#        We probably at least need a new config option to specify kWh or m³ metering.
#        Reference: https://www.gov.uk/guidance/gas-meter-readings-and-bill-calculation
@click.option('--from-date', default='yesterday midnight', type=click.STRING)
@click.option('--to-date', default='today midnight', type=click.STRING)
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

    e_mpan = config.get('electricity', 'mpan', fallback=None)
    e_serial = config.get('electricity', 'serial_number', fallback=None)
    if not e_mpan or not e_serial:
        raise click.ClickException('No electricity meter identifiers')
    e_url = 'https://api.octopus.energy/v1/electricity-meter-points/' \
            f'{e_mpan}/meters/{e_serial}/consumption/'
    agile_url = config.get('electricity', 'agile_rate_url', fallback=None)

    g_mpan = config.get('gas', 'mpan', fallback=None)
    g_serial = config.get('gas', 'serial_number', fallback=None)
    if not g_mpan or not g_serial:
        raise click.ClickException('No gas meter identifiers')
    g_url = 'https://api.octopus.energy/v1/gas-meter-points/' \
            f'{g_mpan}/meters/{g_serial}/consumption/'

    timezone = config.get('electricity', 'unit_rate_low_zone', fallback=None)

    rate_data = {
        'electricity': {
            'standing_charge': config.getfloat(
                'electricity', 'standing_charge', fallback=0.0
            ),
            'unit_rate_high': config.getfloat(
                'electricity', 'unit_rate_high', fallback=0.0
            ),
            'unit_rate_low': config.getfloat(
                'electricity', 'unit_rate_low', fallback=0.0
            ),
            'unit_rate_low_start': config.get(
                'electricity', 'unit_rate_low_start', fallback="00:00"
            ),
            'unit_rate_low_end': config.get(
                'electricity', 'unit_rate_low_end', fallback="00:00"
            ),
            'unit_rate_low_zone': timezone,
            'agile_standing_charge': config.getfloat(
                'electricity', 'agile_standing_charge', fallback=0.0
            ),
            'agile_unit_rates': [],
        },
        'gas': {
            'standing_charge': config.getfloat(
                'gas', 'standing_charge', fallback=0.0
            ),
            'unit_rate': config.getfloat('gas', 'unit_rate', fallback=0.0),
        }
    }

    from_iso = maya.when(from_date, timezone=timezone).iso8601()
    to_iso = maya.when(to_date, timezone=timezone).iso8601()

    click.echo(
        f'Retrieving electricity data for {from_iso} until {to_iso}...',
        nl=False
    )
    e_consumption = retrieve_paginated_data(
        api_key, e_url, from_iso, to_iso
    )
    click.echo(f' {len(e_consumption)} readings.')
    click.echo(
        f'Retrieving Agile rates for {from_iso} until {to_iso}...',
        nl=False
    )
    rate_data['electricity']['agile_unit_rates'] = retrieve_paginated_data(
        api_key, agile_url, from_iso, to_iso
    )
    click.echo(f' {len(rate_data["electricity"]["agile_unit_rates"])} rates.')
    store_series(influx, 'electricity', e_consumption, rate_data['electricity'])

    click.echo(
        f'Retrieving gas data for {from_iso} until {to_iso}...',
        nl=False
    )
    g_consumption = retrieve_paginated_data(
        api_key, g_url, from_iso, to_iso
    )
    click.echo(f' {len(g_consumption)} readings.')
    store_series(influx, 'gas', g_consumption, rate_data['gas'])


if __name__ == '__main__':
    cmd()