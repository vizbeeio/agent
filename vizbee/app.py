import os
import logging

from collections import OrderedDict

import click
import records
import requests

from apscheduler.schedulers.blocking import BlockingScheduler
from cerberus import Validator
from sqlalchemy.exc import DatabaseError

from yaml import load, dump
from yaml.error import YAMLError

from .schema import schema


logger = logging.getLogger(__name__)


API_URL = 'https://api.vizbee.io/v1'


class Item():
    @property
    def url_prefix(self):
        raise NotImplementedError()

    def log(self, message, **kwargs):
        slug = click.style(f"`{self.slug}`", fg='cyan')
        kwargs.update(dict(slug=slug))
        self.app.log(message, **kwargs)

    def push(self, open_=False):
        payload = self.payload

        self.log("Pushing: {slug}")

        response = self.app.request(
            f'/{self.url_prefix}/{self.slug}',
            data=payload,
        )

        status = response.status_code

        if status not in (200, 201):
            if status == 422:
                json = response.json()
                errors = json['errors']

                errors = self.app.format_errors(errors)

                self.log(
                    "Errors: {errors}",
                    level='warning',
                    errors=click.style(str(errors), fg='red')
                )
                return False

            self.log(
                "The server unexpectedly responded with `{status}` status",
                status=str(status),
                level='warning',
            )
            return False

        json = response.json()
        verb = 'created' if status == 201 else 'updated'
        url = json['url']
        self.log(
            "Successfully {verb} {slug}: {url}",
            verb=verb,
            url=click.style(url, fg='white')
        )

        if open_:
            click.launch(url)

        return True


class Dataset(Item):
    url_prefix = "datasets"

    def __init__(
        self,
        app,
        slug,
        query,
        connection,
        graph,
        name=None,
        schedule=None,
    ):
        self.app = app
        self.slug = slug
        self.query = query
        self.connection = connection
        self.graph = graph
        self.name = name

        if schedule is None:
            schedule = app.schedule

        self.schedule = schedule

    @property
    def payload(self):
        data = self.execute().dataset.dict

        return dict(
            name=self.name,
            graph=self.graph,
            query=self.query,
            data=data,
        )

    def execute(self):
        self.log("Executing: {slug}")
        try:
            return self.connection.query(self.query)

        except DatabaseError as e:
            self.log(str(e), level='critical')

    def schedule_job(self, scheduler):
        schedule = self.schedule

        if schedule is None:
            raise ValueError(
                f"No scheduling rule found for `{self.slug}`"
            )

        duration, unit = schedule.split(' ')

        scheduler.add_job(self.push, 'interval', **{unit: int(duration)})


class Dashboard(Item):
    url_prefix = "dashboards"

    def __init__(self, app, slug, datasets, name=None):
        self.app = app
        self.slug = slug
        self.name = name
        self.datasets = datasets

    @property
    def payload(self):
        return dict(
            name=self.name,
            datasets=self.datasets,
        )


class App():
    def __init__(self, api_url, client_id, client_secret, cli, filename):
        self.api_url = api_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.cli = cli
        self.daemonized = False

        config = self.load_config(filename)
        connections = config['connections']

        try:
            self.connections = {
                key: records.Database(url)
                for key, url in connections.items()
            }

        except Exception as e:
            self.log(
                "Error initializing connections: {e}",
                e=e,
                level='critical',
            )

        self.schedule = config.get('schedule')

        datasets = OrderedDict()

        for key, dataset in config['datasets'].items():
            datasets[key] = Dataset(
                self,
                key,
                dataset['query'],
                self.connections[dataset.get('connection', 'default')],
                dataset.get('graph'),
                dataset.get('name'),
                dataset.get('schedule'),
            )

        self.datasets = datasets

        dashboards = OrderedDict()

        configured_dashboards = config.get('dashboards')

        if configured_dashboards is not None:
            for key, dashboard in config['dashboards'].items():
                dashboards[key] = Dashboard(
                    self,
                    key,
                    dashboard.get('datasets'),
                    dashboard.get('name'),
                )

        self.dashboards = dashboards

    def format_errors(self, errors):
        if isinstance(errors, dict):
            errors = dump(errors)

        errors = str(errors)

        if self.daemonized:
            return errors

        return f"\n\n{errors}"

    def request(self, url, method='put', data=None):
        headers = {
            'Content-type': 'application/json',
            'Accept': 'application/json',
        }

        from requests.exceptions import RequestException

        try:
            return getattr(requests, method)(
                self.api_url + url,
                json=data,
                auth=(self.client_id, self.client_secret),
                headers=headers,
                allow_redirects=False,
            )

        except RequestException as e:
            self.log(
                "Error sending request: \n{e}",
                e=str(e),
                level='critical',
            )

    def load_config(self, filename):
        try:
            with open(filename, 'r') as f:
                template = "".join(f.readlines())
                template = template.format(**os.environ)
                config = load(template)

                validator = Validator()

                if not validator.validate(config, schema):
                    raise ValueError(validator.errors)

                self.schema = schema
                return config

        except FileNotFoundError:
            self.log(
                "`{filename}` not found",
                filename=filename,
                level='critical',
            )

        except (YAMLError, ValueError) as e:
            self.log(
                "Error parsing config: {e}",
                e=self.format_errors(e),
                level='critical',
            )

        except KeyError as e:
            self.log(
                "Error parsing config: \n{e} not found in environment.",
                e=str(e),
                level='critical',
            )

    def log(self, message, level='info', **context):
        message = message.format(**context)

        if level in ('critical', 'warning'):
            message = click.style(message, fg='red')

        if self.daemonized:
            getattr(logger, level)(message)

        else:
            click.echo(message)

        if level == 'critical':
            self.cli.exit(1)

    def get(self, type_, slug):
        try:
            return getattr(self, f"{type_}s")[slug]

        except KeyError:
            self.log(
                "Invalid {type_} `{slug}`",
                slug=slug,
                type_=type_,
                level='critical',
            )

    def list(self, type_):
        response = self.request(f'/{type_}s/', method='get')
        json = response.json()
        return [
           item['slug'] for item in json
        ]

    def orphans(self, type_):
        remotes = self.list(type_)
        return sorted(
            set(remotes) - set(
                getattr(self, f"{type_}s").keys()
            )
        )

    def delete(self, type_, slug):
        self.log(
            "Deleting {type_} `{slug}`",
            slug=slug,
            type_=type_,
            level='info',
        )
        self.request(f'/{type_}s/{slug}', method='delete')

    def start(self, sync=True):
        scheduler = BlockingScheduler()

        try:
            for dataset in self.datasets.values():
                dataset.schedule_job(scheduler)

        except ValueError as e:
            self.log(
                str(e),
                level='critical',
            )

        if sync:
            self.log("Triggering initial sync", level='info')
            if not self.sync():
                return False

        self.log("Start processing jobs", level='info')
        scheduler.start()

    def sync(self):
        for collection in (self.datasets, self.dashboards):
            for item in collection.values():
                if not item.push():
                    self.log("Sync failed", level='critical')
                    return False

        return True
