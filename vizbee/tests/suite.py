import os
import records
import responses

from tempfile import mkstemp
from unittest import TestCase
from click.testing import CliRunner

from ..cli import cli
from ..app import API_URL


class CliTest(TestCase):
    def setUp(self):

        db_file = mkstemp()[1]
        db_url = f"sqlite:///{db_file}"

        self.runner = CliRunner()

        os.environ['DATABASE_URL'] = db_url
        os.environ['API_URL'] = API_URL

        db = records.Database(db_url)
        db.query("create table user(username text, created_at datetime);")
        db.query("""
            insert into user(username, created_at)
            values ("paul", "2017-01-20 12:28:59");
        """)
        db.query("""
            insert into user(username, created_at)
            values ("jeanne", "2017-01-21 13:56:23");
        """)
        db.query("""
            insert into user(username, created_at)
            values ("john", "2017-01-21 08:07:42");
        """)
        self.db = db

    def mock_server(self, url, method='put', **kwargs):
        responses.add(
            getattr(responses, method.upper()),
            f"{API_URL}{url}",
            **kwargs
        )

    def invoke(self, command, *args, filename='vizbee/tests/files/vizbee.yml'):
        return self.runner.invoke(
            cli, [
                '--config',
                filename,
                command,
                *args,
            ],
            catch_exceptions=False,
        )


class RootTest(CliTest):
    @responses.activate
    def test_start_no_rule(self):
        result = self.invoke('start')
        self.assertEqual(result.exit_code, 1)
        self.assertEqual("\n".join([
                "No scheduling rule found for `daily-users`",
                "",
            ]),
            result.output,
        )

    @responses.activate
    def test_sync(self):
        self.mock_server(
            '/dashboards/main-report',
            json=dict(url='/an/url'),
            status=201,
        )
        self.mock_server(
            '/datasets/daily-users',
            json=dict(url='/an/url'),
            status=201,
        )
        result = self.invoke('sync')
        self.assertEqual(result.exit_code, 0)
        self.assertEqual("\n".join([
                "Executing: `daily-users`",
                "Pushing: `daily-users`",
                "Successfully created `daily-users`: /an/url",
                "Pushing: `main-report`",
                "Successfully created `main-report`: /an/url",
                "",
            ]),
            result.output,
        )


class DatasetTest(CliTest):
    def test_missing_file(self):
        result = self.invoke(
            'dataset',
            'execute',
            filename='not.found.yml',
        )
        self.assertIn('`not.found.yml` not found\n', result.output)
        self.assertEqual(result.exit_code, 1)

    def test_invalid_file(self):
        result = self.invoke(
            'dataset',
            'execute',
            filename='vizbee/tests/files/invalid.yml',
        )
        self.assertIn('Error parsing config', result.output)
        self.assertEqual(result.exit_code, 1)

    def test_missing_env(self):
        result = self.invoke(
            'dataset',
            'execute',
            filename='vizbee/tests/files/missing-env.yml',
        )
        self.assertIn('not found in environment', result.output)
        self.assertEqual(result.exit_code, 1)

    def test_invalid_connection(self):
        os.environ['DATABASE_URL'] = 'invalid'
        result = self.invoke('dataset', 'execute')
        self.assertEqual(result.exit_code, 1)
        self.assertIn('Error initializing connections', result.output)

    def test_execute_no_datasets(self):
        result = self.invoke(
            'dataset',
            'execute',
            filename='vizbee/tests/files/no-datasets.yml',
        )
        self.assertIn("{'datasets': ['required field']}", result.output)
        self.assertEqual(result.exit_code, 1)

    def test_execute_invalid_dataset(self):
        result = self.invoke('dataset', 'execute', 'invalid')
        self.assertIn('Invalid dataset `invalid`\n', result.output)
        self.assertEqual(result.exit_code, 1)

    def test_execute_failing_query(self):
        result = self.invoke(
            'dataset',
            'execute',
            'failing',
            filename='vizbee/tests/files/failing-query.yml',
        )
        self.assertEqual(result.exit_code, 1)
        self.assertIn(
            "no such column: unknown",
            result.output,
        )

    def test_execute(self):
        result = self.invoke('dataset', 'execute', 'daily-users')
        self.assertEqual(result.exit_code, 0)
        self.assertIn(
            "Executing: `daily-users`",
            result.output,
        )
        self.assertIn(
            "count(username)|day",
            result.output,
        )
        self.assertIn(
            "2              |2017-01-21",
            result.output,
        )

    def test_list(self):
        result = self.invoke('dataset', 'list')
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(
            "daily-users\n",
            result.output,
        )

    @responses.activate
    def test_list_remote(self):
        self.mock_server(
            '/datasets/',
            json=[
                dict(slug='dataset1'),
                dict(slug='dataset2'),
            ],
            status=200,
            method='get',
        )
        result = self.invoke('dataset', 'list', '--remote')
        self.assertEqual(result.exit_code, 0)
        self.assertEqual("dataset1\ndataset2\n", result.output)

    @responses.activate
    def test_push(self):
        self.mock_server(
            '/datasets/daily-users',
            json=dict(url='/an/url'),
            status=201,
        )
        result = self.invoke('dataset', 'push', 'daily-users')
        self.assertEqual(result.exit_code, 0)
        self.assertIn(
            "Successfully created `daily-users`: /an/url",
            result.output,
        )

    @responses.activate
    def test_push_schema_error(self):
        self.mock_server(
            '/datasets/daily-users',
            json=dict(errors=dict(graph=['invalid value'])),
            status=422,
        )
        result = self.invoke('dataset', 'push', 'daily-users')
        self.assertEqual(result.exit_code, 0)
        self.assertIn(
            "Errors: \n\ngraph: [invalid value]\n\n",
            result.output,
        )

    @responses.activate
    def test_push_server_error(self):
        self.mock_server(
            '/datasets/daily-users',
            status=500,
        )
        result = self.invoke('dataset', 'push', 'daily-users')
        self.assertEqual(result.exit_code, 0)
        self.assertIn(
            "The server unexpectedly responded with `500` status",
            result.output,
        )

    @responses.activate
    def test_prune(self):
        self.mock_server(
            '/datasets/',
            json=[
                dict(slug='dataset1'),
                dict(slug='dataset2'),
            ],
            status=200,
            method='get',
        )
        result = self.invoke('dataset', 'prune')
        self.assertEqual(result.exit_code, 0)
        self.assertEqual("\n".join([
                "You're about to delete: dataset1, dataset2",
                "Do you want to continue? [y/N]: ",
                "",
            ]),
            result.output,
        )

    @responses.activate
    def test_prune_forced(self):
        self.mock_server(
            '/datasets/',
            json=[
                dict(slug='dataset1'),
                dict(slug='dataset2'),
            ],
            status=200,
            method='get',
        )
        for slug in ('dataset1', 'dataset2'):
            self.mock_server(
                f'/datasets/{slug}',
                status=204,
                method='delete',
            )

        result = self.invoke('dataset', 'prune', '--force')
        self.assertEqual(result.exit_code, 0)
        self.assertEqual("\n".join([
                "Deleting dataset `dataset1`",
                "Deleting dataset `dataset2`",
                "",
            ]),
            result.output,
        )


class DashboardTest(CliTest):
    @responses.activate
    def test_push(self):
        self.mock_server(
            '/dashboards/main-report',
            json=dict(url='/an/url'),
            status=201,
        )
        result = self.invoke('dashboard', 'push', 'main-report')
        self.assertEqual(result.exit_code, 0)
        self.assertIn(
            "Successfully created `main-report`: /an/url",
            result.output,
        )

    def test_list(self):
        result = self.invoke('dashboard', 'list')
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(
            "main-report\n",
            result.output,
        )

    @responses.activate
    def test_list_remote(self):
        self.mock_server(
            '/dashboards/',
            json=[
                dict(slug='report1'),
                dict(slug='report2'),
            ],
            status=200,
            method='get',
        )
        result = self.invoke('dashboard', 'list', '--remote')
        self.assertEqual(result.exit_code, 0)
        self.assertEqual("report1\nreport2\n", result.output)

    @responses.activate
    def test_prune(self):
        self.mock_server(
            '/dashboards/',
            json=[
                dict(slug='report1'),
                dict(slug='report2'),
            ],
            status=200,
            method='get',
        )

        for slug in ('report1', 'report2'):
            self.mock_server(
                f'/dashboards/{slug}',
                status=204,
                method='delete',
            )

        result = self.invoke('dashboard', 'prune', '--force')
        self.assertEqual(result.exit_code, 0)
        self.assertEqual("\n".join([
                "Deleting dashboard `report1`",
                "Deleting dashboard `report2`",
                "",
            ]),
            result.output,
        )
