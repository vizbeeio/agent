import click

from .app import (
    API_URL,
    App,
)


@click.group()
@click.option(
    '--config',
    '-c',
    required=False,
    default=".vizbee.yml",
    help="The YAML configuration file path.",
)
@click.option(
    '--client-id',
    envvar='CLIENT_ID',
    help="The application id.",
)
@click.option(
    '--client-secret',
    envvar='CLIENT_SECRET',
    help="The application secret.",
)
@click.option(
    '--api-url',
    envvar='API_URL',
    default=API_URL,
    help="The api url.",
)
@click.pass_context
def cli(context, config, client_id, client_secret, api_url):
    app = App(api_url, client_id, client_secret, context, config)
    context.obj = app


@cli.group('dataset')
def dataset():
    """Manage datasets."""
    pass


def _list(app, type_, remote=False):
    kpis = getattr(app, f"{type_}s").keys()

    if remote:
        kpis = app.list(type_)

    for key in kpis:
        click.echo(key)


def _prune(app, type_, force=False):
    orphans = app.orphans(type_)

    if len(orphans) == 0:
        app.log("Remote is up to date", level="info")
        return True

    slugs = ", ".join(orphans)

    if not force and not click.confirm(
        click.style(f"You're about to delete: {slugs}\n", fg='red')
        + 'Do you want to continue?'
    ):
        return False

    for orphan in orphans:
        app.delete(type_, orphan)


def _push(app, type_, slug, open):
    item = app.get(type_, slug)
    return item.push(open)


@dataset.command()
@click.argument('dataset')
@click.option('--json', is_flag=True)
@click.pass_obj
def execute(app, dataset, json):
    """Execute given dataset query."""
    dataset = app.get('dataset', dataset)
    results = dataset.execute()
    dataset = results.dataset

    if json:
        dataset = dataset.json

    click.echo(dataset)


@dataset.command(name='list')
@click.option('--remote', is_flag=True)
@click.pass_obj
def dataset_list(app, remote=False):
    """List available datasets."""
    return _list(app, 'dataset', remote=remote)


@dataset.command(name='push')
@click.argument('dataset')
@click.option('--open', is_flag=True)
@click.pass_obj
def dataset_push(app, dataset, open):
    """Push given dataset."""
    return _push(app, 'dataset', dataset, open)


@dataset.command(name='prune')
@click.option('--force', is_flag=True)
@click.pass_obj
def dataset_prune(app, force):
    """Delete remote orphan datasets."""
    _prune(app, 'dataset', force=force)


@dataset.command()
@click.argument('dataset')
@click.option('--execute', is_flag=True)
@click.pass_obj
def dataset_edit(app, dataset, execute):
    """Edit given dataset query."""
    # TODO: save
    dataset = app.get('dataset', dataset)
    dataset.query = click.edit(dataset.query, extension='.sql')
    results = dataset.execute()
    click.echo(results.dataset)


@cli.group('dashboard')
def dashboard():
    """Manage dashboards."""
    pass


@dashboard.command(name='list')
@click.option('--remote', is_flag=True)
@click.pass_obj
def dashboard_list(app, remote=False):
    """List available dashboards."""
    return _list(app, 'dashboard', remote=remote)


@dashboard.command(name='push')
@click.argument('dashboard')
@click.option('--open', is_flag=True)
@click.pass_obj
def dashboard_push(app, dashboard, open):
    """Push given dashboard."""
    return _push(app, 'dashboard', dashboard, open)


@dashboard.command(name='prune')
@click.option('--force', is_flag=True)
@click.pass_obj
def dashboard_prune(app, force):
    """Delete remote orphan dashboards."""
    _prune(app, 'dashboard', force=force)


@cli.command()
@click.pass_obj
def sync(app):
    """Push all datasets and dashboards."""
    return app.sync()


@cli.command()
@click.pass_obj
def start(app):
    """Start scheduler."""
    app.start()


if __name__ == '__main__':
    cli.main()
