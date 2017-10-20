from setuptools import setup
from vizbee import __version__


with open('README.md', 'r') as f:
    setup(
        name='vizbee',
        version=__version__,
        description='vizbee.io agent',
        long_description=f.read(),
        author='jean-philippe serafin',
        author_email='serafinjp@gmail.com',
        url='https://gitlab.com/vizbee/agent',
        packages=['vizbee'],
        install_requires=[
            'apscheduler',
            'cerberus',
            'click',
            'pyaml',
            'records',
            'requests',
            'simplejson',
        ],
        license='MIT',
        entry_points=dict(
            console_scripts=['vizbee=vizbee.cli:cli.main'],
        ),
        tests_require=['responses'],
        test_suite="vizbee.tests.suite",
        extras_require=dict(
            postgresql=['psycopg2'],
            oracle=['cx_oracle'],
            mysql=['mysqlclient'],
            redshift=['sqlalchemy-redshift'],
            sqlserver=['pyodbc'],
            sybase=['pyodbc']
        ),
    )
