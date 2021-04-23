import logging
from concurrent.futures.process import ProcessPoolExecutor

from console_progressbar import ProgressBar
from invoke import task

import config
import lib

logger = logging.getLogger('task')


@task
def migrate_data(c, tables, max_workers=30, page_size=30, source_schema='public', destination_schema='public'):
    source_session = lib.get_source_session(source_schema, destination_schema)

    with ProcessPoolExecutor(max_workers) as executor:
        for table in tables.split(','):
            model = getattr(lib.get_models_module(), lib.guess_model_name(table))
            pages = int(source_session.query(model).count() / page_size)
            pb = ProgressBar(total=pages, prefix=f'Sending {pages} pages')

            for page in range(pages):
                logger.info('sending page %d', page+1)
                executor.submit(lib.persist_destination_data, page+1, table, page_size, source_schema, destination_schema)
                pb.next()
                source_session.expunge_all()


@task
def add_lazyness(c):
    lib.add_lazyness("model.py")


@task
def replace_types(c):
    lib.replace_types("model.py")


@task
def create_models(c, tables, source_schema='public', destination_schema=None):
    logger.info(f'generating model for {tables}')

    if destination_schema is None:
        destination_schema = source_schema

    c.run(f'sqlacodegen --tables {tables} --schema {source_schema} --noinflect '
          f'--outfile model.py {config.source_connection}', echo=True)
    lib.replace_types()
    lib.add_lazyness()
    lib.replace_for_destination_schema(source_schema, destination_schema)


@task
def update_database(c, create_only=False, keep_migrations=False):
    c.run('alembic revision --autogenerate', echo=True)

    if not create_only:
        c.run('alembic upgrade head', echo=True)
        lib.drop_alembic_version()

        if not keep_migrations:
            lib.delete_migrations()


@task
def create_migrations(c):
    update_database(c, True)


@task(help={
    'tables': 'Comma separated source database tables name you wish to migrate to destination database',
    'max-workers': 'Maximum number of forked parallel processes to use, defaults to 30 (very machine dependant)',
    'page-size': 'The process will paginate all data and each page runs on a parallel process, this param sets the '
                 'page size, defaults to 30',
    'source-schema': 'For databases that supports schemas this option can be used to set the source schema, '
                     'defaults to public',
    'destination-schema': 'For databases that supports schemas this option can be used to set the destination schema, '
                          'defaults to public',
})
def transfer_data(c, tables, max_workers=30, page_size=30, source_schema='public', destination_schema='public'):
    """
    Transfers data from the source database to the destination database, given the tables specified
    """

    create_models(c, tables, source_schema, destination_schema)
    update_database(c)
    migrate_data(c, tables, max_workers, page_size, source_schema, destination_schema)
