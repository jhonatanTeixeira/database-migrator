import logging
from concurrent.futures.process import ProcessPoolExecutor

from console_progressbar import ProgressBar
from invoke import task

import config
import lib

logger = logging.getLogger('task')


@task
def migrate_data(c, tables, max_workers=30, page_size=30):
    source_session = lib.get_source_session()

    with ProcessPoolExecutor(max_workers) as executor:
        for table in tables.split(','):
            model = getattr(lib.models, lib.guess_model_name(table))
            pages = int(source_session.query(model).count() / page_size)
            pb = ProgressBar(total=pages, prefix=f'Sending {pages} pages')

            for page in range(pages):
                logger.info('sending page %d', page+1)
                executor.submit(lib.persist_destination_data, page+1, table, page_size)
                pb.next()
                source_session.expunge_all()


@task
def add_lazyness(c):
    lib.add_lazyness("model.py")


@task
def replace_types(c):
    lib.replace_types("model.py")


@task
def create_models(c, tables, schema='public'):
    logger.info(f'generating model for {tables}')
    c.run(f'sqlacodegen --tables {tables} --schema {schema} --noinflect '
          f'--outfile model.py {config.source_connection}', echo=True)
    lib.replace_types()
    lib.add_lazyness()


@task
def update_database(c, create_only=False, keep_migrations=False):
    c.run('alembic revision --autogenerate', echo=True)
    lib.remove_migration_drops()

    if not create_only:
        c.run('alembic upgrade head', echo=True)
        lib.drop_alembic_version()

        if not keep_migrations:
            lib.delete_migrations()


@task
def create_migrations(c):
    update_database(c, True)


@task
def transfer_data(c, tables, schema='public', max_workers=30, page_size=30):
    create_models(c, tables, schema)
    update_database(c)
    migrate_data(c, tables, max_workers, page_size)
