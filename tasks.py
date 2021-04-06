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
def create_models(c, tables, source_schema='public', destination_schema='public'):
    logger.info(f'generating model for {tables}')
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


@task
def transfer_data(c, tables, max_workers=30, page_size=30, source_schema='public', destination_schema='public'):
    create_models(c, tables, source_schema, destination_schema)
    update_database(c)
    migrate_data(c, tables, max_workers, page_size, source_schema, destination_schema)
