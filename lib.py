import logging
import os
import re
from copy import deepcopy
from importlib import import_module

from console_progressbar import ProgressBar
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy_pagination import paginate

import config

logging.basicConfig(level=config.base_logging_level)
logging.getLogger('sqlalchemy.engine').setLevel(config.sql_logging_level)
logger = logging.getLogger('lib')

session_factory = sessionmaker()


TYPE_MAP = {
    'NUMBER': 'DECIMAL'
}


def get_models_module(models_module='model'):
    return import_module(models_module)


def read_file(file):
    with open(file, "r") as file:
        return file.read()


def write_file(content, file):
    with open(file, "w") as file:
        file.write(content)


def get_source_session(source_schema='public', destination_schema='public') -> Session:
    source_engine = create_engine(
        config.source_connection,
        optimize_limits=True,
        use_binds_for_limits=False
    ).execution_options(schema_translate_map={destination_schema: source_schema})

    return session_factory(bind=source_engine)


def get_destination_session() -> Session:
    destination_engine = create_engine(config.destination_connection)
    return session_factory(bind=destination_engine)


def guess_model_name(table):
    return "".join(x.capitalize() for x in table.split('_'))


def persist_destination_data(page, table, page_size=30, source_schema='public', destination_schema='public',
                             models_module='model'):
    source_session = get_source_session(source_schema, destination_schema)
    destination_session = get_destination_session()
    models = get_models_module(models_module)

    model = getattr(models, guess_model_name(table))

    pk = [f"{table}.{col.name}" for col in inspect(model).primary_key]
    items = paginate(source_session.query(model).order_by(text(",".join(pk))), page, page_size).items
    sources = [deepcopy(row) for row in items]

    pb = ProgressBar(total=page_size+1, prefix=f'Page {page} Pid: {os.getpid()}')

    for source in sources:
        logger.info('merging data data %s', source)
        destination_session.merge(source)
        pb.next()

    destination_session.flush()
    destination_session.commit()
    destination_session.expunge_all()
    destination_session.close()
    pb.next()
    logger.info('persited page %d', page)


def add_lazyness(model_file='model.py'):
    logger.debug("reading models file")
    code = read_file(model_file)

    relationships = set(re.findall("relationship\([^\)]*", code))

    [code := code.replace(relationship, '%s, lazy="joined"' % relationship) for relationship in relationships]

    write_file(code, model_file)
    logger.debug('added auto join to entities')


def replace_types(model_file='model.py'):
    logger.debug('reading models file')
    code = read_file(model_file)

    sqla_imports = re.search('from sqlalchemy import (?P<types>[^\n]+)', code)
    types = [type.strip() for type in sqla_imports.group('types').split(',')]

    for type in TYPE_MAP:
        if not re.search('%s\(' % type, code):
            continue

        replace_for = TYPE_MAP[type]
        code = code.replace('%s(' % type, '%s(' % replace_for)
        types.append(replace_for)
        logger.debug('type %s found for replacement with %s', type, replace_for)

    code = code.replace(sqla_imports.group('types'), ', '.join(set(types)))

    write_file(code, model_file)
    logger.debug('replaced all necessary types')


def replace_for_destination_schema(source_schema, destination_schema, model_file='model.py'):
    code = read_file(model_file)

    code = code.replace(f"'schema': '{source_schema}'", f"'schema': '{destination_schema}'")

    write_file(code, model_file)
    logger.debug(f'replaced source schema {source_schema} for destination schema {destination_schema}')


def drop_alembic_version():
    destination_session = get_destination_session()
    destination_session.execute(text('drop table alembic_version'))
    destination_session.commit()
    logger.info('alembic version table droped')


def get_migrations():
    version_dir = 'alembic/versions'
    return [os.path.join(version_dir, file) for file in os.listdir(version_dir) if file.endswith('.py')]


def delete_migrations():
    [os.remove(file) for file in get_migrations()]
    logger.info('generated version deleted')
