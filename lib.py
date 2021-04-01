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

logging.basicConfig(level=logging.ERROR)
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logger = logging.getLogger('lib')

session_factory = sessionmaker()

models = import_module('model')

TYPE_MAP = {
    'NUMBER': 'DECIMAL'
}


def get_source_session() -> Session:
    source_engine = create_engine(
        config.source_connection,
        optimize_limits=True,
        use_binds_for_limits=False
    )

    return session_factory(bind=source_engine)


def get_destination_session() -> Session:
    destination_engine = create_engine(config.destination_connection)
    return session_factory(bind=destination_engine)


def guess_model_name(table):
    return "".join(x.capitalize() for x in table.split('_'))


def persist_destination_data(page, table, page_size=30):
    source_session = get_source_session()
    destination_session = get_destination_session()

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
    with open(model_file, "r") as file:
        logger.debug("reading models file")
        code = file.read()

    with open(model_file, "w") as file:
        relationships = set(re.findall("relationship\([^\)]*", code))

        [code := code.replace(relationship, '%s, lazy="joined"' % relationship) for relationship in relationships]

        file.write(code)
        logger.debug('added auto join to entities')


def replace_types(model_file='model.py'):
    with open(model_file, "r") as file:
        logger.debug('reading models file')
        code = file.read()

    with open(model_file, 'w') as file:
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

        file.write(code)
        logger.debug('replaced all necessary types')


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


def remove_migration_drops():
    for file in get_migrations():
        with open(file, 'r') as reader:
            code = reader.read()

        with open(file, 'w') as writer:
            [code := code.replace(match, 'None') for match in re.findall('op.drop_[^\n]+', code)]
            writer.write(code)

    logger.debug('removed drops from migrations')
