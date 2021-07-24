import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))

warehouse_schema = config.get('WAREHOUSE', 'SCHEMA')

create_warehouse_schema = "CREATE SCHEMA IF NOT EXISTS {};".format(warehouse_schema)

drop_authors_table = "DROP TABLE IF EXISTS {}.authors;".format(warehouse_schema)
drop_reviews_table = "DROP TABLE IF EXISTS .reviews;".format(warehouse_schema)
drop_books_table = "DROP TABLE IF EXISTS {}.books;".format(warehouse_schema)
drop_users_table = "DROP TABLE IF EXISTS {}.users;".format(warehouse_schema)

create_de_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    title VARCHAR PRIMARY KEY DISTKEY,
    url VARCHAR,
    abstract VARCHAR,
    body_text VARCHAR,
    body_html VARCHAR
)
DISTSTYLE KEY
;
""".format(warehouse_schema)

create_en_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    title VARCHAR PRIMARY KEY DISTKEY,
    url VARCHAR,
    abstract VARCHAR,
    body_text VARCHAR,
    body_html VARCHAR
)
DISTSTYLE KEY
;
""".format(warehouse_schema)
create_es_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    title VARCHAR PRIMARY KEY DISTKEY,
    url VARCHAR,
    abstract VARCHAR,
    body_text VARCHAR,
    body_html VARCHAR
)
DISTSTYLE KEY
;
""".format(warehouse_schema)
create_fr_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    title VARCHAR PRIMARY KEY DISTKEY,
    url VARCHAR,
    abstract VARCHAR,
    body_text VARCHAR,
    body_html VARCHAR
)
DISTSTYLE KEY
;
""".format(warehouse_schema)
create_he_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    title VARCHAR PRIMARY KEY DISTKEY,
    url VARCHAR,
    abstract VARCHAR,
    body_text VARCHAR,
    body_html VARCHAR
)
DISTSTYLE KEY
;
""".format(warehouse_schema)
create_hu_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    title VARCHAR PRIMARY KEY DISTKEY,
    url VARCHAR,
    abstract VARCHAR,
    body_text VARCHAR,
    body_html VARCHAR
)
DISTSTYLE KEY
;
""".format(warehouse_schema)
create_it_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    title VARCHAR PRIMARY KEY DISTKEY,
    url VARCHAR,
    abstract VARCHAR,
    body_text VARCHAR,
    body_html VARCHAR
)
DISTSTYLE KEY
;
""".format(warehouse_schema)
create_ja_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    title VARCHAR PRIMARY KEY DISTKEY,
    url VARCHAR,
    abstract VARCHAR,
    body_text VARCHAR,
    body_html VARCHAR
)
DISTSTYLE KEY
;
""".format(warehouse_schema)
create_nl_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    title VARCHAR PRIMARY KEY DISTKEY,
    url VARCHAR,
    abstract VARCHAR,
    body_text VARCHAR,
    body_html VARCHAR
)
DISTSTYLE KEY
;
""".format(warehouse_schema)


create_pl_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    title VARCHAR PRIMARY KEY DISTKEY,
    url VARCHAR,
    abstract VARCHAR,
    body_text VARCHAR,
    body_html VARCHAR
)
DISTSTYLE KEY
;
""".format(warehouse_schema)

create_pt_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    title VARCHAR PRIMARY KEY DISTKEY,
    url VARCHAR,
    abstract VARCHAR,
    body_text VARCHAR,
    body_html VARCHAR
)
DISTSTYLE KEY
;
""".format(warehouse_schema)

create_ru_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    title VARCHAR PRIMARY KEY DISTKEY,
    url VARCHAR,
    abstract VARCHAR,
    body_text VARCHAR,
    body_html VARCHAR
)
DISTSTYLE KEY
;
""".format(warehouse_schema)



drop_warehouse_tables = [drop_de_table,drop_en_table ,drop_es_table ,drop_fr_table,drop_he_table,drop_hu_table, drop_it_table,drop_ja_table,drop_nl_table,drop_pl_table,drop_pt_table, drop_ru_table ]
create_warehouse_tables = [create_de_table, create_en_table, create_es_table, create_fr_table, create_he_table, create_hu_table, create_it_table, create_ja_table, create_nl_table, create_pl_table, create_pt_table,drop_ru_table ]