# -*- coding: utf-8 -*-

import ConfigParser
import logging as log
from time import sleep as __sleep__


# ===============================================
# This class provides connection to the databases
# ===============================================

class IsidaSQL(object):
    __instance = None
    __threads_count = 1

    def __init__(self, config_file='isida-plus.cfg'):
        cfg = ConfigParser.RawConfigParser()
        cfg.read(config_file)
        try:
            self.__threads_count = cfg.getint('DB', 'THREADS1')
        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
            pass
        except IOError, e:
            print(u'IOError exception at __config_get__(\'DB\', \'THREADS\'): %s' % e)
            del self

        # Starting SQL connectors
        IsidaSQLDriver(config_file).run()

    # Singleton
    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super(IsidaSQL, cls).__new__(cls)
        return cls.__instance


# noinspection PyPep8Naming
class IsidaSQLDriver():
    # Error counter for DB connection
    __err_count_db = 0

    # SQL connection
    __connector = None

    # Default values
    __config = {u'CONFIGFILE': '',
                u'LOGS': {
                    u'DEBUG_LVL': 1,
                    u'LOG_DIR': 'logs',
                    u'PREFIX_DB': 'db'
                },
                u'DB': {
                    u'TYPE': 'mysql',
                    u'HOST': '127.0.0.1',
                    u'PORT': 3306,
                    u'DBNAME': 'isidaplus',
                    u'DBUSER': 'isidaplus',
                    u'DBPASS': 'isidaplus',
                    u'ERRDELAY': 10,
                    u'ERRRETRY': 5
                }}
    __config_parser = ConfigParser.RawConfigParser()

    def run(self):
        # If the connection to the DB is exists
        if self.__connector:
            while 1:
                a = self.fetch_one('SHOW TABLES')
                if a:
                    print(a)
                    __sleep__(5)
                if not a:
                    break

    # ==========================
    # Get value from config file
    def __config_get(self, section, option, use_default=True):
        try:
            if self.__config_parser.get(section, option).isdigit():
                return int(self.__config_parser.get(section, option))
            else:
                return self.__config_parser.get(section, option)
        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError), e:
            if use_default:
                return self.__config[section][option]
            else:
                log.error(u'ConfigParser exception at __config_get__(%s, %s): %s' % (section, option, e))
        except IOError, e:
            log.error(u'IOError exception at __config_get__(%s, %s): %s' % (section, option, e))
            return False

    # ==================
    # Init SQL connector
    def __init__(self, config_file):
        # Open config file
        self.__config[u'CONFIGFILE'] = config_file
        self.__config_parser.read(self.__config[u'CONFIGFILE'])

        # Define debug options
        self.__config[u'LOGS'][u'DEBUG_LVL'] = self.__config_get(u'LOGS', u'DEBUG_LVL')
        self.__config[u'LOGS'][u'LOG_DIR'] = self.__config_get(u'LOGS', u'LOG_DIR')
        self.__config[u'LOGS'][u'PREFIX_DB'] = self.__config_get(u'LOGS', u'PREFIX_DB')

        # Define logging parameters
        log.basicConfig(format=u'[%(asctime)s %(levelname)s] %(message)s',
                        level=self.__config[u'LOGS'][u'DEBUG_LVL'],
                        filename=u'%s/%s.log' %
                                 (self.__config[u'LOGS'][u'LOG_DIR'], self.__config[u'LOGS'][u'PREFIX_DB']))

        # Connect to the DB
        self.__err_count_db = 0
        self.__define_db_params()
        self.__db_connect()

    # ==========================
    # Define database parameters
    def __define_db_params(self):
        for db_param in self.__config[u'DB']:
            self.__config[u'DB'][db_param] = self.__config_get(u'DB', db_param, False)

    # ==========================
    # Connection to the database
    def __db_connect(self):
        if self.__config[u'DB'][u'TYPE'] == 'mysql':
            import mysql.connector as mysqldb
            try:
                self.__connector = mysqldb.connect(
                    database=self.__config[u'DB'][u'DBNAME'],
                    user=self.__config[u'DB'][u'DBUSER'],
                    host=self.__config[u'DB'][u'HOST'],
                    password=self.__config[u'DB'][u'DBPASS'],
                    port=self.__config[u'DB'][u'PORT']
                )
                self.__err_count_db = 0
            except mysqldb.Error, e:
                self.__err_count_db += 1
                log.error(u'MySQL exception #%d at __db_connect(): %s' % (self.__err_count_db, e))
                if self.__err_count_db < self.__config[u'DB'][u'ERRRETRY']:
                    log.error(u'Next try in %d seconds...' % self.__config[u'DB'][u'ERRDELAY'])
                    __sleep__(self.__config[u'DB'][u'ERRDELAY'])
                    self.__db_connect()
                else:
                    log.error(u'Reached limit for exceptions at __db_connect(). Execution stops.')
                    return False

        elif self.__config[u'DB'][u'TYPE'] == 'sqlite3':
            log.error(u'Error: __db_connect() to the SQLite3 database not supported yet...')
            exit(0)

        elif self.__config[u'DB'][u'TYPE'] == 'pgsql':
            log.error(u'Error: __db_connect() to the PostgreSQL database not supported yet...')
            exit(0)

    # =========================
    # Write data with SQL query
    def execute(self, query):
        if self.__config[u'DB'][u'TYPE'] == 'mysql':
            from mysql.connector import Error as MySQLError
            from mysql.connector import ProgrammingError as MySQLProgrammingError
            try:
                cursor = self.__connector.cursor()
                cursor.execute(query)
                self.__connector.commit()
                self.__err_count_db = 0
                return True
            except MySQLProgrammingError, e:
                log.error(u'MySQL programming exception at execute("%s"): %s' % (query, e))
                return False
            except MySQLError:
                if self.__err_count_db < self.__config[u'DB'][u'ERRRETRY']:
                    self.__db_connect()
                    return self.execute(query)
                else:
                    return False

        elif self.__config[u'DB'][u'TYPE'] == 'sqlite3':
            log.error(u'Error: execute() to the SQLite3 database not supported yet...')
            exit(0)

        elif self.__config[u'DB'][u'TYPE'] == 'pgsql':
            log.error(u'Error: execute() to the PostgreSQL database not supported yet...')
            exit(0)

    # ========================
    # Read data with SQL query
    def __fetch_data(self, query, return_all):
        if self.__config[u'DB'][u'TYPE'] == 'mysql':
            from mysql.connector import Error as MySQLError
            from mysql.connector import ProgrammingError as MySQLProgrammingError
            try:
                cursor = self.__connector.cursor()
                cursor.execute(query)
                self.__err_count_db = 0
                if return_all:
                    return cursor.fetchall()
                else:
                    return cursor.fetchone()
            except MySQLProgrammingError, e:
                log.error(u'MySQL programming exception at execute("%s"): %s' % (query, e))
                return False
            except MySQLError:
                if self.__err_count_db < self.__config[u'DB'][u'ERRRETRY']:
                    self.__db_connect()
                    return self.__fetch_data(query, return_all)
                else:
                    return False

        elif self.__config[u'DB'][u'TYPE'] == 'sqlite3':
            log.error(u'Error: execute() to the SQLite3 database not supported yet...')
            exit(0)

        elif self.__config[u'DB'][u'TYPE'] == 'pgsql':
            log.error(u'Error: execute() to the PostgreSQL database not supported yet...')
            exit(0)

    # ============================
    # Fetch one row from SQL query
    def fetch_one(self, query):
        return self.__fetch_data(query, False)

    # =============================
    # Fetch all rows from SQL query
    def fetch_all(self, query):
        return self.__fetch_data(query, True)
