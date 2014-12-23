# -*- coding: utf-8 -*-

import ConfigParser
import logging as log
import threading
from time import sleep as __sleep__


# ===============================================
# This class provides connection to the databases
# ===============================================

class IsidaSQL(object):
    __instance = None
    __threads = []
    __threads_count = 1
    __threads_iterator = -1

    def __init__(self, config_file='isida-plus.cfg'):
        cfg = ConfigParser.RawConfigParser()
        cfg.read(config_file)
        try:
            self.__threads_count = cfg.getint('DB', 'THREADS')
        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
            pass
        except IOError, e:
            print(u'IOError exception at __config_get__(\'DB\', \'THREADS\'): %s' % e)
            del self

        # Starting SQL connectors
        for th in range(0, self.__threads_count):
            self.__threads.append({'object': IsidaSQLThread(config_file, th)})
            self.__threads[th]['thread'] = threading.Thread(target=self.__threads[th]['object'].run)
            self.__threads[th]['thread'].start()
            __sleep__(0.5)

    # Singleton
    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super(IsidaSQL, cls).__new__(cls)
        return cls.__instance

    # ============================
    # Fetch one row from SQL query
    def fetch_one(self, query):
        if self.__threads_iterator + 1 < self.__threads_count:
            self.__threads_iterator += 1
        else:
            self.__threads_iterator = 0
        return self.__threads[self.__threads_iterator]['object'].fetch_data(query, False)

    # =============================
    # Fetch all rows from SQL query
    def fetch_all(self, query):
        if self.__threads_iterator + 1 < self.__threads_count:
            self.__threads_iterator += 1
        else:
            self.__threads_iterator = 0
        return self.__threads[self.__threads_iterator]['object'].fetch_data(query, True)

    # ================
    # Execute an query
    def execute(self, query):
        if self.__threads_iterator + 1 < self.__threads_count:
            self.__threads_iterator += 1
        else:
            self.__threads_iterator = 0
        return self.__threads[self.__threads_iterator]['object'].execute(query)


# =================================
# Use this class only if you need
# a separate connection to iSida DB
# =================================

# noinspection PyPep8Naming
class IsidaSQLThread():
    # Thread number
    __number = None

    # Error counter for DB connection
    __err_count_db = 0
    __err_count_sql = 0

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
        delay = 60
        if self.__config['DB']['ERRDELAY'] >= 10:
            delay = self.__config['DB']['ERRDELAY']
        # Infinite loop
        while 1:
            __sleep__(delay)

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
    def __init__(self, config_file, thread_num=None):
        # Set thread number
        if thread_num is not None:
            self.__number = thread_num

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
        self.__err_count_sql = 0
        self.__define_db_params()
        self.__db_connect()

        log.debug(u'SQL connector #%s started' % self.__number)

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
                log.error(u'[thread %s] MySQL exception #%d at __db_connect(): %s' %
                          (self.__number, self.__err_count_db, e))
                if self.__err_count_db < self.__config[u'DB'][u'ERRRETRY']:
                    log.error(u'[thread %s] Next try in %d seconds...' %
                              (self.__number, self.__config[u'DB'][u'ERRDELAY']))
                    __sleep__(self.__config[u'DB'][u'ERRDELAY'])
                    self.__db_connect()
                else:
                    log.error(u'[thread %s] Reached limit for exceptions at __db_connect(). Execution stops.' %
                              self.__number)
                    return False

        elif self.__config[u'DB'][u'TYPE'] == 'sqlite3':
            log.error(u'Error: __db_connect() to the SQLite3 database not supported yet...')
            exit(0)

        elif self.__config[u'DB'][u'TYPE'] == 'pgsql':
            log.error(u'Error: __db_connect() to the PostgreSQL database not supported yet...')
            exit(0)

    # ============================================
    # Write data with SQL query
    # If success - returns number of affected rows
    def execute(self, query):
        log.debug(u'[thread %s] processing execute(\"%s\")' % (self.__number, query))
        if self.__config[u'DB'][u'TYPE'] == 'mysql':
            from mysql.connector import Error as MySQLError
            from mysql.connector import ProgrammingError as MySQLProgrammingError

            try:
                cursor = self.__connector.cursor()
                affected_rows = cursor.execute(query)
                self.__connector.commit()
                self.__err_count_db = 0
                self.__err_count_sql = 0
                return affected_rows
            except MySQLProgrammingError, e:
                self.__err_count_sql += 1
                log.error(u'[thread %s] MySQL programming exception at execute("%s"): %s' % (self.__number, query, e))
                return False
            except MySQLError, e:
                self.__err_count_sql += 1
                log.error(u'[thread %s] MySQL exception at execute("%s"): %s' % (self.__number, query, e))
                self.__connector.disconnect()
                self.__db_connect()
                if self.__err_count_db < self.__config[u'DB'][u'ERRRETRY']:
                    if self.__err_count_sql < self.__config[u'DB'][u'ERRRETRY']:
                        return self.execute(query)
                    else:
                        self.__err_count_sql = 0
                        return False
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
    def fetch_data(self, query, return_all):
        log.debug(u'[thread %s] processing fetch_data(\"%s\")' % (self.__number, query))
        if self.__config[u'DB'][u'TYPE'] == 'mysql':
            from mysql.connector import Error as MySQLError
            from mysql.connector import ProgrammingError as MySQLProgrammingError

            try:
                cursor = self.__connector.cursor()
                cursor.execute(query)
                self.__err_count_db = 0
                self.__err_count_sql = 0
                if return_all:
                    return cursor.fetchall()
                else:
                    return cursor.fetchone()
            except MySQLProgrammingError, e:
                self.__err_count_sql += 1
                log.error(u'[thread %s] MySQL programming exception at execute("%s"): %s' % (self.__number, query, e))
                return False
            except MySQLError, e:
                self.__err_count_sql += 1
                log.error(u'[thread %s] MySQL exception at execute("%s"): %s' % (self.__number, query, e))
                self.__connector.disconnect()
                self.__db_connect()
                if self.__err_count_db < self.__config[u'DB'][u'ERRRETRY']:
                    if self.__err_count_sql < self.__config[u'DB'][u'ERRRETRY']:
                        return self.fetch_data(query, return_all)
                    else:
                        self.__err_count_sql = 0
                        return False
                else:
                    return False

        elif self.__config[u'DB'][u'TYPE'] == 'sqlite3':
            log.error(u'Error: execute() to the SQLite3 database not supported yet...')
            exit(0)

        elif self.__config[u'DB'][u'TYPE'] == 'pgsql':
            log.error(u'Error: execute() to the PostgreSQL database not supported yet...')
            exit(0)
