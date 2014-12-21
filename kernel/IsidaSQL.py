# -*- coding: utf-8 -*-

import ConfigParser
from time import sleep as __sleep__


# ===============================================
# This class provides connection to the databases
# ===============================================

class IsidaSQL():
    # Error counter for DB connection
    __err_count = 0
    # Error counter for SQL processing
    __err_countr = 0

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
                    u'ERRRETRY': 3
                }}
    __config_parser = ConfigParser.RawConfigParser()

    # ==========================
    # Get value from config file
    def __config_get(self, section, option, use_default=True):
        try:
            if self.__config_parser.get(section, option).isdigit():
                return int(self.__config_parser.get(section, option))
            else:
                return self.__config_parser.get(section, option)
        except ConfigParser, e:
            if use_default:
                return self.__config[section][option]
            else:
                print u'ConfigParser exception at __config_get__(%s, %s): %s' % (section, option, e)
        except IOError, e:
            print u'IOError exception at __config_get__(%s, %s): %s' % (section, option, e)
            self.__del__()
            exit(-1)

    # ==================
    # Init SQL connector
    def __init__(self, config_file='isida-plus.cfg'):
        # Open config file
        self.__config[u'CONFIGFILE'] = config_file
        self.__config_parser.read(self.__config[u'CONFIGFILE'])

        # Define debug options
        self.__config[u'LOGS'][u'DEBUG_LVL'] = self.__config_get(u'LOGS', u'DEBUG_LVL')
        self.__config[u'LOGS'][u'LOG_DIR'] = self.__config_get(u'LOGS', u'LOG_DIR')
        self.__config[u'LOGS'][u'PREFIX_DB'] = self.__config_get(u'LOGS', u'PREFIX_DB')

        # Connect to the DB
        self.__err_count = 0
        self.__define_db_params()
        test_connect = self.__db_connect()
        test_connect.close()

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
                return mysqldb.connect(
                    database=self.__config[u'DB'][u'DBNAME'],
                    user=self.__config[u'DB'][u'DBUSER'],
                    host=self.__config[u'DB'][u'HOST'],
                    password=self.__config[u'DB'][u'DBPASS'],
                    port=self.__config[u'DB'][u'PORT']
                )
            except mysqldb.Error, e:
                self.__err_count += 1
                if self.__err_count <= self.__config[u'DB'][u'ERRRETRY']:
                    print u'MySQL exception at __db_connect(): %s' % e
                    print u'Next try in %d seconds...' % self.__config[u'DB'][u'ERRDELAY']
                    __sleep__(self.__config[u'DB'][u'ERRDELAY'])
                    self.__define_db_params()
                    return self.__db_connect()
                else:
                    self.__del__()
                    exit(-1)

        elif self.__config[u'DB'][u'TYPE'] == 'sqlite3':
            print u'Error: __db_connect() to the SQLite3 database not supported yet...'
            exit(0)

        elif self.__config[u'DB'][u'TYPE'] == 'pgsql':
            print u'Error: __db_connect() to the PostgreSQL database not supported yet...'
            exit(0)

    # =========================
    # Write data with SQL query
    def execute(self, query):
        self.__err_countr = 0
        if self.__config[u'DB'][u'TYPE'] == 'mysql':
            from mysql.connector import Error as MySQLError
            from mysql.connector import ProgrammingError as MySQLProgrammingError
            try:
                connector = self.__db_connect()
                cursor = connector.cursor()
                cursor.execute(query)
                connector.commit()
                return True
            except MySQLProgrammingError, e:
                print u'MySQL programming exception at execute("%s"): %s' % (query, e)
                return False
            except MySQLError, e:
                self.__err_countr += 1
                if self.__err_countr <= self.__config[u'DB'][u'ERRRETRY']:
                    print u'MySQL exception at execute("%s"): %s' % (query, e)
                    print u'Next try in %d seconds...' % self.__config[u'DB'][u'ERRDELAY']
                    __sleep__(self.__config[u'DB'][u'ERRDELAY'])
                    self.__define_db_params()
                    self.__db_connect()
                    return self.execute(query)
                else:
                    return False

        elif self.__config[u'DB'][u'TYPE'] == 'sqlite3':
            print u'Error: execute() to the SQLite3 database not supported yet...'
            exit(0)

        elif self.__config[u'DB'][u'TYPE'] == 'pgsql':
            print u'Error: execute() to the PostgreSQL database not supported yet...'
            exit(0)

    # ========================
    # Read data with SQL query
    def __fetch_data(self, query, return_all):
        self.__err_countr = 0
        if self.__config[u'DB'][u'TYPE'] == 'mysql':
            from mysql.connector import Error as MySQLError
            from mysql.connector import ProgrammingError as MySQLProgrammingError
            try:
                connector = self.__db_connect()
                cursor = connector.cursor()
                cursor.execute(query)
                if return_all:
                    return cursor.fetchall()
                else:
                    return cursor.fetchone()
            except MySQLProgrammingError, e:
                print u'MySQL programming exception at execute("%s"): %s' % (query, e)
                return False
            except MySQLError, e:
                self.__err_countr += 1
                if self.__err_countr <= self.__config[u'DB'][u'ERRRETRY']:
                    print u'MySQL exception at execute("%s"): %s' % (query, e)
                    print u'Next try in %d seconds...' % self.__config[u'Base'][u'err_delay']
                    __sleep__(self.__config[u'Base'][u'err_delay'])
                    self.__define_db_params()
                    self.__db_connect()
                    return self.__fetch_data(query, return_all)
                else:
                    return False

        elif self.__config[u'DB'][u'TYPE'] == 'sqlite3':
            print u'Error: execute() to the SQLite3 database not supported yet...'
            exit(0)

        elif self.__config[u'DB'][u'TYPE'] == 'pgsql':
            print u'Error: execute() to the PostgreSQL database not supported yet...'
            exit(0)

    # ============================
    # Fetch one row from SQL query
    def fetch_one(self, query):
        return self.__fetch_data(query, False)

    # =============================
    # Fetch all rows from SQL query
    def fetch_all(self, query):
        return self.__fetch_data(query, True)
