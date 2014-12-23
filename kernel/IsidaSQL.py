# -*- coding: utf-8 -*-

import ConfigParser
import logging
import threading
from time import sleep as __sleep__

log = logging.getLogger('DBLogger')


# ===============================================
# This class provides connection to the databases
# ===============================================

class IsidaSQL(object):
    __instance = None
    __cfg_file = ''
    __threads = []
    __threads_count = 1
    __threads_iterator = -1

    def __init__(self, config_file='isida-plus.cfg'):
        self.__cfg_file = config_file
        if not self.__threads:
            cfg = ConfigParser.RawConfigParser()
            cfg.read(self.__cfg_file)

            try:
                # Define debug options
                self.__threads_count = cfg.getint(u'DB', u'THREADS')

                # Define logging parameters
                log.setLevel(cfg.getint(u'LOGS', u'DEBUG_LVL'))
                fh = logging.FileHandler(u'%s/%s.log' %
                                         (cfg.get(u'LOGS', u'LOG_DIR'), cfg.get(u'LOGS', u'PREFIX_DB')))
                fh.setLevel(cfg.getint(u'LOGS', u'DEBUG_LVL'))
                formatter = logging.Formatter(u'[%(asctime)s %(levelname)s] %(message)s')
                fh.setFormatter(formatter)
                log.addHandler(fh)

            except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
                pass
            except IOError, e:
                print(u'IOError exception with get config values in IsidaSQL.__init__(): %s' % e)

            # Starting SQL connectors
            for th in range(0, self.__threads_count):
                self.__threads.append({'object': IsidaSQLThread(self.__cfg_file, th)})
                self.__threads[th]['thread'] = threading.Thread(target=self.__threads[th]['object'].run)
                self.__threads[th]['thread'].start()
                __sleep__(0.5)

    # Singleton
    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super(IsidaSQL, cls).__new__(cls)
        return cls.__instance

    # Stop DB connections
    def stop(self):
        if len(self.__threads) > 0:
            for th in range(0, len(self.__threads)):
                self.__threads[th]['object'].kill_flag = True
            # Old threads will stop them self. WIll not wait em.
            self.__threads = []
            return True
        else:
            log.error(u'Can not stop connections to the DB. They are not exists or will die soon!')
            return False

    # Start DB connections
    def start(self):
        if len(self.__threads) == 0:
            for th in range(0, self.__threads_count):
                self.__threads.append({'object': IsidaSQLThread(self.__cfg_file, th)})
                self.__threads[th]['thread'] = threading.Thread(target=self.__threads[th]['object'].run)
                self.__threads[th]['thread'].start()
                __sleep__(0.5)
            return True
        else:
            log.error(u'Can not start new connections to the DB. They are already exists!')
            return False

    # Restart DB connections
    def restart(self):
        self.stop()
        return self.start()

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
    kill_flag = False

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
                    u'ERRRETRY': 3
                }}
    __config_parser = ConfigParser.RawConfigParser()

    def run(self):
        delay = 60
        if self.__config['DB']['ERRDELAY'] >= 10:
            delay = self.__config['DB']['ERRDELAY']

        # Infinite loop
        while not self.kill_flag:
            __sleep__(delay)

        # Disconnect from the database
        self.__connector.disconnect()
        log.debug(u'[Old DB thread %s] Has been stopped.' % self.__number)

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
                log.error(u'[DB thread %s] MySQL exception #%d at __db_connect(): %s' %
                          (self.__number, self.__err_count_db, e))
                if self.__err_count_db < self.__config[u'DB'][u'ERRRETRY']:
                    log.error(u'[DB thread %s] Next try in %d seconds...' %
                              (self.__number, self.__config[u'DB'][u'ERRDELAY']))
                    __sleep__(self.__config[u'DB'][u'ERRDELAY'])
                    self.__db_connect()
                else:
                    log.error(u'[DB thread %s] Reached limit for exceptions at __db_connect(). Execution stops.' %
                              self.__number)
                    return False

        elif self.__config[u'DB'][u'TYPE'] == 'sqlite3':
            log.error(u'__db_connect() to the SQLite3 database not supported yet...')
            exit(0)

        elif self.__config[u'DB'][u'TYPE'] == 'pgsql':
            log.error(u'__db_connect() to the PostgreSQL database not supported yet...')
            exit(0)

    # ============================================
    # Write data with SQL query
    # If success - returns number of affected rows
    def execute(self, query):
        log.debug(u'[DB thread %s] processing execute(\"%s\")' % (self.__number, query))
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
                log.error(u'[DB thread %s] MySQL programming exception at execute("%s"): %s' %
                          (self.__number, query, e))
                return False
            except MySQLError, e:
                self.__err_count_sql += 1
                log.error(u'[DB thread %s] MySQL exception at execute("%s"): %s' % (self.__number, query, e))
                self.__connector.disconnect()
                self.__db_connect()
                if self.__err_count_db < self.__config[u'DB'][u'ERRRETRY']:
                    if self.__err_count_sql < self.__config[u'DB'][u'ERRRETRY']:
                        if self.__err_count_db == 0:
                            log.error(u'[DB thread %s] Next try in %d seconds...' %
                                      (self.__number, self.__config[u'DB'][u'ERRDELAY']))
                            __sleep__(self.__config[u'DB'][u'ERRDELAY'])
                        return self.execute(query)
                    else:
                        self.__err_count_sql = 0
                        return False
                else:
                    return False

        elif self.__config[u'DB'][u'TYPE'] == 'sqlite3':
            log.error(u'execute() to the SQLite3 database not supported yet...')
            exit(0)

        elif self.__config[u'DB'][u'TYPE'] == 'pgsql':
            log.error(u'execute() to the PostgreSQL database not supported yet...')
            exit(0)

    # ========================
    # Read data with SQL query
    def fetch_data(self, query, return_all):
        log.debug(u'[DB thread %s] processing fetch_data(\"%s\")' % (self.__number, query))
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
                log.error(u'[DB thread %s] MySQL programming exception at execute("%s"): %s' %
                          (self.__number, query, e))
                return False
            except MySQLError, e:
                self.__err_count_sql += 1
                log.error(u'[DB thread %s] MySQL exception at execute("%s"): %s' % (self.__number, query, e))
                self.__connector.disconnect()
                self.__db_connect()
                if self.__err_count_db < self.__config[u'DB'][u'ERRRETRY']:
                    if self.__err_count_sql < self.__config[u'DB'][u'ERRRETRY']:
                        if self.__err_count_db == 0:
                            log.error(u'[DB thread %s] Next try in %d seconds...' %
                                      (self.__number, self.__config[u'DB'][u'ERRDELAY']))
                            __sleep__(self.__config[u'DB'][u'ERRDELAY'])
                        return self.fetch_data(query, return_all)
                    else:
                        self.__err_count_sql = 0
                        return False
                else:
                    return False

        elif self.__config[u'DB'][u'TYPE'] == 'sqlite3':
            log.error(u'execute() to the SQLite3 database not supported yet...')
            exit(0)

        elif self.__config[u'DB'][u'TYPE'] == 'pgsql':
            log.error(u'execute() to the PostgreSQL database not supported yet...')
            exit(0)
