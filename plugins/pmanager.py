# -*- coding: utf-8 -*-

import plugins.template as tpl
import os
from settings.config import plugin_dir


# =======================================
# Class for parsing commands as singleton
# Used in msg_handler.py to find plugin/method for commands
class CommandParser():
    _instance = None
    plugins = []

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(CommandParser, cls).__new__(cls)
        return cls._instance

    def refresh_cmd_list(self):
        plugins = []
        for p in tpl.db_fetch_all('SELECT name, supcommands FROM plugins WHERE loaded=1'):
            print(p)


# ==============================================
# Find all plugins in plugins directory and DB,
# check them and make list of supported commands
def init():
    db_list = []
    for p in tpl.db_fetch_all('SELECT file FROM plugins'):
        db_list.append(p[0])

    dir_list = []
    for p in os.listdir('%s/' % plugin_dir):
        if p not in ['pmanager.py', '__init__.py'] and p[-3:] == '.py':
            dir_list.append(p)

    for p in db_list:
        if p not in dir_list:
            print('Plugin %s have DB record, but file was not found in %s.' % (p, plugin_dir))

    for p in dir_list:
        if p in db_list:
            # Make plugin instance and get vars
            plugc = getattr(__import__(name='%s.%s' % (plugin_dir, p[:-3]), fromlist=['Main']), 'Main')
            plug = plugc()
            tpl.db_execute('UPDATE plugins SET name="%s", description="%s",'
                           ' supcommands="%s", version="%s" WHERE file="%s"' %
                           (plug.Name, plug.Description, plug.SupCommands, plug.Ver, p))

            # If plugin marked as auto loaded
            if tpl.db_fetch_one('SELECT autoload FROM plugins WHERE file="%s"' % p)[0] == 1:
                print('Loading plugin %s ...' % p)
                if plug.self_test():
                    tpl.db_execute('UPDATE plugins SET loaded="1" WHERE file="%s"' % p)
                else:
                    tpl.db_execute('UPDATE plugins SET loaded="0" WHERE file="%s"' % p)

            # If plugin just exists
            else:
                if plug.self_test():
                    print('Plugin %s is ready to loading' % p)

        # New plugin?
        else:
            try:
                plugc = getattr(__import__(name='%s.%s' % (plugin_dir, p[:-3]), fromlist=['Main']), 'Main')
                plug = plugc()
                tpl.db_execute('INSERT INTO plugins (file, name, description, supcommands, version, autoload)'
                               'VALUES ("%s", "%s", "%s", "%s", "%s", 0)' %
                               (p, plug.Name, plug.Description, plug.SupCommands, plug.Ver))
                print('Plugin %s (%s) is ready to loading' % (plug.Name, p))
            except Exception, e:
                print('Plugin %s have errors and cannot be loaded: %s' % (p, e))

    parser = CommandParser()
    parser.refresh_cmd_list()

# =================
# === First run ===
init()

