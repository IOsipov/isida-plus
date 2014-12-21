# -*- coding: utf-8 -*-

# ======================================
# Providing sql commands for the plugins
from bot_kernel import IsidaSQL as __Isql


def db_fetch_one(query=None):
    if query:
        db = __Isql.IsidaSQL()
        return db.fetch_one(query)


def db_fetch_all(query=None):
    if query:
        db = __Isql.IsidaSQL()
        return db.fetch_all(query)


def db_execute(query=None):
    if query:
        db = __Isql.IsidaSQL()
        return db.execute(query)


# ==========================
# Providing sending messages
from bot_kernel.msg_sender import sending as send_msg


# ===============
# Plugin template
class Main(object):
    _instance = None
    Name = 'Template'
    Description = 'Template plugin. Used as example.'
    Ver = '0.1'
    SupCommands = [
        ['test', 'Check bot activity', 'test_func']
    ]

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(Main, cls).__new__(cls)
        return cls._instance

    def self_test(self, msg_from=None, msg_body=None):
        if msg_from:
            send_msg(msg_from, "Plugin \"%s (v%s)\": self-test passed." % (self.Name, self.Ver))
        else:
            print("Plugin \"%s (v%s)\": self-test passed." % (self.Name, self.Ver))
        return True

    def test_func(self, msg_from=None, msg_body=None):
        send_msg(msg_from, 'Test passed!')