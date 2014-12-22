# -*- coding: utf-8 -*-

# ======================================
# Providing sql commands for the plugins
from kernel import IsidaSQL as __Isql


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
# from kernel.msg_sender import sending as send_msg


# ===============
# Plugin template
class Main():
    __someiter = 0

    Name = 'Template'
    Description = 'Template plugin. Used as example.'
    Ver = '0.1'
    URL = 'Native'
    SupCommands = [
        ['test', 'Check bot activity'],
        ['test stop', 'Stop checking']
    ]

    def self_test(self, msg=None):
        if msg:
            return "Plugin \"%s (v%s)\": self-test passed." % (self.Name, self.Ver)
        else:
            print("Plugin \"%s (v%s)\": self-test passed." % (self.Name, self.Ver))
            return True

    def run(self, msg):
        if msg['body'] == self.SupCommands[0][0]:
            self.__someiter += 1
            r = {
                'message': 'Test %d passed!' % self.__someiter,
                'continue': True,
                'args': self.__someiter
            }
            return r

    def dialog(self, args, msg):
        if msg['body'] == self.SupCommands[0][0]:
            self.__someiter += 1
            r = {
                'message': 'Test %d passed (args: %d)!' % (self.__someiter, args),
                'continue': True,
                'args': self.__someiter
            }
            return r
        if msg['body'] == self.SupCommands[1][0]:
            r = {
                'message': 'Test passed %d times. Thanks' % self.__someiter,
                'continue': False
            }
            return r