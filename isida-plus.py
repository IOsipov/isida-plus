#!/usr/bin/python
# -*- coding: utf-8 -*-

# =============================
# Set UTF-8 as default encoding
import sys
if sys.version_info < (3, 0):
    reload(sys)
    sys.setdefaultencoding('utf8')
# =============================

import logging
import ConfigParser
import sleekxmpp


class IsidaPlus(sleekxmpp.ClientXMPP):
    # Default values
    __config = {u'CONFIGFILE': 'isida-plus.cfg',
                u'LOGS': {
                    u'DEBUG_LVL': 1,
                    u'LOG_DIR': 'logs',
                    u'PREFIX_MAIN': 'kernel'
                },
                u'XMPP': {
                    u'NICKNAME': 'iSida-plus',
                    u'JID': 'bot_jid@example.com',
                    u'PASSWORD': 'PA55W0RD',
                    u'STATUS': 'online',
                    u'STATUS_MSG': 'Hello world',
                    u'PRIORITY': 0
                },
                u'INIT_CONFIGS': {
                    u'OWNER': 'your_jid@example.com',
                    u'CHATROOM': 'conference@example.com',
                    u'ACTION_CHAR': '.'
                }}
    __config_parser = ConfigParser.RawConfigParser()

    # ==================
    # Bot initialization
    def __init__(self):
        # Open config file
        self.__config_parser.read(self.__config[u'CONFIGFILE'])

        # Define log/debug parameters
        self.__config[u'LOGS'][u'DEBUG_LVL'] = self.__config_get(u'LOGS', u'DEBUG_LVL')
        self.__config[u'LOGS'][u'LOG_DIR'] = self.__config_get(u'LOGS', u'LOG_DIR')
        self.__config[u'LOGS'][u'PREFIX_MAIN'] = self.__config_get(u'LOGS', u'PREFIX_MAIN')

        logging.basicConfig(format=u'[%(asctime)s %(levelname)s] %(message)s',
                            level=logging.DEBUG,
                            filename=u'%s/%s.log' %
                                     (self.__config[u'LOGS'][u'LOG_DIR'], self.__config[u'LOGS'][u'PREFIX_MAIN']))

        # Define all parameters
        self.__define_params()

        # Initialize sleekxmpp
        super(IsidaPlus, self).__init__(self.__config[u'XMPP'][u'JID'], self.__config[u'XMPP'][u'PASSWORD'])

        # Make handlers
        self.add_event_handler('session_start', self.start)
        self.add_event_handler('message', self.message)
        self.add_event_handler("groupchat_message", self.muc_message)

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

    # ========================
    # Define kernel parameters
    def __define_params(self):
        for param in self.__config[u'XMPP']:
            self.__config[u'XMPP'][param] = self.__config_get(u'XMPP', param, False)
        for param in self.__config[u'INIT_CONFIGS']:
            self.__config[u'INIT_CONFIGS'][param] = self.__config_get(u'INIT_CONFIGS', param, False)

    # =================
    # Starting function
    def start(self, event):
        self.send_presence()
        self.get_roster()
        self.plugin['xep_0045'].joinMUC(self.__config[u'INIT_CONFIGS'][u'CHATROOM'],
                                        self.__config[u'XMPP'][u'NICKNAME'],
                                        wait=True)

    # ============================
    # On receiving private message
    def message(self, msg):
        if msg['type'] in ['normal', 'chat']:
            msg.reply("You are sending:\n%s" % msg['body']).send()

    # =========================
    # On receiving chat message
    def muc_message(self, msg):
        if msg['mucnick'] != self.__config[u'XMPP'][u'NICKNAME']:
            if msg['body'][0] == self.__config[u'INIT_CONFIGS'][u'ACTION_CHAR']:
                self.send_message(mto=msg['from'].bare,
                                  mbody="%s, I heard that." % msg['mucnick'],
                                  mtype='groupchat')


# ================
# === THE MAIN ===
isidabot = IsidaPlus()
isidabot.register_plugin('xep_0030')    # Service Discovery
isidabot.register_plugin('xep_0004')    # Data Forms
isidabot.register_plugin('xep_0060')    # PubSub
isidabot.register_plugin('xep_0199')    # XMPP Ping
isidabot.register_plugin('xep_0045')    # MUC

if isidabot.connect():
    isidabot.process(threaded=False)
    print("Done")
else:
    print("Unable to connect.")