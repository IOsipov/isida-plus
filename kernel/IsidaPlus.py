# -*- coding: utf-8 -*-

import logging as log
import ConfigParser
import sleekxmpp
import thread
from threading import Event
from time import time


# noinspection PyMethodMayBeStatic
class IsidaPlus(sleekxmpp.ClientXMPP):
    """
    Make default values.
    Later they will be filled from config file.
    """
    __config = {u'CONFIGFILE': '',
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
    __threads = {}

    """
    Bot initialization
    """
    def __init__(self, config_file):
        self.__config[u'CONFIGFILE'] = config_file

        # Open config file
        self.__config_parser.read(self.__config[u'CONFIGFILE'])

        # Define log/debug parameters
        self.__config[u'LOGS'][u'DEBUG_LVL'] = self.__config_get(u'LOGS', u'DEBUG_LVL')
        self.__config[u'LOGS'][u'LOG_DIR'] = self.__config_get(u'LOGS', u'LOG_DIR')
        self.__config[u'LOGS'][u'PREFIX_MAIN'] = self.__config_get(u'LOGS', u'PREFIX_MAIN')

        # Logging level for sleekxmpp lib
        log.basicConfig(format=u'[%(asctime)s %(levelname)s] %(message)s',
                        level=self.__config[u'LOGS'][u'DEBUG_LVL'],
                        filename=u'%s/%s.log' %
                                 (self.__config[u'LOGS'][u'LOG_DIR'], self.__config[u'LOGS'][u'PREFIX_MAIN']))

        # Define all parameters
        self.__define_params()

        # Initialize sleekxmpp
        super(IsidaPlus, self).__init__(self.__config[u'XMPP'][u'JID'], self.__config[u'XMPP'][u'PASSWORD'])

        # Get default nick and room
        self.nick = self.__config[u'XMPP'][u'NICKNAME']
        self.room = self.__config[u'INIT_CONFIGS'][u'CHATROOM']

        # Make handlers
        self.add_event_handler('session_start', self.__start)
        self.add_event_handler('muc::%s::got_online' % self.room, self.__muc_online)
        self.add_event_handler('message', self.__message)
        self.add_event_handler("groupchat_message", self.__muc_message)

    """
    Get value from config file
    """
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
                log.error(u'ConfigParser exception at __config_get__(%s, %s): %s' % (section, option, e))
        except IOError, e:
            log.error(u'IOError exception at __config_get__(%s, %s): %s' % (section, option, e))
            self.__del__()
            exit(-1)

    """
    Define kernel parameters
    """
    def __define_params(self):
        for param in self.__config[u'XMPP']:
            self.__config[u'XMPP'][param] = self.__config_get(u'XMPP', param, False)
        for param in self.__config[u'INIT_CONFIGS']:
            self.__config[u'INIT_CONFIGS'][param] = self.__config_get(u'INIT_CONFIGS', param, False)

    """
    Starting function
    """
    def __start(self, event):
        self.send_presence()
        self.get_roster()
        self.plugin['xep_0045'].joinMUC(self.room, self.nick, wait=True)

    """
    On receiving any message
    """
    def __message(self, msg):
        if msg['type'] in ['normal', 'chat']:
            if msg['body'] == '.test':
                msg['body'] = msg['body'][1:]
                from plugins.template import Main as Ptemplate
                msg.reply(Ptemplate().run(msg)).send()
            else:
                reply_msg = u'You are sending: %s' % msg['body']
                msg.reply(reply_msg).send()

    """
    On receiving muc message
    """
    def __muc_message(self, msg):
        if msg['mucnick'] != self.nick:
            if msg['body'][0] == self.__config[u'INIT_CONFIGS'][u'ACTION_CHAR']:
                # Remove action char
                msg['body'] = msg['body'][1:]

                # Check already processed dialog
                t = self.__find_interlocutor(msg['from'])
                if not t:
                    # If new - find first unused id for new thread
                    threadid = 1
                    while threadid in self.__threads:
                        threadid += 1

                    log.info('Making new thread ID: %d' % threadid)
                    thread.start_new_thread(self.__dialog, (msg, threadid))
                else:
                    log.info('Sending message to thread ID: %d from %s' % (t, msg['from'].full))
                    self.__threads[t]['msg'] = msg
                    self.__threads[t]['event'].set()

    """
    Find interlocutor in dialog threads
    Return: thread id, otherwise None
    """
    def __find_interlocutor(self, jid):
        for t in self.__threads.iteritems():
            if t[1]['msg']['from'] == jid:
                return t[0]
        return None

    """
    Function for separate thread per each dialog (even for one response)
    """
    def __dialog(self, msg, threadid):

        if msg['body'] == 'test' or 'test stop':
            # Make an record about new thread
            self.__threads[threadid] = {
                'plugin': 'template',
                'starttime': int(time()),
                'msg': msg,
                'event': Event()
            }

            # Create plugin class
            from plugins.template import Main as Ptemplate
            p = Ptemplate()

            # Get plugin answer message
            reply_object = p.run(msg)
            self.send_message(mto=msg['from'].bare,
                              mbody='%s, %s' % (msg['mucnick'], reply_object['message']),
                              mtype='groupchat')

            # If plugin asked for next message
            while reply_object['continue']:
                # Waiting until __muc_message() will set flag again
                self.__threads[threadid]['event'].clear()
                log.info('Thread ID: %d is waiting now...' % threadid)
                self.__threads[threadid]['event'].wait()

                reply_object = p.dialog(reply_object['args'], self.__threads[threadid]['msg'])
                self.send_message(mto=msg['from'].bare,
                                  mbody='%s, %s' % (msg['mucnick'], reply_object['message']),
                                  mtype='groupchat')

            # Remove thread from collector
            self.__threads.pop(threadid)
            log.info('Thread ID: %d has finished' % threadid)

        else:
            reply_msg = u'%s, I heard that.' % msg['mucnick']
            self.send_message(mto=msg['from'].bare,
                              mbody=reply_msg,
                              mtype='groupchat')


    """
    On muc presence
    """
    def __muc_online(self, presence):
        if presence['muc']['nick'] != self.nick:
            # self.send_message(mto=presence['from'].bare,
            #                   mbody="Hello, %s %s" % (presence['muc']['role'],
            #                                           presence['muc']['nick']),
            #                   mtype='groupchat')
            pass

