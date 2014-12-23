# -*- coding: utf-8 -*-

import logging
import ConfigParser
import thread
from threading import Event
from time import time

import sleekxmpp
from kernel.IsidaSQL import IsidaSQL

log = logging.getLogger('KernelLogger')


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
                    u'PREFIX_MAIN': 'kernel',
                    u'PREFIX_FULL': 'full'
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

    # Kernel supported commands
    __kcmds = ['db']

    # Collector for exchanging messages between kernel and threads
    __threads = {}
    __db = None

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
        self.__config[u'LOGS'][u'PREFIX_FULL'] = self.__config_get(u'LOGS', u'PREFIX_FULL')

        # Define logging parameters for the full logs
        logging.basicConfig(format=u'[%(asctime)s %(levelname)s] %(message)s',
                            level=logging.INFO,
                            filename=u'%s/%s.log' %
                                     (self.__config[u'LOGS'][u'LOG_DIR'], self.__config[u'LOGS'][u'PREFIX_FULL']))

        # Define logging parameters for kernel
        log.setLevel(self.__config[u'LOGS'][u'DEBUG_LVL'])
        fh = logging.FileHandler(u'%s/%s.log' %
                                 (self.__config[u'LOGS'][u'LOG_DIR'], self.__config[u'LOGS'][u'PREFIX_MAIN']))
        fh.setLevel(self.__config[u'LOGS'][u'DEBUG_LVL'])
        formatter = logging.Formatter(u'[%(asctime)s %(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        log.addHandler(fh)

        # Define all parameters
        self.__define_params()

        # Initialize sleekxmpp
        super(IsidaPlus, self).__init__(self.__config[u'XMPP'][u'JID'], self.__config[u'XMPP'][u'PASSWORD'])

        # Get default nick and room
        self.nick = self.__config[u'XMPP'][u'NICKNAME']
        self.room = self.__config[u'INIT_CONFIGS'][u'CHATROOM']

        # Initialize DB connections
        self.__db = IsidaSQL()

        # Make handlers
        self.add_event_handler('session_start', self.__start)
        self.add_event_handler('muc::%s::got_online' % self.room, self.__muc_online)
        self.add_event_handler('message', self.__message)
        self.add_event_handler("groupchat_message", self.__muc_message)

    """
    Destructor
    """

    def on_shutdown(self):
        self.__db.stop()

    """
    Get value from config file
    """

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

        # TODO: get conferences from the DB
        self.plugin['xep_0045'].joinMUC(self.room, self.nick, wait=True)

    """
    On receiving any message
    """

    def __message(self, msg):
        if msg['type'] in ['normal', 'chat']:
            log.debug(u'[RECV] <-- %s' % (msg['body']))
            self.__message_processing(msg)

    """
    On receiving muc message
    """

    def __muc_message(self, msg):
        if msg['mucnick'] != self.nick:
            log.debug(u'[RECV] <== %s' % (msg['body']))
            if msg['body'][0] == self.__config[u'INIT_CONFIGS'][u'ACTION_CHAR']:
                # Remove action char
                msg['body'] = msg['body'][1:]
                self.__message_processing(msg)

    """
    Processing message
    """

    def __message_processing(self, msg):
        # Check already processed dialog
        t = self.__find_interlocutor(msg['from'], msg['type'])
        if not t:
            # If new - find first unused id for new thread
            threadid = 1
            while threadid in self.__threads:
                threadid += 1

            log.debug('Making new thread ID: %d' % threadid)
            thread.start_new_thread(self.__dialog, (msg, threadid))
        else:
            log.debug('Sending message to thread ID: %d from %s' % (t, msg['from'].full))
            self.__threads[t]['msg'] = msg
            self.__threads[t]['event'].set()

    """
    Find interlocutor in dialog threads
    Return: thread id, otherwise None
    """

    def __find_interlocutor(self, msg_from, msg_type):
        for t in self.__threads.iteritems():
            # Threads created per user and per each window
            # So, if the same user will write from the
            #  chat room and from private messages
            #  - that's will be different threads
            if t[1]['msg']['type'] == msg_type:
                if t[1]['msg']['from'] == msg_from:
                    return t[0]
        return None

    """
    Function for separate thread per each dialog (even for one response)
    """

    def __dialog(self, msg, threadid):
        # Make an record about new thread
        self.__threads[threadid] = {
            'starttime': int(time()),
            'msg': msg,
            'event': Event()
        }

        # At first, try to find is that an kernel command
        for cmd in self.__kcmds:
            if msg['body'][:len(cmd)] == cmd:
                self.__kernel_commands(msg, threadid)
                return

        # TODO: find plugin which supports command in message

        if msg['body'] in ['test', 'test start', 'test stop']:
            # Create plugin class
            from plugins.template.template import Main as Ptemplate
            p = Ptemplate()

            # Get plugin answer message and reply
            reply_object = p.run(msg)
            self.__send_message(msg, reply_object)

            # If plugin asked for next message
            while reply_object['continue']:
                # Waiting until __muc_message() will set flag again
                # (When user have put next message)
                self.__threads[threadid]['event'].clear()
                log.debug('Thread ID: %d is waiting now...' % threadid)
                self.__threads[threadid]['event'].wait()

                # Sending new user message to an plugin and reply
                reply_object = p.dialog(reply_object['args'], self.__threads[threadid]['msg'])
                self.__send_message(msg, reply_object)

        else:
            # Make echo
            self.__send_message(msg)

        # Remove thread from collector
        self.__threads.pop(threadid)
        log.debug('Thread ID: %d has finished' % threadid)

    """
    Message sending
    """

    def __send_message(self, msg, reply_object=None):
        if not reply_object:
            reply_object = {'message': 'I heard that'}

        # Is this a chatroom message?
        if msg['mucnick']:
            log.debug(u'[SEND] %s ==> %s' % (msg['from'].full, reply_object['message']))
            self.send_message(mto=msg['from'].bare,
                              mbody='%s, %s' % (msg['mucnick'], reply_object['message']),
                              mtype='groupchat')
        else:
            log.debug(u'[SEND] %s --> %s' % (msg['from'].full, reply_object['message']))
            # Avoiding issues with update msg object
            jfrom = msg['from']
            msg.reply(reply_object['message']).send()
            msg['from'] = jfrom

    """
    On muc presence
    """

    def __muc_online(self, presence):
        if presence['muc']['nick'] != self.nick:
            # self.send_message(mto=presence['from'].bare,
            # mbody="Hello, %s %s" % (presence['muc']['role'],
            #                                           presence['muc']['nick']),
            #                   mtype='groupchat')
            pass

    """
    Kernel commands processing
    """

    def __kernel_commands(self, msg, threadid):
        if msg['body'] == 'db stop':
            self.__send_message(msg, {'message': 'Stopping DB connections...'})
            if self.__db.stop():
                self.__send_message(msg, {'message': 'Done'})
            else:
                self.__send_message(msg, {'message': 'Failed. Please check DB logs.'})
        elif msg['body'] == 'db start':
            self.__send_message(msg, {'message': 'Starting DB connections...'})
            if self.__db.start():
                self.__send_message(msg, {'message': 'Done'})
            else:
                self.__send_message(msg, {'message': 'Failed. Please check DB logs.'})
        elif msg['body'] == 'db restart':
            self.__send_message(msg, {'message': 'Restarting DB connections...'})
            if self.__db.restart():
                self.__send_message(msg, {'message': 'Done'})
            else:
                self.__send_message(msg, {'message': 'Failed. Please check DB logs.'})
        else:
            self.__send_message(msg, {'message': 'Please check ".help" command to find usage examples.'})
        self.__threads.pop(threadid)