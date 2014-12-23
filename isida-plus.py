#!/usr/bin/python
# -*- coding: utf-8 -*-

from kernel.IsidaPlus import IsidaPlus

# Set UTF-8 as default encoding.
import sys
if sys.version_info < (3, 0):
    reload(sys)
    sys.setdefaultencoding('utf8')


"""
================
=== THE MAIN ===
"""
isidabot = IsidaPlus('isida-plus.cfg')
isidabot.register_plugin('xep_0030')    # Service Discovery
isidabot.register_plugin('xep_0004')    # Data Forms
isidabot.register_plugin('xep_0060')    # PubSub
isidabot.register_plugin('xep_0199')    # XMPP Ping
isidabot.register_plugin('xep_0045')    # MUC
isidabot.register_plugin('xep_0012')    # Last activity
isidabot.register_plugin('xep_0092')    # Software Version
isidabot.register_plugin('xep_0202')    # Entity Time


if isidabot.connect():
    # Start bot
    isidabot.process(block=True)

    # Close DB connections
    print(u'Closing DB connections...')
    isidabot.on_shutdown()

else:
    print(u'Unable to connect')