# cassbot

from __future__ import with_statement

import time
import shlex
from functools import wraps
from itertools import imap, izip
from fnmatch import fnmatch
from twisted.words.protocols import irc
from twisted.internet import defer, protocol, endpoints
from twisted.python import log
from twisted.plugin import getPlugins, IPlugin
from twisted.application import internet, service
from zope.interface import Interface, implements, directlyProvides
import cassbot_plugins

try:
    import cPickle as pickle
except ImportError:
    import pickle


class enabled_but_not_found:
    def __init__(self):
        self.when_found = defer.Deferred()


try:
    IBotPlugin
except NameError:
    class IBotPlugin(Interface):
        def name():
            """
            Return the name of this plugin.
            """

        def description():
            """
            Return a string describing what this plugin does, or None if there is
            no need for a description.
            """

        def __call__():
            """
            Create an IBotPluginInstance instance of this class.
            """

class IBotPluginInstance(Interface):
    def interestingMethods():
        """
        Return a list of method names in which this plugin is interested.
        When the corresponding method is called on the bot's CassBotCore
        instance, then it will be called on this plugin as well (with an
        extra parameter, the CassBotCore instance, preceding the others.)

        This may be periodically re-called in order to refresh the plugin
        list, or a plugin can call bot.service.scan_plugins() to force an
        update.
        """

    def implementedCommands():
        """
        Return a list of command names corresponding to the commands this
        plugin wants to handle. A command name is the first word in a message
        directed at the bot (by private message, or in a channel addressed
        specifically to the bot). Multiple plugins may implement a command
        (and each will run), but it is probably best not to take advantage of
        this, to avoid confusion.

        All the returned names should correspond to methods on this class
        named ('command_' + name). These methods will be called with the
        following parameters:

            (bot, user, channel, args)

        ..where bot is the CassBotCore instance to which this command came,
        user is the user who issued the command, channel is the channel by
        whence it came (this will be the same as user if via private message),
        and args is a list of the words which followed the command.
        """

    def saveState():
        """
        Return some pickleable object which contains all the configuration
        or other state info that this plugin wants to save. The next time
        the plugin is initialized, if the state is successfully unpickled
        and correlated with this plugin, it will be restored to it using
        the loadState() call.

        If None is returned, no state will be saved for this plugin.
        """

    def loadState(state):
        """
        If this plugin previously offered a state object to save (see
        saveState) and the cassbot system was able to unpickle it and
        associate it with this plugin again, then it will be restored using
        this call.

        This plugin should not depend on any state being returned to it, or
        on this call being made at all. __init__ should create a basic
        "empty" state, and only if this method is called will any previous
        state info be available.
        """

class BaseBotPlugin_meta(type):
    def __new__(cls, name, bases, attrs):
        newcls = super(BaseBotPlugin_meta, cls).__new__(cls, name, bases, attrs)
        # why isn't zope.interface.classProvides inherited? that's dumb
        directlyProvides(newcls, IBotPlugin, IPlugin)
        return newcls

class BaseBotPlugin(object):
    implements(IBotPluginInstance)
    __metaclass__ = BaseBotPlugin_meta

    def __init__(self):
        if self.__class__ is BaseBotPlugin:
            # keep this one virtual
            raise NotImplementedError

    @classmethod
    def name(cls):
        """
        Default implementation; just return the name of the class.
        """
        return cls.__name__

    @classmethod
    def description(cls):
        """
        Default implementation; just return the docstring of the class.
        """
        return cls.__doc__

    @classmethod
    def interestingMethods(cls):
        """
        Default implementation; express interest in all overrideable methods
        that match method names on this class.
        """

        for mname in CassBotCore.overrideable:
            try:
                x = getattr(cls, mname)
                if callable(x):
                    yield mname
            except AttributeError:
                pass

    @classmethod
    def implementedCommands(cls):
        """
        Default implementation; offer to handle all commands suggested by
        methods on this class named 'command_*'.
        """

        for name, value in cls.__dict__.iteritems():
            if name.startswith('command_') and callable(value):
                yield name[8:]

    def saveState(self):
        return None

    def loadState(self, s):
        pass


def noop(*a, **kw):
    pass

def removekey(dicty, key):
    try:
        del dicty[key]
    except KeyError:
        pass


class CassBotCore(irc.IRCClient):
    overrideable = (
        'created',
        'yourHost',
        'myInfo',
        'luserClient',
        'bounce',
        'isupport',
        'luserChannels',
        'luserOp',
        'luserMe',
        'privmsg',
        'joined',
        'left',
        'chanSynced',
        'noticed',
        'modeChanged',
        'serverModeChanged',
        'channelModeChanged',
        'signedOn',
        'kickedFrom',
        'nickChanged',
        'userJoined',
        'userLeft',
        'userQuit',
        'userKicked',
        'action',
        'topicUpdated',
        'userRenamed',
        'receivedMOTD',
        'msg'
    )

    def __init__(self, nickname='cassbot'):
        # state that will be saved and reset on this object by the service
        self.nickname = nickname
        self.join_channels = ()
        self.cmd_prefix = None

        self.channels = set()
        self.chan_modemap = {}
        self.is_channel_synced = {}
        self.server_modemap = {}
        self.topic_map = {}
        self.channel_memberships = {}
        self.is_signed_on = False
        self.init_time = time.time()

        for mname in self.overrideable:
            realmethod = getattr(self, mname, noop)
            wrappedmethod = self.make_watch_wrapper(mname, realmethod)
            setattr(self, mname, wrappedmethod)

    def make_watch_wrapper(self, mname, realmethod):
        @defer.inlineCallbacks
        def wrapper(*a, **kw):
            realresult = yield realmethod(*a, **kw)
            watchers = self.service.watcher_map.get(mname, ())
            for w in watchers:
                pluginmethod = getattr(w, mname, noop)
                try:
                    yield pluginmethod(self, *a, **kw)
                except Exception, e:
                    log.err(None, 'Exception in plugin %s for method %r'
                                  % (w.name(), mname))
            defer.returnValue(realresult)
        wrapper.func_name = 'wrapper_for_%s' % mname
        return wrapper

    def add_channel(self, channel):
        self.channels.add(channel)

    def leave_channel(self, channel):
        self.channels.discard(channel)
        removekey(self.topic_map, channel)
        removekey(self.chan_modemap, channel)
        removekey(self.is_channel_synced, channel)
        removekey(self.channel_memberships, channel)

    def dispatch_command(self, user, channel, cmd, args):
        cmd = cmd.lower().replace('-', '_')
        mname = 'command_' + cmd
        dlist = []
        for p in self.service.command_map.get(cmd, ()):
            try:
                pluginmethod = getattr(p, mname)
            except AttributeError:
                continue
            d = defer.maybeDeferred(pluginmethod, self, user, channel, args)
            d.addErrback(self.handle_command_error, p, user, channel, cmd, args)
            dlist.append(d)
        if len(dlist) == 0:
            return self.command_not_found(user, channel, cmd)
        return defer.DeferredList(dlist)

    def handle_command_error(self, err, plugin, user, channel, cmd, args):
        log.err(err, "Exception in plugin %s while in %r command"
                     % (plugin.name(), cmd))
        return self.address_msg(user, channel,
                                "Error in the %r command: %s" % (cmd, err.value))

    @defer.inlineCallbacks
    def address_msg(self, user, channel, msg, prefix=True):
        if '!' in user:
            user = user.split('!', 1)[0]
        transform = lambda m:m
        if channel == self.nickname:
            channel = user
        elif prefix:
            transform = lambda m: '%s: %s' % (user, m)
        for line in msg.split('\n'):
            yield self.msg(channel, transform(line))

    def command_not_found(self, user, channel, cmd):
        return self.address_msg(user, channel, "Sorry, I don't understand %r. :(" % cmd)

    ### methods called by the protocol

    def myInfo(self, servername, version, umodes, cmodes):
        self.servername = servername
        self.serverversion = version
        self.available_umodes = umodes
        self.available_cmodes = cmodes

    def yourHost(self, info):
        self.serverdaemon_info = info

    def luserMe(self, info):
        self.serverhost_info = info

    def privmsg(self, user, channel, message):
        cmdstr = None
        if channel == self.nickname:
            cmdstr = message
        if message.startswith('%s:' % (self.nickname,)):
            cmdstr = message[len(self.nickname)+1:]
        elif self.cmd_prefix is not None and message.startswith(self.cmd_prefix):
            cmdstr = message[len(self.cmd_prefix):]
        if cmdstr is not None:
            parts = shlex.split(cmdstr.strip())
            cmd = parts[0]
            args = parts[1:]
            self.dispatch_command(user, channel, cmd, args)

    def joined(self, channel):
        self.channel_memberships[channel] = set()
        self.is_channel_synced[channel] = False
        self.add_channel(channel)
        self.requestChannelMode(channel)

    def left(self, channel):
        self.leave_channel(channel)

    def kickedFrom(self, channel, kicker, message):
        self.leave_channel(channel)

    def modeChanged(self, user, channel, beingset, modes, args):
        if len(args) == 0:
            args = [None] * len(modes)
        if len(modes) != len(args):
            log.msg('Unexpected mode change message: modes=%r, args=%r. How'
                    ' do I interpret this?' % (modes, args))
            return
        if user == channel:
            for m, a in izip(modes, args):
                self.serverModeChanged(user, beingset, m, a)
        else:
            for m, a in izip(modes, args):
                self.channelModeChanged(user, channel, beingset, m, a)

    def serverModeChanged(self, user, beingset, mode, arg):
        if beingset:
            self.server_modemap.setdefault(user, {})[mode] = arg
        else:
            removekey(self.server_modemap[user], mode)

    def channelModeChanged(self, user, channel, beingset, mode, arg):
        modeset = self.chan_modemap.setdefault(channel, {}).setdefault(arg, set())
        if beingset:
            modeset.add(mode)
        else:
            modeset.discard(mode)

    def signedOn(self):
        self.factory.prot = self
        self.factory.resetDelay()
        for chan in self.join_channels:
            self.join(chan)
        self.is_signed_on = True
        self.sign_on_time = time.time()

    def userJoined(self, user, channel):
        self.channel_memberships.setdefault(channel, set()).add(user)

    def userLeft(self, user, channel):
        self.channel_memberships.setdefault(channel, set()).discard(user)
        removekey(self.chan_modemap.get(channel, {}), user)

    def userKicked(self, kickee, channel, kicker, message):
        self.userLeft(kickee, channel)

    def userQuit(self, user, channel):
        self.userLeft(user, channel)
        self.server_modemap.pop(user, None)

    def chanSynced(self, channel):
        self.is_channel_synced[channel] = True

    def topicUpdated(self, user, channel, newTopic):
        self.topic_map[channel] = newTopic

    def userRenamed(self, oldname, newname):
        for cm in self.channel_memberships.itervalues():
            if oldname in cm:
                cm.add(newname)
                cm.remove(oldname)
        for modemap in self.chan_modemap.itervalues():
            modemap[newname] = modemap.pop(oldname, set())
        modes = self.server_modemap.pop(oldname, None)
        if modes:
            self.server_modemap[newname] = modes

    def connectionLost(self, reason):
        self.is_signed_on = False
        try:
            del self.factory.prot
        except AttributeError:
            pass
        return irc.IRCClient.connectionLost(self, reason)

    def lineReceived(self, line):
        if getattr(self, 'debug_show_input', False):
            print "LINE: %r" % line
        return irc.IRCClient.lineReceived(self, line)

    def irc_RPL_NAMREPLY(self, prefix, params):
        channel, nlist = params[-2:]
        memb = self.channel_memberships.setdefault(channel, set())
        chanmap = self.chan_modemap.setdefault(channel, {})
        for name in nlist.split():
            if name.startswith('@'):
                name = name[1:]
                self.modeChanged(None, channel, True, 'o', (name,))
            if name.startswith('+'):
                name = name[1:]
                self.modeChanged(None, channel, True, 'v', (name,))
            memb.add(name)

    def irc_RPL_ENDOFNAMES(self, prefix, params):
        channel = params[-2]
        self.chanSynced(channel)

    def irc_RPL_CHANNELMODEIS(self, prefix, params):
        channel = params[1]
        modes = params[2]
        modeparams = params[3:]
        added, removed = irc.parseModes(modes, modeparams, self.getChannelModeParams())
        for mode, arg in added:
            self.modeChanged(None, channel, True, mode, (arg,))
        for mode, arg in removed:
            self.modeChanged(None, channel, False, mode, (arg,))

    def requestChannelMode(self, channel):
        self.sendLine('MODE %s' % channel)

def splituser(user):
    parts = user.split('!', 1)
    if len(parts) == 1:
        return (parts[0], '', '')
    hostparts = parts[1].split('@', 1)
    if len(hostparts) == 1:
        return (parts[0], '', hostparts[0])
    return (parts[0], hostparts[0], hostparts[1])

def mask_matches(mask, user):
    mparts = splituser(mask)
    uparts = splituser(user)
    return all(imap(fnmatch, uparts, mparts))


class AuthMap:
    def __init__(self):
        self.memberships = {}
        self.per_channel = {}

    def addPriv(self, mask, privname):
        self.memberships.setdefault(privname, set()).add(mask)

    def removePriv(self, mask, privname):
        try:
            self.memberships[privname].remove(mask)
        except KeyError:
            pass

    def userHas(self, user, privname, skip=set()):
        members = self.whoHas(privname)
        for mask in members:
            if mask_matches(mask, user):
                return True
        # avoid circular paths
        newskip = skip | set(members)
        for m in members:
            if m in skip:
                continue
            if self.userHas(user, m, skip=newskip):
                return True
        return False

    def whoHas(self, privname):
        return self.memberships.get(privname, ())

    def _for_channels(f):
        @wraps(f)
        def wrap(self, channel, *a, **kw):
            try:
                c = self.per_channel[channel]
            except KeyError:
                c = self.per_channel[channel] = AuthMap()
            return f(self, c, *a, **kw)
        return wrap

    @_for_channels
    def addChannelPriv(self, c, mask, privname):
        return c.addPriv(mask, privname)

    @_for_channels
    def removeChannelPriv(self, c, mask, privname):
        return c.removePriv(mask, privname)

    @_for_channels
    def channelUserHas(self, c, user, privname):
        return c.userHas(user, privname)

    @_for_channels
    def channelWhoHas(self, c, privname):
        return c.addPriv(privname)

    del _for_channels

    def saveState(self):
        return (self.memberships,
                dict((k, v.saveState()) for (k, v) in self.per_channel.iteritems()
                                        if v.memberships))

    def loadState(self, newstate):
        # throw away current info!
        self.memberships, per_chan_info = newstate
        for k, v in per_chan_info.iteritems():
            self.per_channel[k] = c = AuthMap()
            c.loadState(v)


class CassBotFactory(protocol.ReconnectingClientFactory):
    protocol = CassBotCore

    def buildProtocol(self, addr):
        p = protocol.ReconnectingClientFactory.buildProtocol(self, addr)
        self.service.initialize_proto_state(p)
        return p

    def clientConnectionFailed(self, connector, reason):
        log.err(reason, 'Connection failed')
        protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        log.err(reason, 'Connection lost')
        protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

class CassBotService(service.MultiService):
    plugin_scan_period = 240
    default_statefile = 'cassbot.state.db'

    def __init__(self, desc, nickname='cassbot', init_channels=(), reactor=None,
                 statefile=None):
        service.MultiService.__init__(self)

        self.statefile = statefile or self.default_statefile
        self.state = {
            'nickname': nickname,
            'channels': init_channels,
            'cmd_prefix': None,
            'plugins': {},
        }
        self.auth = AuthMap()

        if reactor is None:
            from twisted.internet import reactor
        self.reactor = reactor

        self.endpoint_desc = desc
        self.endpoint = endpoints.clientFromString(reactor, desc)

        self.watcher_map = {}
        self.command_map = {}
        self.scanning_now = False

        # all 'enabled' or 'loaded' plugins have an entry in here, keyed by
        # the plugin name (as given by the .name() classmethod).
        self.pluginmap = {}

        self.pfactory = CassBotFactory()

    def startService(self):
        res = service.MultiService.startService(self)
        self.pfactory.service = self
        connect_endpoint_without_fuss(self.reactor, self.endpoint, self.pfactory)
        try:
            self.loadStateFromFile(self.statefile)
        except (IOError, ValueError):
            pass
        return res

    def stopService(self):
        self.saveStateToFile(self.statefile)
        self.pfactory.stopTrying()
        try:
            self.getbot().transport.loseConnection()
        except AttributeError:
            pass
        self.pfactory.service = None
        return service.MultiService.stopService(self)

    @staticmethod
    def get_plugin_classes():
        for p in getPlugins(IBotPlugin, cassbot_plugins):
            if p is not BaseBotPlugin:
                yield p

    def scan_plugins(self):
        # wrap _really_scan_plugins, in case some callback inside
        # that method asks for another scan.
        if self.scanning_now:
            self.scan_again = True
            return
        self.scanning_now = True
        try:
            while True:
                self.scan_again = False
                self._really_scan_plugins()
                if not self.scan_again:
                    break
        finally:
            self.scanning_now = False

    def _really_scan_plugins(self):
        self.watcher_map = {}
        self.command_map = {}
        for pclass in self.get_plugin_classes():
            pname = pclass.name()
            try:
                p = self.pluginmap[pname]
            except KeyError:
                # not enabled
                continue
            if isinstance(p, enabled_but_not_found):
                # hey, we found it. load it up
                log.msg('Loading plugin %s (first time)...' % pname)
                p = self.enable_plugin_class(pclass, p.when_found, pname)
                if p is None:
                    continue
            try:
                for methodname in p.interestingMethods():
                    self.watcher_map.setdefault(methodname, []).append(p)
            except Exception:
                log.err(None, 'Exception in plugin %s for interestingMethods request'
                              % (p.name(),))
            try:
                for cmdname in p.implementedCommands():
                    self.command_map.setdefault(cmdname, []).append(p)
            except Exception:
                log.err(None, 'Exception in plugin %s for implementedCommands request'
                              % (p.name(),))

    def enable_plugin_by_name(self, pname):
        """
        Return a Deferred that will fire with the plugin with the given
        name, once loaded. If it is already loaded, the Deferred will be
        fired immediately.

        If the plugin is found but there is an error trying to load it,
        the Deferred will be errbacked.

        This may not ever be fired if the requested plugin is never found.
        """

        p = self.pluginmap.get(pname)
        if p is None:
            p = self.pluginmap[pname] = enabled_but_not_found()
        if isinstance(p, enabled_but_not_found):
            self.scan_plugins()
            return p.when_found
        return defer.succeed(p)

    def enable_plugin_class(self, pclass, deferred, pname):
        """
        Enable the given plugin class. Return the new plugin object,
        and also callback the given Deferred with it.

        If there is a problem, errback the given Deferred and return
        None.
        """

        log.msg('Instantiating plugin %s' % pname)
        try:
            self.pluginmap[pname] = p = pclass()
            pstate = self.state['plugins'].get(pname)
            if pstate:
                log.msg('Loading state for plugin %s' % pname)
                p.loadState(pstate)
        except Exception:
            self.pluginmap.pop(pname, None)
            deferred.errback()
            return
        deferred.callback(p)
        return p

    def disable_plugin(self, pname):
        """
        Disable the plugin with the given name. If it was actually loaded and
        enabled before, as expected, save its state first.
        """

        p = self.pluginmap.pop(pname, None)
        if p is not None and not isinstance(p, enabled_but_not_found):
            log.msg('Disabling plugin %s. Saving state.' % pname)
            try:
                pstate = p.saveState()
            except Exception:
                log.err(None, 'Trying to disable plugin %s' % pname)
                pstate = None
            if pstate is None:
                self.state['plugins'].pop(pname, None)
            else:
                self.state['plugins'][pname] = pstate
        self.scan_plugins()

    def initialize_proto_state(self, proto):
        proto.nickname = self.state['nickname']
        proto.join_channels = self.state.get('channels', ())
        proto.cmd_prefix = self.state.get('cmd_prefix', None)
        proto.service = self

    def initialize_plugin_state(self, plugin):
        try:
            pstate = self.state['plugins'][plugin.name()]
        except KeyError:
            pass
        else:
            try:
                plugin.loadState(pstate)
            except Exception:
                log.err(None, "Trying to load state in plugin %s" % plugin.name())

    def saveStateToFile(self, statefile):
        self.state['plugins_enabled'] = self.pluginmap.keys()
        for pname in self.state['plugins_enabled']:
            self.disable_plugin(pname)
        self.state['auth_map'] = self.auth.saveState()
        with open(statefile, 'w') as sfile:
            pickle.dump(self.state, sfile, -1)

    def loadStateFromFile(self, statefile):
        with open(statefile, 'r') as sfile:
            self.state = pickle.load(sfile)
        auth_dat = self.state.get('auth_map')
        if auth_dat is not None:
            self.auth.loadState(auth_dat)
        for pname in self.state.get('plugins_enabled', ()):
            d = self.enable_plugin_by_name(pname)
            d.addErrback(log.err, "Loading plugin %s" % pname)

    def __str__(self):
        return '<%s object [%s]%s>' % (
            self.__class__.__name__,
            self.endpoint_desc,
            ' (connected)' if hasattr(self.pfactory, 'prot') else ''
        )

    def getbot(self):
        return self.pfactory.prot


def require_priv(privname):
    """
    Decorator meant to be applied to command_* methods on cassbot plugins.
    Checks that the user issuing a command has the given privilege, and
    if not, returns an error instead of proceeding.
    """
    def make_wrapper(f):
        command_name = f.func_name
        if not command_name.startswith('command_'):
            raise RuntimeError("require_priv can only decorate command_ methods")
        command_name = command_name[len('command_'):]
        @wraps(f)
        def wrapper(self, bot, user, channel, args):
            if not bot.service.auth.userHas(user, privname):
                return bot.address_msg(user, channel, 'command %s requires privilege %s'
                                                      % (command_name, privname))
            return f(self, bot, user, channel, args)
        return wrapper
    return make_wrapper

def require_priv_in_channel(privname):
    """
    Decorator meant to be applied to command_* methods on cassbot plugins.
    Checks that the user issuing a command has the given privilege in the
    channel where the command was issued, and if not, returns an error instead
    of proceeding.
    """
    def make_wrapper(f):
        command_name = f.func_name
        if not command_name.startswith('command_'):
            raise RuntimeError("require_priv_in_channel can only decorate "
                               "command_ methods")
        command_name = command_name[len('command_'):]
        @wraps(f)
        def wrapper(self, bot, user, channel, args):
            if not bot.service.auth.channelUserHas(channel, user, privname):
                return bot.address_msg(user, channel,
                               'command %s requires privilege %s in this channel'
                               % (command_name, privname))
            return f(self, bot, user, channel, args)
        return wrapper
    return make_wrapper


def natural_list(items):
    if len(items) == 0:
        return '(none)'
    elif len(items) == 1:
        return items[0]
    elif len(items) == 2:
        return '%s and %s' % tuple(items)
    else:
        return '%s, and %s' % (', '.join(items[:-1]), items[-1])


def connect_endpoint_without_fuss(reactor, endpoint, factory):
    """
    Twisted's endpoint.connect function carefully wraps your factory inside
    a special _WrappingFactory which wraps up your protocol instances (with
    _WrappingProtocol, naturally) so it can get a Deferred firing when a
    connection is complete. We don't care about any of that, and also this
    _WrappingFactory does not properly pass on callbacks like
    clientConnectionLost to the real factory, so it breaks
    ReconnectingClientFactory.

    Since I still like the endpoint definition stuff, we'll bypass the other
    part.
    """

    if isinstance(endpoint, endpoints.TCP4ClientEndpoint):
        return reactor.connectTCP(endpoint._host, endpoint._port, factory,
                                  timeout=endpoint._timeout,
                                  bindAddress=endpoint._bindAddress)
    elif isinstance(endpoint, endpoints.SSL4ClientEndpoint):
        return reactor.connectSSL(endpoint._host, endpoint._port, factory,
                                  endpoint._sslContextFactory,
                                  timeout=endpoint._timeout,
                                  bindAddress=endpoint._bindAddress)
    else:
        raise RuntimeError("Don't know how to handle endpoint %s" % endpoint)

# vim: set et sw=4 ts=4 :
