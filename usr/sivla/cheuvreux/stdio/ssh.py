import os
from socket import timeout as SocketTimeout
from xml.dom import minidom
import sys
import socket
import paramiko
import traceback

class SSHIdentity(object):

    def __init__(self, username):
        """ Construct a SSHIdentity object

            @param username: SSH Username
        """
        self.username = username
        self.password = None
        self.keyfile = None
        self.passphrase = None

    def setKeyfile(self, file, passphrase):
        '''
            Set a SSH key for this identity
            @param file Key path
            @param passphrase Key pass phrase
        '''
        self.keyfile, self.passphrase = file, passphrase

    def setPassword(self, password):
        """Set connection password"""
        self.password = password

class SSHConfiguration(object):
    """
        Pool
    """
    def __init__(self, mode="uat"):
        """Construct the pool

            @param mode: 'uat' or 'prod'
        """
        self.identities = {}
        self.servers = {}
        self.sshhome = None
        if mode == 'prod':
            xmlfile = 'config.xml'
        else:
            xmlfile = 'config_uat.xml'

        self._loadXml(os.path.join(os.path.expanduser("~"), ".bluebox", xmlfile))

    def _loadXml(self, file):
        """Internal method for loading SSH configuration from a file
        """
        def getText(nodelist):
            rc = []
            for node in nodelist:
                if node.nodeType == node.TEXT_NODE:
                    rc.append(node.data)
            return ''.join(rc)


        xmldoc = minidom.parse(file)
        rootnode = xmldoc.getElementsByTagName('configuration')[0]
        for node in rootnode.childNodes:
            if node.nodeType == minidom.Node.ELEMENT_NODE and node.tagName == 'sshhome':
                self.sshhome = getText(node.childNodes)
            elif node.nodeType == minidom.Node.ELEMENT_NODE and node.tagName == 'identity':
                id = node.getAttribute('id')
                username = getText((node.getElementsByTagName('username')[0]).childNodes)
                identity = SSHIdentity(username)

                keyNode = node.getElementsByTagName('keyfile')
                passNode = node.getElementsByTagName('passphrase')
                if keyNode and passNode:
                    identity.setKeyfile(getText(keyNode[0].childNodes), getText(passNode[0].childNodes))

                passNode = node.getElementsByTagName('password')
                if passNode:
                    identity.setPassword(getText(passNode[0].childNodes))

                self.identities[id] = identity

            elif node.nodeType == minidom.Node.ELEMENT_NODE and node.tagName == 'server':
                hostname = getText((node.getElementsByTagName('hostname')[0]).childNodes)
                id = getText((node.getElementsByTagName('identity')[0]).childNodes)
                self.servers[hostname] = id

    def getClient(self, server):
        """ Build an SSHConnection to a server. """
        connection = SSHConnection(server, self.identities[self.servers[server]], self.sshhome)
        return SSHClient(connection)

class SSHConnection(object):
    """
        SSH Connection to a server
    """
    def __init__(self, hostname, identity, sshhome):
        """Construct the connection

            @param hostname: Server address
            @param identity: SSHIdentity to use to establish the connection.
        """
        self.hostname = hostname
        self.identity = identity
        self.port = 22
        self.transport = None

        self._establishConnection(sshhome)

    def __del__(self):
        self.close()

    def _establishConnection(self, sshhome):
        """
            Internal method for establishing the connection.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.hostname, self.port))

        self.transport = paramiko.Transport(sock)
        self.transport.start_client()

        keys = paramiko.util.load_host_keys(os.path.join(sshhome, 'known_hosts'))

        self._checkServerKey(keys)

        if self.identity.keyfile:
            try:
                self._agent_auth()
            except SSHException:
                self._key_auth()
        else:
            self._password_auth()

    def _checkServerKey(self, keys):
        # check server's host key -- this is important.
        key = self.transport.get_remote_server_key()
        if not keys.has_key(self.hostname):
            raise SSHConnection('Unknown host key!')
        elif not keys[self.hostname].has_key(key.get_name()):
            raise SSHConnection('Unknown host key!')
        elif keys[self.hostname][key.get_name()] != key:
            raise SSHConnection('Host key has changed!!!')

        #Else, it's ok

    def close(self):
        if self.transport:
            self.transport.close()
            self.transport = None

    def _agent_auth(self):
        """
        Attempt to authenticate to the given transport using any of the private
        keys available from an SSH agent.
        """
        agent = paramiko.Agent()
        agent_keys = agent.get_keys()
        if len(agent_keys) == 0:
            raise SSHException('Not keys found')

        for key in agent_keys:
            try:
                self.transport.auth_publickey(self.identity.username, key)
                return
            except paramiko.SSHException:
                raise SSHException('Authentification fail!')

        raise SSHException('Nothing worked')
    
    def _password_auth(self):
        """
        Authenticate using username / password
        """
        self.transport.auth_password(self.identity.username, self.identity.password)

    def _key_auth(self):
        """
        Authenticate using a RSA or DSA key
        """
        try:
            key = paramiko.RSAKey.from_private_key_file(self.identity.keyfile, self.identity.passphrase)
        except paramiko.SSHException:
            key = paramiko.DSSKey.from_private_key_file(self.identity.keyfile, self.identity.passphrase)

        self.transport.auth_publickey(self.identity.username, key)

class SSHClient(object):
    """
    An scp1 implementation, compatible with openssh scp.
    Raises SSHException for all transport related errors. Local filesystem
    and OS errors pass through.

    Main public methods are .put and .get
    The get method is controlled by the remote scp instance, and behaves
    accordingly. This means that symlinks are resolved, and the transfer is
    halted after too many levels of symlinks are detected.
    The put method uses os.walk for recursion, and sends files accordingly.
    Since scp doesn't support symlinks, we send file symlinks as the file
    (matching scp behaviour), but we make no attempt at symlinked directories.

    Convenience methods:
        put_r:  put with recursion
        put_p:  put preserving times
        put_rp: put with recursion, preserving times
        get_r:  get with recursion
        get_p:  get preserving times
        get_rp: get with recursion, preserving times
    """
    def __init__(self, sshconnection, buff_size=16384, socket_timeout=5.0,
                 callback=None):
        """
        Create an scp1 client.

        @param sshconnection: an existing SSHConnection
        @type transport: L{Transport}
        @param buff_size: size of the scp send buffer.
        @type buff_size: int
        @param socket_timeout: channel socket timeout in seconds
        @type socket_timeout: float
        @param callback: callback function for transfer status
        @type callback: func
        """
        self.sshconnection = sshconnection
        self.buff_size = buff_size
        self.socket_timeout = socket_timeout
        self.channel = None
        self.preserve_times = False
        self.callback = callback
        self._recv_dir = ''
        self._utime = None
        self._dirtimes = {}


    def put(self, files, remote_path='.',
            recursive=False, preserve_times=False):
        """
        Transfer files to remote host.

        @param files: A single path, or a list of paths to be transfered.
            recursive must be True to transfer directories.
        @type files: string OR list of strings
        @param remote_path: path in which to receive the files on the remote
            host. defaults to '.'
        @type remote_path: str
        @param recursive: transfer files and directories recursively
        @type recursive: bool
        @param preserve_times: preserve mtime and atime of transfered files
            and directories.
        @type preserve_times: bool
        """
        self.preserve_times = preserve_times
        self.channel = self.sshconnection.transport.open_session()
        self.channel.settimeout(self.socket_timeout)
        scp_command = ('scp -t %s\n', 'scp -r -t %s\n')[recursive]
        self.channel.exec_command(scp_command % remote_path)
        self._recv_confirm()

        if not isinstance(files, (list, tuple)):
            files = [files]

        if recursive:
            self._send_recursive(files)
        else:
            self._send_files(files)

        if self.channel:
            self.channel.close()

    def get(self, remote_path, local_path='',
            recursive=False, preserve_times=False):
        """
        Transfer files from remote host to localhost

        @param remote_path: path to retreive from remote host. since this is
            evaluated by scp on the remote host, shell wildcards and
            environment variables may be used.
        @type remote_path: str
        @param local_path: path in which to receive files locally
        @type local_path: str
        @param recursive: transfer files and directories recursively
        @type recursive: bool
        @param preserve_times: preserve mtime and atime of transfered files
            and directories.
        @type preserve_times: bool
        """
        self._recv_dir = local_path or os.getcwd()
        rcsv = ('', ' -r')[recursive]
        prsv = ('', ' -p')[preserve_times]
        self.channel = self.sshconnection.transport.open_session()
        self.channel.settimeout(self.socket_timeout)
        self.channel.exec_command('scp%s%s -f %s' % (rcsv, prsv, remote_path))
        self._recv_all()

        if self.channel:
            self.channel.close()

    def execCommand(self, cmd):
        self.channel = self.sshconnection.transport.open_session()
        self.channel.settimeout(self.socket_timeout)
        self.channel.exec_command('%s' % (cmd))
        answer = self._recv_answer()

        if self.channel:
            self.channel.close()

        return answer

    def _read_stats(self, name):
        """return just the file stats needed for scp"""
        stats = os.stat(name)
        mode = oct(stats.st_mode)[-4:]
        size = stats.st_size
        atime = int(stats.st_atime)
        mtime = int(stats.st_mtime)
        return (mode, size, mtime, atime)

    def _send_files(self, files):
        for name in files:
            basename = os.path.basename(name)
            (mode, size, mtime, atime) = self._read_stats(name)
            if self.preserve_times:
                self._send_time(mtime, atime)
            file_hdl = file(name, 'rb')
            self.channel.sendall('C%s %d %s\n' % (mode, size, basename))
            self._recv_confirm()
            file_pos = 0
            buff_size = self.buff_size
            chan = self.channel
            while file_pos < size:
                chan.sendall(file_hdl.read(buff_size))
                file_pos = file_hdl.tell()
                if self.callback:
                    self.callback(file_pos, size)
            chan.sendall('\x00')
            file_hdl.close()

    def _send_recursive(self, files):
        for base in files:
            lastdir = base
            for root, dirs, fls in os.walk(base):
                # pop back out to the next dir in the walk
                while lastdir != os.path.commonprefix([lastdir, root]):
                    self._send_popd()
                    lastdir = os.path.split(lastdir)[0]
                self._send_pushd(root)
                lastdir = root
                self._send_files([os.path.join(root, f) for f in fls])

    def _send_pushd(self, directory):
        (mode, size, mtime, atime) = self._read_stats(directory)
        basename = os.path.basename(directory)
        if self.preserve_times:
            self._send_time(mtime, atime)
        self.channel.sendall('D%s 0 %s\n' % (mode, basename))
        self._recv_confirm()

    def _send_popd(self):
        self.channel.sendall('E\n')
        self._recv_confirm()

    def _send_time(self, mtime, atime):
        self.channel.sendall('T%d 0 %d 0\n' % (mtime, atime))
        self._recv_confirm()

    def _recv_answer(self):
        ''' Receive SSH answer '''
        msg = ''
        try:
            msg += self.channel.recv(1024)
        except SocketTimeout:
            raise SSHException('Timeout waiting for scp response')

        return msg

    def _recv_confirm(self):
        # read scp response
        msg = ''
        try:
            msg = self.channel.recv(512)
        except SocketTimeout:
            raise SSHException('Timeout waiting for scp response')
        if msg and msg[0] == '\x00':
            return
        elif msg and msg[0] == '\x01':
            raise SSHException(msg[1:])
        elif self.channel.recv_stderr_ready():
            msg = self.channel.recv_stderr(512)
            raise SSHException(msg)
        elif not msg:
            raise SSHException('No response from server')
        else:
            raise SSHException('Invalid response from server: ' + msg)

    def _recv_all(self):
        # loop over scp commands, and recive as necessary
        command = {'C': self._recv_file,
                   'T': self._set_time,
                   'D': self._recv_pushd,
                   'E': self._recv_popd}
        while not self.channel.closed:
            # wait for command as long as we're open
            self.channel.sendall('\x00')
            msg = self.channel.recv(1024)
            if not msg: # chan closed while recving
                break
            code = msg[0]
            try:
                command[code](msg[1:])
            except KeyError:
                raise SSHException(repr(msg))
        # directory times can't be set until we're done writing files
        self._set_dirtimes()

    def _set_time(self, cmd):
        try:
            times = cmd.split()
            mtime = int(times[0])
            atime = int(times[2]) or mtime
        except:
            self.channel.send('\x01')
            raise SSHException('Bad time format')
        # save for later
        self._utime = (mtime, atime)

    def _recv_file(self, cmd):
        chan = self.channel
        parts = cmd.split()
        try:
            mode = int(parts[0], 8)
            size = int(parts[1])
            path = os.path.join(self._recv_dir, parts[2])
        except:
            chan.send('\x01')
            chan.close()
            raise SSHException('Bad file format')

        try:
            file_hdl = file(path, 'wb')
        except IOError, e:
            chan.send('\x01' + e.message)
            chan.close()
            raise

        buff_size = self.buff_size
        pos = 0
        chan.send('\x00')
        try:
            while pos < size:
                # we have to make sure we don't read the final byte
                if size - pos <= buff_size:
                    buff_size = size - pos
                file_hdl.write(chan.recv(buff_size))
                pos = file_hdl.tell()
                if self.callback:
                    self.callback(pos, size)

            msg = chan.recv(512)
            if msg and msg[0] != '\x00':
                raise SSHException(msg[1:])
        except SocketTimeout:
            chan.close()
            raise SSHException('Error receiving, socket.timeout')

        file_hdl.truncate()
        try:
            os.utime(path, self._utime)
            self._utime = None
            os.chmod(path, mode)
            # should we notify the other end?
        finally:
            file_hdl.close()
        # '\x00' confirmation sent in _recv_all

    def _recv_pushd(self, cmd):
        parts = cmd.split()
        try:
            mode = int(parts[0], 8)
            path = os.path.join(self._recv_dir, parts[2])
        except:
            self.channel.send('\x01')
            raise SSHException('Bad directory format')
        try:
            if not os.path.exists(path):
                os.mkdir(path, mode)
            elif os.path.isdir(path):
                os.chmod(path, mode)
            else:
                raise SSHException('%s: Not a directory' % path)
            self._dirtimes[path] = (self._utime)
            self._utime = None
            self._recv_dir = path
        except (OSError, SSHException), e:
            self.channel.send('\x01' + e.message)
            raise

    def _recv_popd(self, *cmd):
        self._recv_dir = os.path.split(self._recv_dir)[0]

    def _set_dirtimes(self):
        try:
            for d in self._dirtimes:
                os.utime(d, self._dirtimes[d])
        finally:
            self._dirtimes = {}

class SSHException(Exception):
    """SCP exception class"""
    pass

if __name__ == '__main__':
    pool = SSHConfiguration('uat')
    client = pool.getClient('nyhbo001')
    client.put('test.txt', '/tmp')
