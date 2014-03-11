#!/usr/bin/python

import ctypes
import os
from time import time
from errno import errorcode

class MessageQueueError(Exception):
    pass

class Timeout(MessageQueueError):
    pass

class MessageQueueAttributes(ctypes.Structure):
    _fields_ = [("mq_flags", ctypes.c_long),
                ("mq_maxmsg", ctypes.c_long),
                ("mq_msgsize", ctypes.c_long),
                ("mq_curmsgs", ctypes.c_long)]

class MessageQueueTimeSpec(ctypes.Structure):
    _fields_ = [("tv_sec", ctypes.c_long),
                ("tv_nsec", ctypes.c_long)]

class MessageQueue(object):
    """Wrapper around the POSIX message queue interface."""

    @staticmethod
    def _setup_timeout(seconds):
        """Wrapper to setup a timeout structure for send/receive calls."""
        return ctypes.byref(MessageQueueTimeSpec(int(time()) + seconds, 0))

    @staticmethod
    def raise_error():
        """Raise an exception due to a failing system call."""
        errno = ctypes.get_errno()
        if errorcode[errno] == "ETIMEDOUT":
            raise Timeout
        else:
            raise MessageQueueError(errno, errorcode[errno])

    @staticmethod
    def system_max_size():
        """Return the maximum message size allowed by the system."""
        return int(open("/proc/sys/fs/mqueue/msgsize_max").read())

    @staticmethod
    def system_max_messages():
        """Return the maximum number of unread messages allowed by the system."""
        return int(open("/proc/sys/fs/mqueue/msg_max").read())

    @classmethod
    def get_reader(cls, name):
        """Return a message queue reader pointed at the given queue name."""
        return cls(name, mode="r")

    @classmethod
    def get_writer(cls, name):
        """Return a message queue writer pointed at the given queue name."""
        return cls(name, mode="w")

    def __init__(self, name, mode="rw", flags=0, create=False,
                       permissions=0o0644, max_messages=None,
                       max_size=None, persist=False):
        if max_size is None:
            max_size = self.system_max_size()
        if max_messages is None:
            max_messages = self.system_max_messages()

        self.name = name
        if not self.name.startswith("/"):
            self.name = "/" + self.name

        self.creator = create
        self.mode = mode
        self.persist = persist
        self.max_size = max_size
        self.max_messages = max_messages
        self._library = ctypes.CDLL("librt.so", use_errno=True)

        if mode == "w":
            flags |= os.O_WRONLY
        elif mode == "r":
            flags |= os.O_RDONLY
        elif mode == "rw":
            flags |= os.O_RDWR
        if self.creator:
            flags |= os.O_CREAT

        if self.creator:
            attributes = MessageQueueAttributes(0, self.max_messages, self.max_size, 0)
            self._reference = self._library.mq_open(ctypes.c_char_p(self.name),
                                                    ctypes.c_int(flags),
                                                    ctypes.c_int(permissions),
                                                    ctypes.byref(attributes))
        else:
            self._reference = self._library.mq_open(ctypes.c_char_p(self.name),
                                                    ctypes.c_uint(flags))
        if self._reference == -1:
            self.raise_error()

    def send(self, data, priority=None, timeout=None):
        """Send a message to the queue with optional priority and timeout."""
        if priority is None:
            priority = 0
        if timeout is not None:
            ret = self._library.mq_timedsend(self._reference, ctypes.c_char_p(data),
                                             ctypes.c_int(len(data)), priority,
                                             self._setup_timeout(timeout))
        else:
            ret = self._library.mq_send(self._reference, ctypes.c_char_p(data),
                                        ctypes.c_int(len(data)), priority)
        if ret == -1:
            self.raise_error()

    def recv(self, timeout=None):
        """Receive a message from the queue with optional timeout."""
        recv_buffer_size = self.max_size + 1
        recv_buffer = (ctypes.c_char * recv_buffer_size)()
        if timeout is not None:
            ret = self._library.mq_timedreceive(self._reference, ctypes.byref(recv_buffer),
                                                ctypes.c_int(recv_buffer_size), None,
                                                self._setup_timeout(timeout))
        else:
            ret = self._library.mq_receive(self._reference, ctypes.byref(recv_buffer),
                                           ctypes.c_int(recv_buffer_size), None)
        if ret == -1:
            self.raise_error()
        return recv_buffer[:ret]

    def _read_attributes(self):
        """Internal wrapper to read the current attributes of the queue."""
        attributes = MessageQueueAttributes()
        ret = self._library.mq_getattr(self._reference, ctypes.byref(attributes))
        if ret == -1:
            self.raise_error()
        return attributes

    def __len__(self):
        """Return the number of messages currently waiting in the queue."""
        return self._read_attributes().mq_curmsgs

    def empty(self):
        """Is the message queue currently empty."""
        return (len(self) == 0)

    def full(self):
        """Is the message queue currently full."""
        return (len(self) == self.max_messages)

    def __iter__(self):
        """Implement an iterator over the available messages."""
        while True:
            try:
                yield self.recv(timeout=0)
            except Timeout:
                raise StopIteration

    def __del__(self):
        ret = self._library.mq_close(self._reference)
        if ret == -1:
            self.raise_error()
        if self.creator and not self.persist:
            self._library.mq_unlink(self.name)
