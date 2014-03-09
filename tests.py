#!/usr/bin/python

"""Tests for the posixqueue.MessageQueue wrapper around the mq_* methods."""

import os
import unittest
from posixqueue import MessageQueue, MessageQueueError, Timeout

class TestMessageQueue(unittest.TestCase):

    def setUp(self):
        self.queue = MessageQueue(name="unittest", create=True)

    def tearDown(self):
        del self.queue

    def test_msg_size(self):
        """Test that we are able to send messages up-to the maximum configured size."""
        self.queue.send("a" * self.queue.max_size)
        self.assertRaises(MessageQueueError, lambda: self.queue.send("a" * (self.queue.max_size+1)))

    def test_full_empty_len(self):
        """Test the empty, full and len operations against the queue."""
        self.assertTrue(self.queue.empty())
        self.assertTrue(not self.queue.full())
        for n in xrange(self.queue.max_messages):
            self.queue.send("a")
            self.assertTrue(not self.queue.empty())
            self.assertTrue(len(self.queue) == (n+1))
        self.assertTrue(self.queue.full())

    def test_send_recv(self):
        """Ensure we're able to pull out the data we send in."""
        test_messages = [os.urandom(512) for _ in xrange(self.queue.max_messages)]
        for msg in test_messages:
            self.queue.send(msg)
        for msg in test_messages:
            assert self.queue.recv() == msg

    def test_priority(self):
        """Test that messages are ordered by priority on reads."""
        self.queue.send("a", priority=1)
        self.queue.send("b", priority=8)
        self.queue.send("c", priority=5)
        self.assertEqual(self.queue.recv(), "b")
        self.assertEqual(self.queue.recv(), "c")
        self.assertEqual(self.queue.recv(), "a")

    def test_send_timeout(self):
        """Test that the send timeout works properly when the queue is full."""
        for n in xrange(self.queue.max_messages):
            self.queue.send("a")
        self.assertRaises(Timeout, lambda: self.queue.send("a", timeout=1))

    def test_recv_timeout(self):
        """Test that the recv tiemout works properly when the queue is empty."""
        self.assertRaises(Timeout, lambda: self.queue.recv(timeout=1))

if __name__ == "__main__":
    unittest.main()
