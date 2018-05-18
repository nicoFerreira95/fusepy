#!/usr/bin/env python
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging

from __future__ import print_function, absolute_import, division

from errno import EACCES
from os.path import realpath
from sys import argv, exit
from threading import Lock

import os
import kazoo

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

def my_listener(state):
    if state == KazooState.CONNECTED:
        logger.warning("Connection established.")
    elif state == KazooState.SUSPENDED:
        logger.warning("Connection suspended.")
    elif state == KazooState.LOST:
        logger.warning("Connection lost.")

#This is the class that FUSE 
class FUSE_fs(LoggingMixIn, Operations):

    def __init__(self, root):
        self.root = realpath(root)
        self.rwlock = Lock()

    def __call__(self, op, path, *args):
        return super(FUSE_fs, self).__call__(op, self.root + path, *args)

    def mkdir(path, state):
        #We can create a dir if the connection it's still on.
        if (state == KazooState.CONNECTED):
            try:
                if (not(zk.exists(path, None)): #Check if the specified path/node already exists
                    #If it doesn't exist, we create the path to the node
                    zk.ensure_path(path, None)
                else
                    logger.warning("Directory already exists.")
            except Exception as e:
                logger.exception(e)

    def readdir(path, state):
        try:
            for each item in zk.get_children(path, None, False):
                print(item + "/n")
            else
                print ("The given path is not a directory.")
        except Exception as e:
            logger.exception(e)

    def open():


    def create(path, value):
        #The create function creates a znode based on the given path and value, acl=None, ephemeral=False, sequential=False and makepath=False
        try:
            if (zk.exists(path, None) is None):
                zk.create(path, value, None, False, False, False)
        except Exception as e:
            logger.exception(e)

    def write():


    def read():


    def close(state):
        if (state == KazooState.CONNECTED):
            zk.stop()
            print ("Connection has been stopped.")
            zk.close()
            print ("All Zookeeper resources has been freed.")

    def unlink():



if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger()
    
    #This is the part in which we initialize the Apache Zookeeper connection    
    zk = KazooClient("localhost:2181")
    zk.add_listener(my_listener)
    zk.start()

    fuse = FUSE(FUSE_fs(argv[1]), argv[2], foreground=True)

