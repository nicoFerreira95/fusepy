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

    def chmod(self, path, mode):
        """I receive the mode as a param, I have to split it into user and modes
        then use the make_digest_acl to create an ACL object with the right params
        then use that ACL object with the set_acls function to the given path"""

    def chown(self, path, uid, gid):
        """With the given uid with a syscall I can get the username and then use the function
        make_digest_acl to set the username owner of the ACL attached to the zNode (set_acls)
        to set the username owner of the node."""

    def create(path, value):
        #The create function creates a znode based on the given path and value, acl=None, ephemeral=False, sequential=False and makepath=False
        try:
            if (zk.exists(path, None) is None):
                zk.create(path, value, None, False, False, False)
        except Exception as e:
            logger.exception(e)

    def flush(self, path, fh):
        """I think the equivalent flush in ZK is the sync(path) function, given a path it flushes
        the channel between the process and leader. """

    def link():

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

    def open(self, path, flags):
        #It creates a zNode if it doesn't exist.
        try:
            if(not(zk.exists(path,None)) and flags==O_WRONLY):
                #We have to create a new zNode
                zk.create(path,"",None,False,False,Falseq)
        except Exception, e:
            logger.exception(e)

    def read(self, path, size, offset, fh):
    #Here I have to use the get function, it returns a tuple (value, zNodeStat)
    #Ask Raul how to work or show that types of value
    try:
        data,stat = zk.get(path, None)
        return data[offset:offset+size]
    except Exception as e:
        logger.exception(e)

    def readdir(self, path, fh):
        try:
            children_list = zk.get_children(path)
        except Exception as e:
            logging.warning("Can not find the given zNode")
        dir_list = []
        children_list = str(children_list).split('\'')
        for child in children_list:
            if ':' in child:
                dir_list += [child.split(':')]

    def release(self, path, fh):

    def rename(self, old, new):

    def rmdir(self, path): #This syscall deletes a directory, in Zookeeper's case is a zNode with children
        try:
            zk.delete(path, True) #Recursive: True, this function will delete a zNode if it exists and recursively delete all its children
        except Exception as e:
            logger.exception(e)

    def symlink(self, target, source):

    def truncate(self, path, length, fh=None):

    def unlink(self, path):
        try:
            #It deletes the node recursively (all of it's children included).
            if(zk.exists(path, None)):
                zk.delete(path,-1,True)
        except Exception, e:
            logger.exception(e)

    def write(self, path, data, offset, fh):
        #Here I have to use the set function
        #Ask Raul how to work with this function, writing in a zNode what is the exact thing you are doing by setting data in it?

    def close(state):
        if (state == KazooState.CONNECTED):
            zk.stop()
            print ("Connection has been stopped.")
            zk.closeself, path, flags ()
            print ("All Zookeeper resources has been freed.")


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

