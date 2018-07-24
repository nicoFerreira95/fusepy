#!/usr/bin/env python
from __future__ import print_function, absolute_import, division
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging

from errno import EACCES
from os.path import realpath
from sys import argv, exit
from threading import Lock

import os, pwd, grp
import kazoo
import getpass
import StringIO

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

def my_listener(state):
    if state == KazooState.CONNECTED:
        logger.warning("Connection established.")
    elif state == KazooState.SUSPENDED:
        logger.warning("Connection suspended.")
    elif state == KazooState.LOST:
        logger.warning("Connection lost.")

#This is the class that FUSE inherits
class FUSE_fs(LoggingMixIn, Operations):

    def __init__(self, root, zk):
        self.root = realpath(root)
        self.rwlock = Lock()
        self.zk = zk

    def __call__(self, op, path, *args):
        return super(FUSE_fs, self).__call__(op, self.root + path, *args)

    def chmod(self, path, mode):
        """I receive the mode as a param, I have to split it into user and modes
        then use the make_digest_acl to create an ACL object with the right params
        then use that ACL object with the set_acls function to the given path"""
        #From what I read, mode is an integer type, e.g. 777 especifying the octal code for the rwx permissions
        #I convert the mode into a list of numbers. e.g. 777 becomes [7,7,7]
        modes = [int(n) for n in str(mode)]
        counter = 0
        #Now I have to iterate over each mode and make the ACLs corresponding to the permissions the users get
        try:
            for mode in modes:
                mode_b = '{0:03b}'.format(mode)
                list_digit = [int(d) for d in mode_b]
                #ld[0] = Read, ld[1] = Write, ld[2] = Execute
                if ( counter == 0 ): #The first time, the ACL has to be set for the actual user
                    #in list_digit I have the three digits that indicate true or false for read, write and execute
                    curr_user = getpass.getuser()
                    user_acl = self.zk.make_digest_acl(user, '', read=list_digit[0],
                    write=list_digit[1],create=list_digit[1],delete=list_digit[1])
                    self.zk.set_acls(path, user_acl, version=-1)
                elif ( counter == 1): #The second time its for all the members in the actual user group
                    #I have to get all the users in the current user group
                    query = pwd.getpwuid(os.getuid())
                    #query contains the pwd database entries, query[3] contains the gid
                    group_query = grp.getgrgid(query[3])
                    members_list = group_query[3]
                    for member in members_list:
                        member_acl = self.zk.make_digest_acl(member, '',read=list_digit[0],
                    write=list_digit[1],create=list_digit[1],delete=list_digit[1])
                        self.zk.set_acls(path, member_acl, version=-1)
                elif( counter == 2): #The third time its for all users
                    #I have to get the username of all the users registered in the OS
                    for p in pwd.getpwall():
                        all_acl = self.zk.make_digest_acl(p[0], '',read=list_digit[0],
                    write=list_digit[1],create=list_digit[1],delete=list_digit[1])
                        self.zk.set_acls(path, member_acl, version=-1)
                counter += 1
            return True
        except Exception as e:
            logger.exception(e)
            return False

    def chown(self, path, uid, gid):
        """With the given uid with a syscall I can get the username and then use the function
        make_digest_acl to set the username owner of the ACL attached to the zNode (set_acls)
        to set the username owner of the node."""
        try:
            if(self.zk.exists(path, None) is not None):
                #With this command, I get the associated username to a UID
                user_query = pwd.getpwuid(uid)
                user = user_query[0]
                user_acl = self.zk.make_digest_acl(user, '', all=True)
                self.zk.set_acls(path, user_acl, version=-1)
                group_query = grp.getgrgid(gid)
                members_list = group_query[3]
                for member in members_list:
                    member_acl = self.zk.make_digest_acl(member, '', all=True)
                    self.zk.set_acls(path, member_acl, version=-1)
        except Exception, e:
            logger.exception(e)

    def create(self, path, value):
        #The create function creates a znode based on the given path and value, acl=None, ephemeral=False, sequential=False and makepath=False
        try:
            if (self.zk.exists(path) == None):
                self.zk.ensure_path(path)
                self.zk.create(path, value, makepath=True, acl=CREATOR_ALL_ACL)
                return path
        except Exception as e:
            logger.exception(e)

    def flush(self, path, fh):
        try:
            pass
        except:
            logger.exception("Not implemented.")

    def link(self, path, fh):
        try:
            pass
        except:
            logger.exception("Not implemented.")

    def mkdir(self, path, mode):
        try:
            if (self.zk.exists(path, None) == None): #Check if the specified path/node already exists
                #If it doesn't exist, we create the path to the node
                self.zk.ensure_path(path, None)
                return path
            else:
                logger.warning("Directory already exists.")
                return None
        except Exception as e:
            logger.exception(e)

    def open(self, path, flags):
        #It creates a zNode if it doesn't exist.
        try:
            if(not(self.zk.exists(path,None)) and flags==O_WRONLY):
                #We have to create a new zNode
                self.zk.create(path,"",None,False,False,True)
                return 0
        except Exception, e:
            logger.exception(e)

    def read(self, path, size, offset, fh):
    #Here I have to use the get function, it returns a tuple (value, zNodeStat)
    #Ask Raul how to work or show that types of value
        try:
            data,stat = self.zk.get(path, None)
            return data[offset:offset+size]
        except Exception as e:
            logger.exception(e)

    def readdir(self, path, fh):
        try:
            if(self.zk.exists(path)):
                children_list = self.zk.get_children(path)
                dir_list = []
                children_list = str(children_list).split('\'')
                if(len(children_list) > 0):
                    for child in children_list:
                        if ':' in child:
                            dir_list += [child.split(':')]
                    return ['.', '..'] + dir_list
        except Exception as e:
            logging.exception("Can not find the given zNode")
            return -1

    def release(self, path, fh):
         if (state == KazooState.CONNECTED):
            self.zk.stop()
            print ("Connection has been stopped.")
            self.zk.closeself, path, flags ()
            print ("All Zookeeper resources has been freed.")

    def rmdir(self, path): #This syscall deletes a directory, in Zookeeper's case is a zNode with children
        try:
            self.zk.delete(path, True) #Recursive: True, this function will delete a zNode if it exists and recursively delete all its children
        except Exception as e:
            logger.exception(e)

    def symlink(self, target, source):
        try:
            pass
        except:
            logger.exception("Not implemented.")

    def truncate(self, path, length, fh=None):
        try:
            f = open(path, 'r')
            f.truncate(length)
        except Exception, e:
            logger.exception(e)

    def unlink(self, path):
        try:
            #It deletes the node recursively (all of it's children included).
            if(self.zk.exists(path, None)):
                self.zk.delete(path,-1,True)
        except Exception, e:
            logger.exception(e)

    def write(self, path, data, offset, fh):
        """By using the set function you set a value in a given zNode.
        Open the path file in rw+ mode to write.
        Put the pointer in the offset position given in the parameters.
        Read all the file content from the pointer to the end.
        Write the data given in the parameters and set the zNode value with that content."""
        try:
            file = open(path,"rw+")
            file.seek(offset,0)
            content = file.read()
            output = StringIO.StringIO(content)
            output.write(data)
            zk.set(path,output,version==-1)
            return True
        except Exception as e:
            logger.exception(e)
            return False

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

    fuse = FUSE(FUSE_fs(argv[1],zk), argv[2], foreground=True)

