import unittest
import kazoo_fs_interface as kz
from kazoo.client import KazooClient
from kazoo.security import make_digest_acl_credential, CREATOR_ALL_ACL

test_mkdir_path = "/home/nicolas/workspace/2018/SE2/mnt_test/test_mkdir"
test_create_path = "/home/nicolas/workspace/2018/SE2/mnt_test/test_create.txt"
test_completo_path = "/home/nicolas/workspace/2018/SE2/mnt_test/test_completo.txt"

class TestFSMethods(unittest.TestCase):

    kz.logging.basicConfig()
    logger = kz.logging.getLogger()

    zk = kz.KazooClient("localhost:2181")
    zk.add_listener(kz.my_listener)
    zk.start()

    root = "/home/nicolas/workspace/2018/SE2/root_test"
    mountpoint = "/home/nicolas/workspace/2018/SE2/mnt_test"

    fuse = kz.FUSE(kz.FUSE_fs(root, zk), mountpoint, foreground=True)

    def test_mkdir(self):
        self.assertEqual(self.fuse.mkdir(test_mkdir_path,777),test_mkdir_path)

    def test_create(self):
        self.assertEqual(self.fuse.create(test_create_path, b"Hola"),test_create_path)

    def test_chmod(self):
        chmod_test_acl = 777
        self.assertTrue(self.fuse.chmod(test_create_path,chmod_test_acl))

    def test_chown(self):
        self.assertTrue(self.fuse.chown(test_create_path, 1000, 1000))

    def test_open(self):
        self.assertEqual(self.fuse.open(test_create_path,O_WRONLY),0)

    def test_write(self):
        self.assertTrue(self.fuse.write(test_create_path,b"Hola",0, None))
        self.assertTrue(self.fuse.write(test_create_path,b"Que tal como estas",5, None))

    def test_completo(self):
        self.fuse.create(test_completo_path,b"")
        self.fuse.open(test_completo_path,O_WRONLY)
        self.fuse.write(test_completo_path,b"Hola test completo",0,None)


if __name__ == '__main__':
    unittest.main()