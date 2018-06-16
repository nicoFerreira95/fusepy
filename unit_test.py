import unittest
import kazoo_log_test as kz

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
        self.assertEqual(self.fuse.mkdir("/home/nicolas/workspace/2018/SE2/mnt_test/test_mkdir",777),"/home/nicolas/workspace/2018/SE2/mnt_test/test_mkdir")

    def test_create(self):
        self.assertEqual(self.fuse.create("/home/nicolas/workspace/2018/SE2/mnt_test/test_create.txt", b"Hola"),"/home/nicolas/workspace/2018/SE2/mnt_test/test_create.txt")

    def test_chmod(self):
        self.assertTrue(self.fuse.chmod("/home/nicolas/workspace/2018/SE2/mnt_test/test_create.txt",777))

    def test_chown(self):
        self.assertTrue(self.fuse.chown("/home/nicolas/workspace/2018/SE2/mnt_test/test_create.txt", 1000, 1000))
    def test_open(self):
        self.assertEqual(self.fuse.open("/home/nicolas/workspace/2018/SE2/mnt_test/test_create.txt",O_WRONLY),0)

if __name__ == '__main__':
    unittest.main()