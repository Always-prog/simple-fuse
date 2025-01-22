import os
from fuse import FUSE, Operations, FuseOSError

from errno import EISDIR, ENOENT

import os
import errno


class SimpleFS(Operations):
    def __init__(self, stores):
        self.stores = stores

    # =======    
    # Helpers
    # =======

    def _search_file_path(self, filename):
        filename = os.path.basename(filename)
        
        for store in self.stores:
            full_path = os.path.join(store, filename)
            if os.path.exists(full_path):
                return full_path
        raise FuseOSError(ENOENT)

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        full_path = self._search_file_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._search_file_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._search_file_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        full_path = self._search_file_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        for store in self.stores:
            for file in os.listdir(store):
                yield file

    def readlink(self, path):
        pathname = os.readlink(self._search_file_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.store)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._search_file_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._search_file_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        return os.mkdir(self._search_file_path(path), mode)

    def statfs(self, path):
        full_path = self._search_file_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._search_file_path(path))

    def symlink(self, name, target):
        return os.symlink(name, self._search_file_path(target))

    def rename(self, old, new):
        return os.rename(self._search_file_path(old), self._search_file_path(new))

    def link(self, target, name):
        return os.link(self._search_file_path(target), self._search_file_path(name))

    def utimens(self, path, times=None):
        return os.utime(self._search_file_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        full_path = self._search_file_path(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = os.path.join(self.stores[0], os.path.basename(path))
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        full_path = self._search_file_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)


if __name__ == "__main__":
    # Точка доступа к файлам, распределенным по хранилищам
    mount_point = '/home/always/kept/simple-fuse/access-point'

    # Точки хранения файлов (поиск файлов начинается с пепрвой точки)
    stores = ["/home/always/kept/simple-fuse/store/store1", 
              "/home/always/kept/simple-fuse/store/store2"]
    os.makedirs(mount_point, exist_ok=True)

    FUSE(SimpleFS(stores), mount_point, foreground=True)
