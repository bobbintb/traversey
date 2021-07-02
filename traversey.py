import os
# noinspection PyUnresolvedReferences
#from icecream import ic
from sqlalchemy import ForeignKey
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine, MetaData, Table, Column, String


class Traversey:
    def __init__(self, rootdir, db):
        engine = create_engine(f'sqlite:///{db}')
        self.metadata = MetaData(engine)
        self.metadata.reflect()
        self.rootdir = rootdir
        # Create database if it does not exist.
        if not database_exists(engine.url):
            create_database(engine.url)
            # Create a metadata instance
            # Declare a table
            Table('dirs', self.metadata,
                  Column('DirID', String),
                  # If not string then you might get "Python int too large to convert to SQLite INTEGER"
                  Column('DirName', String, primary_key=True),
                  Column('ParentDirID', String, ForeignKey('dirs.DirID')),
                  Column('ParentDir', String, primary_key=True),
                  Column('st_ctime', String),
                  Column('st_atime', String),
                  Column('st_mtime', String),
                  Column('st_ctime_ns', String),
                  Column('st_atime_ns', String),
                  Column('st_mtime_ns', String),
                  Column('st_nlink', String))

            Table('files', self.metadata,
                  Column('FileID', String),
                  Column('FileName', String, primary_key=True),
                  Column('ParentDirID', String, ForeignKey('dirs.DirID')),
                  Column('FilePath', String, primary_key=True),
                  Column('st_size', String),
                  # Size of the file in bytes, if it is a regular file or a symbolic link.
                  # The size of a symbolic link is the length of the pathname it contains,
                  # without a terminating null byte.
                  Column('st_mode', String),  # File mode: file type and file mode bits (permissions).
                  Column('st_uid', String),  # User identifier of the file owner.
                  Column('st_gid', String),  # Group identifier of the file owner.
                  Column('st_ino', String),
                  # Platform dependent, but if non-zero, uniquely identifies the file for a given value of st_dev.
                  Column('st_ctime', String),
                  # Platform dependent: the time of most recent metadata change on Unix,
                  # the time of creation on Windows, expressed in seconds.
                  Column('st_atime', String),  # Time of most recent access expressed in seconds.
                  Column('st_mtime', String),
                  # Time of most recent content modification expressed in seconds.
                  Column('st_ctime_ns', String),
                  # Platform dependent: the time of most recent metadata change on Unix,
                  # the time of creation on Windows, expressed in nanoseconds as an integer.
                  Column('st_atime_ns', String),
                  # Time of most recent access expressed in nanoseconds as an integer.
                  Column('st_mtime_ns', String),
                  # Time of most recent content modification expressed in nanoseconds as an integer.
                  Column('st_nlink', String))

            # Create all tables
            self.metadata.create_all()
        try:
            self.connection = engine.connect()
        except:
            print("error accessing database")
        self.metadata.reflect(bind=engine)
        stmt = self.metadata.tables['dirs'].insert().values(DirID=1,
                                                            DirName=rootdir,
                                                            ParentDir="",
                                                            ParentDirID=None)
        # probably a more efficient way to do this. seems too expensive.
        try:
            self.connection.execute(stmt)
        except:
            pass

    def scan(self):
        adddirs = []
        addfiles = []
        i = 1
        j = 0
        print(f"Scanning for files and folders...")
        for folder, subfolders, files in os.walk(self.rootdir, topdown=True):
            print(f'\r{i} folders and {j} files found.', end='')
            for subfolder in subfolders:
                i = i + 1
                adddirs.append([folder, subfolder])
                print(f'\r{i} folders and {j} files found.', end='')
            for file in files:
                j = j + 1
                addfiles.append([folder, file])
                print(f'\r{i} folders and {j} files found.', end='')

        print(f'\r{(len(adddirs))} folders and {len(addfiles)} files found.')
        length = len(adddirs) + len(addfiles)
        i = 0
        for directory in adddirs:
            self._addDir(directory[0], directory[1])
            print('\r', str(round((i / length) * 100, 2)) + '%', end='')
            i = i + 1
        for file in addfiles:
            self._addFile(file[0], file[1])
            print('\r', str(round((i / length) * 100, 2)) + '%', end='')
            i = i + 1
        print("\r100%\r\nComplete.")

    def _addFile(self, folder, file, verbose=False):
        path = os.path.join(folder, file)
        parent_dir_id = os.stat(folder).st_ino
        stat = os.stat(path)
        stmt = self.metadata.tables['files'].insert().values(FileID=str(stat.st_ino),
                                                             FileName=file,
                                                             FilePath=folder,
                                                             ParentDirID=str(parent_dir_id),
                                                             st_size=str(stat.st_size),
                                                             st_mode=str(stat.st_mode),
                                                             st_uid=str(stat.st_uid),
                                                             st_gid=str(stat.st_gid),
                                                             st_ino=str(stat.st_ino),
                                                             st_ctime=str(stat.st_ctime),
                                                             st_atime=str(stat.st_atime),
                                                             st_mtime=str(stat.st_mtime),
                                                             st_ctime_ns=str(stat.st_ctime_ns),
                                                             st_atime_ns=str(stat.st_atime_ns),
                                                             st_mtime_ns=str(stat.st_mtime_ns),
                                                             st_nlink=str(stat.st_nlink))
        try:
            if verbose:
                print(stmt)
            self.connection.execute(stmt)
        except Exception as e:
            print(e)
        return

    def _addDir(self, folder, subfolder, verbose=False):
        path = os.path.join(folder, subfolder)
        stat = os.stat(path)
        parent_dir_id = os.stat(folder).st_ino
        stmt = self.metadata.tables['dirs'].insert().values(DirID=str(stat.st_ino),
                                                            DirName=subfolder,
                                                            ParentDirID=str(parent_dir_id),
                                                            ParentDir=folder,
                                                            st_ctime=str(stat.st_ctime),
                                                            st_atime=str(stat.st_atime),
                                                            st_mtime=str(stat.st_mtime),
                                                            st_ctime_ns=str(stat.st_ctime_ns),
                                                            st_atime_ns=str(stat.st_atime_ns),
                                                            st_mtime_ns=str(stat.st_mtime_ns))
        try:
            if verbose:
                print(stmt)
            self.connection.execute(stmt)
        except Exception as e:
            print(e)
        return

    def update(self, verbose=False):
        return
