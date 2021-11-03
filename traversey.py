import os
import sqlite3
from sqlite3 import Error

class Traversey:
    def __init__(self, rootdir, db):
        self.rootdir = rootdir
        sql_create_dirs_table = """CREATE TABLE IF NOT EXISTS dirs (
                                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                                            DirID integer,
                                            DirName text NOT NULL,
                                            ParentDirID integer,
                                            ParentDir text NOT NULL ,
                                            st_ctime integer NOT NULL,
                                            st_atime integer NOT NULL,
                                            st_mtime integer NOT NULL,
                                            st_ctime_ns integer,
                                            st_atime_ns integer,
                                            st_mtime_ns integer,
                                            st_nlink integer,
                                            FOREIGN KEY("id") REFERENCES dirs("id")
                                        );"""
        sql_create_files_table = """ CREATE TABLE IF NOT EXISTS files (
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                FileID integer,
                                                FileName text NOT NULL,
                                                ParentDirID integer,
                                                FilePath text NOT NULL,
                                                st_size integer,
                                                st_mode integer,
                                                st_uid integer,
                                                st_gid integer,
                                                st_ino integer,
                                                st_ctime integer,
                                                st_atime integer,
                                                st_mtime integer,
                                                st_ctime_ns integer,
                                                st_atime_ns integer,
                                                st_mtime_ns integer,
                                                st_nlink integer,
                                                FOREIGN KEY("id") REFERENCES dirs("id")
                                            ); """

        # create a database connection
        self.conn = create_connection(db)

        # create tables
        if self.conn is not None:
            # create projects table
            try:
                c = self.conn.cursor()
                c.execute(sql_create_files_table)
            except Error as e:
                print(e)

            # create tasks table
            try:
                c = self.conn.cursor()
                c.execute(sql_create_dirs_table)
            except Error as e:
                print(e)

        else:
            print("Error! cannot create the database connection.")


    def scan(self):
        adddirs = []
        addfiles = []
        i = 1
        j = 0
        print(f"Scanning {self.rootdir} for files and folders...")
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
        self.conn.commit()

    def _addFile(self, folder, file, verbose=False):
        path = os.path.join(folder, file)
        parent_dir_id = os.stat(folder).st_ino
        stat = os.stat(path)
        stmt = """INSERT INTO files (FileID,
                                      FileName,
                                      FilePath,
                                      ParentDirID,
                                      st_size,
                                      st_mode,
                                      st_uid,
                                      st_gid,
                                      st_ino,
                                      st_ctime,
                                      st_atime,
                                      st_mtime,
                                      st_ctime_ns,
                                      st_atime_ns,
                                      st_mtime_ns,
                                      st_nlink)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""";
        data_tuple = (stat.st_ino,
                            file,
                            folder,
                            parent_dir_id,
                            stat.st_size,
                            stat.st_mode,
                            stat.st_uid,
                            stat.st_gid,
                            stat.st_ino,
                            stat.st_ctime,
                            stat.st_atime,
                            stat.st_mtime,
                            stat.st_ctime_ns,
                            stat.st_atime_ns,
                            stat.st_mtime_ns,
                            stat.st_nlink)
        try:
            if verbose:
                print(stmt)
            db_query(self.conn, stmt, data_tuple)
        except Exception as e:
            print(e)
        return

    def _addDir(self, folder, subfolder, verbose=False):
        path = os.path.join(folder, subfolder)
        stat = os.stat(path)
        parent_dir_id = os.stat(folder).st_ino
        stmt = """INSERT INTO dirs (DirID,
                                    DirName,
                                    ParentDirID,
                                    ParentDir,
                                    st_ctime,
                                    st_atime,
                                    st_mtime,
                                    st_ctime_ns,
                                    st_atime_ns,
                                    st_mtime_ns)
                            VALUES (?,?,?,?,?,?,?,?,?,?);"""
        data_tuple = (stat.st_ino,
                      subfolder,
                      parent_dir_id,
                      folder,
                      stat.st_ctime,
                      stat.st_atime,
                      stat.st_mtime,
                      stat.st_ctime_ns,
                      stat.st_atime_ns,
                      stat.st_mtime_ns)
        try:
            if verbose:
                print(stmt)
            self.conn.execute(stmt, data_tuple)
        except Exception as e:
            print(e)
        return


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def db_query(conn, statement, data_tuple):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(statement, data_tuple)
    except Error as e:
        print(e)
