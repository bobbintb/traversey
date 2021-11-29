import os
import sqlite3
from sqlite3 import Error
import platform


# TODO: try catch for file not found or cannot read.
# TODO: simplify code by using view and not doing multiple queries.
# TODO: shorten creation with loop and dictionary.
# TODO: make case sensitive because of linux.

class traverse:
    def __init__(self, db, rootdir=None):
        self.build = {}
        self.rootdir = rootdir
        theos = self._determineOS()
        self.build["sql_create_dirs_table"] = """CREATE TABLE dirs (DirID, 
                                                      DirName NOT NULL, 
                                                      ParentDirID INTEGER,
                                                      st_ctime_ns INTEGER NOT NULL,
                                                      st_atime_ns INTEGER NOT NULL,
                                                      st_mtime_ns INTEGER NOT NULL,
                                                      PRIMARY KEY("DirID"));"""
        self.build["sql_create_files_table"] = """CREATE TABLE files (FileID NOT NULL,
                                                        FileName TEXT NOT NULL,
                                                        ParentDirID TEXT NOT NULL,
                                                        st_size INTEGER NOT NULL,
                                                        st_ctime_ns INTEGER NOT NULL,
                                                        st_atime_ns INTEGER NOT NULL,
                                                        st_mtime_ns INTEGER NOT NULL,
                                                        st_nlink INTEGER NOT NULL,
                                                        FOREIGN KEY("ParentDirID") REFERENCES "dirs"("DirID"),
	                                                    PRIMARY KEY("FileName","ParentDirID")); """
        self.build["sql_create_filedata_view"] = f"""CREATE VIEW filedata as
                        select files.FileName,
                        dirs.DirName,
                        (select dirs.DirName || '{theos}' || files.FileName) as "File Path",
                        st_size as Size,
                        files.FileID,
                        files.ParentDirID,
                        (Select datetime(files.st_ctime_ns/1000000000, 'unixepoch', 'localtime')) as "Date Created", 
                        (Select datetime(files.st_mtime_ns/1000000000, 'unixepoch', 'localtime')) as "Date Modified", 
                        fullHash as Hash
                        from files join dirs on dirs.DirID = files.ParentDirID;"""

        # create a database connection
        self.conn = create_connection(db)

        # create tables
        if self.conn and rootdir is not None:
            for key in self.build:
                try:
                    c = self.conn.cursor()
                    c.execute(self.build[key])
                except Error as e:
                    print(e)

            self.inodes = set()
            self._scan()
            self.conn.commit()

    def _determineOS(self):
        if "Windows" in platform.system():
            return '\\'
        else:
            return "/"

    def _scan(self):
        adddirs = [[self.rootdir, self.rootdir]]
        addfiles = []
        for folder, subfolders, files in os.walk(self.rootdir, topdown=True):
            for subfolder in subfolders:
                adddirs.append([folder, subfolder])
            for file in files:
                addfiles.append([folder, file])
        for directory in adddirs:
            self._addDir(directory[0], directory[1])
        for file in addfiles:
            self._addFile(file[0], file[1])

    def _addFile(self, folder, file):
        path = os.path.join(folder, file)
        parent_dir_id = os.stat(folder).st_ino
        stat = os.stat(path)
        self.inodes.add(stat.st_ino)
        stmt = """INSERT INTO files (FileID,
                                         FileName,
                                         ParentDirID,
                                         st_size,
                                         st_ctime_ns,
                                         st_atime_ns,
                                         st_mtime_ns,
                                         st_nlink)
                        VALUES (?,?,?,?,?,?,?,?);"""
        data_tuple = (str(stat.st_ino),
                      file,
                      parent_dir_id,
                      stat.st_size,
                      stat.st_ctime_ns,
                      stat.st_atime_ns,
                      stat.st_mtime_ns,
                      stat.st_nlink)
        db_query(self.conn, stmt, data_tuple)
        return

    def _addDir(self, folder, subfolder):
        path = os.path.join(folder, subfolder)
        stat = os.stat(path)
        parent_dir_id = os.stat(folder).st_ino
        stmt = """INSERT INTO dirs (DirID,
                                    DirName,
                                    ParentDirID,
                                    st_ctime_ns,
                                    st_atime_ns,
                                    st_mtime_ns)
                            VALUES (?,?,?,?,?,?);"""
        data_tuple = (str(stat.st_ino),
                      path,
                      str(parent_dir_id),
                      str(stat.st_ctime_ns),
                      str(stat.st_atime_ns),
                      str(stat.st_mtime_ns))
        self.conn.execute(stmt, data_tuple)
        return

    def _update(self, item, **kwargs):
        list = []
        for key, value in kwargs.items():
            list.append(f'{key} = "{value}"')
        joined_string = ", ".join(list)
        if os.path.isfile(item):
            stmt = """SELECT * from {dirs} where DirName = "{item}";""".format(dirs="dirs", item=os.path.dirname(item))
            thing = db_query(self.conn, stmt)
            q = thing.fetchone()
            file = os.path.basename(item)
            stmt = """UPDATE files SET {joined_string} where ParentDirID = "{dir}" AND FileName = "{file}";""".format(
                joined_string=joined_string, dir=q[2],
                file=file)
        else:
            stmt = """UPDATE dirs SET {joined_string} where DirName = "{item}";""".format(joined_string=joined_string,
                                                                                          item=item)
        db_query(self.conn, stmt)
        self.conn.commit()

        return

    def delete(self, item):
        try:
            if os.path.isfile(item):
                stmt = """SELECT * from {dirs} where DirName = "{item}";""".format(dirs="dirs",
                                                                                   item=os.path.dirname(item))
                thing = db_query(self.conn, stmt)
                q = thing.fetchone()
                file = os.path.basename(item)
                stmt = """DELETE from {files} where ParentDirID = "{dir}" AND FileName = "{file}";""".format(
                    files="files", dir=q[2], file=file)
                db_query(self.conn, stmt)
            else:
                folder = os.path.basename(item)
                subfolder = os.path.dirname(item)
                self._addFile(folder, subfolder)
                stmt = """DELETE from {dirs} where DirName = "{item}";""".format(dirs="dirs", item=item)
                db_query(self.conn, stmt)
            return "Success"
        except Error as e:
            return e

    def addColumn(self, table=None, column=None):
        stmt = """ALTER TABLE {table} ADD COLUMN {column} TEXT;""".format(table=table, column=column)
        db_query(self.conn, stmt)

    def get(self, item):
        if os.path.isfile(item):
            stmt = """SELECT * from {dirs} where DirName = "{item}";""".format(dirs="dirs", item=os.path.dirname(item))
            thing = db_query(self.conn, stmt)
            q = thing.fetchone()
            file = os.path.basename(item)
            stmt = """SELECT * from {files} where ParentDirID = "{dir}" AND FileName = "{file}";""".format(
                files="files", dir=q[2], file=file)
            response = db_query(self.conn, stmt).fetchone()
            response = {"FileID": response[0],
                        "FileName": response[1],
                        "ParentDirID": response[2],
                        "st_size": response[3],
                        "st_ctime_ns": response[4],
                        "st_atime_ns": response[5],
                        "st_mtime_ns": response[6],
                        "st_nlink": response[7]}
            return response
        else:
            folder = os.path.basename(item)
            subfolder = os.path.dirname(item)
            self._addFile(folder, subfolder)
            stmt = """SELECT * from {dirs} where DirName = "{item}";""".format(dirs="dirs", item=item)
            response = db_query(self.conn, stmt).fetchone()
            response = {"DirID": response[0],
                        "DirName": response[1],
                        "ParentDirID": response[2],
                        "st_ctime_ns": response[3],
                        "st_atime_ns": response[4],
                        "st_mtime_ns": response[5]}
            return response

    def set(self, item, **kwargs):
        if kwargs:
            self._update(item, **kwargs)
            exit()
        if os.path.isfile(item):
            file = os.path.basename(item)
            folder = os.path.dirname(item)
            self._addFile(folder, file)
        if os.path.isdir(item):
            folder = os.path.basename(item)
            subfolder = os.path.dirname(item)
            self._addDir(folder, subfolder)
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


def db_query(conn, stmt, data_tuple=None):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        if data_tuple:
            return c.execute(stmt, data_tuple)
        else:
            return c.execute(stmt)
    except Error as e:
        print(e)
