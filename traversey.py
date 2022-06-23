import os
import sqlite3
from sqlite3 import Error
import platform

# TODO: try catch for file not found or cannot read.
# TODO: make case sensitive because of linux.

class db:
    def __init__(self, db_file):
        """ create a database connection to the SQLite database
        specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        self.os_delimiter = self._determineOS()
        try:
            self.conn = sqlite3.connect(db_file)
        except Error as e:
            print(e)

    def traverse(self, rootdir):
        build = {}
        build["sql_create_dirs_table"] = """CREATE TABLE dirs (DirID, 
                                                      DirName NOT NULL, 
                                                      ParentDirID INTEGER,
                                                      st_ctime_ns INTEGER NOT NULL,
                                                      st_atime_ns INTEGER NOT NULL,
                                                      st_mtime_ns INTEGER NOT NULL,
                                                      PRIMARY KEY("DirID"));"""
        build["sql_create_files_table"] = f"""CREATE TABLE files (FileID NOT NULL,
                                                        FileName NOT NULL,
                                                        ParentDirID INTEGER NOT NULL,
                                                        st_size INTEGER NOT NULL,
                                                        st_ctime_ns INTEGER NOT NULL,
                                                        st_atime_ns INTEGER NOT NULL,
                                                        st_mtime_ns INTEGER NOT NULL,
                                                        st_nlink INTEGER NOT NULL,
                                                        FOREIGN KEY("ParentDirID") REFERENCES "dirs"("DirID"),
                                                        PRIMARY KEY("FileName","ParentDirID")); """
        build["sql_create_filedata_view"] = f"""CREATE VIEW filedata as
                        select files.FileName,
                        dirs.DirName,
                        (select dirs.DirName || '{self.os_delimiter}' || files.FileName) as "FilePath",
                        st_size as Size,
                        files.FileID,
                        files.ParentDirID,
                        (Select datetime(files.st_ctime_ns/1000000000, 'unixepoch', 'localtime')) as "DateCreated",
                        (Select datetime(files.st_mtime_ns/1000000000, 'unixepoch', 'localtime')) as "DateModified"
                        from files join dirs on dirs.DirID = files.ParentDirID;"""

        # create tables
        if self.conn:
            for key in build:
                try:
                    c = self.conn.cursor()
                    c.execute(build[key])
                except Error as e:
                    print(e)

            self.inodes = set()
            self._scan(rootdir)
            self.conn.commit()

    def _determineOS(self):
        if "Windows" in platform.system():
            return '\\'
        else:
            return "/"

    def _scan(self, rootdir):
        adddirs = [[rootdir, rootdir]]
        addfiles = []
        x=0
        y=0
        for folder, subfolders, files in os.walk(rootdir, topdown=True):
            for subfolder in subfolders:
                adddirs.append([folder, subfolder])
                x=x+1
            for file in files:
                addfiles.append([folder, file])
                y=y+1
            print("Directories found: " + "{:,}".format(x + 1) + "\nFiles found: " + "{:,}".format(y + 1), end="\033[F")
        print("")
        print("")
        for i, directory in enumerate(adddirs):
            try:
                db._addDir(self, directory[0], directory[1])
                print("Adding directory", str("{:,}".format(i + 1)) + "/" + str("{:,}".format(len(adddirs))), end="\r")
            except Error as e:
                print(e)
        print("")
        for j, file in enumerate(addfiles):
            try:
                db._addFile(self, file[0], file[1])
                print("Adding file", str("{:,}".format(j + 1)) + "/" + str("{:,}".format(len(addfiles))), end="\r")
            except Error as e:
                print(e)
        print("")

    def _addFile(self, folder, file):
        """ Don't use this function. Use set instead. """
        path = os.path.join(folder, file)
        parent_dir_id = os.stat(folder).st_ino
        try:
            stat = os.stat(path)
        except FileNotFoundError as fnf:
            print(fnf)
            if os.path.islink(path):
                print("     The path is a symbolic link.")
                print("     The link points to: " + os.readlink(path))
            if not os.path.exists(path):
                print("     The file does not exist.")
            return
        except Exception as e:
            print(e)
        #self.inodes.add(stat.st_ino)
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
        self.conn.execute(stmt, data_tuple)
        return

    def _addDir(self, folder, subfolder):
        """ Don't use this function. Use set instead. """
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
        try:
            self.conn.execute(stmt, data_tuple)
            # Unique constraint violation is because of case-insensitivity. Getting same inode because it's using
            # the same directory.
        except Error as e:
            print(e)
            print(str(stat.st_ino))
            print(path)
        return

    def _deleteDir(self, item):
        stmt = """DELETE from dirs where DirName = "{item}";""".format(item=os.path.normpath(item))
        response = self._db_query(stmt)
        self.conn.commit()
        return response

    def _deleteFile(self, item):
        stmt = """DELETE from files where (FileName, ParentDirID) = 
            (SELECT FileName, ParentDirID from filedata where FilePath = "{item}");""".format(item=os.path.normpath(item))
        response = self._db_query(stmt)
        self.conn.commit()
        return response

    def _update(self, item, **kwargs):
        """ Don't use this function. Use set instead. """
        list = []
        for key, value in kwargs.items():
            list.append(f'{key} = "{value}"')
        joined_string = ", ".join(list)
        if os.path.isfile(item):
            stmt = """SELECT * from dirs where DirName = "{item}";""".format(item=os.path.dirname(item))
            thing = db._db_query(self, stmt)
            q = thing.fetchone()
            file = os.path.basename(item)
            stmt = """UPDATE files SET {joined_string} where ParentDirID = "{dir}" AND FileName = "{file}";""".format(
                joined_string=joined_string, dir=q[2],
                file=file)
            print(stmt)
        else:
            stmt = """UPDATE dirs SET {joined_string} where DirName = "{item}";""".format(joined_string=joined_string,
                                                                                          item=item)
        print(db._db_query(self, stmt))

        print(self.conn.commit())

        return

    def _db_query(self, stmt, data_tuple=None):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            c = self.conn.cursor()
            if data_tuple:
                return c.execute(stmt, data_tuple)
            else:
                return c.execute(stmt)
        except Error as e:
            print(e)

    def extend(self, table=None, column=None):
        stmt = """ALTER TABLE {table} ADD COLUMN {column} TEXT;""".format(table=table, column=column)
        db._db_query(self, stmt)

    def get(self, item):
        # TODO: Simplify this function
        if os.path.isfile(item):
            stmt = """SELECT * from filedata where FilePath = '{item}';""".format(item=item)
            response = db._db_query(self, stmt).fetchone()
            # TODO maybe make this dict  comprehension or whatever so it will create key/value pairs without specifying.
            response = {key: response[i] for i, key in enumerate(response)}
            print("test:",response)

            return response
        else:
            folder = os.path.basename(item)
            subfolder = os.path.dirname(item)
            self._addFile(folder, subfolder)
            stmt = """SELECT * from dirs where DirName = "{item}";""".format(item=item)
            response = db._db_query(self, stmt).fetchone()
            response = {"DirID": response[0],
                        "DirName": response[1],
                        "ParentDirID": response[2],
                        "st_ctime_ns": response[3],
                        "st_atime_ns": response[4],
                        "st_mtime_ns": response[5]}
            return response

    def set(self, item, **kwargs):
        """ This function is used to set the values of the database.
        :param item: The item to be set. It can be a file or a directory.
        :param kwargs: The values to be set in the form of a dictionary.
        If there are no values to be set, it is assumed to be a new item.
        """
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

    def delete(self, item, is_directory=False):
        # TODO: figure this out for Windows. No way to know if it is a file or dir being deleted.
        # Option 1: Try deleting it from the files table, if nothing happens, then it is a dir.
        # watchdog cannot tell if a deleted item is a file or directory. This is an issue with Windows.
        # so if on Windows we first try to delete a file, if it fails, then we try to delete a dir.
        if "Windows" in platform.system():
            response = self._deleteFile(item)
            changes = response.connection.total_changes
            if changes == 0:
                response = self._deleteDir(item)
        else:
            if is_directory:
                self._deleteDir(item)
            else:
                self._deleteFile(item)

    execute = _db_query
    #execute_many = sqlite3.connect().executemany()
