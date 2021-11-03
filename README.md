A library for traversing a given directory and creating a database of files and folders with their properties. Two tables are created, `files` and `dirs`.
Usage:
`import traversey`

`dir = "C:\\Users\\User\\Downloads"`
`td = traversey.traverse(dir, 'database.db')`
`td.scan()`
