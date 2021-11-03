A library for traversing a given directory and creating a database of files and folders with their properties. Two tables are created, `files` and `dirs`.
Usage:

```
import traversey

dir = "C:\\Users\\User\\Downloads"
td = traversey.traverse(dir, 'database.db')
```

Due to limitations with sqlite and its ability to handle large integers, most of the schema is textual and not numerical. For example, if you query an item to get the creation time, it will be a string. Although that string will be numbers, it will programatically need to be converted to an int in Python to be treated as such. SQLite also does not have a date/time type. 
