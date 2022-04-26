# dbgz
DBGZ (Data block "GNU" zip) is a lightweight seekable compressed binary format for storing and retrieving data. The format is designed to hold schema and data comprising primary objects, such as numbers, strings and arrays, as well as more complicated instances via msgpack, such as dictionaries and python objects. It is based on the bgzip file format used in biology and other fields.

This utility can be used to write and read DBGZ files in python.

## Installation

Install using pip

```bash
pip install dbgz
```

or from source:
```bash
pip git+https://github.com/filipinascimento/dbgz.git
```

## Usage
First import dbgz:
```python
import dbgz
```

Defining a scheme

```python
scheme = [
    ("anInteger","i"),
    ("aFloat","f"),
    ("aString","s"),
    ("anIntArray","I"),
    ("aFloatArray","F"),
    ("anStringArray","S"),
    ("anyType","a"), #any data type
]
```

Writing some data to a dbgz file
```python
from tqdm.auto import tqdm # Optional, to print progress bar
# pip install tqdm

totalCount = 1000000;
with dbgz.DBGZWriter("test.dbgz",scheme) as fd:
    # New entries can be added as:
    fd.write(anInteger=1, aString="1")
    fd.write(anInteger=2, aString="2", aFloat=5)
    fd.write(anInteger=3, aString="3",anIntArray=list(range(10)), aFloatArray=[0.1,0.2,0.3,0.5])

    # Here is a loop to write a lot of data:
    for index in tqdm(range(totalCount)):
        fd.write(
            anInteger=index,
            aFloat=index*0.01,
            anIntArray=list(range(index,index+10)),
            aString=str(index),
            aFloatArray=[index+0.1,index-0.2,index+0.3,index+0.4],
            anStringArray=[str(index),str(index+1),str(index+2),str(index+3)],
            anyType={"a": index, "b": index+1, "c": index+2}
        )
```

Reading the dbgz file sequencially:
```python
with dbgz.DBGZReader("test.dbgz") as fd:
    print(fd.scheme)
    for entry in tqdm(fd.entries,total=fd.entriesCount):
        assert entry["anInteger"] == int(entry["aString"])
```

Loading a dbgz file manually by using the `read()` method:
```python
with dbgz.DBGZReader("test.dbgz") as fd:
    pbar = tqdm(total=fd.entriesCount)
    print(fd.scheme)
    while True:
        entries = fd.read(10)
        if(not entries):
            break
        for entry in entries:
            assert entry["anInteger"] == int(entry["aString"])
        pbar.update(len(entries))
pbar.refresh()
pbar.close()
```

Saving dictionary to file and loading it again
```python
with dbgz.DBGZReader("test.dbgz") as fd:
    indexDictionary = fd.generateIndex("anInteger",
        indicesPath=None,
        filterFunction=lambda entry: entry["anInteger"]<10,
        useDictionary=True,
        showProgressbar = True
        )
    for key,values in indexDictionary.items():
        print(key,values)
        for value in values:
            assert int(key) == fd.readAt(value)[0]["anInteger"]
```

Saving dictionary to file and loading it again
```python
with dbgz.DBGZReader("test.dbgz") as fd:
    fd.generateIndex(
        key = "anInteger",
        indicesPath="test_byAnInteger.idbgz", 
        filterFunction=lambda entry: entry["anInteger"]<10,
        useDictionary=True,
        showProgressbar = True
        )

    indexDictionary = dbgz.readIndicesDictionary("test_by.idbgz")
    for key,values in indexDictionary.items():
        print(key,values)
        for value in values:
            assert int(key) == fd.readAt(value)[0]["anInteger"]

```


Using a custom key generator for the index:
```python
with dbgz.DBGZReader("test.dbgz") as fd:
    fd.generateIndex(
        keyFunction=lambda entry: entry["anyType"]["b"] if entry["anyType"] else None,
        indicesPath="test_byAnyType_b.idbgz", 
        filterFunction=lambda entry: entry["anInteger"]<10,
        useDictionary=True,
        showProgressbar = True
        )

    indexDictionary = dbgz.readIndicesDictionary("test_byAnyType_b.idbgz")
    for key,values in indexDictionary.items():
        print(key,values)
        for value in values:
            entry = fd.readAt(value)[0]
            assert int(key) == entry["anyType"]["b"] if entry["anyType"] else None

```