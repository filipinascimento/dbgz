
from .bgzf import BgzfWriter
from .bgzf import BgzfReader
import struct
import os
import msgpack

# _sizeStructCache = {}

def _calcSize(format):
  structSize = struct.calcsize(format)
  # _sizeStructCache[format] = structSize;
  return structSize


def any2Data(anyObject):
  data = msgpack.packb(anyObject,use_bin_type=True)
  data = struct.pack("<Q",int(len(data)))+data
  return data

def data2Any(data,currentPointer):
  pointerSize = _calcSize("<Q")
  dataLength, = struct.unpack("<Q",data[currentPointer:currentPointer+pointerSize])
  if dataLength == 0:
    # print("data length is 0")
    return (None,currentPointer+pointerSize)
  return (
    msgpack.unpackb(data[currentPointer+pointerSize:currentPointer+pointerSize+dataLength],raw=False),
    currentPointer+dataLength+pointerSize)

def string2Data(text):
  if(text is not None):
    textData = text.encode("utf8")
  else:
    textData = "".encode("utf8")
  data = struct.pack("<Q",int(len(textData)))
  data+=textData
  return data

def data2String(data,currentPointer):
  pointerSize = _calcSize("<Q")
  stringLength, = struct.unpack("<Q",data[currentPointer:currentPointer+pointerSize])
  return (
    data[currentPointer+pointerSize:currentPointer+pointerSize+stringLength].decode("utf8"),
    currentPointer+stringLength+pointerSize)

def int2Data(value):
  return struct.pack("<q",value)

def intArray2Data(values):
  return struct.pack("<Q%uq"%len(values),len(values),*values)

def data2Int(data,currentPointer):
  pointerSize = _calcSize("<q")
  value, = struct.unpack("<q",data[currentPointer:currentPointer+pointerSize])
  return (value,currentPointer+pointerSize)

def data2IntArray(data,currentPointer):
  offset = _calcSize("<Q")
  count, = struct.unpack("<Q",data[currentPointer:currentPointer+offset])
  pointerSize = count*_calcSize("<q")
  values=()
  if(count):
    values = struct.unpack("<%uq"%count,data[(currentPointer+offset):(currentPointer+offset+pointerSize)])
  return (values,currentPointer+offset+pointerSize)

def uint2Data(value):
  return struct.pack("<Q",value)

def uintArray2Data(values):
  return struct.pack("<Q%uQ"%len(values),len(values),*values)

def data2UInt(data,currentPointer):
  pointerSize = _calcSize("<Q")
  value, = struct.unpack("<Q",data[currentPointer:currentPointer+pointerSize])
  return (value,currentPointer+pointerSize)

def data2UIntArray(data,currentPointer):
  offset = _calcSize("<Q")
  count, = struct.unpack("<Q",data[currentPointer:currentPointer+offset])
  pointerSize = count*_calcSize("<Q")
  values=()
  if(count):
    values = struct.unpack("<%uQ"%count,data[(currentPointer+offset):(currentPointer+offset+pointerSize)])
  return (values,currentPointer+offset+pointerSize)
  
def double2Data(value):
  return struct.pack("<d",value)

def doubleArray2Data(values):
  return struct.pack("<Q%ud"%len(values),len(values),*values)

def data2Double(data,currentPointer):
  pointerSize = _calcSize("<d")
  value, = struct.unpack("<d",data[currentPointer:currentPointer+pointerSize])
  return (value,currentPointer+pointerSize)
  
def data2DoubleArray(data,currentPointer):
  offset = _calcSize("<Q")
  count, = struct.unpack("<Q",data[currentPointer:currentPointer+offset])
  pointerSize = count*_calcSize("<d")
  values=()
  if(count):
    values = struct.unpack("<%ud"%count,data[(currentPointer+offset):(currentPointer+offset+pointerSize)])
  return (values,currentPointer+offset+pointerSize)
  
 
def float2Data(value):
  return struct.pack("<f",value)

def floatArray2Data(values):
  return struct.pack("<Q%uf"%len(values),len(values),*values)

def data2Float(data,currentPointer):
  pointerSize = _calcSize("<f")
  value, = struct.unpack("<f",data[currentPointer:currentPointer+pointerSize])
  return (value,currentPointer+pointerSize)

def data2FloatArray(data,currentPointer):
  offset = _calcSize("<Q")
  count, = struct.unpack("<Q",data[currentPointer:currentPointer+offset])
  pointerSize = count*_calcSize("<f")
  values=()
  if(count):
    values = struct.unpack("<%uf"%count,data[(currentPointer+offset):(currentPointer+offset+pointerSize)])
  return (values,currentPointer+offset+pointerSize)
  


def stringArray2Data(textArray):
  data = struct.pack("<Q",int(len(textArray)))
  for text in textArray:
    data+=string2Data(text)
  return data

def data2StringArray(data,currentPointer):
  offset = _calcSize("<Q")
  count, = struct.unpack("<Q",data[currentPointer:currentPointer+offset])
  values = []
  pointerSize = 0
  currentPointer+=offset
  for i in range(count):
    (value,currentPointer) = data2String(data,currentPointer)
    values.append(value)

  return (tuple(values),currentPointer)


#default,encode,decode
_typesDictionary = {
  "i": (0,int2Data,data2Int),
  "u": (0,uint2Data,data2UInt),
  "f": (float("NaN"),float2Data,data2Float),
  "d": (float("NaN"),double2Data,data2Double),
  "s": ("",string2Data,data2String),

  "I": ((),intArray2Data,data2IntArray),
  "U": ((),uintArray2Data,data2UIntArray),
  "F": ((),floatArray2Data,data2FloatArray),
  "D": ((),doubleArray2Data,data2DoubleArray),
  "S": ((),stringArray2Data,data2StringArray),

  "a": (None,any2Data,data2Any),
}

class DBGZWriter():
  def __init__(self, filename, scheme, *args, **kwargs):
    self.scheme = scheme
    self.fd = BgzfWriter(filename,mode="wb",*args, **kwargs)
    self.writeScheme()
    self.aggregatedData = b''
    self.totalEntries = 0
  def __enter__(self):
    #ttysetattr etc goes here before opening and returning the file object
    return self
  def __exit__(self, type, value, traceback):
    #Exception handling here
    self.close()
  def close(self):
    self._aggregatedUpdate(True)
    self.fd.close(extraData = struct.pack("<Q",self.totalEntries))
  def write(self,**kargs):
    data = b''
    values = [entry[0] for entry in self.index2Type]
    for index,name in enumerate(self.index2Name):
      if(name in kargs):
        values[index] = kargs[name]
    self.writeFromArray(values)

  def writeFromArray(self,values):
    entryData = b""
    for (index,(default,encode,decode)) in enumerate(self.index2Type):
      currentValue = values[index]
      if(currentValue is None):
        currentValue = default
      entryData+=encode(currentValue)
    self.totalEntries+=1
    finalData = (struct.pack("<Q",len(entryData))+entryData)
    self.aggregatedData+=finalData
    self._aggregatedUpdate()
  
  def writeScheme(self):
    self.name2Index = {}
    self.index2Name = []
    self.index2Type = []
    data = b''
    for (typeIndex,(typeName,typeType)) in enumerate(self.scheme):
      assert typeName not in self.name2Index
      self.name2Index[typeName] = typeIndex
      self.index2Name.append(typeName)
      self.index2Type.append(_typesDictionary[typeType])
      data+=string2Data(typeName)
      data+=typeType.encode("utf8")
    self.fd.write(struct.pack("<Q",len(data)))
    self.fd.write(data)
  def _aggregatedUpdate(self,flush=False):
    if(flush or len(self.aggregatedData)>2000):
        self.fd.write(self.aggregatedData)
        self.aggregatedData=b''

class DBGZReader():
  def __init__(self, filename, *args, **kwargs):
    self.fd = BgzfReader(filename, mode="rb",*args, **kwargs)
    self.entriesCount = self.readEntriesCount()
    self._readScheme()
    self.startPosition = self.fd.tell()
  def __enter__(self):
    #ttysetattr etc goes here before opening and returning the file object
    return self
  def __exit__(self, type, value, traceback):
    #Exception handling here
    self.close()
  def reset(self):
      self.fd.seek(self.startPosition)
  def currentPosition(self):
      return self.fd.tell()
  
  @property
  def entries(self):
    while True:
      entries = self.read(100)
      if(not entries):
        break
      for entry in entries:
        yield entry

  @property
  def entriesAsList(self):
    while True:
      entries = self.readAsList(100)
      if(not entries):
        break
      for entry in entries:
        yield entry

  def readEntriesCount(self):
    count=0
    startPoint = self.fd._handle.tell()
    pointerSize = _calcSize("<Q")
    self.fd._handle.seek(-pointerSize, os.SEEK_END)
    count, = struct.unpack("<Q",self.fd._handle.read(pointerSize))
    self.fd._handle.seek(startPoint)
    return count

  def _readScheme(self):
    pointerSize = _calcSize("<Q")
    dataSize, = struct.unpack("<Q",self.fd.read(pointerSize))
    data = self.fd.read(dataSize)
    currentPointer = 0
    self.scheme = []
    self.name2Index = {}
    self.index2Name = []
    self.index2Type = []
    while(currentPointer < dataSize):
      text,currentPointer = data2String(data,currentPointer)
      dataType = data[currentPointer:currentPointer+1].decode("utf8")
      currentPointer+=1
      self.scheme.append((text,dataType));
    for (typeIndex,(typeName,typeType)) in enumerate(self.scheme):
      assert typeName not in self.name2Index
      self.name2Index[typeName] = typeIndex
      self.index2Name.append(typeName)
      self.index2Type.append(_typesDictionary[typeType])
  def read(self,count=1,getPositions=False):
    pointerSize = _calcSize("<Q")
    entries = []
    for _ in range(count):
      position = self.fd.tell()
      sizeData = self.fd.read(pointerSize)
      if(not sizeData):
        break
      dataSize, = struct.unpack("<Q",sizeData)
      data = self.fd.read(dataSize)
      entry = {}
      currentPointer = 0
      for (index,(default,encode,decode)) in enumerate(self.index2Type):
        entry[self.index2Name[index]],currentPointer = decode(data,currentPointer)
      if(getPositions):
        entry["_position"] =  position
      entries.append(entry)
    return entries
  def readAsList(self,count=1,getPositions=False):
    pointerSize = _calcSize("<Q")
    entries = []
    for _ in range(count):
      position = self.fd.tell()
      sizeData = self.fd.read(pointerSize)
      if(not sizeData):
        break;
      dataSize, = struct.unpack("<Q",sizeData)
      data = self.fd.read(dataSize)
      entry = []
      currentPointer = 0
      for (index,(default,encode,decode)) in enumerate(self.index2Type):
        entryValue,currentPointer = decode(data,currentPointer)
        entry.append(entryValue)
      if(getPositions):
        entry.append(position)
      entries.append(entry)
    return entries
  def readAt(self,position,count=1, getPositions=False):
    self.fd.seek(int(position))
    return self.read(count,getPositions)
  def readAsListAt(self,position,count=1, getPositions=False):
    self.fd.seek(int(position))
    return self.readAsList(count,getPositions)
  
  def generateIndex(self,
    key = None,
    keyFunction = None,
    filterFunction=None,
    indicesPath=None,
    useDictionary=True,
    showProgressbar = True,
    maxCount = -1,
  ):
    """
    Generates an index for the given key.

    Parameters
    ----------
    key : str
      The key used to construct the index.
      (either key or keyFunction must be given)
    keyFunction : function
      A function that takes an entry and returns the value of the key or list of keys.
      If not given, the key is assumed to be a property key.
      (either key or keyFunction must be given)
    filterFunction : function
      A function that takes an entry and returns True if the entry should be included
      in the index.
      (optional)
    indicesPath : str
      The path to the directory where the index will be stored.
      If not given, the index will not be saved automatically.
      (optional)
    useDictionary : bool
      If True, will use the dictionary representation for the keyFunction and filterFunction.
      (detaults to True)
    showProgressbar : bool
      If True, will show a progressbar while indexing.
      (defaults to True)
    maxCount : int
      If given, will only index the first maxCount entries.
      (defaults to -1, which means all entries)
    
    Returns
    -------
    index : dict
      The index (only if indicesPath is not given)
    """

    if(showProgressbar):
      from tqdm.auto import tqdm
    if(keyFunction is None):
      if(key is None):
        raise Exception("At least key or keyFunction must be given")
      if(key not in self.name2Index):
        raise Exception("Property '"+key+"' not found in the scheme")
      propertyIndex = self.name2Index[key]
    #no point in using a dictionary if we don't have a filter or key function
    if((filterFunction is None) and (keyFunction is None)):
      useDictionary = False
    if(showProgressbar):
      estimatedCount = self.entriesCount
      if(maxCount>=0):
        estimatedCount = maxCount
      pbar = tqdm(total=estimatedCount)
    savedPosition = self.currentPosition()
    self.reset()
    if(indicesPath is not None):
      fd = BgzfWriter(indicesPath,"w")
    else:
      indexDictionary = {}
    entriesCount = 0
    while True:
      if(useDictionary):
        entries = self.read(100,True)
      else:
        entries = self.readAsList(100,True)
      if(entries):
        for entry in entries:
          if(showProgressbar):
            pbar.update(1)
          if(filterFunction is not None and not filterFunction(entry)):
            continue
          entriesCount+=1
          if(maxCount>=0 and entriesCount>maxCount):
            break
          else:
            if(keyFunction is not None):
                propertyValues = keyFunction(entry)
            else:
              if(useDictionary):
                propertyValues = entry[key]
              else:
                propertyValues = entry[propertyIndex]
            if(useDictionary):
                position = entry["_position"]
            else:
              position = entry[-1]
            
            if(propertyValues is not None):
              if(not isinstance(propertyValues,tuple) and not isinstance(propertyValues,list)):
                propertyValues = (propertyValues,)
              for propertyValue in propertyValues:
                if(propertyValue is not None and propertyValue!=""):
                  if(indicesPath is not None):
                    data = str(propertyValue).encode("utf8")
                    fd.write(struct.pack("<QQ",len(data)+8,position)+data)
                  else:
                    if(propertyValue not in indexDictionary):
                      indexDictionary[propertyValue] = []
                    indexDictionary[propertyValue].append(position)
      else:
        break
      if(maxCount>=0 and entriesCount>maxCount):
        break
    
    self.fd.seek(savedPosition)
    if(showProgressbar):
        pbar.refresh()
        pbar.close()
    if(indicesPath is not None):
      fd.close(extraData = struct.pack("<Q",entriesCount))
      return None
    else:
      return indexDictionary

  def close(self):
    self.fd.close()


def readIndicesDictionary(indicesDataPath, showProgressbar=False):
  if(showProgressbar):
    from tqdm.auto import tqdm
  with BgzfReader(indicesDataPath,"rb") as fd:
    if(showProgressbar):
      startPoint = fd._handle.tell()
      pointerSize = _calcSize("<Q")
      fd._handle.seek(-pointerSize, os.SEEK_END)
      totalEntriesCount, = struct.unpack("<Q",fd._handle.read(pointerSize))
      fd._handle.seek(startPoint)
      pbar = tqdm(total=totalEntriesCount)
    value2Positions = {}
    while True:
      data = fd.read(8*2)
      if(len(data)==8*2):
        if(showProgressbar):
          pbar.update(1)
        dataSize,position = struct.unpack("<QQ",data)
        value = fd.read(dataSize-8).decode("utf-8")
        if(value not in value2Positions):
          value2Positions[value] = []
        value2Positions[value].append(position)
      else: 
          break
    if(showProgressbar):
      pbar.close()
    return value2Positions
