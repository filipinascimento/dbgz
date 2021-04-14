
from .bgzf import BgzfWriter
from .bgzf import BgzfReader
import struct


_sizeStructCache = {}

def _calcSize(format):
  structSize = struct.calcsize(format)
  _sizeStructCache[format] = structSize;
  return structSize

def string2Data(text):
  textData = text.encode("utf8");
  data = struct.pack("<q",int(len(textData)))
  data+=textData
  return data

def data2String(data,currentPointer):
  pointerSize = _calcSize("<q")
  stringLength, = struct.unpack("<q",data[currentPointer:currentPointer+pointerSize])
  return (
    data[currentPointer+pointerSize:currentPointer+pointerSize+stringLength].decode("utf8"),
    currentPointer+stringLength+pointerSize)

def int2Data(value):
  return struct.pack("<i",value)

def intArray2Data(values):
  return struct.pack("<q%ui"%len(values),len(values),*values)

def data2Int(data,currentPointer):
  pointerSize = _calcSize("<i")
  value, = struct.unpack("<i",data[currentPointer:currentPointer+pointerSize])
  return (value,currentPointer+pointerSize)

def data2IntArray(data,currentPointer):
  offset = _calcSize("<q")
  count, = struct.unpack("<q",data[currentPointer:currentPointer+offset])
  pointerSize = count*_calcSize("<i")
  values=()
  if(count):
    values = struct.unpack("<%ui"%count,data[(currentPointer+offset):(currentPointer+offset+pointerSize)])
  return (values,currentPointer+offset+pointerSize)

def uint2Data(value):
  return struct.pack("<q",value)

def uintArray2Data(values):
  return struct.pack("<q%uq"%len(values),len(values),*values)

def data2UInt(data,currentPointer):
  pointerSize = _calcSize("<q")
  value, = struct.unpack("<q",data[currentPointer:currentPointer+pointerSize])
  return (value,currentPointer+pointerSize)

def data2UIntArray(data,currentPointer):
  offset = _calcSize("<q")
  count, = struct.unpack("<q",data[currentPointer:currentPointer+offset])
  pointerSize = count*_calcSize("<q")
  values=()
  if(count):
    values = struct.unpack("<%uq"%count,data[(currentPointer+offset):(currentPointer+offset+pointerSize)])
  return (values,currentPointer+offset+pointerSize)
  
def double2Data(value):
  return struct.pack("<d",value)

def doubleArray2Data(values):
  return struct.pack("<q%ud"%len(values),len(values),*values)

def data2Double(data,currentPointer):
  pointerSize = _calcSize("<d")
  value, = struct.unpack("<d",data[currentPointer:currentPointer+pointerSize])
  return (value,currentPointer+pointerSize)
  
def data2DoubleArray(data,currentPointer):
  offset = _calcSize("<q")
  count, = struct.unpack("<q",data[currentPointer:currentPointer+offset])
  pointerSize = count*_calcSize("<d")
  values=()
  if(count):
    values = struct.unpack("<%ud"%count,data[(currentPointer+offset):(currentPointer+offset+pointerSize)])
  return (values,currentPointer+offset+pointerSize)
  
 
def float2Data(value):
  return struct.pack("<f",value)

def floatArray2Data(values):
  return struct.pack("<q%uf"%len(values),len(values),*values)

def data2Float(data,currentPointer):
  pointerSize = _calcSize("<f")
  value, = struct.unpack("<f",data[currentPointer:currentPointer+pointerSize])
  return (value,currentPointer+pointerSize)

def data2FloatArray(data,currentPointer):
  offset = _calcSize("<q")
  count, = struct.unpack("<q",data[currentPointer:currentPointer+offset])
  pointerSize = count*_calcSize("<f")
  values=()
  if(count):
    values = struct.unpack("<%uf"%count,data[(currentPointer+offset):(currentPointer+offset+pointerSize)])
  return (values,currentPointer+offset+pointerSize)
  

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
  # "S": ((),stringArray2Data,data2StringArray),
}

class DBGZWriter():
  def __init__(self, filename, scheme, *args, **kwargs):
    self.scheme = scheme
    self.fd = BgzfWriter(filename,*args, **kwargs)
    self.writeScheme()
    self.aggregatedData = b''
  def __enter__(self):
    #ttysetattr etc goes here before opening and returning the file object
    return self
  def __exit__(self, type, value, traceback):
    #Exception handling here
    self.close()
  def close(self):
    self._aggregatedUpdate(True)
    self.fd.close()
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
      entryData+=encode(values[index])
    finalData = (struct.pack("<q",len(entryData))+entryData)
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
    self.fd.write(struct.pack("<q",len(data)))
    self.fd.write(data)
  def _aggregatedUpdate(self,flush=False):
    if(flush or len(self.aggregatedData)>2000):
        self.fd.write(self.aggregatedData);
        self.aggregatedData=b''

class DBGZReader():
  def __init__(self, filename, *args, **kwargs):
    self.fd = BgzfReader(filename,*args, **kwargs)
    print("READING SCHEME...");
    self._readScheme()
  def __enter__(self):
    #ttysetattr etc goes here before opening and returning the file object
    return self
  def __exit__(self, type, value, traceback):
    #Exception handling here
    self.close()
  def _readScheme(self):
    pointerSize = _calcSize("<q")
    dataSize, = struct.unpack("<q",self.fd.read(pointerSize))
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
  def read(self,count=1):
    pointerSize = _calcSize("<q")
    entries = []
    for _ in range(count):
      sizeData = self.fd.read(pointerSize)
      if(not sizeData):
        break;
      dataSize, = struct.unpack("<q",sizeData)
      data = self.fd.read(dataSize)
      entry = {}
      currentPointer = 0
      for (index,(default,encode,decode)) in enumerate(self.index2Type):
        entry[self.index2Name[index]],currentPointer = decode(data,currentPointer)
      entries.append(entry)
    return entries
  def readAsList(self,count=1):
    pointerSize = _calcSize("<q")
    entries = []
    for _ in range(count):
      sizeData = self.fd.read(pointerSize)
      if(not sizeData):
        break;
      dataSize, = struct.unpack("<q",sizeData)
      data = self.fd.read(dataSize)
      entry = []
      currentPointer = 0
      for (index,(default,encode,decode)) in enumerate(self.index2Type):
        entryValue,currentPointer = decode(data,currentPointer)
        entry.append(entryValue)
      entries.append(entry)
    return entries
  def close(self):
    self.fd.close()