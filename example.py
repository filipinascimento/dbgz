
from dbgz.dbgz import DBGZWriter
from dbgz.dbgz import DBGZReader
from tqdm.auto import tqdm
scheme = [
  ("Integer","i"),
  ("Float","f"),
  ("String","s"),
  ("IntArray","I"),
  ("FloatArray","F"),
]

with DBGZWriter("test.dbgz",scheme,mode="wb") as fd:
  # fd.write(Integer=1,String="a string")
  # fd.write(Integer=2,String="another string", Float=5)
  # fd.write(Integer=3,IntArray=list(range(10)),FloatArray=[0.1,0.2,0.3,0.5])
  for index in tqdm(range(10000000)):
    fd.write(Integer=index,Float=index*0.01,IntArray=list(range(index,index+10)),String=str(index),FloatArray=[index+0.1,index-0.2,index+0.3,index+0.4])

def loadit():
  pbar = tqdm(total=10000000)
  with DBGZReader("test.dbgz",mode="rb") as fd:
    print(fd.scheme)
    while True:
      entries = fd.read(10)
      if(not entries):
        break
      for entry in entries:
        assert entry["Integer"] == int(entry["String"])
      pbar.update(len(entries))

loadit()


