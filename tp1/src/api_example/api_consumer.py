import pandas
import pyarrow
import time
import sys
from pyarrow import parquet

def get_data(): 
  return [{"a":"b"}]

def store_data(data, counter):
  print str(counter) + ': ' + str(data)

if __name__ == '__main__':
  if len(sys.argv) != 4:
      print('Usage: <access_token> <access_token_secret> <first_file_no>')
      sys.exit(1)
  access_token = sys.argv[1]
  access_token_secret = sys.argv[2]
  counter = int(sys.argv[3])
  
  data = []
  while True:
    if len(data) >= 1000:
       store_data(data, counter)
       counter += 1
       data = []
    datum = get_data()
    data.extend(datum)
    
    time.sleep(60)
