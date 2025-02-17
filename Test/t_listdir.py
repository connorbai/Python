from os import listdir
from os.path import isfile, join
import glob

mypath = 'D:/Downloads'
onlyFiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

print(onlyFiles)

print(glob.glob(mypath + "/*"))

if __name__ == '__main__':
    pass
