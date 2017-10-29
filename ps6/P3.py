dir = '/global/scratch/paciorek/wikistats_full'

### read data and do some checks ###

lines = sc.textFile(dir + '/' + 'dated') 

lines.getNumPartitions()  # 16590 (480 input files) for full dataset

# note delayed evaluation

lines.count()  # 9467817626 for full dataset

# watch the UI and watch wwall as computation progresses

testLines = lines.take(10)

testLines[0]

testLines[9]

import re
from operator import add
def find(line, regex = "[Rr]ecession", language = None):
    vals = line.split(’ ’)
    if len(vals) < 6:
        return(False)
    tmp = re.search(regex, vals[3])
    if tmp is None or (language != None and vals[2] != language):
        return(False)
    else:
        return(True)

lines.filter(find).take(100)

recess = lines.filter(find).repartition(480)

recess.count()

def stratify(line):
    # create key-value pairs where:
    # key = date-time-language
    # value = number of website hits
    vals = line.split(' ')
    return(vals[0] + '-' + vals[1] + '-' + vals[2], int(vals[4]))

counts = recess.map(stratify).reduceByKey(add)

def transform(vals):
    # split key info back into separate fields
    key = vals[0].split('-')
    return(",".join((key[0], key[1], key[2], str(vals[1]))))

### output to file ###
# have one partition because one file per partition is written out
outputDir = 'rec'
out = counts.map(transform).repartition(1)
out.saveAsTextFile(outputDir)