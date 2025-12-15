from pyspark import SparkConf, SparkContext
import collections

# conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# sc = SparkContext(conf = conf)

# lines = sc.textFile("./resources/ml-100k/u.data")
# ratings = lines.map(lambda x: x.split()[2])
# result = ratings.countByValue()

# sortedResults = collections.OrderedDict(sorted(result.items()))
# for key, value in sortedResults.items():
#     print("%s %i" % (key, value))



# conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
# sc = SparkContext(conf = conf)

# def parseLine(line):
#     fields = line.split(',')
#     age = int(fields[2])
#     numFriends = int(fields[3])
#     return (age, numFriends)

# lines = sc.textFile("./resources/fakefriends.csv")
# rdd = lines.map(parseLine)
# totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
# results = averagesByAge.collect()
# for result in results:
#     print(result)


# conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
# sc = SparkContext(conf = conf)

# def parseLine(line):
#     fields = line.split(',')
#     stationID = fields[0]
#     entryType = fields[2]
#     temperature = float(fields[3])
#     return (stationID, entryType, temperature)

# lines = sc.textFile("./resources/1800.csv")
# parsedLines = lines.map(parseLine)
# minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
# minTemps = minTemps.map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: min(x,y))
# results_min = minTemps.collect()

# maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
# maxTemps = maxTemps.map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: max(x,y))
# results_max = maxTemps.collect()


# for result in results_min:
#     print(result[0] + "\t{:.2f}F".format(result[1]))
    
# for result in results_max:
#     print(result[0] + "\t{:.2f}F".format(result[1]))

# def normalizeWords(text):
#     return re.compile(r'\W+', re.UNICODE).split(text.lower())

# conf = SparkConf().setMaster("local").setAppName("WordCount")
# sc = SparkContext(conf = conf)

# input = sc.textFile("file:///sparkcourse/book.txt")
# words = input.flatMap(normalizeWords)

# wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
# results = wordCountsSorted.collect()

# for result in results:
#     count = str(result[0])
#     word = result[1].encode('ascii', 'ignore')
#     if (word):
#         print(word.decode() + ":\t\t" + count)

conf = SparkConf().setMaster("local[*]").setAppName("TotalAmountSpent")
sc = SparkContext(conf=conf)

def totamt(line):
    fields = line.split(',')
    cid = int(fields[0])
    amount = float(fields[2])
    return (cid, amount)

lines = sc.textFile("./resources/customer-orders.csv")
tots = lines.map(totamt).reduceByKey(lambda x, y: x+y)
sortedTotsAsc = tots.map(lambda x: (x[1], x[0])).sortByKey(ascending=True)
sortedTotsDesc = tots.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
totsResults = tots.collect()
ascResults = sortedTotsAsc.collect()
descResults = sortedTotsDesc.collect()


for result in ascResults:
    print(result)
print("-----------------------------------------------------------------------")
for result in descResults:
    print(result)

