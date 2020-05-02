import sys
from pyspark import SparkConf, SparkContext
from datetime import datetime

#Example of datetime
#timestamp = "2008-05-15 12:01:00"
#datetimeObject = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
#dayOfTheWeek = datetimeObject.strftime("%a")
#hour = datetimeObject.hour
    
#Initialize Spark application
conf = SparkConf().setAppName("Lab_7")
sc = SparkContext(conf = conf)

inputPath  = sys.argv[1]
outputPath = sys.argv[2] 
criticality_threshold = sys.argv[3]

def discardLines(line):
    values = line.split["\t"]
    if values[3]=='0' and values[4]=='0':
        return False
    else: return True
    
#Read the files
registersRDD = sc.textFile(inputPath+"\registerSample.csv")
stationsRDD = sc.textFile(inputPath+"\stations.csv")

#Discard the headers
registerRDDnoHeader = registersRDD.filter(lambda line: line.startswith("station")==False)
stationsRDDnoHeader = stationsRDD.filter(lambda line: line.startswith("id")==False)

#Filter from registration with 0 free slots and 0 occuped slots
registersFilteredRDD = registerRDDnoHeader.filter(discardLines)

'''TASK 1
Write an application that:
1) Computes the criticality value for each pair (Si,Tj).'''

#???
# Map the registers in this way:
# KEY -> Tj = (dayOfWeek, hourOfDay)
# VALUE -> number of free slots in that Tj

def extractPairs(line):  
    values = line.split("\t")
    timestamp = values[1]
    datetimeObject = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")   
    # Extract for make key
    dayOfTheWeek = datetimeObject.strftime("%a")
    hour = datetimeObject.hour
    # Extract free slots
    free_slots = int(values[3])
    # Return (key, value)
    return ( (dayOfTheWeek,hour), free_slots )

Tj1RDD = registersFilteredRDD.flatMap(extractPairs)
# In this way I obtain: (e.g)
# ( (Monday, 15:00), [2, 3, 2, 0, 0, 6, 2] )
# ( (Monday, 16:00), [2, 3, 5, 5, 5, 0, 1] )
Tj2RDD = Tj1RDD.groupByKey().cache()# -> I'll do many operations 
                                    #    on this RDD

# Now i have to compute the Criticality
def criticality(values):
    countNum = 0
    countDen = 0
    for v in values:
        if v==0:
            countNum += 1
        countDen +=1
    return (countNum/countDen)
            
TjCriticality = Tj2RDD.mapValues(criticality())

'''
2) Selects only the pairs having a criticality value greater than a minimum criticality
threshold. The minimum criticality threshold is an argument of the application.
'''
# How to filter criticality????
resultRDD = TjCriticality.filter(lambda key,value: value > criticality_threshold)

'''
3) Selects the most critical timeslot for each station (consider only timeslots with a
criticality greater than the minimum criticality threshold). If there are two or more
timeslots characterized by the highest criticality value for a station, select only one of
those timeslots. Specifically, select the one associated with the earliest hour. If also
the hour is the same, consider the lexicographical order of the name of the week day.
'''

'''
4) Stores in one single (KML) file the information about the most critical timeslot for
each station. Specifically, the output (KML) file must contain one marker of type
Placemark for each pair (Si, most critical timeslot for Si) characterized by the
following features:
- StationId
- Day of the week and hour of the critical timeslot
- Criticality value
- Coordinates of the station (longitude, latitude)
Do not include in the output (KML) file the stations for which there are no timeslots
satisfying the minimum criticality threshold'''
