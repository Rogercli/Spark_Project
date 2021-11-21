#!/home/roger/anaconda3/bin/pythons
from pyspark import SparkContext

sc=SparkContext('local',"spark_project_one")

#creates RDD
car_rdd=sc.textFile('data.csv')

def extract_key_value(row):
    line=row.split(',')
    vin=line[2]
    incident=line[1]
    make=line[3]
    year=line[5]
    return (vin,(incident,make,year))



# since value of map_key_RDD is an interator object, need to turn iterable object into list
def propagate_car_info(value):
    val_lst=list(value)
    car_info=[]
    accidents=[]
    for row in val_lst:
        if 'I' in row:
            car_info.append(row[1:])
        elif 'A' in row:
            accidents.append(row[0])
        else:
            continue
    propogated_accident_list=list(zip(accidents,car_info))
    return propogated_accident_list


def extract_accident_count(value):
    make,year=value[1]
    return ((make,year),1)



# create pair RDD by performing map operation to create an RDD consisting of a tuple with key and value pair
# output key is vin number and output value is a nested tuple of incident type, make, and year
map_key_rdd=car_rdd.map(lambda x:extract_key_value(x))

# performs group aggregation by keys and propagates car info to the values(accidents) according to their keys
#since we wish to keep original car_Rdd values to flatmap, we use groupByKey since it will shuffle grouped keys
#where as reduceByKey will reduce the keys first according to a function before shuffling 
group_key_rdd = map_key_rdd.groupByKey().flatMap(lambda kv:propagate_car_info((kv[1])))


# performs map operation to count number of occurances
# output key is vehicle_make and year, and value is 1
map_count_rdd = group_key_rdd.map(lambda x: extract_accident_count(x))

# groups by key and reduces the number of records per key through summation
# reduceByKey will group all the values with same key into one executor. Then it iterates among the values, # where x is the “previous” value and y is the “new” value
reduce_count_rdd=map_count_rdd.reduceByKey(lambda x,y:x+y)

# using collect(), an result_RDD is of type list, which we can then iterate over
result_rdd=reduce_count_rdd.collect()

for k,v in result_rdd:
    print(k[0]+'-'+k[1]+',',v)
