import pyspark
import itertools
from graphframes import  *
import sys
threshold=int(sys.argv[1])
input_path=str(sys.argv[2])
output_path=str(sys.argv[3])
sc = pyspark.SparkContext()
sql_cont=pyspark.sql.SQLContext(sc)


rdd= sc.textFile(input_path)
header = rdd.first()
df=rdd.filter(lambda x:x!= header).map(lambda x:x.split(',')).groupByKey().mapValues(set).filter(lambda x:len(x[1])>=threshold).cache()

df2=df.collectAsMap()
pairs=[]
for i in  itertools.combinations(df.map(lambda x:x[0]).collect(),2):
    intersection_pairs=set(df2[i[0]]).intersection(set(df2[i[1]]))
    if len(intersection_pairs)>=threshold:
        pairs.extend([(i[0],i[1]),(i[1],i[0])])

p=sc.parallelize(pairs).flatMap(lambda x:x).distinct().map(lambda x:(x,1)).collect()
vertices=sql_cont.createDataFrame(p,["id","djhkg"])
edges=sql_cont.createDataFrame(pairs,["src","dst"])
g = GraphFrame(vertices, edges)

res=g.labelPropagation(maxIter=5).rdd.map(lambda x :(x[2],x[0])).groupByKey().mapValues(list).map(lambda x : sorted(x[1])).sortBy(lambda x:(len(x),x[0])).collect()

with open (output_path,'w') as f:
    for i in res:
        for j in i:
            f.write("'"+j+"',")
        f.seek(f.tell()-1)
        f.write("\n")


