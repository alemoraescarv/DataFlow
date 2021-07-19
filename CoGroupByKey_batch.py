#example on merging two Pcollections using Apache Beam
#link for the documentation: https://beam.apache.org/documentation/transforms/python/aggregation/cogroupbykey/

# We are using a batch example

#libs
import apache_beam as beam
from apache_beam import Create, Map, ParDo, Flatten
from apache_beam import Values, GroupByKey, CoGroupByKey, CombineGlobally, CombinePerKey
from apache_beam import pvalue, window, WindowInto

with beam.Pipeline() as pipeline:
    
    timestamps= [('Hello','2021-07-16 13:19:00'),('Hello_world','2021-07-16 13:19:00'),('Hello_everyone','2021-07-16 13:19:00'),
                 ('Hello_cloud','2021-07-16 13:19:00')]
    p1 = pipeline | "Timestamps" >> Create(timestamps)
    
        #creating sample data 
    p2 = pipeline | "creating a sample data" >> Create([('Hello','sh 1'),('Hello','sh 1.1'),
    ('Hello_world','sh 2'),
    ('Hello_everyone','sh 3'),
    ('Hello_cloud','sh 4')])
    
    ({"schdedule":p2,"timestamp":p1}) | "merging" >> CoGroupByKey() | "merge print">> Map(print)

#output 
#
#('Hello', {'schdedule': ['sh 1', 'sh 1.1'], 'timestamp': ['2021-07-16 13:19:00']})
#('Hello_world', {'schdedule': ['sh 2'], 'timestamp': ['2021-07-16 13:19:00']})
#('Hello_everyone', {'schdedule': ['sh 3'], 'timestamp': ['2021-07-16 13:19:00']})
#('Hello_cloud', {'schdedule': ['sh 4'], 'timestamp': ['2021-07-16 13:19:00']})

#NOTICE that the merge was done by the Key in this case element[1] in the PCollection

