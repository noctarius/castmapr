CastMapR
========

CastMapR - MapReduce for Hazelcast

# What is CastMapR

CastMapR is a MapReduce implementation on top of Hazelcast 3. Currently it supports MapReduce algorithms on IMap but other Collection implementation like Multimap, IList, ISet are planned.

# How to use CastMapR

The API of CastMapR is heavily inspired by the MapReduce implementation of Infinispan (thanks to the guys of RedHat!) and it is completely transparently usable for HazelcastClient connections.

# Simple example

```java
HazelcastInstance instance = Hazelcast.newHazelcastInstance();
IMap<Integer, Integer> map = instance.getMap( "someMap" );

// Retrieve an instance of MapReduceTaskFactory
MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( instance );

// Use the factory to build a MapReduceTask using the given IMap
MapReduceTask<Integer, Integer, String, Integer> task = factory.build( map );

// Configure and submit it to the cluster
Map<String, Integer> result = task
	.mapper( new MyMapper() )
	.reducer( new MyReducer() )
	.submit();
``` 

To define a Mapper just extend the Mapper class and implement your algorithm in the map method.
```java
public class MyMapper extends Mapper<Integer, Integer, String, Integer> {
  @Override
  public void map( Integer key, Integer value, Collector<String, Integer> collector )
  {
    collector.emit( String.valueOf( key ), value );
  }
}
```

For reducing algorithms you need to implement the Reducer class. For Reducer implementation that are capable of running in distributed environments and (maybe multiple times) can be marked with @Distributable.
```java
@Distributable
public static class MyReducer implements Reducer<String, Integer>
{
  @Override
  public Integer reduce( String key, Iterator<Integer> values )
  {
    int sum = 0;
    while ( values.hasNext() )
    {
      sum += values.next();
    }
    return sum;
  }
}
```