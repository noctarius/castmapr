CastMapR
========

CastMapR - MapReduce for Hazelcast

# What is CastMapR

CastMapR is a MapReduce implementation on top of Hazelcast 3. Currently it supports MapReduce algorithms on IMap, Multimap and IList.

# How to use CastMapR

The API of CastMapR is heavily inspired by the MapReduce implementation of Infinispan (thanks to the guys of RedHat!) and it is completely transparently usable for HazelcastClient connections.

CastMapR supports executing MapReduce tasks on IMap, IList and MultiMap whereas the behavior differs a bit from implementation to implementation.

### IMap ###
+ For IMap it behaves as expected, the Mapper::map method is called once per key-value pair.

### MultiMap ###
+ For MultiMap the Mapper::map method is called once per key-value pair too. That means the MultiMap becomes expanded to a flat representation of key-value pairs before mapping it (a single key can be expected multiple times).

### IList ###
+ For IList the Mapper::map method is called once per entry in the list. The key always yields to null and the value is the entry in the list. 

# Adding CastMapR to your project

If you use Maven there are snapshots in Sonatype OSS Maven repositories and releases in Maven Central. Just add the following lines to you pom.xml:
Latest Release:
```xml
<dependencies>
  <dependency>
    <groupId>com.noctarius.castmapr</groupId>
    <artifactId>castmapr</artifactId>
    <version>1.0.0</version>
  </dependency>
</dependencies>
```

Latest Snapshot:
```xml
<dependencies>
  <dependency>
    <groupId>com.noctarius.castmapr</groupId>
    <artifactId>castmapr</artifactId>
    <version>1.1.0-SNAPSHOT</version>
  </dependency>
</dependencies>
<repositories>
  <repository>
    <id>sonatype-nexus-public</id>
    <name>SonaType public snapshots and releases repository</name>
    <url>https://oss.sonatype.org/content/groups/public</url>
    <releases>
      <enabled>true</enabled>
    </releases>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
  </repository>
</repositories>
```

For non Maven users you can download the latest snapshot here:
[Download](https://oss.sonatype.org/content/repositories/snapshots/com/noctarius/castmapr/castmapr/0.0.1-SNAPSHOT/)

# Javadoc

The Javadoc of the snapshot can be found [here](https://noctarius.ci.cloudbees.com/job/CastMapR/javadoc/).

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

For reducing algorithms you need to implement the Reducer class. For Reducer implementation that are capable of running in distributed environments and (maybe multiple times) can be marked with @Distributable or by implementing DistributableReducer.
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
```java
public static class MyReducer implements DistributableReducer<String, Integer>
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
