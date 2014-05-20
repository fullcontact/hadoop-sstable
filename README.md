# Hadoop SSTable: Splittable Input Format for Reading Cassandra SSTables Directly

Hadoop SSTable is an InputFormat implementation that supports reading and splitting Cassandra SSTables. Leveraging this input format MapReduce jobs can make use of Cassandra data for offline data analysis.

## Communication

- [GitHub Issues](https://github.com/fullcontact/hadoop-sstable/issues)

## Binaries

Binaries and dependency information for Maven, Ivy, Gradle and others can be found at [http://search.maven.org]

Example for Gradle:

```
compile 'com.fullcontact:hadoop-sstable:x.y.z'
```

## Build

To build:

```
$ git clone git@github.com:fullcontact/hadoop-sstable.git
$ cd hadoop-sstable
$ ./gradlew build
```

## Bugs and Feedback

For bugs, questions and discussions please use the [Github Issues](https://github.com/fullcontact/hadoop-sstable/issues).

 
## LICENSE

Copyright 2014 FullContact, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.