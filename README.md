# CT414 Map Reduce Application
Java implementation of the MapReduce programming model for CT414

### mr1.java
Uses a number of large text for testing program. Assigns the Map or Reduce functions to a Thread Pool with a configurable number of threads. number of threads can be passed to the program as a command line parameter. Measures as accurately as possible how long it takes to process the full set of large input text files used for testing.

### mr2.java
Another version of the program that also implements parallel processing for the Group phase. Tests and measures the total time taken to index the same set of large text files.