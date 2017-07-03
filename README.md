# hadoop_project

## Introduction
hadoop_project in Java using Hadoop MapReduce model.

The purpose of this project is to parse records of visitor sessions. Each record contains the visitor unique id ("pid") and an array of actions (activities), each having a timestamp ("time"). Hadoop_project allows you to return a result of the 100 most recent visitors grouped by "pid", and ordered by last seen max("time")

## Prerequisites
Java

Maven

Hadoop

## Design

1 driver configure 2 mapreducers, which are dedupByPid and topK.
 
**MapReducer1: dedupByPid**
  
   - Mapper: parse the input Json file 
  
   - Reducer: de-duplicate each visitor's all actions by choosing the max time value


**MapReducer2: topk**
  
   - Mapper: Find the 100 most recent records at each slave node
   
   - Reducer: Find the 100 most recent records from all slave nodes

## For example

Assuming we have 3 visitors and each have 3 actions, we will get the result of the 2 most recent visitors and their last seen time.


**MapReducer1: dedupByPid**

Input for Mapper1
```
{
  "pid": "visitor1",
  "action": [
    {"time": "1491110000000","language": "language"},
    {"time": "1491110000001","language": "language"},
    {"time": "1491110000002","language": "language"}
  ]
}
{
  "pid": "visitor2",
  "action": [
    {"time": "1491110000003","language": "language"},
    {"time": "1491110000004","language": "language"},
    {"time": "1491110000005","language": "language"}
  ]
}
{
  "pid": "visitor3",
  "action": [
    {"time": "1491110000006","language": "language"},
    {"time": "1491110000007","language": "language"},
    {"time": "1491110000008","language": "language"}
  ]
}
```

Output for Mapper1 && Input for Reducer1
```
visitor1   {1491110000000,1491110000001,1491110000002}

visitor2   {1491110000003,1491110000004,1491110000005}

visitor3   {1491110000006,1491110000007,1491110000008}
```

Output for Reducer1
```
visitor1   1491110000002

visitor2   1491110000005

visitor3   1491110000008
```

Reducer1's result is mapper2's input


**MapReducer2: topk, where k = 2 in the following example**

Input for Mapper2
```
visitor1   1491110000002

visitor2   1491110000005

visitor3   1491110000008
```
Output for Mapper2(output of one slave node) && Input for Reducer2(shuffle and merged from all slave nodes)
```
visitor1   1491110000002

visitor2   1491110000005
```
Output for Reducer2 (top k, where k = 2)
```
visitor1   1491110000002

visitor2   1491110000005
```

