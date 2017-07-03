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
  
   - Reducer: find last seen time of each visitor, from all of their activities


**MapReducer2: topk**
  
   - Find the 100 most recent visitors and their last seen time from last step's result

## For example

Assume we have 3 visitors and each have 3 actions. We will get result of the 2 most recent visitors and their last seen time.

**Input for MapReducer1 (dedupByPid):**
```
{
  "pid": "visitor1",
  "action": [
    {
      "time": "1491110000000",
      "language": "language"
    },
    {
      "time": "1491110000001",
      "language": "language"
    },
    {
      "time": "1491110000002",
      "language": "language"
    }
  ]
}
{
  "pid": "visitor2",
  "action": [
    {
      "time": "1491110000003",
      "language": "language"
    },
    {
      "time": "1491110000004",
      "language": "language"
    },
    {
      "time": "1491110000005",
      "language": "language"
    }
  ]
}
{
  "pid": "visitor3",
  "action": [
    {
      "time": "1491110000006",
      "language": "language"
    },
    {
      "time": "1491110000007",
      "language": "language"
    },
    {
      "time": "1491110000008",
      "language": "language"
    }
  ]
}
```

**Output for MapReducer1 (dedupByPid):**
```
visitor1 1491110000002

visitor2 1491110000005

visitor3 1491110000008
```

**Input for MapReducer2 (topk):**
```
visitor1 1491110000002

visitor2 1491110000005

visitor3 1491110000008
```

**Output for MapReducer2 (topk):**
```
visitor1 1491110000002

visitor2 1491110000005
```

