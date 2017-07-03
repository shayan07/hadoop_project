# hadoop_project

## Introduction
hadoop_project write in Java using Hadoop MapReduce model.

The purpose of this project is to parse records of visitor sessions. Each record contains the visitor unique id ("pid") and an array of actions (activities), each having a timestamp ("time"). Hadoop_project allows you to return a result of the 100 most recent visitors grouped by "pid", and ordered by last seen max("time")

## Prerequisites
Java

Maven

Hadoop

## Design

1 driver configure 2 mapreducers, which are dedupByPid and topK.
 
##### **MapReducer1: dedupByPid**
  
  Mapper: parse the input Json file 
  
  Reducer: find last seen time of each visitor, from their all activities


##### **MapReducer2: topk**
  
  find the 100 most recent visitors and their last seen time from last step's result

