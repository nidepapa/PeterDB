## Project 4 Report


### 1. Basic information
- Team #: Yufan Zhao
- Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222p-winter23-nidepapa
- Student 1 UCI NetID: 88127297
- Student 1 Name: yufan zhao


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns).

| name      | type        | length |
|-----------|-------------|--------|
| table_id  | int         |        |
| attr_name | varchar(50) |        |
| file_name | varchar(50) |        |



### 3. Filter
1. store the condition value in the iterator
2. for each record , compare the target value of filtered field with condition value
3. if EQ, output



### 4. Project
1. store the project values vector
2. for each satisfied record, only convert the projected value to RawData and then output



### 5. Block Nested Loop Join
1. Use given buffers to build a hash table using unordered_map
2. Load tuples from the outer table into memory to build in-memory hash table 
3. Continuously retrieve tuples from inner table and use it to probe the in-memory hash table 
4. If reaches the end of the inner table, reload tuples from the outer table 
5. Repeat until the end of the outer table is reached


### 6. Index Nested Loop Join
1. Continuously retrieve tuples from outer table
2. Get the join attribute and use it to set the index scanner on inner table
3. Retrieve tuples from the inner table and join records


### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).



### 8. Aggregation
- Describe how your basic aggregation works.
1. Maintain a float variable and a counter , since AVG needs counter
2. For every tuple in outer table, update the float variable based on the aggregate operation and add the counter by one
3. Calculate the final result using the float variable and counter


- Describe how your group-based aggregation works. (If you have implemented this feature)



### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)?
  Clearly list the changes on files and CMakeLists.txt, if any.



- Other implementation details:



### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)