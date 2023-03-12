## Project 3 Report

### 1. Basic information

- Team #: Yufan Zhao
- Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222p-winter23-nidepapa
- Student 1 UCI NetID: 88127297
- Student 1 Name: yufan zhao

### 2. Meta-data page in an index file
- 

| readCounter | writeCounter | appendCounter | root PageNum | ~~key Type~~ |
|-------------|--------------|---------------|--------------|--------------|
| 4 byte      | 4 bytes      | 4 bytes       | 4 bytes      | ~~4 bytes~~  |

### 3. Index Entry Format

- Show your index entry design (structure).

    - entries on internal nodes:

| key    | pageNum | slotNum | childPointer |
|--------|---------|---------|--------------|
| ? byte | 4 bytes | 2 bytes | 4 bytes      |

      - entries on leaf nodes:

| key    | pageNum | slotNum |  
|--------|---------|---------|
| ? byte | 4 bytes | 2 bytes |

### 4. Page Format

- Show your internal-page (non-leaf node) design.
  ![no-leaf node.png](no-leaf%20node.png)

- Show your leaf-page (leaf node) design.
  ![leaf node.png](leaf%20node.png)

### 5. Describe the following operation logic.

- Split
    1. leaf node:
        1. if the node is full, break the node at the m/2 position
        2. insert the data entry into one of the new nodes in increasing order
        3. copy up the first key of the right node up to parents

    2. no-leaf node:
    1. if the node is full, break the node at the m/2 position
    2. insert the key into one of the new nodes in increasing order
    3. push up the first key of the right node up to parents
    4. if split happens at root node, a new root will be created and point to old roots, height of B+ tree will +1


- Rotation (if applicable)
  not applicable

- Merge/non-lazy deletion (if applicable)

not applicable

- Duplicate key span in a page
  we have different key in leaf node. even the key is identical, RIDs are different.

- Duplicate key span multiple pages (if applicable)
  not applicable

### 6. Implementation Detail

- Have you added your own module or source file (.cc or .h)?
  Clearly list the changes on files and CMakeLists.txt, if any.


- Other implementation details:

### 7. Member contribution (for team of two)

- Explain how you distribute the workload in team.

### 8. Other (optional)

- Freely use this section to tell us about things that are related to the project 3, but not related to the other
  sections (optional)


- Feedback on the project to help improve the project. (optional)
