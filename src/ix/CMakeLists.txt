add_library(ix ix.cc IXFileHandle.cpp IXScanIterator.cpp IXNode.cpp LeafNode.cpp InternalNode.cpp)
add_dependencies(ix pfm googlelog)
target_link_libraries(ix pfm glog)