//
// Created by Fan Zhao on 3/11/23.
//
#include "src/include/rm.h"
#include "src/include/ix.h"
#include <cstdio>
#include <typeinfo>
#include <cstring>
#include "src/include/errorCode.h"
#include <glog/logging.h>

namespace PeterDB{
    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC RM_IndexScanIterator::close() {
        return -1;
    }

}