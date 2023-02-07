//
// Created by Fan Zhao on 2/2/23.
//

#include "src/include/rm.h"

namespace PeterDB {
    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::begin(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                              const std::string &conditionAttribute, const CompOp compOp, const void *value,
                              const std::vector<std::string> &selectedAttrNames) {
        RC rc = rbfmIterator.begin(fileHandle, recordDescriptor, conditionAttribute, compOp, value, selectedAttrNames);
        if(rc) {
            LOG(ERROR) << "Fail to open RM scan iterator @ RM_ScanIterator::begin" << std::endl;
            return RC(RM_ERROR::ITERATOR_BEGIN_FAIL);
        }
        return SUCCESS;
    }

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        RC rc = rbfmIterator.getNextRecord(rid, data);
        if(rc) {
            return RM_EOF;
        }
        return SUCCESS;
    }

    RC RM_ScanIterator::close() {
        return rbfmIterator.close();
    }

}