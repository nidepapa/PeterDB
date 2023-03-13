//
// Created by Fan Zhao on 3/11/23.
//
#include "src/include/rm.h"
#include <typeinfo>


namespace PeterDB {
    RC RM_IndexScanIterator::open(IXFileHandle* ixFileHandle, const Attribute& attr,
                                  const uint8_t* lowKey, const uint8_t* highKey,
                                  bool lowKeyInclusive, bool highKeyInclusive) {
        RC ret = 0;
        ret = ixIterator.open(ixFileHandle, attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
        if(ret) return ret;
        return 0;
    }

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
        RC ret = ixIterator.getNextEntry(rid, key);
        if(ret) {
            return RM_EOF;
        }
        return 0;
    }

    RC RM_IndexScanIterator::close() {
        return ixIterator.close();
    }
}