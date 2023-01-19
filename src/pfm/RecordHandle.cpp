//
// Created by Fan Zhao on 1/18/23.
//
#include "src/include/rbfm.h"

namespace PeterDB {
    RecordHandle::RecordHandle() = default;

    RecordHandle::~RecordHandle() = default;

    // covert a raw data to a byte sequence with metadata as header
    RC RecordHandle::rawDataToRecordByte(char* rawData, const std::vector<Attribute> &attrs, char* recordByte, short & recordLen){
        short attrNum = attrs.size();
        memcpy(recordByte, &attrNum, sizeof(attrNum));

        //todo
    }
    // convert byte(with metadata) into a struct data according to the recordDescriptor
    RC RecordHandle::recordByteToRawData(char *recordByte, const short recordLen, const std::vector<Attribute> &recordDescriptor, char *data) {
        // todo
    }

}