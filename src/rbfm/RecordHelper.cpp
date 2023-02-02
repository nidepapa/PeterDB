//
// Created by Fan Zhao on 1/18/23.
//
#include <cmath>
#include "src/include/rbfm.h"
#include "src/include/errorCode.h"
#include <iostream>
#include <iterator>
#include <cstring>

namespace PeterDB {
    RecordHelper::RecordHelper() = default;

    RecordHelper::~RecordHelper() = default;

    // ("Tom", 25, "UCIrvine", 3.1415, 100)
    // [1 byte for the null-indicators for the fields: bit 00000000] [4 bytes for the length 3] [3 bytes for the string "Tom"] [4 bytes for the integer value 25] [4 bytes for the length 8] [8 bytes for the string "UCIrvine"] [4 bytes for the float value 3.1415] [4 bytes for the integer value 100]
    // covert a raw data to a byte sequence with metadata as header
    RC RecordHelper::rawDataToRecordByte(uint8_t *rawData, const std::vector<Attribute> &recordDescriptor, uint8_t *recordByte,
                                      int16_t &recordLen) {

        // 1. write flag and placeholder
        Flag fg = RECORD_FLAG_DATA;
        memcpy(recordByte, &fg, sizeof(Flag));

        PlaceHolder ph = RECORD_PLACEHOLDER;
        memcpy(recordByte + sizeof(Flag), &ph, sizeof(PlaceHolder));

        // 2. write attrNum
        AttrNum attrNum = recordDescriptor.size();
        memcpy(recordByte + sizeof(Flag) + sizeof(PlaceHolder), &attrNum, sizeof(AttrNum));
        // 3. calculate offset
        int16_t nullFlagLenInByte = ceil(attrNum / 8.0);
        int16_t dirOffset = sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum);
        int16_t valOffset = dirOffset + sizeof(AttrDir) * attrNum;

        // 4. write directory and each attribute raw data
        int16_t dirPos = dirOffset, valPos = valOffset, rawDataPos = sizeof(int8_t) * nullFlagLenInByte;
        // directory move right by 2 byte
        for (short i = 0; i < attrNum; i++, dirPos += sizeof(AttrDir)) {
            if (rawDataIsNullAttr(rawData, i)) {
                short valEndPos = -1;
                memcpy(recordByte + dirPos, &valEndPos, sizeof(AttrDir));
                continue;
            }
            switch (recordDescriptor[i].type) {
                case TypeInt:
                case TypeReal:
                    memcpy(recordByte + valPos, rawData + rawDataPos, recordDescriptor[i].length);
                    rawDataPos += recordDescriptor[i].length;
                    valPos += recordDescriptor[i].length;
                    // set directory
                    memcpy(recordByte + dirPos, &valPos, sizeof(AttrDir));
                    break;
                case TypeVarChar:
                    RawDataStrLen strLen;
                    // get varchar length
                    memcpy(&strLen, rawData + rawDataPos, sizeof(RawDataStrLen));
                    rawDataPos += sizeof(RawDataStrLen);
                    // copy string
                    memcpy(recordByte + valPos, rawData + rawDataPos, strLen);
                    rawDataPos += strLen;
                    valPos += strLen;
                    // set directory
                    memcpy(recordByte + dirPos, &valPos, sizeof(AttrDir));
                    break;
            }
        }
        recordLen = valPos;
        return 0;
    }

    // convert byte(with metadata)  with selected attributes into a struct data according to the recordDescriptor
    RC RecordHelper::recordByteToRawData(uint8_t record[], const std::vector<Attribute> &recordDescriptor,
                                         std::vector<uint16_t> &selectedAttrIndex, uint8_t *rawData) {
        int16_t selectedAttrNum = selectedAttrIndex.size();
        // 1. get null flags
        int16_t nullFlagLenInByte = ceil(selectedAttrNum / 8.0);
        int8_t nullFlag[nullFlagLenInByte];
        memset(nullFlag, 0, nullFlagLenInByte);
        recordGetNullFlag(record, recordDescriptor, selectedAttrIndex, nullFlag);
        memcpy(rawData, nullFlag, nullFlagLenInByte);

        // 2. write into not null value
        short rawDataPos = sizeof(char) * nullFlagLenInByte;
        short attrDirectoryPos = sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum);

        for (short i = 0; i < selectedAttrNum; i++, attrDirectoryPos += sizeof(AttrDir)) {
            int16_t AttrIdx = selectedAttrIndex[i];
            if (!rawDataIsNullAttr(rawData, i)) {
                int16_t attrEndPos = getAttrEndPos(record, AttrIdx);
                int16_t attrBeginPos = getAttrBeginPos(record, AttrIdx);

                switch (recordDescriptor[i].type) {
                    case TypeInt:
                        memcpy(rawData + rawDataPos, record + attrBeginPos, sizeof(int));
                        rawDataPos += sizeof(TypeInt);
                        break;
                    case TypeReal:
                        memcpy(rawData + rawDataPos, record + attrBeginPos, sizeof(float));
                        rawDataPos += sizeof(TypeReal);
                        break;
                    case TypeVarChar:
                        // 1. write varchar length
                        RawDataStrLen strLen;
                        strLen = attrEndPos - attrBeginPos;
                        memcpy(rawData + rawDataPos, &strLen, sizeof(RawDataStrLen));

                        rawDataPos += sizeof(RawDataStrLen);
                        // 2. write varchar
                        memcpy(rawData + rawDataPos, record + attrBeginPos, strLen);
                        rawDataPos += strLen;
                        break;
                }
            }
        }
        return 0;
    }

//    RC RecordHelper::printNullAttr(char *recordByte, const std::vector<Attribute> &recordDescriptor) {
//        char AttrNum[2];
//
//        short attrNum = recordDescriptor.size();
//
//        // 1. write into not null value
//        short attrDirectoryPos = sizeof(short);
//        short valPos = attrDirectoryPos + attrNum * sizeof(short);
//        short prev = valPos, curr = 0;
//
//        for (short i = 0; i < attrNum; i++, attrDirectoryPos += sizeof(short)) {
//            memcpy(&curr, recordByte + attrDirectoryPos, sizeof(short));
//            if (curr > 0) {
//                // is not null
//                memcpy(&curr, recordByte + attrDirectoryPos, sizeof(short));
//                attrDirectoryPos += sizeof(short);
//                std::cout << " prev: " << prev << std::endl;
//                std::cout << " curr: " << curr << std::endl;
//                switch (recordDescriptor[i].type) {
//                    case TypeInt:
//                        char val1[4];
//                        memcpy(val1, recordByte + valPos, sizeof(int));
//                        valPos += sizeof(int);
//                        std::cout << "@ recordByte:" << recordDescriptor[i].name << ": " << *((int *) val1)
//                                  << std::endl;
//                        break;
//                    case TypeReal:
//                        char val2[4];
//                        memcpy(val2, recordByte + valPos, sizeof(float));
//                        valPos += sizeof(float);
//                        std::cout << "@ recordByte:" << recordDescriptor[i].name << ": " << *((float *) val2)
//                                  << std::endl;
//                        break;
//                    case TypeVarChar:
//                        // 1. write varchar length
//                        int strLen;
//                        strLen = curr - prev;
//                        char val3[strLen];
//                        // 2. write varchar
//                        memcpy(val3, recordByte + valPos, strLen);
//                        valPos += strLen;
//                        std::cout << "@ recordByte:" << recordDescriptor[i].name << ": ";
//                        std::copy(val3, val3 + strLen,
//                                  std::ostream_iterator<char>(std::cout, ""));
//                        std::cout << " length: " << strLen << std::endl;
//                        break;
//                }
//
//            }
//            if (curr >= 0) {
//                prev = curr;
//            }
//        }
//        return 0;
//
//
//    }

     bool RecordHelper::rawDataIsNullAttr(uint8_t *rawData, int16_t idx) {
        short byteNumber = idx / 8;
        short bitNumber = idx % 8;
        uint8_t mask = 0x01;
        // ex: 0100 0000
        char tmp = rawData[byteNumber];
        return (tmp >> (7 - bitNumber)) & mask;
    }

    RC RecordHelper::recordGetNullFlag(uint8_t *recordByte, const std::vector<Attribute> &recordDescriptor, std::vector<uint16_t> &selectedAttrIndex, int8_t *nullFlag) {
        int16_t AttrNum = selectedAttrIndex.size();
        // init to 0 explicitly, or will be set random value in c++
        for (int i = 0; i < sizeof(nullFlag); i++) {
            nullFlag[i] = 0;
        }
        for (int i = 0; i < AttrNum; i++) {
            int16_t attrIdx = selectedAttrIndex[i];
            short valPos = 0;
            memcpy(&valPos, recordByte + sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum) + sizeof(AttrDir) * attrIdx, sizeof(AttrDir));
            if (valPos == ATTR_DIR_EMPTY) {
                // is null
                int16_t bytePos = i / 8;
                int16_t bitPos = i % 8;
                nullFlag[bytePos] = nullFlag[bytePos] | (0x01 << (7 - bitPos));
            }
        }
        return 0;
    }

    int16_t RecordHelper::getAttrBeginPos(uint8_t* byteSeq, int16_t attrIndex) {
        int16_t curOffset = getAttrEndPos(byteSeq, attrIndex);
        if(curOffset == ATTR_DIR_EMPTY) {   // Null attribute
            return ERR_GENERAL;
        }
        int16_t prevOffset = -1;
        // Looking for previous attribute that is not null
        for(int i = attrIndex - 1; i >= 0; i--) {
            prevOffset = getAttrEndPos(byteSeq, i);
            if(prevOffset != ATTR_DIR_EMPTY) {
                break;
            }
        }
        if(prevOffset == -1) {
            prevOffset = sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum) + getRecordAttrNum(byteSeq) * sizeof(AttrDir);
        }
        return prevOffset;
    }

    int16_t RecordHelper::getAttrEndPos(uint8_t* byteSeq, int16_t attrIndex) {
        int16_t dirOffset = sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum) + attrIndex * sizeof(AttrDir);
        int16_t attrEndPos;
        memcpy(&attrEndPos, byteSeq + dirOffset, sizeof(AttrDir));
        return attrEndPos;
    }

    int16_t RecordHelper::getRecordAttrNum(uint8_t* byteSeq){
        int16_t attrNum;
        memcpy(&attrNum, byteSeq + sizeof(Flag) + sizeof(PlaceHolder), sizeof(AttrNum));
        return attrNum;
    }

    bool RecordHelper::recordIsAttrNull(uint8_t* byteSeq, int16_t attrIndex){
        int16_t end = getAttrEndPos(byteSeq, attrIndex);
        if(end == ATTR_DIR_EMPTY) return true;
        return false;
    }

    RC RecordHelper::recordGetAttr(uint8_t *recordByte, int16_t attrIndex, uint8_t *attr, int16_t & attrLen){
        int16_t attrEndPos = getAttrEndPos(recordByte, attrIndex);
        int16_t attrBeginPos = getAttrBeginPos(recordByte, attrIndex);
        attrLen = attrEndPos - attrBeginPos;
        memcpy(attr, recordByte + attrBeginPos, attrLen);
        return SUCCESS;
    }


}