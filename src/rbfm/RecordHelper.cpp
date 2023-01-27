//
// Created by Fan Zhao on 1/18/23.
//
#include <cmath>
#include "src/include/rbfm.h"
#include <iostream>
#include <iterator>
#include <cstring>

namespace PeterDB {
    RecordHelper::RecordHelper() = default;

    RecordHelper::~RecordHelper() = default;

    // ("Tom", 25, "UCIrvine", 3.1415, 100)
    // [1 byte for the null-indicators for the fields: bit 00000000] [4 bytes for the length 3] [3 bytes for the string "Tom"] [4 bytes for the integer value 25] [4 bytes for the length 8] [8 bytes for the string "UCIrvine"] [4 bytes for the float value 3.1415] [4 bytes for the integer value 100]
    // covert a raw data to a byte sequence with metadata as header
    RC
    RecordHelper::rawDataToRecordByte(int8_t *rawData, const std::vector<Attribute> &recordDescriptor, int8_t *recordByte,
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
            if (isNullAttr(rawData, i)) {
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

    // convert byte(with metadata) into a struct data according to the recordDescriptor
    RC RecordHelper::recordByteToRawData(int8_t *recordByte, const int16_t recordLen,
                                         const std::vector<Attribute> &recordDescriptor, int8_t *rawData) {
        int16_t attrNum = recordDescriptor.size();
        // 1. get null flags
        int16_t nullFlagLenInByte = ceil(attrNum / 8.0);
        int8_t nullFlag[nullFlagLenInByte];
        getNullFlag(recordByte, recordDescriptor, nullFlag);
        memcpy(rawData, nullFlag, nullFlagLenInByte);

        // 2. write into not null value
        short rawDataPos = sizeof(char) * nullFlagLenInByte;
        short attrDirectoryPos = sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum);
        short valPos = attrDirectoryPos + attrNum * sizeof(AttrDir);
        short prev = valPos, curr = 0;

        for (short i = 0; i < attrNum; i++, attrDirectoryPos += sizeof(AttrDir)) {
            if (!isNullAttr(rawData, i)) {
                memcpy(&curr, recordByte + attrDirectoryPos, sizeof(AttrDir));

                switch (recordDescriptor[i].type) {
                    case TypeInt:
                        memcpy(rawData + rawDataPos, recordByte + valPos, sizeof(int));
                        valPos += sizeof(int);
                        rawDataPos += sizeof(int);
                        break;
                    case TypeReal:
                        memcpy(rawData + rawDataPos, recordByte + valPos, sizeof(float));
                        valPos += sizeof(float);
                        rawDataPos += sizeof(float);
                        break;
                    case TypeVarChar:
                        // 1. write varchar length
                        RawDataStrLen strLen;
                        strLen = curr - prev;
                        memcpy(rawData + rawDataPos, &strLen, sizeof(RawDataStrLen));

                        rawDataPos += sizeof(RawDataStrLen);
                        // 2. write varchar
                        memcpy(rawData + rawDataPos, recordByte + valPos, strLen);
                        valPos += strLen;
                        rawDataPos += strLen;
                        break;
                }
            }

            if (curr >= 0) {
                // update previous ending
                prev = curr;
            }
        }
        return 0;
    }

    RC RecordHelper::printNullAttr(char *recordByte, const std::vector<Attribute> &recordDescriptor) {
        char AttrNum[2];

        short attrNum = recordDescriptor.size();

        // 1. write into not null value
        short attrDirectoryPos = sizeof(short);
        short valPos = attrDirectoryPos + attrNum * sizeof(short);
        short prev = valPos, curr = 0;

        for (short i = 0; i < attrNum; i++, attrDirectoryPos += sizeof(short)) {
            memcpy(&curr, recordByte + attrDirectoryPos, sizeof(short));
            if (curr > 0) {
                // is not null
                memcpy(&curr, recordByte + attrDirectoryPos, sizeof(short));
                attrDirectoryPos += sizeof(short);
                std::cout << " prev: " << prev << std::endl;
                std::cout << " curr: " << curr << std::endl;
                switch (recordDescriptor[i].type) {
                    case TypeInt:
                        char val1[4];
                        memcpy(val1, recordByte + valPos, sizeof(int));
                        valPos += sizeof(int);
                        std::cout << "@ recordByte:" << recordDescriptor[i].name << ": " << *((int *) val1)
                                  << std::endl;
                        break;
                    case TypeReal:
                        char val2[4];
                        memcpy(val2, recordByte + valPos, sizeof(float));
                        valPos += sizeof(float);
                        std::cout << "@ recordByte:" << recordDescriptor[i].name << ": " << *((float *) val2)
                                  << std::endl;
                        break;
                    case TypeVarChar:
                        // 1. write varchar length
                        int strLen;
                        strLen = curr - prev;
                        char val3[strLen];
                        // 2. write varchar
                        memcpy(val3, recordByte + valPos, strLen);
                        valPos += strLen;
                        std::cout << "@ recordByte:" << recordDescriptor[i].name << ": ";
                        std::copy(val3, val3 + strLen,
                                  std::ostream_iterator<char>(std::cout, ""));
                        std::cout << " length: " << strLen << std::endl;
                        break;
                }

            }
            if (curr >= 0) {
                prev = curr;
            }
        }
        return 0;


    }

    bool RecordHelper::isNullAttr(int8_t *rawData, int16_t idx) {
        short byteNumber = idx / 8;
        short bitNumber = idx % 8;
        uint8_t mask = 0x01;
        // ex: 0100 0000
        char tmp = rawData[byteNumber];
        return (tmp >> (7 - bitNumber)) & mask;
    }

    RC RecordHelper::getNullFlag(int8_t *recordByte, const std::vector<Attribute> &recordDescriptor, int8_t *nullFlag) {
        int16_t AttrNum = recordDescriptor.size();
        // init to 0 explicitly, or will be set random value in c++
        for (int i = 0; i < sizeof(nullFlag); i++) {
            nullFlag[i] = 0;
        }
        for (int i = 0; i < AttrNum; i++) {
            short valPos = 0;
            memcpy(&valPos, recordByte + sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum) + sizeof(AttrDir) * i, sizeof(AttrDir));
            if (valPos < 0) {
                // is null
                int16_t bytePos = i / 8;
                int16_t bitPos = i % 8;
                nullFlag[bytePos] = nullFlag[bytePos] | (0x01 << (7 - bitPos));
            }
        }
        return 0;
    }

}