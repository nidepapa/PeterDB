//
// Created by Fan Zhao on 1/18/23.
//
#include <cmath>
#include "src/include/rbfm.h"
#include "src/include/errorCode.h"
#include <iostream>
#include <cstring>
#include <glog/logging.h>

namespace PeterDB {
    RecordHelper::RecordHelper() = default;

    RecordHelper::~RecordHelper() = default;

    // ("Tom", 25, "UCIrvine", 3.1415, 100)
    // [1 byte for the null-indicators for the fields: bit 00000000] [4 bytes for the length 3] [3 bytes for the string "Tom"] [4 bytes for the integer value 25] [4 bytes for the length 8] [8 bytes for the string "UCIrvine"] [4 bytes for the float value 3.1415] [4 bytes for the integer value 100]
    // covert a raw data to a byte sequence with metadata as header
    RC
    RecordHelper::rawDataToRecord(uint8_t *rawData, const std::vector<Attribute> &recordDescriptor, uint8_t *recordByte,
                                  int16_t &recordLen) {

        // 1. write flag and placeholder
        int16_t fg = RECORD_FLAG_DATA;
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
                    if (strLen > PAGE_SIZE) {
                        LOG(ERROR) << "strLen error" << std::endl;
                    }
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
    RC RecordHelper::recordToRawData(uint8_t record[], const std::vector<Attribute> &recordDescriptor,
                                     std::vector<uint16_t> &selectedAttrIndex, uint8_t *rawData) {
        int16_t selectedAttrNum = selectedAttrIndex.size();
        // 1. get null flags
        int16_t nullFlagLenInByte = ceil(selectedAttrNum / 8.0);
        int8_t nullFlag[nullFlagLenInByte];
        memset(nullFlag, 0, nullFlagLenInByte);
        recordGetNullFlag(record, recordDescriptor, selectedAttrIndex, nullFlag, nullFlagLenInByte);
        memcpy(rawData, nullFlag, nullFlagLenInByte);

        // 2. write into not null value
        short rawDataPos = sizeof(char) * nullFlagLenInByte;
        short attrDirectoryPos = sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum);
        //AttrIdx is idx in record
        for (short i = 0; i < selectedAttrNum; i++, attrDirectoryPos += sizeof(AttrDir)) {
            int16_t AttrIdx = selectedAttrIndex[i];
            if (!rawDataIsNullAttr(rawData, i)) {
                int16_t attrEndPos = recordGetAttrEndPos(record, AttrIdx);
                int16_t attrBeginPos = recordGetAttrBeginPos(record, AttrIdx);

                switch (recordDescriptor[AttrIdx].type) {
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

    // check if the ith attribute in raw data is null or not
    bool RecordHelper::rawDataIsNullAttr(uint8_t *rawData, int16_t idx) {
        short byteNumber = idx / 8;
        short bitNumber = idx % 8;
        uint8_t mask = 0x01;
        // ex: 0100 0000
        char tmp = rawData[byteNumber];
        return (tmp >> (7 - bitNumber)) & mask;
    }

    // get the raw data null flag from internal record
    RC RecordHelper::recordGetNullFlag(uint8_t *recordByte, const std::vector<Attribute> &recordDescriptor,
                                       std::vector<uint16_t> &selectedAttrIndex, int8_t *nullFlag,
                                       int16_t nullFlagByteNum) {
        int16_t AttrNum = selectedAttrIndex.size();
        // init to 0 explicitly, or will be set random value in c++
        for (int i = 0; i < nullFlagByteNum; i++) {
            nullFlag[i] = 0;
        }
        for (int i = 0; i < AttrNum; i++) {
            int16_t attrIdx = selectedAttrIndex[i];
            short valPos = 0;
            memcpy(&valPos,
                   recordByte + sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum) + sizeof(AttrDir) * attrIdx,
                   sizeof(AttrDir));
            if (valPos == ATTR_DIR_EMPTY) {
                // is null
                int16_t bytePos = i / 8;
                int16_t bitPos = i % 8;
                nullFlag[bytePos] = nullFlag[bytePos] | (0x01 << (7 - bitPos));
            }
        }
        return 0;
    }


    int16_t RecordHelper::recordGetAttrBeginPos(uint8_t *byteSeq, int16_t attrIndex) {
        int16_t curOffset = recordGetAttrEndPos(byteSeq, attrIndex);
        if (curOffset == ATTR_DIR_EMPTY) {   // Null attribute
            return RC(PAGE_ERROR::RECORD_NULL_ATTRIBUTE);
        }
        int16_t prevOffset = -1;
        // Looking for previous attribute that is not null
        for (int i = attrIndex - 1; i >= 0; i--) {
            prevOffset = recordGetAttrEndPos(byteSeq, i);
            if (prevOffset != ATTR_DIR_EMPTY) {
                break;
            }
        }
        if (prevOffset == -1) {
            prevOffset =
                    sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum) + recordGetAttrNum(byteSeq) * sizeof(AttrDir);
        }
        return prevOffset;
    }

    int16_t RecordHelper::recordGetAttrEndPos(uint8_t *byteSeq, int16_t attrIndex) {
        int16_t dirOffset = sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum) + attrIndex * sizeof(AttrDir);
        int16_t attrEndPos;
        memcpy(&attrEndPos, byteSeq + dirOffset, sizeof(AttrDir));
        return attrEndPos;
    }

    int16_t RecordHelper::recordGetAttrLen(uint8_t *byteSeq, int16_t attrIndex) {
        int16_t attrEndPos = recordGetAttrEndPos(byteSeq, attrIndex);
        int16_t attrBeginPos = recordGetAttrBeginPos(byteSeq, attrIndex);
        int16_t attrLen = attrEndPos - attrBeginPos;
        return attrLen;
    }

    int16_t RecordHelper::recordGetAttrNum(uint8_t *byteSeq) {
        int16_t attrNum;
        memcpy(&attrNum, byteSeq + sizeof(Flag) + sizeof(PlaceHolder), sizeof(AttrNum));
        return attrNum;
    }

    bool RecordHelper::recordIsAttrNull(uint8_t *byteSeq, int16_t attrIndex) {
        int16_t end = recordGetAttrEndPos(byteSeq, attrIndex);
        if (end == ATTR_DIR_EMPTY) return true;
        return false;
    }

    // the attr will have a 1-byte null flag in the head
    RC
    RecordHelper::recordGetAttr(uint8_t *recordByte, uint16_t attrIndex, const std::vector<Attribute> &recordDescriptor,
                                uint8_t *attr) {
        int8_t nullFlag[1];
        std::vector<uint16_t> attrIdx = {attrIndex};
        memset(nullFlag, 0, 1);
        recordGetNullFlag(recordByte, recordDescriptor, attrIdx, nullFlag, 1);
        memcpy(attr, nullFlag, 1);

        int16_t attrEndPos = recordGetAttrEndPos(recordByte, attrIndex);
        int16_t attrBeginPos = recordGetAttrBeginPos(recordByte, attrIndex);
        if (attrEndPos == -1) {
            return RC(PAGE_ERROR::ATTR_IS_NULL);
        } else {
            int16_t attrLen = attrEndPos - attrBeginPos;
            memcpy(attr + 1, recordByte + attrBeginPos, attrLen);
        }
        return SUCCESS;
    }

// ==================================================
// todo Definitions for Record struct
// ==================================================
    RC Record::fromRawRecord(RawRecord *rawRecord, const std::vector<Attribute> &recordDescriptor, const std::vector<uint16_t> &selectedAttrIndex, int16_t &recordLen) {
        this->getDirectory()->header.Flag = FLAG_DATA;
        this->getDirectory()->header.AttrNum = recordDescriptor.size();
        const int nullByteSize = rawRecord->getNullByteSize(selectedAttrIndex.size());
        int16_t rawRecordOffset = nullByteSize, recordOffset = 0;
        for (short i = 0; i < recordDescriptor.size(); i++) {
            auto result = std::find(selectedAttrIndex.begin(), selectedAttrIndex.end(), i);
            // ith field does not exist in the raw data
            if (result == selectedAttrIndex.end()){
                this->getDirectoryEntry(i)->setNull();
                continue;
            }
            // ith exist but is null in raw data
            auto rawIdx = result - selectedAttrIndex.begin();
            if (rawRecord->isNullField(rawIdx)) {
                this->getDirectoryEntry(i)->setNull();
                continue;
            }
            this->getDirectoryEntry(i)->setOffset(recordOffset);
            switch (recordDescriptor[i].type) {
                case TypeInt:
                case TypeReal:
                    rawRecordOffset += 4;
                    recordOffset += 4;
                    // set directory
                    break;
                case TypeVarChar:
                    int32_t *strLen = (int32_t*)((uint8_t*)(rawRecord) + rawRecordOffset);
                    rawRecordOffset +=  sizeof(int32_t) + *strLen;
                    recordOffset += sizeof(int32_t) + *strLen;
                    break;
            }
        }
        recordLen = this->getDirectorySize() + recordOffset;
        memcpy(this->getDataSectionStart(), (uint8_t*)(rawRecord) + nullByteSize, recordOffset);
        return SUCCESS;
    }

// ==================================================
// todo Definitions for RawRecord struct
// ==================================================
    RC RawRecord::fromRecord(Record *record, const std::vector<Attribute> &recordDescriptor,
                             const std::vector<uint16_t> &selectedAttrIndex, int16_t &recordLen) {
        int16_t selectedAttrNum = selectedAttrIndex.size();
        // 1. init null flags
        int16_t nullFlagLenInByte = ceil(selectedAttrNum / 8.0);
        this->initNullByte(nullFlagLenInByte);

        // 2. write into not null value
        short rawDataPos = this->getNullByteSize(selectedAttrNum);
        //AttrIdx is idx in record; i is idx in rawRecord;
        for (short i = 0; i < selectedAttrNum; i++) {
            int16_t AttrIdx = selectedAttrIndex[i];
            if (record->getDirectory()->getEntry(AttrIdx)->isNull()) {
                this->setFieldNull(i);
                continue;
            }

            switch (recordDescriptor[AttrIdx].type) {
                case TypeInt:
                case TypeReal:
                    memcpy(this + rawDataPos, record->getFieldPtr<int32_t>(AttrIdx), sizeof(int32_t));
                    rawDataPos += sizeof(int32_t);
                    break;
                case TypeVarChar:
                    // 1. write varchar length
                    auto str = record->getField<std::string>(AttrIdx);
                    auto strLen = str.size();
                    memcpy(this + rawDataPos, &strLen, sizeof(RawDataStrLen));
                    rawDataPos += sizeof(RawDataStrLen);
                    // 2. write varchar
                    memcpy(this + rawDataPos, &str, strLen);
                    rawDataPos += strLen;
                    break;
            }
        }

        return SUCCESS;
    }

    RC RawRecord::size(const std::vector<Attribute>& attributes, int *size) const {
        int offset = 0;
        // Increment by null byte size
        offset += getNullByteSize(attributes.size());

        for (int i = 0; i < attributes.size(); i++) {
            const auto& attr = attributes[i];
            if(this->isNullField(i)) {
                continue;
            }
            switch (attr.type) {
                case TypeInt: {
                    offset += sizeof(uint32_t);
                    break;
                }
                case TypeReal: {
                    offset += sizeof(float);
                    break;
                }
                case TypeVarChar: {
                    const int32_t strLen = *(int32_t*)((const uint8_t*)(this) + offset);
                    offset += sizeof(strLen) + strLen;
                    break;
                }
                default: {
                    CHECK(false);
                    break;
                }
            }
        }
        *size = offset;
        return SUCCESS;
    }

    RC RawRecord::join(const std::vector<Attribute> &attrs, const RawRecord *rightRecord,
                       const std::vector<Attribute> &rightAttrs, RawRecord *joinRecord,
                       const std::vector<Attribute> &joinAttrs) const {
        // Set null byte
        memset(joinRecord, 0, joinRecord->getNullByteSize(joinAttrs.size()));
        int offset = 0;
        for (int i = 0; i < attrs.size(); i++) {
            if (this->isNullField(i)) {
                joinRecord->setFieldNull(i + offset);
            }
        }
        offset += attrs.size();
        for (int i = 0; i < rightAttrs.size(); i++) {
            if (rightRecord->isNullField(i)) {
                joinRecord->setFieldNull(i + offset);
            }
        }

        // Copy left data
        uint8_t *dest = (uint8_t*)joinRecord->dataSection(joinAttrs.size());
        int leftSize;
        this->size(attrs, &leftSize);
        int leftDataSectionSize = leftSize - this->getNullByteSize(attrs.size());
        memcpy(dest, this->dataSection(attrs.size()), leftDataSectionSize);
        dest += leftDataSectionSize;

        // Copy right data
        int rightSize;
        this->size(rightAttrs, &rightSize);
        int rightDataSectionSize = rightSize - rightRecord->getNullByteSize(rightAttrs.size());
        memcpy(dest, rightRecord->dataSection(rightAttrs.size()), rightDataSectionSize);

        int joinSize;
        joinRecord->size(joinAttrs, &joinSize);
        assert(joinRecord->getNullByteSize(joinAttrs.size()) + leftDataSectionSize + rightDataSectionSize == joinSize);
        return 0;
    }

}

