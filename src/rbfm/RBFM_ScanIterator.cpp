//
// Created by Fan Zhao on 1/31/23.
//
#include "src/include/rbfm.h"
#include <cstring>
#include <glog/logging.h>
#include "src/include/errorCode.h"

namespace PeterDB {
    RC RBFM_ScanIterator::begin(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &selectedAttrNames) {
        // init basic variable
        this->fileHandle = fileHandle;
        this->recordDescriptor = recordDescriptor;

        for (int i; i < selectedAttrNames.size(); i++){
            // to find the index j of this attr in recordDescriptor
            for (int j ; j< recordDescriptor.size(); j++){
                if (recordDescriptor[j].name == selectedAttrNames[i]) this->selectedAttrIdx.push_back(j);
            }
        }

        this->curPageNum = 0;
        //todo which slot to start
        this->curSlotNum = 0;

        // init conditions
        // todo NO-OP
        this->compOp = compOp;
        this->conditionAttrIdx = CONDITION_ATTR_IDX_INVALID;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            if (recordDescriptor[i].name == conditionAttribute) {
                this->conditionAttrIdx = i;
                this->conditionAttr = recordDescriptor[i];
                break;
            }
        }
        if (this->conditionAttrIdx == CONDITION_ATTR_IDX_INVALID) {
            LOG(ERROR) << "CONDITION_ATTR_IDX_INVALID @ RBFM_ScanIterator::begin" << std::endl;
            return ERR_GENERAL;
        }
        if (!value) {
            LOG(ERROR) << "condition value empty @ RBFM_ScanIterator::begin" << std::endl;
            return ERR_GENERAL;
        }

        switch (conditionAttr.type) {
            case TypeInt:
                memcpy(this->conditionVal, (uint8_t *) value, sizeof(int));
                break;
            case TypeReal:
                memcpy(this->conditionVal, (uint8_t *) value, sizeof(float));
                break;
            case TypeVarChar:
                memcpy(&this->conditionStrLen, (uint8_t *) value, sizeof(RawDataStrLen));
                memcpy(this->conditionVal, (uint8_t *) value + sizeof(RawDataStrLen), conditionStrLen);
                break;
        }

        return SUCCESS;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        if (curPageNum >= fileHandle.getNumberOfPages()) {
            LOG(ERROR) << "PageNum exceeded! @ RBFM_ScanIterator::getNextRecord" << std::endl;
            return RBFM_EOF;
        }

        int16_t recordLen = 0;
        while (curPageNum < fileHandle.getNumberOfPages()) {
            PageHelper thisPage(fileHandle, curPageNum);
            RC rc = thisPage.getNextRecordData(curSlotNum, recordData, recordLen);
            if (rc) {
                // next page
                if (rc == ERR_RBFILE_SLOT_EXCEEDED) {
                    curPageNum++;
                    curSlotNum = 0;
                }
                continue;
            }
            // get the real data record
            if (this->compOp == NO_OP) {
                // no condition, return
                break;
            } else if (RecordHelper::recordIsAttrNull(recordData, conditionAttrIdx)) {
                // condition attribute is null, should skip this record
                continue;
            } else {
                // check if the record meet the condition
                uint8_t attr[PAGE_SIZE];
                int16_t attrLen;
                RecordHelper::recordGetAttr(recordData, conditionAttrIdx, attr, attrLen);
                if (recordMeetCondition(attr, attrLen))break;
            }


        }

        if (curPageNum > fileHandle.getNumberOfPages()) return RBFM_EOF;

        // will return the original real data record RID
        rid.pageNum = curPageNum;
        rid.slotNum = curSlotNum;

        // convert data to rawdata
        RecordHelper::recordByteToRawData(recordData, recordDescriptor, selectedAttrIdx, (uint8_t *)data);

        return SUCCESS;
    }

    bool RBFM_ScanIterator::recordMeetCondition(uint8_t *attrVal, int16_t attrLen) {
        AttrType at = recordDescriptor[conditionAttrIdx].type;

        switch(at){
            case TypeInt:
                int recordAttrInt;
                int conditionAttrInt;
                memcpy(&recordAttrInt, attrVal, sizeof(int));
                memcpy(&conditionAttrInt, conditionVal, sizeof(int));
                return compareInt(recordAttrInt, conditionAttrInt);

            case TypeReal:
                float recordAttrFloat;
                float conditionAttrFloat;
                memcpy(&recordAttrFloat, attrVal, sizeof(float));
                memcpy(&conditionAttrFloat, conditionVal, sizeof(float));
                return compareFloat(recordAttrFloat, conditionAttrFloat);

            case TypeVarChar:
                char recordAttrStr[attrLen];
                char conditionAttrStr[attrLen];
                memcpy(recordAttrStr, attrVal, sizeof(attrLen));
                memcpy(conditionAttrStr, conditionVal, sizeof(attrLen));
                return compareStr(std::string(recordAttrStr), std::string(conditionAttrStr));
        }
    }

    bool RBFM_ScanIterator::compareInt(int a, int b){
        switch(compOp){
            case EQ_OP:
                return a == b;
            case  LT_OP:
                return a < b;
            case LE_OP:
                return a<= b;
            case GT_OP:
                return a > b;
            case GE_OP:
                return a >= b;
            case NE_OP:
                return a != b;
            default:
                LOG(ERROR) << "compOp Err!" << std::endl;
                return false;
        }
    }

    bool RBFM_ScanIterator::compareFloat(float a, float b){
        switch(compOp){
            case EQ_OP:
                return a == b;
            case  LT_OP:
                return a < b;
            case LE_OP:
                return a<= b;
            case GT_OP:
                return a > b;
            case GE_OP:
                return a >= b;
            case NE_OP:
                return a != b;
            default:
                LOG(ERROR) << "compOp Err!" << std::endl;
                return false;
        }
    }

    bool RBFM_ScanIterator::compareStr(std::string a, std::string b){
        switch(compOp){
            case EQ_OP:
                return a == b;
            case  LT_OP:
                return a < b;
            case LE_OP:
                return a<= b;
            case GT_OP:
                return a > b;
            case GE_OP:
                return a >= b;
            case NE_OP:
                return a != b;
            default:
                LOG(ERROR) << "compOp Err!" << std::endl;
                return false;
        }
    }

}