//
// Created by Fan Zhao on 1/18/23.
//

#include "src/include/rbfm.h"
#include <cstring>
#include <glog/logging.h>
#include "src/include/errorCode.h"


namespace PeterDB {
    PageHelper::PageHelper(FileHandle &fileHandle, PageNum pageNum) : fh(fileHandle), pageNum(pageNum) {
        fileHandle.readPage(pageNum, dataSeq);
        memcpy(&freeBytePointer, dataSeq + getFreeBytePointerOffset(), sizeof(short));
        memcpy(&slotCounter, dataSeq + getSlotCounterOffset(), sizeof(short));
    }

    PageHelper::~PageHelper() = default;

    RC PageHelper::insertRecordInByte(uint8_t *byteSeq, int16_t recLength, RID &rid, bool setUnoriginal) {
        // find the next available slot ID unless add one more slot
        int16_t slotId = getAvlSlotOffsetIdx();
        // [offset, recLength]
        setRecordOffset(slotId, freeBytePointer);
        setRecordLen(slotId, recLength, setUnoriginal);

        // write data byte
        memcpy(dataSeq + freeBytePointer, byteSeq, recLength);
        // update freeBytePointer
        freeBytePointer += recLength;
        memcpy(dataSeq + getFreeBytePointerOffset(), &freeBytePointer, sizeof(FreeBytePointer));
        // flush page on disk
        fh.writePage(pageNum, dataSeq);
        // save rid
        rid.pageNum = this->pageNum;
        rid.slotNum = slotId;

        return 0;
    }

    RC PageHelper::getRecordByte(int16_t slotNum, uint8_t *byteSeq, int16_t &recLength) {
        // get the start byte of record
        int16_t recordOffset = getRecordBeginPos(slotNum);
        recLength = getRecordLen(slotNum);

        // read data byte to recordByte
        memcpy(byteSeq, dataSeq + recordOffset, recLength);

        return 0;
    }

    RC PageHelper::deleteRecord(uint16_t slotIndex) {
        if (slotIndex > slotCounter) {
            LOG(ERROR) << "slotNum: " << slotIndex << "exceeded @ PageHelper::deleteRecord" << std::endl;
            return ERR_GENERAL;
        }
        if (!isRecordValid(slotIndex)) {
            LOG(ERROR) << "slotNum: " << slotIndex << "invalid @ PageHelper::deleteRecord" << std::endl;
            return ERR_GENERAL;
        }

        int16_t recordOffset = getRecordBeginPos(slotIndex);
        int16_t recordLen = getRecordLen(slotIndex);

        shiftRecord(recordOffset + recordLen, recordLen, SHIFT_LEFT);

        setRecordOffset(slotIndex, SLOT_OFFSET_EMPTY);
        setRecordLen(slotIndex, SLOT_LEN_EMPTY, false);

        // flush page on disk
        fh.writePage(pageNum, dataSeq);

        return 0;
    }

    //<short, short>[offset, length]
    int16_t PageHelper::getSlotSize() {
        return sizeof(SlotOffset) + sizeof(SlotLen);
    }

    int16_t PageHelper::getFlagsLength() {
        return sizeof(FreeBytePointer) + sizeof(SlotCounter);
    }

    int16_t PageHelper::getHeaderLength() {
        // Num + Free + slot_table
        return getFlagsLength() + getSlotSize() * slotCounter;
    }

    int16_t PageHelper::getSlotCounterOffset() {
        return PAGE_SIZE - getFlagsLength();
    }

    int16_t PageHelper::getFreeBytePointerOffset() {
        return PAGE_SIZE - sizeof(FreeBytePointer);

    }

    // start from 1
    int16_t PageHelper::getSlotOffset(int16_t slotNum) {
        return getSlotCounterOffset() - getSlotSize() * slotNum;
    }

    // get the present available slot offset and it might change slotCounter
    int16_t PageHelper::getAvlSlotOffsetIdx() {
        // find empty slot from the tail
        int16_t pos = 0;
        for (short i = 1; i <= slotCounter; i++) {
            if (isRecordDeleted(i)){
                pos = i;
                break;
            }
        }
        if (pos > 0) return pos;

        // no empty slot, create a new slot and update slotCounter
        slotCounter++;
        setSlotCounter(slotCounter);
        return slotCounter;
    }

    bool PageHelper::IsFreeSpaceEnough(int recLength) {
        short freeSpace = PAGE_SIZE - freeBytePointer - getHeaderLength();
        // record + 1 slot !!!
        return freeSpace >= (recLength + getSlotSize());
    }

    // dataStartPos: start from the data that needs to be moved
    RC PageHelper::shiftRecord(int16_t dataStartPos, int16_t distance, int8_t direction) {
        int16_t dataSeqNeedsBeMovedLen = freeBytePointer - dataStartPos;

        if (dataSeqNeedsBeMovedLen != 0) {
            if (direction == SHIFT_LEFT){
                memmove(dataSeq + dataStartPos - distance, dataSeq + dataStartPos, dataSeqNeedsBeMovedLen);
            }else if (direction == SHIFT_RIGHT) {
                memmove(dataSeq + dataStartPos + distance, dataSeq + dataStartPos, dataSeqNeedsBeMovedLen);
            }

            // update slots
            int16_t curSlotOffset;
            for (int16_t i = 1; i <= slotCounter; i++) {
                memcpy(&curSlotOffset, dataSeq + getSlotOffset(i), sizeof(SlotOffset));
                if (curSlotOffset == SLOT_OFFSET_EMPTY || curSlotOffset < dataStartPos) continue;
                if (direction == SHIFT_LEFT) { curSlotOffset -= distance;
                }else if (direction == SHIFT_RIGHT) { curSlotOffset += distance; }
                memcpy(dataSeq + getSlotOffset(i), &curSlotOffset, sizeof(SlotOffset));
            }
        }
        // update freePointer
        if (direction == SHIFT_LEFT) freeBytePointer -= distance;
        if (direction == SHIFT_RIGHT) freeBytePointer += distance;
        setFreeBytePointer(freeBytePointer);
        return SUCCESS;
    }

    void PageHelper::setFreeBytePointer(FreeBytePointer newPtr) {
        memcpy(dataSeq + getFreeBytePointerOffset(), &newPtr, sizeof(FreeBytePointer));
    }

    void PageHelper::setRecordLen(int16_t slotIndex, SlotLen sl, bool setUnoriginalSign) {
        if (setUnoriginalSign) sl *= (-1);
        memcpy(dataSeq + getSlotOffset(slotIndex) + sizeof(SlotOffset), &sl, sizeof(SlotLen));
    }

    void PageHelper::setRecordOffset(int16_t slotIndex, SlotOffset so) {
        memcpy(dataSeq + getSlotOffset(slotIndex), &so, sizeof(SlotOffset));
    }

    void PageHelper::setRecordFlag(int16_t slotIndex, Flag fg) {
        memcpy(dataSeq + getRecordBeginPos(slotIndex), &fg, sizeof(Flag));
    }

    // always return unsigned length
    int16_t PageHelper::getRecordLen(int16_t slotIndex) {
        int16_t recordLen;
        memcpy(&recordLen, dataSeq + getSlotOffset(slotIndex) + sizeof(SlotOffset), sizeof(SlotLen));
        // PointerRecord's recordLen is negative, but we need abs value anyway;
        return (int16_t)abs(recordLen);
    }

    int16_t PageHelper::getRecordBeginPos(int16_t slotIndex) {
        int16_t recordBegin;
        memcpy(&recordBegin, dataSeq + getSlotOffset(slotIndex), sizeof(SlotOffset));
        return recordBegin;
    }

    int8_t PageHelper::getRecordFlag(int16_t slotIndex) {
        Flag fg;
        memcpy(&fg, dataSeq + getRecordBeginPos(slotIndex), sizeof(Flag));
        return fg;
    }

    int8_t PageHelper::getRecordAttrNum(int16_t slotIndex) {
        AttrNum an;
        memcpy(&an, dataSeq + getRecordBeginPos(slotIndex) + sizeof(Flag) + sizeof(PlaceHolder), sizeof(AttrNum));
        return an;
    }


    RC PageHelper::getRecordPointer(int16_t slotIndex, uint32_t &ridPageNum, uint16_t &ridSlotNum) {
        if (!isRecordValid(slotIndex)) return ERR_RBFILE_SLOT_INVALID;
        int16_t recBegin = getRecordBeginPos(slotIndex);
        Flag fg;
        memcpy(&fg, dataSeq + recBegin, sizeof(Flag));

        if(fg != RECORD_FLAG_POINTER) {
            LOG(ERROR) << "Record is not a pointer!" << std::endl;
            return ERR_GENERAL;
        }
        memcpy(&ridPageNum, dataSeq + recBegin + sizeof(Flag), sizeof(uint32_t));
        memcpy(&ridSlotNum, dataSeq + recBegin + sizeof(Flag) + sizeof(uint32_t), sizeof(uint16_t));

        return SUCCESS;
    }

    RC PageHelper::getRecordAttr(int16_t slotIndex, int16_t attrIdx, uint8_t *attrVal) {
        if (!isRecordValid(slotIndex)) return ERR_RBFILE_SLOT_INVALID;
        if (isAttrNull(slotIndex, attrIdx)) return ERR_GENERAL;
        int16_t attrLen = getAttrLen(slotIndex, attrIdx);
        memcpy(attrVal, dataSeq + getRecordBeginPos(slotIndex) + getAttrBeginPos(slotIndex, attrIdx), attrLen);
    }

    RC PageHelper::updateRecord(int16_t slotIndex, uint8_t byteSeq[], int16_t recLength, bool setUnoriginal) {
        if (!isRecordValid(slotIndex)) {
            LOG(ERROR) << "slotNum: " << slotIndex << "invalid @ PageHelper::updateRecord" << std::endl;
            return ERR_GENERAL;
        }

        int16_t oldRecLen = getRecordLen(slotIndex);
        int16_t oldRecBeg = getRecordBeginPos(slotIndex);

        // shift data block to spare enough new space
        if (oldRecLen <= recLength) {
            shiftRecord(oldRecBeg + oldRecLen, recLength - oldRecLen, SHIFT_RIGHT);
        } else {
            shiftRecord(oldRecBeg + oldRecLen, oldRecLen - recLength, SHIFT_LEFT);
        }
        // update data
        memcpy(dataSeq + oldRecBeg, byteSeq, recLength);
        // if it is unoriginal record, need to set a sign
        setRecordLen(slotIndex, recLength, setUnoriginal);

        fh.writePage(pageNum, dataSeq);

        return SUCCESS;
    }

    RC PageHelper::setRecordPointToNewRID(int16_t curSlotIndex, const RID &newRecordRID, bool setUnoriginal) {
        if (!isRecordValid(curSlotIndex)) return ERR_RBFILE_SLOT_INVALID;
        int16_t recordBeg = getRecordBeginPos(curSlotIndex);
        int16_t oldRecLen = getRecordLen(curSlotIndex);
        // 1B flag + 4B pageNum + 2B slotNum
        memcpy(dataSeq + recordBeg, &RECORD_FLAG_POINTER, sizeof(Flag));
        memcpy(dataSeq + recordBeg + sizeof(Flag), &newRecordRID.pageNum, sizeof(uint32_t));
        memcpy(dataSeq + recordBeg + sizeof(Flag) + sizeof(uint32_t), &newRecordRID.slotNum, sizeof(uint16_t));

        int16_t newRecLen = sizeof(Flag) + sizeof(newRecordRID.pageNum) + sizeof(newRecordRID.slotNum);
        setRecordLen(curSlotIndex, newRecLen,setUnoriginal);
        // Shift records left

        int16_t dist = oldRecLen - newRecLen;
        shiftRecord(recordBeg + oldRecLen, dist, SHIFT_LEFT);
        // todo not sure
        fh.writePage(pageNum, dataSeq);
        return SUCCESS;
    }

    int16_t PageHelper::getAttrBeginPos(int16_t slotIndex, int16_t attrIndex) {
        int16_t curPos = getAttrEndPos(slotIndex, attrIndex);
        // attr is null
        if (curPos == ATTR_DIR_EMPTY) return ATTR_DIR_EMPTY;
        // find the first not null attr offset
        int16_t prevPos;
        for (short i = attrIndex - 1; i >= 0; i--) {
            prevPos = getAttrEndPos(slotIndex, i);
            if (prevPos != ATTR_DIR_EMPTY) break;
        }
        // previous attr are all null
        if (prevPos == ATTR_DIR_EMPTY)
            prevPos = sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum) +
                      getRecordAttrNum(slotIndex) * sizeof(AttrDir);
        return prevPos;
    }

    int16_t PageHelper::getAttrEndPos(int16_t slotIndex, int16_t attrIndex) {
        int16_t recordOffset = getRecordBeginPos(slotIndex);
        int16_t dirOffset = sizeof(Flag) + sizeof(PlaceHolder) + sizeof(AttrNum) + attrIndex * sizeof(AttrDir);
        int16_t attrEndPos;
        memcpy(&attrEndPos, dataSeq + recordOffset + dirOffset, sizeof(AttrDir));
        return attrEndPos;
    }

    int16_t PageHelper::getAttrLen(int16_t slotIndex, int16_t attrIndex) {
        int16_t end = getAttrEndPos(slotIndex, attrIndex);
        int16_t begin = getAttrBeginPos(slotIndex, attrIndex);
        return end - begin;
    }

    void PageHelper::setSlotCounter(SlotCounter sc){
        memcpy(dataSeq + getSlotCounterOffset(), &sc, sizeof(SlotCounter));
    }

    RC PageHelper::getNextRecordData(int16_t & slotIndex, uint8_t *byteSeq, int16_t & recordLen){
        RC rc;
        slotIndex++;
        if (slotIndex > slotCounter){
            LOG(ERROR) << "Record offset exceeded! @ PageHelper::getNextRecordData" << std::endl;
            return ERR_RBFILE_SLOT_EXCEEDED;
        }
        if (!isRecordDeleted(slotIndex)){
            LOG(ERROR) << "Record is deleted! @ PageHelper::getNextRecordData" << std::endl;
            return ERR_GENERAL;
        }
        if (!isOriginal(slotIndex)){
            LOG(ERROR) << "Record is not original! @ PageHelper::getNextRecordData" << std::endl;
            return ERR_RBFILE_REC_UNORIGINAL;
        }

        uint16_t realDataSlotId = slotIndex;
        uint32_t realDataPageId = pageNum;

        while (realDataPageId < fh.getNumberOfPages()){
            PageHelper realDataPage(fh, realDataPageId);
            if (realDataPage.isRecordData(realDataSlotId))break;
            realDataPage.getRecordPointer(realDataSlotId, realDataPageId, realDataSlotId);
        }

        if (realDataPageId >= fh.getNumberOfPages()){
            LOG(ERROR) << "Record is not original! @ PageHelper::getNextRecordData" << std::endl;
            return ERR_RBFILE_PAGE_EXCEEDED;
        }
        PageHelper realDataPage(fh, realDataPageId);
        rc = realDataPage.getRecordByte(realDataSlotId, byteSeq, recordLen);
        if (rc){
            LOG(ERROR) << "getRecordByte Err! @ PageHelper::getNextRecordData" << std::endl;
            return ERR_GENERAL;
        }

        return SUCCESS;
    }

    // must call isRecordValid before the following 6 functions!!!
    bool PageHelper::isRecordPointer(int16_t slotIndex) {
        if (!isRecordValid(slotIndex)) {
            LOG(ERROR) << "Record is invalid! @ PageHelper::isRecordPointer" << std::endl;
            return false;
        }
        Flag fg;
        fg = getRecordFlag(slotIndex);
        if (fg == RECORD_FLAG_POINTER) return true;
        return false;
    }

    bool PageHelper::isRecordData(int16_t slotIndex) {
        if (!isRecordValid(slotIndex)) {
            LOG(ERROR) << "Record is invalid! @ PageHelper::isRecordData" << std::endl;
            return false;
        }
        Flag fg;
        fg = getRecordFlag(slotIndex);
        if (fg == RECORD_FLAG_DATA) return true;
        return false;
    }

    bool PageHelper::isRecordDeleted(int16_t slotIndex) {
        int16_t recordBeg = getRecordBeginPos(slotIndex);
        if (recordBeg == SLOT_OFFSET_EMPTY) return true;
        return false;
    }

    bool PageHelper::isRecordValid(int16_t slotIndex) {
        if (slotIndex <= slotCounter && !isRecordDeleted(slotIndex)) return true;
        return false;
    }

    bool PageHelper::isOriginal(int16_t slotIndex) {
        if (!isRecordValid(slotIndex)) {
            LOG(ERROR) << "Record is invalid! @ PageHelper::isOriginalRID" << std::endl;
            return false;
        }
        int16_t recordLen;
        memcpy(&recordLen, dataSeq + getSlotOffset(slotIndex) + sizeof(SlotOffset), sizeof(SlotLen));
        if (recordLen < 0) return false;
        return true;
    }

    bool PageHelper::isAttrNull(int16_t slotIndex, int16_t attrIndex) {
        int16_t end = getAttrEndPos(slotIndex, attrIndex);
        if (end == ATTR_DIR_EMPTY) return true;
        return false;
    }
}



