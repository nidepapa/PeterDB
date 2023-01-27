//
// Created by Fan Zhao on 1/18/23.
//

#include "src/include/rbfm.h"
#include <cstring>


namespace PeterDB {
    PageHelper::PageHelper(FileHandle& fileHandle, PageNum pageNum):fh(fileHandle), pageNum(pageNum) {
        fileHandle.readPage(pageNum, dataSeq);
        memcpy(&freeBytePointer, dataSeq + getFreeBytePointerOffset(), sizeof(short));
        memcpy(&slotCounter, dataSeq + getSlotCounterOffset(), sizeof(short));
    }

    PageHelper::~PageHelper() = default;

    RC PageHelper::insertRecordInByte(int8_t *byteSeq, int16_t recLength, RID& rid){
        // update slot counter
        slotCounter++;
        memcpy(dataSeq + getSlotCounterOffset(), &slotCounter, sizeof(SlotCounter));
        // add one more slot [offset, recLength]
        memcpy(dataSeq + getSlotOffset(slotCounter), &freeBytePointer, sizeof(SlotOffset));
        memcpy(dataSeq + getSlotOffset(slotCounter) + sizeof(SlotOffset), &recLength, sizeof(SlotLen));
        // write data byte
        memcpy(dataSeq + freeBytePointer, byteSeq, recLength);
        // update slot value and freeBytePointer
        freeBytePointer += recLength;
        memcpy(dataSeq + getFreeBytePointerOffset(), &freeBytePointer, sizeof(FreeBytePointer));
        // flush page on disk
        fh.writePage(pageNum, dataSeq);
        // save rid
        rid.pageNum = pageNum;
        rid.slotNum = slotCounter;

        return 0;
    }
    RC PageHelper::getRecordInByte(int16_t slotNum, int8_t *recordByteSeq, int16_t& recLength){
        // get the start byte of record
        int16_t slotOffset = getSlotOffset(slotNum);
        int16_t recordOffset;

        memcpy(&recordOffset, dataSeq + slotOffset, sizeof(SlotOffset));
        memcpy(&recLength, dataSeq + slotOffset + sizeof(SlotOffset), sizeof(SlotLen));
        // read data byte to recordByte
        memcpy(recordByteSeq, dataSeq + recordOffset, recLength);

        return 0;
    }
    //<short, short>[offset, length]
    int16_t PageHelper::getSlotSize(){
        return sizeof(SlotOffset) + sizeof(SlotLen) ;
    }

    int16_t PageHelper::getFlagsLength(){
        return sizeof(FreeBytePointer) + sizeof(SlotCounter);
    }

    int16_t PageHelper::getHeaderLength(){
        // Num + Free + slot_table
        return  getFlagsLength() + getSlotSize() * slotCounter;
    }

    int16_t PageHelper::getSlotCounterOffset(){
        return PAGE_SIZE - getFlagsLength();
    }

    int16_t PageHelper::getFreeBytePointerOffset(){
        return PAGE_SIZE - sizeof(FreeBytePointer);

    }
    // start from 1
    int16_t PageHelper::getSlotOffset(int16_t slotNum){
        return getSlotCounterOffset() - getSlotSize() * slotNum;
    }

    bool PageHelper::IsFreeSpaceEnough(int recLength){
        short freeSpace = PAGE_SIZE - freeBytePointer - getHeaderLength();
        // record + 1 slot !!!
        return freeSpace >= (recLength + getSlotSize());
    }
}


