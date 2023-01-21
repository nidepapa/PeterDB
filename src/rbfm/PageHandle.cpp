//
// Created by Fan Zhao on 1/18/23.
//

#include "src/include/rbfm.h"

namespace PeterDB {
    PageHandle::PageHandle(FileHandle& fileHandle, PageNum pageNum):fh(fileHandle), pageNum(pageNum) {
        fileHandle.readPage(pageNum, dataSeq);
        memcpy(&freeBytePointer, dataSeq + getFreeBytePointerOffset(), sizeof(short));
        memcpy(&slotCounter, dataSeq + getSlotCounterOffset(), sizeof(short));
    }

    PageHandle::~PageHandle() = default;

    RC PageHandle::insertRecordInByte(char *byteSeq, short recLength, RID& rid){
        // update slot counter
        slotCounter++;
        memcpy(dataSeq + getSlotCounterOffset(), &slotCounter, sizeof(short));
        // add one more slot [offset, recLength]
        memcpy(dataSeq + getSlotOffset(slotCounter), &freeBytePointer, sizeof(short));
        memcpy(dataSeq + getSlotOffset(slotCounter) + sizeof(short), &recLength, sizeof(short));
        // write data byte
        memcpy(dataSeq + freeBytePointer, byteSeq, recLength);
        // update slot value and freeBytePointer
        freeBytePointer += recLength;
        memcpy(dataSeq + getFreeBytePointerOffset(), &freeBytePointer, sizeof(short));
        // flush page on disk
        fh.writePage(pageNum, dataSeq);
        // save rid
        rid.pageNum = pageNum;
        rid.slotNum = slotCounter;

        return 0;
    }
    RC PageHandle::getRecordInByte(short slotNum, char *recordByteSeq, short& recLength){
        // get the start byte of record
        short slotOffset = getSlotOffset(slotNum);
        short recordOffset;

        memcpy(&recordOffset, dataSeq + slotOffset, sizeof(short));
        memcpy(&recLength, dataSeq + slotOffset + sizeof(short), sizeof(short));
        // read data byte to recordByte
        memcpy(recordByteSeq, dataSeq + recordOffset, recLength);

        return 0;
    }
    //<short, short>[offset, length]
    short PageHandle::getSlotSize(){
        return 2 * sizeof(short);
    }

    short PageHandle::getFlagsLength(){
        return sizeof(typeid(freeBytePointer).name()) + sizeof(typeid(slotCounter).name());
    }

    short PageHandle::getHeaderLength(){
        // Num + Free + slot_table
        return  getFlagsLength() + getSlotSize() * slotCounter;
    }

    short PageHandle::getSlotCounterOffset(){
        return PAGE_SIZE - getFlagsLength();
    }
    short PageHandle::getFreeBytePointerOffset(){
        return PAGE_SIZE - sizeof(typeid(freeBytePointer).name());
    }
    // start from 1
    short PageHandle::getSlotOffset(short slotNum){
        return getSlotCounterOffset() - getSlotSize() * slotNum;
    }

    bool PageHandle::IsFreeSpaceEnough(int recLength){
        short freeSpace = PAGE_SIZE - freeBytePointer - getHeaderLength();
        return freeSpace >= recLength;
    }
}


