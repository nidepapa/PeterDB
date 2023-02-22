//
// Created by Fan Zhao on 2/21/23.
//

#ifndef PETERDB_IXNODE_H
#define PETERDB_IXNODE_H

#include "ix.h"

namespace PeterDB {
    struct internalEntry {
        int32_t indicator;

        virtual int16_t getKeyLength() {
            return sizeof(int32_t);
        }

        virtual int16_t getEntryLength() {
            return getKeyLength() + sizeof(int32_t);
        }

        template <typename T>
        T getKey() const {
            return *(T*)(uint8_t *)this;
        }

        int32_t getLeftChild() {
            int32_t ptr;
            memcpy(&ptr, (uint8_t *) this - IX::NEXT_POINTER_LEN, IX::NEXT_POINTER_LEN);
            return ptr;
        }

        virtual int32_t getRightChild() {
            int32_t ptr;
            memcpy(&ptr, (uint8_t *) this + getKeyLength(), IX::NEXT_POINTER_LEN);
            return ptr;
        }

        virtual internalEntry* getNextEntry() {
            return (internalEntry *)((uint8_t *)this + getEntryLength());
        }
    };

    struct internalStrEntry : internalEntry {
        int16_t getKeyLength() override {
            return indicator + sizeof(int32_t);
        }

        int16_t getEntryLength() override {
            return getKeyLength() +  sizeof(int32_t);
        }

        std::string getKey() const {
            int32_t size = indicator;
            char* data = (char*)(size + 1);
            return std::string(data, size);
        }

        int32_t getRightChild() override {
            int32_t ptr;
            memcpy(&ptr, (uint8_t *) this + getKeyLength(), IX::NEXT_POINTER_LEN);
            return ptr;
        }

        internalStrEntry* getNextEntry() override {
            return (internalStrEntry *)((uint8_t *)this + getEntryLength());
        }
    };

    struct leafEntry {
        int32_t indicator;

        virtual int16_t getKeyLength() {
            return sizeof(int32_t);
        }

        virtual int16_t getEntryLength() {
            // key | rid.page| rid.slot
            return getKeyLength() + sizeof(int32_t) + sizeof(int16_t);
        }

        template <typename T>
        T getKey() const {
            return *(T*)(uint8_t *)this;
        }

        virtual leafEntry* getNextEntry() {
            return (leafEntry *)((uint8_t *)this + getEntryLength());
        }

        virtual void getRID(int32_t & pageNum, int16_t & slotNum){
            memcpy(&pageNum, (uint8_t *)this + getKeyLength(), sizeof(int32_t));
            memcpy(&slotNum, (uint8_t *)this + getKeyLength() + sizeof(int32_t), sizeof(int16_t));
        }
    };

    struct leafStrEntry : leafEntry {
        int16_t getKeyLength() override {
            return indicator + sizeof(int32_t);
        }

        int16_t getEntryLength() {
            // key | rid.page| rid.slot
            return getKeyLength() + sizeof(int32_t) + sizeof(int16_t);
        }

        leafStrEntry* getNextEntry() override {
            return (leafStrEntry *)((uint8_t *)this + getEntryLength());
        }

        std::string getKey() const {
            int32_t size = indicator;
            char* data = (char*)(size + 1);
            return std::string(data, size);
        }

        void getRID(int32_t & pageNum, int16_t & slotNum){
            memcpy(&pageNum, (uint8_t *)this + getKeyLength(), sizeof(int32_t));
            memcpy(&slotNum, (uint8_t *)this + getKeyLength() + sizeof(int32_t), sizeof(int16_t));
        }
    };

    class IXNode {
    public:
        IXFileHandle &ixFileHandle;
        uint32_t pageNum;
        uint16_t freeBytePointer;
        uint16_t keyCounter;
        uint16_t nodeType;

        uint8_t data[PAGE_SIZE] = {};
        // for sanity
        uint8_t origin[PAGE_SIZE] = {};

        // existing node
        IXNode(IXFileHandle &ixFileHandle, uint32_t pageNum) : ixFileHandle(ixFileHandle), pageNum(pageNum) {
            ixFileHandle.readPage(pageNum, data);
            memcpy(origin, data, PAGE_SIZE);

            freeBytePointer = getFreeBytePointerFromData();
            nodeType = getNodeTypeFromData();
            keyCounter = getkeyCounterFromData();
        }

        // new node
        IXNode(IXFileHandle &fileHandle, uint32_t page, uint16_t type, uint16_t freeByte, uint16_t counter) :
                ixFileHandle(fileHandle), pageNum(page), nodeType(type), freeBytePointer(freeByte),
                keyCounter(counter) {
            ixFileHandle.readPage(pageNum, data);
            memcpy(origin, data, PAGE_SIZE);
        }

        // new node when splitting
        IXNode(uint8_t *newData, int16_t dataLen, IXFileHandle &fileHandle, uint32_t page, uint16_t type,
               uint16_t freeByte, uint16_t counter) :
                ixFileHandle(fileHandle), pageNum(page), nodeType(type), freeBytePointer(freeByte),
                keyCounter(counter) {
            memcpy(data, newData, dataLen);
            memcpy(origin, newData, dataLen);
        }

        ~IXNode() {
            flushHeader();
            if (memcmp(origin, data, PAGE_SIZE) != 0) {
                ixFileHandle.writePage(pageNum, data);
            }
        };

        //getter
        uint16_t getNodeType() const { return this->nodeType; }

        uint16_t getFreeBytePointer() const { return this->freeBytePointer; }

        uint16_t getkeyCounter() const { return this->keyCounter; }

        uint16_t getNodeTypeFromData() const {
            uint16_t type;
            memcpy(&type, data + PAGE_SIZE - IX::NODE_TYPE_LEN,
                   IX::NODE_TYPE_LEN);
            return type;
        }

        uint16_t getFreeBytePointerFromData() {
            uint16_t ptr;
            memcpy(&ptr, data + PAGE_SIZE - IX::NODE_TYPE_LEN - IX::FREEBYTEPOINTER_LEN,
                   IX::FREEBYTEPOINTER_LEN);
            return ptr;
        };

        uint16_t getkeyCounterFromData() const {
            uint16_t counter;
            memcpy(&counter, data + PAGE_SIZE - IX::NODE_TYPE_LEN - IX::FREEBYTEPOINTER_LEN - IX::KEY_COUNTER_LEN,
                   IX::KEY_COUNTER_LEN);
            return counter;

        }

        // setter
        void setNodeType(uint16_t type) {
            memcpy(data + PAGE_SIZE - IX::NODE_TYPE_LEN, &type, IX::NODE_TYPE_LEN);
            this->nodeType = type;
        }

        void setFreeBytePointer(uint16_t pointer) {
            memcpy(data + PAGE_SIZE - IX::NODE_TYPE_LEN - IX::FREEBYTEPOINTER_LEN, &pointer, IX::FREEBYTEPOINTER_LEN);
            this->freeBytePointer = pointer;
        }

        void setkeyCounter(uint16_t counter) {
            memcpy(data + PAGE_SIZE - IX::NODE_TYPE_LEN - IX::FREEBYTEPOINTER_LEN - IX::KEY_COUNTER_LEN,
                   &counter, IX::KEY_COUNTER_LEN);
            this->keyCounter = counter;
        }

        void flushHeader() {
            setNodeType(nodeType);
            setkeyCounter(keyCounter);
            setFreeBytePointer(freeBytePointer);
        }

        RC shiftDataLeft(int16_t dataNeedShiftStartPos, int16_t dist);

        RC shiftDataRight(int16_t dataNeedMoveStartPos, int16_t dist);
    };

    class InternalNode : public IXNode {
        // Open existed page
        InternalNode(IXFileHandle &ixfileHandle, uint32_t page);

        // Initialize new page containing one entry
        InternalNode(IXFileHandle &ixfileHandle, uint32_t page, uint32_t leftPage, uint8_t *key, uint32_t rightPage,
                     const Attribute &attr);

        // Initialize new page with existing entries
        InternalNode(IXFileHandle &ixfileHandle, uint32_t page, uint8_t *entryData, int16_t dataLen,
                     int16_t entryCounter);

        ~InternalNode();

        // Get target child page, if not exist, append one
        RC getTargetChild(uint32_t &childPtr, const uint8_t *key, const RID &rid, const Attribute &attr);

        RC
        findPosToInsertKey(int16_t &curPos, const uint8_t *keyToInsert, const RID &ridToInsert, const Attribute &attr);

        RC insertIndex(uint8_t *middleKey, uint32_t &newChildPage, bool &isNewChildExist, const uint8_t *keyToInsert,
                       const RID &ridToInsert, const Attribute &attr, uint32_t childPtrToInsert);

        RC insertIndexWithEnoughSpace(const uint8_t *key, const RID &rid, const Attribute &attr, uint32_t childPage);

        RC writeIndex(int16_t pos, const uint8_t *key, const RID &rid, const Attribute &attr, uint32_t newPageNum);

        RC splitPageAndInsertIndex(uint8_t *middleCompKey, uint32_t &newIndexPage, const uint8_t *keyToInsert,
                                   const RID &ridToInsert, const Attribute &attr, uint32_t childPtrToInsert);

        RC print(const Attribute &attr, std::ostream &out);

        int16_t getEntryLen(const uint8_t *key, const Attribute &attr) const {

        };

        bool hasEnoughSpace(const uint8_t *key, const Attribute &attr);

        int16_t getIndexHeaderLen();

        int16_t getFreeSpace();
    };

    class LeafNode : public IXNode {
    public:
        uint32_t nextPtr;

        LeafNode();

        ~LeafNode();

        // getter
        uint32_t getNextPtr() const { return this->nextPtr; }

        uint32_t getNextPtrFromData() const {
            uint32_t ptr;
            memcpy(&ptr, data + PAGE_SIZE - IX::NODE_TYPE_LEN - IX::FREEBYTEPOINTER_LEN - IX::KEY_COUNTER_LEN -
                         IX::NEXT_POINTER_LEN,
                   IX::NEXT_POINTER_LEN);
            return ptr;
        }

        //setter
        void setkeyCounter(uint32_t ptr) {
            memcpy(data + PAGE_SIZE - IX::NODE_TYPE_LEN - IX::FREEBYTEPOINTER_LEN - IX::KEY_COUNTER_LEN -
                   IX::NEXT_POINTER_LEN,
                   &ptr, IX::NEXT_POINTER_LEN);
            this->nextPtr = ptr;
        }
    };
}




#endif //PETERDB_IXNODE_H
