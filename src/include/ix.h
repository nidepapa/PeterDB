#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    namespace IX{
        const int32_t File_Header_Page_Size = 4096;
        const int32_t NULL_PTR = -1;

        const int32_t NODE_TYPE_LEN = 2;
        const int32_t FREEBYTEPOINTER_LEN = 2;
        const int32_t KEY_COUNTER_LEN = 2;
        const int32_t NEXT_POINTER_LEN = 4;

        const int16_t INTERNAL_NODE = 1;
        const int16_t LEAF_NODE = 2;

        const int32_t NEXT_POINTER_NULL = 0;
    }
    struct internalEntry {
        int32_t indicator;

        int16_t getKeyLength(AttrType type) const {
            if (type == TypeVarChar){
                return indicator + sizeof(int32_t);
            }else{
                return sizeof(int32_t);
            }
        }

        int16_t getEntryLength(AttrType type) const{
            return getKeyLength(type) + sizeof(int32_t);
        }

        template <typename T>
        T* getKeyPtr() const {
            return (T *)(uint8_t*)this;
        }

        template <typename T>
        T getKey() const {
            return *getKeyPtr<T>();
        }

        int32_t getLeftChild() {
            int32_t ptr;
            memcpy(&ptr, (uint8_t *) this - IX::NEXT_POINTER_LEN, IX::NEXT_POINTER_LEN);
            return ptr;
        }

        int32_t getRightChild(AttrType type) {
            int32_t ptr;
            memcpy(&ptr, (uint8_t *) this + getKeyLength(type), IX::NEXT_POINTER_LEN);
            return ptr;
        }

        internalEntry *getNextEntry(AttrType type) {
            return (internalEntry *) ((uint8_t *) this + getEntryLength(type));
        }

        void setKey(AttrType type, uint8_t *key) {
            memcpy((uint8_t *) this, key, getKeyLength(type));
        }

        void setRightChild(AttrType type, uint32_t ptr){
            memcpy((uint8_t *) this + getKeyLength(type), &ptr, IX::NEXT_POINTER_LEN);
        }

    };

    struct leafEntry {
        int32_t indicator;

        int16_t getKeyLength(AttrType type) {
            if (type == TypeVarChar){
                return indicator + sizeof(int32_t);
            }else{
                return sizeof(int32_t);
            }
        }

        int16_t getEntryLength(AttrType type) {
            // key | rid.page| rid.slot
            return getKeyLength(type) + sizeof(int32_t) + sizeof(int16_t);
        }

        leafEntry *getNextEntry(AttrType type) {
            return (leafEntry *) ((uint8_t *) this + getEntryLength(type));
        }

        template <typename T>
        T* getKeyPtr() const {
            return (T *)(uint8_t*)this;
        }

        template <typename T>
        T getKey() const {
            return *getKeyPtr<T>();
        }

        void getRID(AttrType type, uint32_t &pageNum, uint16_t &slotNum) {
            memcpy(&pageNum, (uint8_t *) this + getKeyLength(type), sizeof(uint32_t));
            memcpy(&slotNum, (uint8_t *) this + getKeyLength(type) + sizeof(uint32_t), sizeof(uint16_t));
        }

        void setKey(AttrType type, uint8_t *key) {
            memcpy((uint8_t *) this, key, getKeyLength(type));
        }

        void setRID(AttrType type, const uint32_t &pageNum, const uint16_t &slotNum){
            memcpy((uint8_t *) this + getKeyLength(type), &pageNum, sizeof(uint32_t));
            memcpy((uint8_t *) this + getKeyLength(type) + sizeof(uint32_t), &slotNum, sizeof(uint16_t));
        }
    };
    template <>
    inline std::string internalEntry::getKey() const {
        auto *size = (int32_t *)this;
        char* data = (char*)(size + 1);
        return std::string(data, *size);
    }

    template <>
    inline std::string leafEntry::getKey() const {
        auto *size = (int32_t *)this;
        char* data = (char*)(size + 1);
        return std::string(data, *size);
    }
    class IndexManager {

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);


        RC insertEntryRecur(IXFileHandle &ixFileHandle, int32_t nodePointer, leafEntry *entry, internalEntry *newChildEntry, const Attribute &attribute);
        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        RC findTargetLeafNode(IXFileHandle &ixFileHandle, uint32_t& targetLeaf, const uint8_t* key, const Attribute& attr);
            // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

        static bool isFileExists(const std::string fileName);

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IX_ScanIterator {
    public:
        IXFileHandle* ixFileHandle;
        Attribute attr;
        const uint8_t* lowKey;
        bool lowKeyInclusive;
        const uint8_t* highKey;
        bool highKeyInclusive;

        uint32_t curLeafPage;
        int16_t remainDataLen;
        bool entryExceedUpperBound;

        // Constructor
        IX_ScanIterator(){
            lowKey = nullptr;
            highKey = nullptr;
            curLeafPage = IX::NULL_PTR;
            remainDataLen = 0;
            entryExceedUpperBound = false;
        };

        // Destructor
        ~IX_ScanIterator() = default;

        RC open(IXFileHandle* fileHandle, const Attribute& attr, const uint8_t* lowKey,
                                 const uint8_t* highKey, bool lowKeyInclusive, bool highKeyInclusive);

        RC getNextNonEmptyPage();
        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;
        int32_t rootPagePtr;
        // int, real, varchar, for sanity check
        int32_t KeyType;

        std::string fileName;
        FILE *fileInMemory;

        // Constructor
        IXFileHandle();
        // Destructor
        ~IXFileHandle();

        RC open(const std::string& filename);
        RC close();

        RC readPage(uint32_t pageNum, void* data);
        RC writePage(uint32_t pageNum, const void* data);
        RC appendPage(const void* data);
        RC appendEmptyPage();
        RC initHiddenPage();
        RC readMetaData();
        RC flushMetaData();

        RC createRootPage();

        uint32_t getRoot();
        RC setRoot(int32_t newRoot);

        std::string getFileName() const;
        bool isOpen();
        bool isRootNull() const;

        uint32_t getNumberOfPages() const;
        uint32_t getLastPageIndex() const;



        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);


    };


    class IXNode {
    public:
        IXFileHandle & ixFileHandle;
        uint32_t pageNum;
        uint16_t freeBytePointer;
        uint16_t keyCounter;
        uint16_t nodeType;

        uint8_t data[PAGE_SIZE];
        // for sanity
        uint8_t origin[PAGE_SIZE];

        // existing node
        IXNode(IXFileHandle &ixFileHandle, uint32_t pageNum) : ixFileHandle(ixFileHandle), pageNum(pageNum) {
            ixFileHandle.readPage(pageNum, data);
            memcpy(origin, data, PAGE_SIZE);

            freeBytePointer = getFreeBytePointerFromData();
            nodeType = getNodeTypeFromData();
            keyCounter = getkeyCounterFromData();
        }

        // new node
        IXNode(IXFileHandle &ixFileHandle, uint32_t page, uint16_t type, uint16_t freeByte, uint16_t counter) :
                ixFileHandle(ixFileHandle), pageNum(page), nodeType(type), freeBytePointer(freeByte),
                keyCounter(counter) {
            ixFileHandle.readPage(pageNum, data);
            memcpy(origin, data, PAGE_SIZE);
        }

        // new node when splitting
        IXNode(uint8_t *newData, int16_t dataLen, IXFileHandle &ixFileHandle, uint32_t page, uint16_t type,
               uint16_t freeByte, uint16_t counter) :
                ixFileHandle(ixFileHandle), pageNum(page), nodeType(type), freeBytePointer(freeByte),
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

        uint32_t getPageNum() const { return this->pageNum; }

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

        bool isRoot(){
            if (getPageNum() == ixFileHandle.getRoot())return true;
            return false;
        }
    };

    class InternalNode : public IXNode {
    public:
        // Open existed page
        InternalNode(IXFileHandle &ixfileHandle, uint32_t page) : IXNode(ixfileHandle, page) {};

        // Initialize new page containing one entry
        InternalNode(IXFileHandle &ixfileHandle, uint32_t page, uint32_t leftPage, internalEntry *key,
                     const Attribute &attr) : IXNode(ixfileHandle, page, IX::INTERNAL_NODE, 0, 1) {
            int16_t pos = 0;
            // Write left page pointer
            memcpy(data + pos, &leftPage, IX::NEXT_POINTER_LEN);
            pos += IX::NEXT_POINTER_LEN;
            int16_t entryLen = key->getEntryLength(attr.type);
            memcpy(data + pos, (uint8_t *) key, entryLen);
            pos += entryLen;

            setFreeBytePointer(pos);
            setkeyCounter(1);
        }

        // Initialize new page with existing entries
        InternalNode(uint8_t *entryData, int16_t dataLen, IXFileHandle &ixfileHandle, uint32_t page,
                     int16_t entryCounter) : IXNode(entryData, dataLen, ixfileHandle, page, IX::INTERNAL_NODE, dataLen,
                                                    entryCounter) {};

        ~InternalNode() = default;

        // Get target child page, if not exist, append one
        RC getTargetChild(leafEntry *key, const Attribute &attr, int32_t & childPage);
        // start from first key
        RC
        findPosToInsertKey(internalEntry *firstGEEntry, leafEntry *key, const Attribute &attr);

        RC
        findPosToInsertKey(int16_t& curPos, internalEntry *key, const Attribute &attr);

        RC insertEntry(internalEntry* key, const Attribute &attr);

        RC writeEntry(internalEntry *key, const Attribute &attribute, int16_t pos);

        RC splitPageAndInsertIndex(internalEntry *key, const Attribute &attr, internalEntry *newChildEntry);

        RC print(const Attribute &attr, std::ostream &out);

        bool hasEnoughSpace(const uint8_t *key, const Attribute &attr);

        uint16_t getFreeSpace() const {
            return PAGE_SIZE - freeBytePointer - IX::NODE_TYPE_LEN - IX::FREEBYTEPOINTER_LEN - IX::KEY_COUNTER_LEN -
                   IX::NEXT_POINTER_LEN;
        }

        static bool isKeyMeetCompCondition(internalEntry *key1, leafEntry *key2, const Attribute& attr, const CompOp op);
        static bool isKeyMeetCompCondition(internalEntry *key1, internalEntry *key2, const Attribute& attr, const CompOp op);
    };

    class LeafNode : public IXNode {
    public:

        int32_t nextPtr;

        // Initialize existing pages
        LeafNode(IXFileHandle &ixFileHandle, uint32_t page) : IXNode(ixFileHandle, page) {
            nextPtr = getNextPtrFromData();
        };

        // For new page
        LeafNode(IXFileHandle &ixFileHandle, uint32_t page,  int32_t next) : IXNode(ixFileHandle, page, IX::LEAF_NODE,
                                                                                    0, 0) {
            setNextPtr(next);
        };

        // Initialize new page with existing entries
        LeafNode(uint8_t *entryData, int16_t dataLen, IXFileHandle &ixFileHandle, uint32_t page, int32_t next,
                 int16_t entryCounter) : IXNode(entryData, dataLen, ixFileHandle, page, IX::LEAF_NODE, dataLen,
                                                entryCounter) {
            setNextPtr(next);
        };

        ~LeafNode() {
            setNextPtr(nextPtr);
        };

        // getter
        int32_t getNextPtr() const { return this->nextPtr; }

        int32_t getNextPtrFromData() const {
            int32_t ptr;
            memcpy(&ptr, data + PAGE_SIZE - IX::NODE_TYPE_LEN - IX::FREEBYTEPOINTER_LEN - IX::KEY_COUNTER_LEN -
                         IX::NEXT_POINTER_LEN,
                   IX::NEXT_POINTER_LEN);
            return ptr;
        }

        uint16_t getFreeSpace() const {
            return PAGE_SIZE - freeBytePointer - IX::NODE_TYPE_LEN - IX::FREEBYTEPOINTER_LEN - IX::KEY_COUNTER_LEN -
                   IX::NEXT_POINTER_LEN;
        }

        uint16_t getMaxFreeSpace() const {
            return PAGE_SIZE  - IX::NODE_TYPE_LEN - IX::FREEBYTEPOINTER_LEN - IX::KEY_COUNTER_LEN -
                   IX::NEXT_POINTER_LEN;
        }

        RC getEntry(int16_t pos, leafEntry* leaf);
        //setter
        void setNextPtr(int32_t ptr) {
            memcpy(data + PAGE_SIZE - IX::NODE_TYPE_LEN - IX::FREEBYTEPOINTER_LEN - IX::KEY_COUNTER_LEN -
                   IX::NEXT_POINTER_LEN,
                   &ptr, IX::NEXT_POINTER_LEN);
            this->nextPtr = ptr;
        }

        RC writeEntry(leafEntry *key, const Attribute &attribute, int16_t pos);

        RC insertEntry(leafEntry *key, const Attribute &attribute);

        internalEntry* splitNode(leafEntry *key, const Attribute &attr);

        static bool isKeyMeetCompCondition(leafEntry *key1, leafEntry *key2, const Attribute& attr, const CompOp op);

        RC findFirstKeyMeetCompCondition(int16_t& pos, const uint8_t* key, const Attribute& attr, CompOp op);

        bool isEmpty(){ return keyCounter == 0;}
    };
}// namespace PeterDB
#endif // _ix_h_
