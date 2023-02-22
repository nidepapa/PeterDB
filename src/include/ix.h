#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    namespace IX{
        const int32_t File_Header_Page_Size = 4096;
        const int32_t NULL_PTR = -1;

        const int32_t NODE_TYPE_LEN = 2;
        const int32_t FREEBYTEPOINTER_LEN = 2;
        const int32_t KEY_COUNTER_LEN = 2;
        const int32_t NEXT_POINTER_LEN = 4;
    }


    class IX_ScanIterator;

    class IXFileHandle;

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

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

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

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

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
}// namespace PeterDB
#endif // _ix_h_
