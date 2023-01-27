#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096

#include <string>
#include <vector>

namespace PeterDB {

    typedef unsigned PageNum;
    typedef int RC;
    // metadata of one file
    struct FileHeader{
        unsigned pageCounter;
        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;
    };

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fileName);                        // Destroy a file
        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file
        bool isFileExists(const std::string fileName);
    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    };

    class FileHandle {
        friend class PagedFileManager;
    public:

        FileHandle();                                                       // Default constructor
        ~FileHandle();                                                      // Destructor

        RC openFile(const std::string& fileName);                        // open a file
        RC closeFile();                                                     // close a file

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount);                 // Put current counter values into variables
        bool isFileOpen();
    private:
        uint32_t pageCounter;
        uint32_t readPageCounter;
        uint32_t writePageCounter;
        uint32_t appendPageCounter;

        FILE *fileInMemory;                                                 // in memory file
        std::string fileName;
        bool fileIsOpen;

        RC flushMetadata();
        RC readMetadata();
    };

    const int File_Header_Page_Size = PAGE_SIZE;
} // namespace PeterDB

#endif // _pfm_h_