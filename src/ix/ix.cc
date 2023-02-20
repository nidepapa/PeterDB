#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        if (isFileExists(fileName)) return RC(IX_ERROR::FILE_EXIST);
        fopen(fileName.c_str(), "w+b");

        // init metadata of file
        IXFileHandle ixFileHandle;
        ixFileHandle.open(fileName);
        ixFileHandle.initHiddenPage();
        return SUCCESS;
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        if (!isFileExists(fileName)) return RC(IX_ERROR::FILE_NOT_EXIST);
        if (remove(fileName.c_str()) != 0) return RC(IX_ERROR::FILE_DELETE_FAIL);
        return SUCCESS;
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        if (!isFileExists(fileName)) return RC(IX_ERROR::FILE_NOT_EXIST);
        return ixFileHandle.open(fileName);
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        return ixFileHandle.close();
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
    }

    bool IndexManager::isFileExists(const std::string fileName) {
        FILE *fp = fopen(fileName.c_str(), "r");
        if (!fp) return false;
        fclose(fp);
        return true;
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        rootPagePtr = NULL_PTR;
        fileInMemory = nullptr;
    }

    IXFileHandle::~IXFileHandle() {
        flushMetaData();
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return SUCCESS;
    }

    RC IXFileHandle::open(const std::string &filename) {
        if (isOpen()) return static_cast<RC>(IX_ERROR::ERR_FILE_ALREADY_OPEN);
        // open file as binary
        fileInMemory = fopen(fileName.c_str(), "r+b");
        if (!isOpen())return static_cast<RC>(IX_ERROR::ERR_FILE_OPEN_FAIL);
        this->fileName = fileName;
        return readMetaData();
    }

    RC IXFileHandle::close() {
        if (!isOpen()) return RC(IX_ERROR::FILE_NOT_OPEN);
        flushMetaData();
        fclose(fileInMemory);
        return SUCCESS;
    }

    bool IXFileHandle::isOpen() {
        if (this->fileInMemory == NULL) return false;
        return true;
    }

    RC IXFileHandle::readMetaData() {
        fseek(fileInMemory, 0, SEEK_SET);
        clearerr(PeterDB::IXFileHandle::fileInMemory);
        fread(&ixReadPageCounter, sizeof(unsigned), 4, fileInMemory);
        return SUCCESS;
    }

    RC IXFileHandle::flushMetaData() {
        clearerr(PeterDB::IXFileHandle::fileInMemory);
        fseek(fileInMemory, 0, SEEK_SET);
        fwrite(&ixReadPageCounter, sizeof(unsigned), 4, fileInMemory);
        fflush(fileInMemory);
        return SUCCESS;
    }

    RC IXFileHandle::readPage(uint32_t pageNum, void *data) {
        if (!isOpen()) return RC(IX_ERROR::FILE_NOT_OPEN);
        if (getNumberOfPages() <= pageNum) {
            return RC(IX_ERROR::FILE_NO_ENOUGH_PAGE);
        }
        fseek(fileInMemory, IXFile_Header_Page_Size + PAGE_SIZE * pageNum, SEEK_SET);
        size_t result = fread(data, PAGE_SIZE, 1, fileInMemory);
        assert(result == 1);
        ixReadPageCounter = ixReadPageCounter + 1;
        return flushMetaData();
    }

    RC IXFileHandle::writePage(uint32_t pageNum, const void *data) {
        if (!isOpen()) return RC(IX_ERROR::FILE_NOT_OPEN);
        if (getNumberOfPages() <= pageNum) {
            return RC(IX_ERROR::FILE_NO_ENOUGH_PAGE);
        }
        fseek(fileInMemory, IXFile_Header_Page_Size + PAGE_SIZE * pageNum, SEEK_SET);
        fwrite(data, PAGE_SIZE, 1, fileInMemory);
        fflush(fileInMemory);
        ixWritePageCounter = ixWritePageCounter + 1;
        return flushMetaData();
    }

    RC IXFileHandle::appendPage(const void *data) {
        if (!isOpen()) return RC(IX_ERROR::FILE_NOT_OPEN);
        fseek(fileInMemory, IXFile_Header_Page_Size + PAGE_SIZE * getNumberOfPages(), SEEK_SET);
        fwrite(data, PAGE_SIZE, 1, fileInMemory);
        ixAppendPageCounter = ixAppendPageCounter + 1;
        return flushMetaData();
    }

    RC IXFileHandle::appendEmptyPage() {
        if (!isOpen()) return RC(IX_ERROR::FILE_NOT_OPEN);
        uint8_t emptyPage[PAGE_SIZE];
        memset(emptyPage, 0, PAGE_SIZE);
        return appendPage(emptyPage);
    }

    RC IXFileHandle::initHiddenPage() {
        if (!isOpen()) return RC(IX_ERROR::FILE_NOT_OPEN);
        uint8_t emptyPage[IXFile_Header_Page_Size];
        memset(emptyPage, 0, IXFile_Header_Page_Size);
        fseek(fileInMemory, 0, SEEK_SET);
        fwrite(emptyPage, IXFile_Header_Page_Size, 1, fileInMemory);
        return flushMetaData();
    }

    RC IXFileHandle::createRootPage() {
        if (!isOpen()) return RC(IX_ERROR::FILE_NOT_OPEN);
        assert(appendEmptyPage() == 0);
        rootPagePtr = getLastPageIndex();
        flushMetaData();
        return SUCCESS;
    }

    uint32_t IXFileHandle::getRoot() {
        assert(isRootNull() == false);
        return rootPagePtr;
    }

    RC IXFileHandle::setRoot(int32_t newRoot) {
        rootPagePtr = newRoot;
        if (!isOpen()) return RC(IX_ERROR::FILE_NOT_OPEN);
        clearerr(PeterDB::IXFileHandle::fileInMemory);
        return flushMetaData();
    }

    bool IXFileHandle::isRootNull() const {
        if (rootPagePtr == NULL_PTR)return true;
        return false;
    }

    uint32_t IXFileHandle::getNumberOfPages() const {
        return ixAppendPageCounter;
    }

    uint32_t IXFileHandle::getLastPageIndex() const {
        return ixAppendPageCounter - 1;
    }

    std::string IXFileHandle::getFileName() const {
        return fileName;
    }
} // namespace PeterDB