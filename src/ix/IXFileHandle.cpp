//
// Created by Fan Zhao on 2/19/23.
//
#include "src/include/ix.h"

namespace PeterDB{
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
        this->fileName = filename;
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

        fread(&ixReadPageCounter, sizeof(unsigned), 1, fileInMemory);
        fread(&ixWritePageCounter, sizeof(unsigned), 1, fileInMemory);
        fread(&ixAppendPageCounter, sizeof(unsigned), 1, fileInMemory);
        fread(&rootPagePtr, sizeof(int32_t), 1, fileInMemory);
        fread(&KeyType, sizeof(int32_t), 1, fileInMemory);

        return SUCCESS;
    }

    RC IXFileHandle::flushMetaData() {
        clearerr(PeterDB::IXFileHandle::fileInMemory);
        fseek(fileInMemory, 0, SEEK_SET);

        fwrite(&ixReadPageCounter, sizeof(unsigned), 1, fileInMemory);
        fwrite(&ixWritePageCounter, sizeof(unsigned), 1, fileInMemory);
        fwrite(&ixAppendPageCounter, sizeof(unsigned), 1, fileInMemory);
        fwrite(&rootPagePtr, sizeof(int32_t), 1, fileInMemory);
        fwrite(&KeyType, sizeof(int32_t), 1, fileInMemory);

        fflush(fileInMemory);
        return SUCCESS;
    }

    RC IXFileHandle::readPage(uint32_t pageNum, void *data) {
        if (!isOpen()) return RC(IX_ERROR::FILE_NOT_OPEN);
        if (getNumberOfPages() <= pageNum) {
            return RC(IX_ERROR::FILE_NO_ENOUGH_PAGE);
        }
        fseek(fileInMemory, IX::File_Header_Page_Size + PAGE_SIZE * pageNum, SEEK_SET);
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
        fseek(fileInMemory, IX::File_Header_Page_Size + PAGE_SIZE * pageNum, SEEK_SET);
        fwrite(data, PAGE_SIZE, 1, fileInMemory);
        fflush(fileInMemory);
        ixWritePageCounter = ixWritePageCounter + 1;
        return flushMetaData();
    }

    RC IXFileHandle::appendPage(const void *data) {
        if (!isOpen()) return RC(IX_ERROR::FILE_NOT_OPEN);
        fseek(fileInMemory, IX::File_Header_Page_Size + PAGE_SIZE * getNumberOfPages(), SEEK_SET);
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
        uint8_t emptyPage[IX::File_Header_Page_Size];
        memset(emptyPage, 0, IX::File_Header_Page_Size);
        fseek(fileInMemory, 0, SEEK_SET);
        fwrite(emptyPage, IX::File_Header_Page_Size, 1, fileInMemory);
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
        if (rootPagePtr == IX::NULL_PTR)return true;
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
}