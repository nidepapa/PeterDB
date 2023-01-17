#include "src/include/pfm.h"
#include <cstdio>

using namespace std;

namespace PeterDB {

    FILE *fileInMemory = NULL;


    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const string &fileName) {
        FILE *fp;
        size_t result;
        fp = fopen(fileName.c_str(), "w+b");
        if (fp == NULL) return -1;

        // init metadata of file
        FileHeader header = {0, 0, 0, 0};
        result = fwrite(&header, sizeof(FileHeader), 1, fp);
        if (result < 1) {
            fclose(fp);
            return -1;
        }

        fclose(fp);
        return 0;
    }

    RC PagedFileManager::destroyFile(const string &fileName) {
        // remove file
        if (remove(fileName.c_str()) != 0) return -1;
        return 0;
    }

    RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
        size_t result;
        // open file as binary
        fileInMemory = fopen(fileName.c_str(), "r+b");
        if (fileInMemory == NULL) goto err;
        // read file header
        FileHeader fh;
        result = fread(&fh, sizeof(FileHeader), 1, fileInMemory);
        if (result != sizeof(FileHeader)) goto err;

        fileHandle.readPageCounter = fh.readPageCounter;
        fileHandle.writePageCounter = fh.writePageCounter;
        fileHandle.appendPageCounter = fh.appendPageCounter;

        return 0;
        err:
        fclose(fileInMemory);
        return -1;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if (!fileHandle.fileIsOpen){
            return -1;
        }
        // file handle, flush metadata to file
        FileHeader hdr;
        fileHandle.collectCounterValues(hdr.readPageCounter, hdr.writePageCounter, hdr.appendPageCounter);
        fseek(fileInMemory, 0, SEEK_SET);
        fwrite(&hdr, File_Header_Page_Size, 1, fileInMemory);

        fclose(fileInMemory);
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        size_t result;
        // check if pageNum is valid
        if (getNumberOfPages() <= pageNum) {
            return -1;
        }
        // point to that page
        fseek(fileInMemory, File_Header_Page_Size + PAGE_SIZE * pageNum, SEEK_SET);
        // retrieve data
        result = fread(data, PAGE_SIZE, 1, fileInMemory);
        if (result < 1) return -1;
        // update counter
        readPageCounter = readPageCounter + 1;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        // check if pageNum is valid
        if (getNumberOfPages() <= pageNum) {
            return -1;
        }
        // point to that page
        fseek(fileInMemory, File_Header_Page_Size + PAGE_SIZE * pageNum, SEEK_SET);
        // overwrite data into page
        fwrite(data, PAGE_SIZE, 1, fileInMemory);
        // update counter
        writePageCounter = writePageCounter + 1;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(fileInMemory, File_Header_Page_Size + PAGE_SIZE * hdr.pageNum, SEEK_SET);
        fwrite(data, PAGE_SIZE, 1, fileInMemory);
        appendPageCounter = appendPageCounter + 1;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return this->hdr.pageNum;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }


} // namespace PeterDB

