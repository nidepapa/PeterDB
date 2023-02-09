#include "src/include/pfm.h"
#include <cstdio>
#include <typeinfo>
#include <cstring>

using namespace std;

namespace PeterDB {

    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const string &fileName) {
        if (isFileExists(fileName)) return -1;

        FILE* fp = fopen(fileName.c_str(), "w+b");

        // init metadata of file
        FileHeader header = {0, 0, 0, 0};
        fwrite(&header, File_Header_Page_Size, 1, fp);
        fflush(fp);
        fclose(fp);
        return 0;
    }

    RC PagedFileManager::destroyFile(const string &fileName) {
        if (!isFileExists(fileName)) return -1;
        // remove file
        if (remove(fileName.c_str()) != 0) return -1;
        return 0;
    }

    RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
        if (!isFileExists(fileName)){
            return RC(FILE_ERROR::FILE_NOT_EXIST);
        }
        return fileHandle.openFile(fileName);
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return fileHandle.closeFile();
    }

    bool PagedFileManager::isFileExists(const std::string fileName){
        FILE* fp = fopen(fileName.c_str(), "r");
        if (!fp) return false;
        fclose(fp);
        return true;
    }

    FileHandle::FileHandle() {
        pageCounter = 0;
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::openFile(const std::string& fileName){
        RC code = 0;
        // open file as binary
        fileInMemory = fopen(fileName.c_str(), "r+b");
        fileIsOpen = true;
        FileHandle::fileName = fileName;
        code = readMetadata();
        if (code) goto err;
        return 0;
        err:
        fclose(fileInMemory);
        return -1;
    };

    RC FileHandle::closeFile(){
        if (!fileIsOpen) return -1;
        fileIsOpen = false;
        flushMetadata();
        fclose(fileInMemory);
        return 0;
    }

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
        flushMetadata();
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
        // write into file on disk
        fflush(fileInMemory);
        if (ferror(fileInMemory)){
            // todo error
            return -1;
        }
        // update counter
        writePageCounter = writePageCounter + 1;
        flushMetadata();
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(fileInMemory, File_Header_Page_Size + PAGE_SIZE * pageCounter, SEEK_SET);
        fwrite(data, PAGE_SIZE, 1, fileInMemory);
        appendPageCounter = appendPageCounter + 1;
        pageCounter = pageCounter + 1;
        flushMetadata();
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return pageCounter;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

    RC FileHandle::flushMetadata(){
        if (!fileIsOpen){
            return -1;
        }
        clearerr(PeterDB::FileHandle::fileInMemory);
        fseek(fileInMemory, 0, SEEK_SET);

        fwrite(&pageCounter, sizeof(unsigned),4, fileInMemory);
        fflush(fileInMemory);
        return 0;
    }

    RC FileHandle::readMetadata(){
        if (!fileIsOpen) return -1;
        fseek(fileInMemory, 0 ,SEEK_SET);
        clearerr(PeterDB::FileHandle::fileInMemory);
        fread(&pageCounter, sizeof(unsigned), 4, fileInMemory);
        return 0;
    }

    bool FileHandle::isFileOpen(){
        return this->fileIsOpen;
    }


} // namespace PeterDB

