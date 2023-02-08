#include "src/include/pfm.h"
#include <cstdio>
#include <typeinfo>
#include <cstring>
#include "src/include/errorCode.h"
#include <glog/logging.h>

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
        if (isFileExists(fileName)) return RC(FILE_ERROR::FILE_NOT_EXIST);

        FILE* fp = fopen(fileName.c_str(), "w+b");

        // init metadata of file
        FileHeader header = {0, 0, 0, 0};
        fwrite(&header, File_Header_Page_Size, 1, fp);
        fflush(fp);
        fclose(fp);
        return SUCCESS;
    }

    RC PagedFileManager::destroyFile(const string &fileName) {
        if (!isFileExists(fileName)) return RC(FILE_ERROR::FILE_NOT_EXIST);
        if (remove(fileName.c_str()) != 0) return RC(FILE_ERROR::FILE_REMOVE_FAIL);
        return SUCCESS;
    }

    RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
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
        RC rc = 0;
        // reopenfile is fine;
        // open file as binary
        fileInMemory = fopen(fileName.c_str(), "r+b");
        fileIsOpen = true;
        FileHandle::fileName = fileName;
        rc = readMetadata();
        if (rc) {
            LOG(ERROR) << "read meta data Err" << "@FileHandle::openFile()" << endl;
            if (fileInMemory) {
                fclose(fileInMemory);
            }
            return rc;
        }
        fclose(fileInMemory);
        return SUCCESS;
    };

    RC FileHandle::closeFile(){
        if (!fileIsOpen) return RC(FILE_ERROR::FILE_NOT_OPEN);
        RC rc = flushMetadata();
        if (rc){
            LOG(ERROR) << "flush metadata error" << "@FileHandle::closeFile()" << endl;
            return rc;
        }
        fileIsOpen = false;
        fclose(fileInMemory);
        return SUCCESS;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        size_t result;
        // check if pageNum is valid
        if (getNumberOfPages() <= pageNum) {
            return RC(FILE_ERROR::FILE_NO_ENOUGH_PAGE);
        }
        // point to that page
        fseek(fileInMemory, File_Header_Page_Size + PAGE_SIZE * pageNum, SEEK_SET);
        // retrieve data
        result = fread(data, PAGE_SIZE, 1, fileInMemory);
        if (result < 1) return RC(FILE_ERROR::FILE_READ_FAIL);
        // update counter
        readPageCounter = readPageCounter + 1;
        return flushMetadata();
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        // check if pageNum is valid
        if (getNumberOfPages() <= pageNum) {
            return RC(FILE_ERROR::FILE_NO_ENOUGH_PAGE);
        }
        // point to that page
        fseek(fileInMemory, File_Header_Page_Size + PAGE_SIZE * pageNum, SEEK_SET);
        // overwrite data into page
        fwrite(data, PAGE_SIZE, 1, fileInMemory);
        // write into file on disk
        fflush(fileInMemory);
        // update counter
        writePageCounter = writePageCounter + 1;
        return flushMetadata();
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(fileInMemory, 0 , SEEK_SET);
        int fileLen = ftell(fileInMemory);
        if (fileLen < File_Header_Page_Size){
            LOG(ERROR)<< "fileInMemory err"<<endl;
        }
        fseek(fileInMemory, 0 , SEEK_SET);
        fseek(fileInMemory, File_Header_Page_Size + PAGE_SIZE * pageCounter, SEEK_SET);
        fwrite(data, PAGE_SIZE, 1, fileInMemory);
        appendPageCounter = appendPageCounter + 1;
        pageCounter = pageCounter + 1;
        return flushMetadata();
    }

    unsigned FileHandle::getNumberOfPages() {
        return pageCounter;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return SUCCESS;
    }

    RC FileHandle::flushMetadata(){
        if (!fileIsOpen) return RC(FILE_ERROR::FILE_NOT_OPEN);
        if (fileInMemory == NULL) return RC(FILE_ERROR::FILE_NOT_OPEN);

        clearerr(PeterDB::FileHandle::fileInMemory);
        fseek(fileInMemory, 0, SEEK_SET);
        uint32_t counters[4];
        fwrite(&pageCounter, sizeof(uint32_t),4, fileInMemory);
        fflush(fileInMemory);
        return SUCCESS;
    }

    RC FileHandle::readMetadata(){
        if (!fileIsOpen) return RC(FILE_ERROR::FILE_NOT_OPEN);
        if (fileInMemory == NULL) return RC(FILE_ERROR::FILE_NOT_OPEN);
        fseek(fileInMemory, 0 ,SEEK_SET);
        clearerr(PeterDB::FileHandle::fileInMemory);
        fread(&pageCounter, sizeof(uint32_t), 4, fileInMemory);
        return SUCCESS;
    }

    bool FileHandle::isFileOpen(){
        return this->fileIsOpen;
    }


} // namespace PeterDB

