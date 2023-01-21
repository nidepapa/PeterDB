#include <cmath>
#include "src/include/rbfm.h"
#include <iostream>
#include <sstream>
#include <cstring>

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return PagedFileManager::instance().createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return PagedFileManager::instance().destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return PagedFileManager::instance().openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return PagedFileManager::instance().closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        RC rc = 0;
        // 1. check file state
        if (!fileHandle.isFileOpen()) return -1;
        // 2. convert raw data to byte sequence
        RecordHandle rh = RecordHandle();
        char pageBuffer[PAGE_SIZE] = {};
        short recByteLen = 0;


        rc = rh.rawDataToRecordByte((char*) data, recordDescriptor, pageBuffer, recByteLen);
        if(rc) {
            std::cout << "Fail to Convert Record to Byte Seq @ RecordBasedFileManager::insertRecord" << std::endl;
            return rc;
        }

        //rh.printNullAttr(pageBuffer, recordDescriptor);

        char buffer[recByteLen];
        memcpy(buffer, pageBuffer, recByteLen);

        // 3. find a page
        PageNum availablePageNum;
        getAvailablePage(fileHandle, recByteLen, availablePageNum);
        PageHandle thisPage(fileHandle, availablePageNum);

        // 4. insert binary data
        thisPage.insertRecordInByte(buffer, recByteLen, rid);

        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        // 1. check file state and page validity
        if (!fileHandle.isFileOpen() || rid.pageNum > fileHandle.getNumberOfPages() - 1) return -1;

        // 2. get record in byte
        PageHandle thisPage(fileHandle, rid.pageNum);
        char buffer[PAGE_SIZE] = {};
        short recByteLen = 0;
        thisPage.getRecordInByte(rid.slotNum, buffer, recByteLen);

        // 3. convert binary to raw data
        RecordHandle rh;
        rh.recordByteToRawData(buffer, recByteLen, recordDescriptor, (char *)data);

        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        int dataPos = ceil(recordDescriptor.size() / 8.0);
        RecordHandle rh;
        const std::string separator = " ";
        char buffer[PAGE_SIZE];

        for(short i = 0; i < recordDescriptor.size(); i++) {
            // start
            out << recordDescriptor[i].name << ":" << separator;
            // value
            if(rh.isNullAttr((char*)data, i)) {
                out << "NULL";
            }else{
                switch (recordDescriptor[i].type) {
                    case TypeInt:
                        int intVal;
                        memcpy(&intVal, (char *)data + dataPos, sizeof(int));
                        dataPos += sizeof(int);
                        out << intVal;
                        break;
                    case TypeReal:
                        float floatVal;
                        memcpy(&floatVal, (char *)data + dataPos, sizeof(float));
                        dataPos += sizeof(float);
                        out << floatVal;
                        break;
                    case TypeVarChar:
                        unsigned strLen;
                        // Get String Len
                        memcpy(&strLen, (char *)data + dataPos, sizeof(int));
                        dataPos += sizeof(int);
                        memcpy(buffer, (char *)data + dataPos, strLen);
                        buffer[strLen] = '\0';
                        dataPos += strLen;
                        out << buffer;
                        break;
                    default:
                        std::cout << "Invalid DataType Error" << std::endl;
                        break;
                }
            }
            // ending
            if(i != recordDescriptor.size() - 1) {
                out << ',' << separator;
            }
            else {
                out << std::endl;
            }
        }
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

    RC RecordBasedFileManager::getAvailablePage(FileHandle& fileHandle, short recLength, PageNum& availablePageNum){
        unsigned pageCount = fileHandle.getNumberOfPages();
        char data[PAGE_SIZE] = {};
        // no pages add a new page
        if (pageCount == 0){
            fileHandle.appendPage(data);
            availablePageNum = fileHandle.getNumberOfPages() - 1;
            return 0;
        }

        // check if last page is available
        PageNum lastPageNum = fileHandle.getNumberOfPages() - 1;
        PageHandle lastPage(fileHandle, lastPageNum);
        if (lastPage.IsFreeSpaceEnough(recLength)){
            availablePageNum = lastPageNum;
            return 0;
        }

        // traverse all pages from beginning
        for (PageNum i = 0 ; i <= lastPageNum; i++){
            PageHandle ithPage(fileHandle, i);
            if (ithPage.IsFreeSpaceEnough(recLength)){
                availablePageNum = i;
                return 0;
            }
        }
        // no available page, append a new page
        fileHandle.appendPage(data);
        availablePageNum = fileHandle.getNumberOfPages() - 1;
        return 0;
    }

} // namespace PeterDB

