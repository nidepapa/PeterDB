#include <cmath>
#include "src/include/rbfm.h"
#include <iostream>
#include <sstream>
#include <cstring>
#include "src/include/errorCode.h"
#include <glog/logging.h>

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
        if (!fileHandle.isFileOpen()) return ERR_RBFILE_NOT_OPEN;
        // 2. convert raw data to byte sequence
        uint8_t pageBuffer[PAGE_SIZE] = {};
        short recByteLen = 0;

        rc = RecordHelper::rawDataToRecordByte((uint8_t*) data, recordDescriptor, pageBuffer, recByteLen);
        if(rc) {
            std::cout << "Fail to Convert Record to Byte Seq @ RecordBasedFileManager::insertRecord" << std::endl;
            return rc;
        }

        uint8_t buffer[recByteLen];
        memset(buffer, 0, recByteLen);
        memcpy(buffer, pageBuffer, recByteLen);

        // 3. find a page
        PageNum availablePageNum;
        getAvailablePage(fileHandle, recByteLen, availablePageNum);
        PageHelper thisPage(fileHandle, availablePageNum);

        // 4. insert binary data
        thisPage.insertRecordInByte(buffer, recByteLen, rid, false);
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        // 1. check file state and page validity
        if (!fileHandle.isFileOpen()) return ERR_RBFILE_NOT_OPEN;
        if ( rid.pageNum > fileHandle.getNumberOfPages() - 1) return ERR_RBFILE_PAGE_EXCEEDED;

        // 2. get real data RID
        uint32_t curPageID = rid.pageNum;
        uint16_t curSlotID = rid.slotNum;
        while (curPageID < fileHandle.getNumberOfPages()){
            PageHelper thisPage(fileHandle, curPageID);
            if (!thisPage.isRecordValid(curSlotID)){
                LOG(ERROR) << "Record is invalid! @ RecordBasedFileManager::readRecord" << std::endl;
                return ERR_RBFILE_SLOT_INVALID;
            }
            if (thisPage.isRecordData(curSlotID)) break;
            thisPage.getRecordPointer(curSlotID,curPageID,curSlotID);
            if (curPageID >= fileHandle.getNumberOfPages()){
                LOG(ERROR) << "Target Page not exist! @ RecordBasedFileManager::readRecord" << std::endl;
                return ERR_RBFILE_PAGE_EXCEEDED;
            }
        }

        // 3. get real data record in byte
        PageHelper thisPage(fileHandle, curPageID);

        uint8_t buffer[PAGE_SIZE] = {};
        short recByteLen = 0;
        thisPage.getRecordByte(curSlotID, buffer, recByteLen);

        // 4. convert binary to raw data
        RecordHelper::recordByteToRawData(buffer, recByteLen, recordDescriptor, (uint8_t *)data);

        return SUCCESS;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        // 1. check file state and page validity
        if (!fileHandle.isFileOpen()) return ERR_RBFILE_NOT_OPEN;
        if ( rid.pageNum > fileHandle.getNumberOfPages() - 1) return ERR_RBFILE_PAGE_EXCEEDED;

        // 2. get real data RID
        uint32_t curPageID = rid.pageNum;
        uint16_t curSlotID = rid.slotNum;
        while (curPageID < fileHandle.getNumberOfPages()){
            PageHelper thisPage(fileHandle, curPageID);
            if (!thisPage.isRecordValid(curSlotID)){
                LOG(ERROR) << "Record is invalid! @ RecordBasedFileManager::readRecord" << std::endl;
                return ERR_RBFILE_SLOT_INVALID;
            }
            if (thisPage.isRecordData(curSlotID)) break;

            uint16_t oldSlotID = curSlotID;
            thisPage.getRecordPointer(curSlotID,curPageID,curSlotID);
            // delete all pointer on the way
            thisPage.deleteRecord(oldSlotID);
            if (curPageID >= fileHandle.getNumberOfPages()){
                LOG(ERROR) << "Target Page not exist! @ RecordBasedFileManager::deleteRecord" << std::endl;
                return ERR_RBFILE_PAGE_EXCEEDED;
            }

        }
        // 2. get real data record
        PageHelper thisPage(fileHandle, curPageID);
        thisPage.deleteRecord(curSlotID);
        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        int dataPos = ceil(recordDescriptor.size() / 8.0);
        RecordHelper rh;
        const std::string separator = " ";
        char buffer[PAGE_SIZE];

        for(short i = 0; i < recordDescriptor.size(); i++) {
            // start
            out << recordDescriptor[i].name << ":" << separator;
            // value
            if(rh.isNullAttr((uint8_t*)data, i)) {
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


        // 1. check file state and page validity
        if (!fileHandle.isFileOpen()) return ERR_RBFILE_NOT_OPEN;
        if ( rid.pageNum > fileHandle.getNumberOfPages() - 1) return ERR_RBFILE_PAGE_EXCEEDED;
        // 2. get real data RID
        uint32_t curPageID = rid.pageNum;
        uint16_t curSlotID = rid.slotNum;
        bool setUnoriginal = false;
        while (curPageID < fileHandle.getNumberOfPages()){
            PageHelper thisPage(fileHandle, curPageID);
            if (!thisPage.isRecordValid(curSlotID)){
                LOG(ERROR) << "Record is invalid! @ RecordBasedFileManager::readRecord" << std::endl;
                return ERR_RBFILE_SLOT_INVALID;
            }
            if (thisPage.isRecordData(curSlotID)) break;
            // data get pointed to is not original record
            setUnoriginal = true;
            thisPage.getRecordPointer(curSlotID,curPageID,curSlotID);
            if (curPageID >= fileHandle.getNumberOfPages()){
                LOG(ERROR) << "Target Page not exist! @ RecordBasedFileManager::readRecord" << std::endl;
                return ERR_RBFILE_PAGE_EXCEEDED;
            }
        }

        // 3. convert raw data to byte sequence
        uint8_t pageBuffer[PAGE_SIZE] = {};
        short recByteLen = 0;

        RC rc = RecordHelper::rawDataToRecordByte((uint8_t*) data, recordDescriptor, pageBuffer, recByteLen);
        if(rc) {
            std::cout << "Fail to Convert Record to Byte Seq @ RecordBasedFileManager::updateRecord" << std::endl;
            return rc;
        }

        uint8_t buffer[recByteLen];
        memset(buffer, 0, recByteLen);
        memcpy(buffer, pageBuffer, recByteLen);

        // find new space for new record
        PageNum newPage;
        PageHelper thisPage(fileHandle, curPageID);
        int16_t oldRecLen = thisPage.getRecordLen(curSlotID);

        if (oldRecLen >= recByteLen || (oldRecLen < recByteLen &&  thisPage.IsFreeSpaceEnough(recByteLen - oldRecLen))){
            // store in current page
            thisPage.updateRecord(curSlotID, buffer, recByteLen, setUnoriginal);
        }else{
            // store in some other available page
            RID newRecordRID;
            getAvailablePage(fileHandle, recByteLen, newPage);
            PageHelper nextPage(fileHandle, newPage);
            // this data must be pointed to , so set it to unoriginal
            nextPage.insertRecordInByte(buffer, recByteLen, newRecordRID, true);

            thisPage.setRecordPointToNewRID(curSlotID, newRecordRID, setUnoriginal);
        }
        return SUCCESS;
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
        uint8_t data[PAGE_SIZE] = {};
        // we have pages
        if (pageCount > 0){
            // check if last page is available
            PageNum lastPageNum = pageCount - 1;
            PageHelper lastPage(fileHandle, lastPageNum);
            if (lastPage.IsFreeSpaceEnough(recLength)){
                availablePageNum = lastPageNum;
                return 0;
            }

            // traverse all pages from beginning
            for (PageNum i = 0 ; i < lastPageNum; i++){
                PageHelper ithPage(fileHandle, i);
                if (ithPage.IsFreeSpaceEnough(recLength)){
                    availablePageNum = i;
                    return 0;
                }
            }
        }

        // no available page, append a new page
        fileHandle.appendPage(data);
        availablePageNum = fileHandle.getNumberOfPages() - 1;
        return 0;
    }

} // namespace PeterDB

