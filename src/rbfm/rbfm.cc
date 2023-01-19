#include "src/include/rbfm.h"

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
        // 1. check file state
        if (!fileHandle.isFileOpen()) return -1;
        // 2. convert raw data to byte sequence
        RecordHandle rh = RecordHandle();
        char pageBuffer[PAGE_SIZE] = {};
        short recByteLen = 0;
        char buffer[recByteLen];

        rh.rawDataToRecordByte((char*) data, recordDescriptor, pageBuffer, recByteLen);
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
        // traverse all pages reversely
        for (PageNum i = pageCount - 1; i >= 0; i--){
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

