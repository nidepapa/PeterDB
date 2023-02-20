#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

/*==============================================
 * IndexManager
 * =============================================
 */
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

/*==============================================
 * IX_ScanIterator
 * =============================================
 */
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


} // namespace PeterDB