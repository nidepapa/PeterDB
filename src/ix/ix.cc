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
        FILE *x = fopen(fileName.c_str(), "w+b");
        fflush(x);
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
        if (!isFileExists(ixFileHandle.getFileName())) return RC(IX_ERROR::FILE_NOT_EXIST);
        return ixFileHandle.close();
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        if (!ixFileHandle.isOpen()) return RC(IX_ERROR::FILE_NOT_OPEN);

        uint8_t data[PAGE_SIZE];
        auto entry = (leafEntry *) data;
        entry->setKey(attribute.type, (uint8_t *) key);
        entry->setRID(attribute.type, rid.pageNum, rid.slotNum);
        assert(entry->getKeyLength(attribute.type) == attribute.length);

        if (ixFileHandle.isRootNull()) {
            RC ret = ixFileHandle.appendEmptyPage();
            assert(ret == 0);
            uint32_t leafPage = ixFileHandle.getLastPageIndex();
            LeafNode LeafNode(ixFileHandle, leafPage, IX::NEXT_POINTER_NULL);
            ret = LeafNode.insertEntry(entry, attribute);
            assert(ret == 0);
            ixFileHandle.setRoot(leafPage);
            return SUCCESS;
        }

        internalEntry *newChildEntry = nullptr;
        RC ret = insertEntryRecur(ixFileHandle, ixFileHandle.getRoot(), entry, newChildEntry, attribute);
        assert(ret == 0);
        return SUCCESS;
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

    RC IndexManager::insertEntryRecur(IXFileHandle &ixFileHandle, int32_t nodePointer, leafEntry *entry,
                                      internalEntry *newChildEntry, const Attribute &attribute) {
        RC ret;
        IXNode ixnode(ixFileHandle, nodePointer);

        if (ixnode.getNodeType() == IX::INTERNAL_NODE) {
            // No-leaf NODE N
            InternalNode noLeaf(ixFileHandle, nodePointer);
            //find i such that Ki ≤ entry’s key value < Ki+1;
            int32_t subtree = IX::NULL_PTR;
            ret = noLeaf.getTargetChild(entry, attribute, subtree);
            if (ret) return RC(IX_ERROR::NOLEAF_GETTARGET_CHILD_FAIL);
            ret = insertEntryRecur(ixFileHandle, subtree, entry, newChildEntry,attribute);
            assert(ret == SUCCESS);
            if (newChildEntry == nullptr) return SUCCESS;
            else{
                // we split child, must insert *newchildentry in N
                if (noLeaf.getFreeSpace() > newChildEntry->getEntryLength(attribute.type)){
                    ret = noLeaf.insertEntry(newChildEntry, attribute);
                    if (ret) return RC(IX_ERROR::NOLEAF_INSERT_ENTRY_FAIL);
                    newChildEntry = nullptr;
                    return SUCCESS;
                }else{
                    // we split child
                    ret = noLeaf.splitPageAndInsertIndex(newChildEntry, attribute, newChildEntry);
                    if (ret) return RC(IX_ERROR::NOLEAF_SPLIT_FAIL);
                    if (noLeaf.isRoot()){
                        RC ret = ixFileHandle.appendEmptyPage();
                        assert(ret = SUCCESS);
                        auto newRootPage = ixFileHandle.getLastPageIndex();
                        InternalNode newRoot(ixFileHandle, newRootPage, noLeaf.getPageNum(),newChildEntry,attribute);
                        ixFileHandle.setRoot(newRootPage);
                    }
                    return SUCCESS;
                }
            }
        }
        else if (ixnode.getNodeType() == IX::LEAF_NODE) {
            // LEAF NODE L
            LeafNode leaf(ixFileHandle, nodePointer);
            // if L has space,
            if (leaf.getFreeSpace() > entry->getEntryLength(attribute.type)) {
                ret = leaf.insertEntry(entry, attribute);
                if (ret) return RC(IX_ERROR::LEAF_INSERT_ENTRY_FAIL);
                newChildEntry = nullptr;
                return SUCCESS;
            } else {
                // todo change to RC mode;
                // split L: first d entries stay, rest move to brand new node L2;
                newChildEntry = leaf.splitNode(entry, attribute);
                return SUCCESS;
            }

        }
        return SUCCESS;
    }

} // namespace PeterDB