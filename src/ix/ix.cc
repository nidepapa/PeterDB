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
        if (!ixFileHandle.isOpen()) {
            auto name = ixFileHandle.getFileName();
            if (name == "") return RC(IX_ERROR::FILE_NOT_EXIST);
            ixFileHandle.open(name);
        }

        uint8_t data[PAGE_SIZE];
        memset(data, 0, PAGE_SIZE);
        genCompositeEntry(attribute, key, rid, data);
        auto entry = (leafEntry*)data;

        if (ixFileHandle.isRootNull()) {
            RC ret = ixFileHandle.createRootPage();
            assert(ret == 0);
            uint32_t rootPage = ixFileHandle.getRoot();
            LeafNode LeafNode(ixFileHandle, rootPage, IX::NULL_PTR);

            ret = LeafNode.insertEntry(entry, attribute);
            assert(ret == 0);
            return SUCCESS;
        }

        uint8_t buffer[PAGE_SIZE];
        auto newChildEntry = (internalEntry*)buffer;
        bool isNewChildExist = false;
        RC ret = insertEntryRecur(ixFileHandle, ixFileHandle.getRoot(), entry, newChildEntry, isNewChildExist, attribute);
        if(ret) return ret;
        return SUCCESS;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        RC ret = 0;

        if (ixFileHandle.isRootNull()) return RC(IX_ERROR::ROOT_NOT_EXIST);
        // make the composite entry
        uint8_t entryToDel[PAGE_SIZE];
        memset(entryToDel, 0 , PAGE_SIZE);
        genCompositeEntry(attribute, key, rid, entryToDel);

        int32_t leafPage;
        ret = findTargetLeafNode(ixFileHandle, leafPage, entryToDel, attribute);

        if (ret) return ret;
        LeafNode leaf(ixFileHandle, leafPage);
        ret = leaf.deleteEntry((leafEntry *)entryToDel, attribute);
        if (ret) return ret;
        return SUCCESS;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        RC ret = 0;
        ret = ix_ScanIterator.open(&ixFileHandle, attribute, (uint8_t *) lowKey, (uint8_t *) highKey, lowKeyInclusive,
                                   highKeyInclusive);
        if (ret) return ret;
        return SUCCESS;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        RC ret = 0;
        if (ixFileHandle.isRootNull()) return RC(IX_ERROR::ROOT_NOT_EXIST);
        if (!ixFileHandle.isOpen()){
            ixFileHandle.open(ixFileHandle.getFileName());
        }
        int16_t nodeType;
        {
            IXNode node(ixFileHandle, ixFileHandle.getRoot());
            nodeType = node.getNodeType();
        }

        if (nodeType == IX::INTERNAL_NODE) {
            InternalNode internal(ixFileHandle, ixFileHandle.getRoot());
            internal.print(attribute, out);
        } else if (nodeType == IX::LEAF_NODE) {
            LeafNode leaf(ixFileHandle, ixFileHandle.getRoot());
            leaf.print(attribute, out);
        } else {
            return RC(IX_ERROR::PAGE_TYPE_UNKNOWN);
        }
        return 0;
    }

    bool IndexManager::isFileExists(const std::string fileName) {
        FILE *fp = fopen(fileName.c_str(), "r");
        if (!fp) return false;
        fclose(fp);
        return true;
    }

    RC IndexManager::insertEntryRecur(IXFileHandle &ixFileHandle, int32_t nodePointer, leafEntry *entry,
                                      internalEntry *newChildEntry, bool &isNewChildExist, const Attribute &attribute) {
        RC ret;
        IXNode ixnode(ixFileHandle, nodePointer);

        if (ixnode.getNodeType() == IX::INTERNAL_NODE) {
            // No-leaf NODE N
            InternalNode noLeaf(ixFileHandle, nodePointer);
            //find i such that Ki ≤ entry’s key value < Ki+1;
            int32_t subtree = IX::NULL_PTR;
            ret = noLeaf.getTargetChild(entry, attribute, subtree);
            if (ret) return RC(IX_ERROR::NOLEAF_GETTARGET_CHILD_FAIL);
            ret = insertEntryRecur(ixFileHandle, subtree, entry, newChildEntry,isNewChildExist, attribute);
            if(ret) return ret;
            if (!isNewChildExist) return SUCCESS;
            else {
                // Insert <returned middle composite key, new child page pointer> into current index page
                int16_t entryLen = newChildEntry->getEntryLength(attribute.type);
                uint8_t tmpEntry[entryLen];
                memset(tmpEntry, 0, entryLen);
                memcpy(tmpEntry, newChildEntry, entryLen);
                ret = noLeaf.splitOrInsertNode((internalEntry*)tmpEntry, attribute, newChildEntry, isNewChildExist);
                if(ret) return ret;
            }
        } else if (ixnode.getNodeType() == IX::LEAF_NODE) {
            // LEAF NODE L
            LeafNode leaf(ixFileHandle, nodePointer);
            ret = leaf.insertOrSplitEntry(entry, attribute, newChildEntry, isNewChildExist);
            if (ret) return ret;
            // Corner Case: Leaf node needs to split and there isn't any no-leaf page yet
            if (isNewChildExist && leaf.getPageNum() == ixFileHandle.getRoot()) {
                // Insert new no-leaf page
                ret = ixFileHandle.appendEmptyPage();
                if(ret) return ret;
                uint32_t newInternalPageNum = ixFileHandle.getLastPageIndex();

                InternalNode newInternal(ixFileHandle, newInternalPageNum,
                                         nodePointer, newChildEntry, attribute);
                ixFileHandle.setRoot(newInternalPageNum);
                isNewChildExist = false;
            }
        }
        return SUCCESS;
    }

    RC IndexManager::findTargetLeafNode(IXFileHandle &ixFileHandle, int32_t &targetLeaf, const uint8_t *key,
                                        const Attribute &attr) {
        RC ret = 0;
        if (ixFileHandle.isRootNull()) return RC(IX_ERROR::ROOT_NOT_EXIST);

        int32_t curPageNum = ixFileHandle.getRoot();
        while (curPageNum != IX::NULL_PTR && curPageNum < ixFileHandle.getNumberOfPages()) {
            IXNode node(ixFileHandle, curPageNum);
            if (node.getNodeType() == IX::LEAF_NODE) {
                //*nodepointer is a leaf, return nodepointer
                break;
            }else if(node.getNodeType() == IX::INTERNAL_NODE){
                InternalNode internal(ixFileHandle, curPageNum);
                ret = internal.getTargetChild((leafEntry *) key, attr, curPageNum);
                if (ret) return ret;
            }else{
                LOG(ERROR) << "Node Type Invalid! @ IndexManager::findTargetLeafNode" << std::endl;
                return RC(IX_ERROR::NODE_TYPE_INVALID);
            }
        }
        if (curPageNum != IX::NULL_PTR && curPageNum < ixFileHandle.getNumberOfPages()) {
            targetLeaf = curPageNum;
        } else { return RC(IX_ERROR::LEAF_FOUND_FAIL); }

        return SUCCESS;
    }

    RC IndexManager::genCompositeEntry(const Attribute &attribute,const void *key, const RID &rid, uint8_t *entry ){
        ((leafEntry *) entry)->setKey(attribute.type, (uint8_t *) key);
        ((leafEntry *) entry)->setRID(attribute.type, rid.pageNum, rid.slotNum);
    }
} // namespace PeterDB