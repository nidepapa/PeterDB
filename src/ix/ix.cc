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
        IXNode ixnode(ixFileHandle, nodePointer);

        if (ixnode.getNodeType() == IX::INTERNAL_NODE) {
            //todo
        }
        else if (ixnode.getNodeType() == IX::LEAF_NODE) {
            // LEAF NODE L
            LeafNode leaf(ixFileHandle, nodePointer);
            // if L has space,
            if (leaf.getFreeSpace() > entry->getEntryLength(attribute.type)) {
                leaf.insertEntry(entry, attribute);
                newChildEntry = nullptr;
                return SUCCESS;
            } else {
                // split L: first d entries stay, rest move to brand new node L2;
                newChildEntry = leaf.splitNode(entry, attribute);
                return SUCCESS;
            }

        }
        return SUCCESS;
    }

    RC LeafNode::writeEntry(leafEntry *key, const Attribute &attribute, int16_t pos) {
        memcpy(data + pos, (uint8_t *) key, key->getEntryLength(attribute.type));
        return SUCCESS;
    }

    RC LeafNode::insertEntry(leafEntry *key, const Attribute &attribute) {
        if (getFreeSpace() < key->getEntryLength(attribute.type))return RC(IX_ERROR::PAGE_NO_ENOUGH_SPACE);

        int16_t pos = 0;
        // if not empty, find the right position
        if (getkeyCounter() != 0) {
            for (int i = 0; i < getkeyCounter(); i++) {
                auto curKey = (leafEntry *) (data + pos);
                if (isKeyMeetCompCondition(curKey, key, attribute, LT_OP))break;
                pos += curKey->getEntryLength(attribute.type);
            }
        }

        if (pos > getFreeBytePointer())return RC(IX_ERROR::FREEBYTE_EXCEEDED);
        RC ret = shiftDataRight(pos, key->getEntryLength(attribute.type));
        assert(ret == SUCCESS);

        writeEntry(key, attribute, pos);
        setFreeBytePointer(getFreeBytePointer() + key->getEntryLength(attribute.type));
        setkeyCounter(getkeyCounter() + 1);
        return SUCCESS;
    }

    bool LeafNode::isKeyMeetCompCondition(leafEntry *key1, leafEntry *key2, const Attribute &attr, const CompOp op) {
        switch (attr.type) {
            case TypeInt: {
                int int1 = key1->getKey<int>();
                int int2 = key2->getKey<int>();
                switch (op) {
                    case GT_OP:
                        return int1 > int2;
                    case GE_OP:
                        return int1 >= int2;
                    case LT_OP:
                        return int1 < int2;
                    case LE_OP:
                        return int1 <= int2;
                    case NE_OP:
                        return int1 != int2;
                    case EQ_OP:
                        return int1 == int2;
                    default:
                        return false;
                }
                break;
            }
            case TypeReal: {
                auto float1 = key1->getKey<float>();
                auto float2 = key2->getKey<float>();
                switch (op) {
                    case GT_OP:
                        return float1 > float2;
                    case GE_OP:
                        return float1 >= float2;
                    case LT_OP:
                        return float1 < float2;
                    case LE_OP:
                        return float1 <= float2;
                    case NE_OP:
                        return float1 != float2;
                    case EQ_OP:
                        return float1 == float2;
                    default:
                        return false;
                }
                break;
            }
            case TypeVarChar:
                auto str1 = key1->getKey<std::string>();
                auto str2 = key2->getKey<std::string>();
                switch (op) {
                    case GT_OP:
                        return str1 > str2;
                    case GE_OP:
                        return str1 >= str2;
                    case LT_OP:
                        return str1 < str2;
                    case LE_OP:
                        return str1 <= str2;
                    case NE_OP:
                        return str1 != str2;
                    case EQ_OP:
                        return str1 == str2;
                    default:
                        return false;
                }
                break;
        }
        return false;
    }

    internalEntry *LeafNode::splitNode(leafEntry *key, const Attribute &attr) {
        RC ret = ixFileHandle.appendEmptyPage();
        assert(ret = SUCCESS);
        auto leaf2Page = ixFileHandle.getLastPageIndex();

        int16_t moveStartPos = 0;
        int16_t moveStartIndex = 0;
        while (moveStartPos < getFreeBytePointer() / 2) {
            moveStartPos += ((leafEntry *) (data + moveStartPos))->getEntryLength(attr.type);
            moveStartIndex++;
        }

        auto curKey = (leafEntry *) (data + moveStartPos);
        bool isSplitFeasible = true;
        if (isKeyMeetCompCondition(key, curKey, attr, CompOp::LT_OP)) {
            // new entry into L1
            if (moveStartPos + key->getEntryLength(attr.type) > getMaxFreeSpace()) {
                isSplitFeasible = false;
                moveStartIndex--;
            }
        } else {
            // new entry into L2
            if (getFreeBytePointer() - moveStartPos +key->getEntryLength(attr.type) > getMaxFreeSpace()) {
                isSplitFeasible = false;
                moveStartIndex++;
            }
        }

        if(!isSplitFeasible) {
            moveStartPos = 0;
            for(int16_t i = 0; i < moveStartIndex; i++) {
                moveStartPos += ((leafEntry *) (data + moveStartPos))->getEntryLength(attr.type);
            }
        }

        // move the data from moveStartPos to L2
        int16_t moveLen = getFreeBytePointer() - moveStartPos;
        LeafNode leaf2(data + moveStartPos, moveLen, ixFileHandle, leaf2Page, getNextPtr(),getkeyCounter() - moveStartIndex);

        // Compact old page and maintain metadata
        setFreeBytePointer(getFreeBytePointer()-  moveLen);
        setkeyCounter(moveStartIndex)

        // Insert new leaf page into the leaf page linked list
        leaf2.setNextPtr(this->nextPtr);
        nextPtr = leaf2Page;

        // Insert new entry into old page or new page
        curKey = (leafEntry *) (data + moveStartPos);
        if(isKeyMeetCompCondition(key, curKey, attr, CompOp::LT_OP)) {
            ret = insertEntry(key, attr);
            assert(ret == SUCCESS);
        }
        else {
            ret = leaf2.insertEntry(key, attr);
            assert(ret == SUCCESS);
        }

        // generate new middle key to copy up
        // newchildentry = & (smallest key value on L2, pointer to L2)
        uint8_t buffer[PAGE_SIZE];
        auto middleKey = (internalEntry*)buffer;
        middleKey->setKey(attr.type, leaf2.getEntry(0, attr)->getKeyPtr<uint8_t>());
        middleKey->setRightChild(attr.type, leaf2Page);
        return middleKey;
    }

    leafEntry *LeafNode::getEntry(int i, const Attribute &attribute) {
        assert(i < getkeyCounter());
        int16_t pos;
        auto entry = (leafEntry *) data;
        for (int j = 0; j < i; j++) {
            entry = entry->getNextEntry(attribute.type);
        }
        return entry;
    }


} // namespace PeterDB