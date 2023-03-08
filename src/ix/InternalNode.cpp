//
// Created by Fan Zhao on 2/24/23.
//
#include "src/include/ix.h"

namespace PeterDB {

    RC InternalNode::getTargetChild(leafEntry *key, const Attribute &attr, int32_t &childPage) {
        RC ret = 0;
        if (key == nullptr) {    // For scanner, get the first child
            memcpy(&childPage, data, IX::NEXT_POINTER_LEN);
            return 0;
        }
        // skip the left most pointer;
        int16_t pos = IX::NEXT_POINTER_LEN;
        ret = findPosToInsertKey(pos, (internalEntry *) key, attr);
        assert(ret == SUCCESS);
        // Get previous child pointer
        pos -= IX::NEXT_POINTER_LEN;
        memcpy(&childPage, data + pos, IX::NEXT_POINTER_LEN);
        return SUCCESS;
    }

    RC InternalNode::insertEntry(internalEntry *key, const Attribute &attr) {
        if (getFreeSpace() < key->getEntryLength(attr.type))return RC(IX_ERROR::PAGE_NO_ENOUGH_SPACE);
        // page will have no pointer to itself
        assert(key->getRightChild(attr.type) != getPageNum());
        int16_t pos = IX::NEXT_POINTER_LEN;
        RC ret = findPosToInsertKey(pos, key, attr);
        assert(ret == SUCCESS);

        if (pos > getFreeBytePointer())return RC(IX_ERROR::FREEBYTE_EXCEEDED);
        ret = shiftDataRight(pos, key->getEntryLength(attr.type));
        assert(ret == SUCCESS);

        writeEntry(key, attr, pos);
        setFreeBytePointer(getFreeBytePointer() + key->getEntryLength(attr.type));
        setkeyCounter(getkeyCounter() + 1);
        return SUCCESS;
    }

    RC InternalNode::splitNode(internalEntry *keyToInsert, const Attribute &attr, internalEntry *newChildEntry) {
        RC ret = ixFileHandle.appendEmptyPage();
        assert(ret == SUCCESS);
        auto internal2Page = ixFileHandle.getLastPageIndex();

        // Push up middle key - 3 Cases
        int16_t newKeyInsertPos;
        ret = findPosToInsertKey(newKeyInsertPos, keyToInsert, attr);
        if (ret) return ret;

        int16_t curIndex = 0;
        int16_t curPos = IX::NEXT_POINTER_LEN, prevPos;
        while (curPos < getFreeBytePointer() / 2) {
            prevPos = curPos;
            curPos += ((internalEntry *) (data + curPos))->getEntryLength(attr.type);
            curIndex++;
        }
        int16_t prevIndex = curIndex - 1;

        auto curKey = (internalEntry *) (data + curPos), prevKey = (internalEntry *) (data + prevPos);
        int16_t moveStartPos, moveLen;
        // ... | Prev Key | Cur Key | ...
        if (newKeyInsertPos <= prevPos) {
            // Case 1: Previous Key will be middle key
            newChildEntry->setCompositeKey(attr.type, prevKey->getKeyPtr<uint8_t>());
            newChildEntry->setRightChild(attr.type, internal2Page);
            // insert new N2 data
            moveStartPos = prevPos + prevKey->getCompositeKeyLength(attr.type);
            moveLen = getFreeBytePointer() - moveStartPos;

            InternalNode internal2(data + moveStartPos, moveLen, ixFileHandle, internal2Page,
                                   getkeyCounter() - curIndex);
            // Compact old page and maintain metadata
            // push up will deliminate prevKey from this node
            setFreeBytePointer(prevPos);
            setkeyCounter(prevIndex);

            // Insert Key into old page
            ret = insertEntry(keyToInsert, attr);
            if (ret) return ret;

        } else if (newKeyInsertPos == curPos) {
            // Case 2: New key will be the middle key
            newChildEntry->setCompositeKey(attr.type, keyToInsert->getKeyPtr<uint8_t>());
            newChildEntry->setRightChild(attr.type, internal2Page);
            // insert new N2 data
            // set newChildEntry right child to N2;
            auto newKeyRightChild = keyToInsert->getRightChild(attr.type);
            uint8_t dataToMove[PAGE_SIZE];
            moveLen = getFreeBytePointer() - curPos + IX::NEXT_POINTER_LEN;
            memcpy(dataToMove, &newKeyRightChild, IX::NEXT_POINTER_LEN);
            memcpy(dataToMove + IX::NEXT_POINTER_LEN, data + curPos, getFreeBytePointer() - curPos);

            InternalNode internal2(dataToMove, moveLen, ixFileHandle, internal2Page,
                                   getkeyCounter() - curIndex);

            setFreeBytePointer(curPos);
            setkeyCounter(prevIndex + 1);
        } else {
            // Case 3: Current Key will be middle page
            newChildEntry->setCompositeKey(attr.type, curKey->getKeyPtr<uint8_t>());
            newChildEntry->setRightChild(attr.type, internal2Page);
            moveStartPos = curPos + curKey->getCompositeKeyLength(attr.type);
            moveLen = getFreeBytePointer() - moveStartPos;

            InternalNode internal2(data + moveStartPos, moveLen, ixFileHandle, internal2Page,
                                   getkeyCounter() - curIndex - 1);

            setFreeBytePointer(curPos);
            setkeyCounter(prevIndex + 1);

            // Insert Key into new page
            ret = internal2.insertEntry(keyToInsert, attr);
            if (ret) return ret;
        }
        return SUCCESS;
    }

    RC InternalNode::splitOrInsertNode(internalEntry *keyToInsert, const Attribute &attr, internalEntry *newChildEntry,
                                       bool &isNewChildExist) {
        // we split child, must insert *newchildentry in N
        if (getFreeSpace() > keyToInsert->getEntryLength(attr.type)) {
            RC ret = insertEntry(keyToInsert, attr);
            if (ret) return RC(IX_ERROR::NOLEAF_INSERT_ENTRY_FAIL);
            isNewChildExist = false;
        } else {
            // we split child
            RC ret = splitNode(keyToInsert, attr, newChildEntry);
            if (ret) return ret;
            isNewChildExist = true;

            if (isRoot()) {
                RC ret = ixFileHandle.appendEmptyPage();
                assert(ret == SUCCESS);
                auto newRootPage = ixFileHandle.getLastPageIndex();
                InternalNode newRoot(ixFileHandle, newRootPage, getPageNum(), newChildEntry, attr);
                ixFileHandle.setRoot(newRootPage);
            }
        }
        return SUCCESS;
    }

    RC InternalNode::findPosToInsertKey(internalEntry *firstGTEntry, leafEntry *key, const Attribute &attr) {
        assert(firstGTEntry == (internalEntry *) (data + IX::NEXT_POINTER_LEN));
        // empty page, insert directly;
        if (getkeyCounter() == 0)return SUCCESS;
        for (int16_t i = 0; i < getkeyCounter(); i++) {
            if (isCompositeKeyMeetCompCondition((uint8_t *) firstGTEntry, (uint8_t *) key, attr, GT_OP)) break;
            firstGTEntry = firstGTEntry->getNextEntry(attr.type);
        }
        return SUCCESS;
    }

    RC InternalNode::findPosToInsertKey(int16_t &curPos, internalEntry *key, const Attribute &attr) {
        // empty page, insert directly;
        curPos = IX::NEXT_POINTER_LEN;
        if (getkeyCounter() == 0)return SUCCESS;
        for (int16_t i = 0; i < getkeyCounter(); i++) {
            if (isCompositeKeyMeetCompCondition((data + curPos), (uint8_t *) key, attr, GT_OP)) break;
            curPos += ((internalEntry *) (data + curPos))->getEntryLength(attr.type);
        }
        return SUCCESS;
    }


    RC InternalNode::writeEntry(internalEntry *key, const Attribute &attribute, int16_t pos) {
        memcpy(data + pos, (uint8_t *) key, key->getEntryLength(attribute.type));
        return SUCCESS;
    }

    RC InternalNode::print(const Attribute &attr, std::ostream &out) {
        // 1. Keys
        out << "{\"keys\": [";
        std::queue<int> children;
        int16_t offset = 0;
        uint32_t child;
        for (int16_t i = 0; i < getkeyCounter(); i++) {
            memcpy(&child, data + offset, IX::NEXT_POINTER_LEN);
            children.push(child);
            offset += IX::NEXT_POINTER_LEN;
            out << "\"";
            auto entry = (internalEntry *) (data + offset);
            switch (attr.type) {
                case TypeInt:
                    out << entry->getKey<int32_t>();
                    break;
                case TypeReal:
                    out << entry->getKey<float>();
                    break;
                case TypeVarChar:
                    //std::cout<<"Str: " << getKeyString(data + offset)<<std::endl;
                    out << entry->getKey<std::string>();
                    break;
                default:
                    return RC(IX_ERROR::KEY_TYPE_INVALID);
            }
            offset += entry->getCompositeKeyLength(attr.type);
            out << "\"";
            if (i != getkeyCounter() - 1) {
                out << ",";
            }
        }
        // Last Child
        memcpy(&child, data + offset, IX::NEXT_POINTER_LEN);
        if (child == IX::NULL_PTR) {
            return RC(IX_ERROR::INDEXPAGE_LAST_CHILD_NOT_EXIST);
        }
        children.push(child);
        out << "]," << std::endl;
        // 2. Children
        out << "\"children\": [" << std::endl;
        while (!children.empty()) {
            int16_t nodeType;
            {
                IXNode node(ixFileHandle, children.front());
                nodeType = node.getNodeType();
            }
            if (nodeType == IX::INTERNAL_NODE) {
                InternalNode internal(ixFileHandle, children.front());
                internal.print(attr, out);
            } else if (nodeType == IX::LEAF_NODE) {
                LeafNode leaf(ixFileHandle, children.front());
                leaf.print(attr, out);
            }
            children.pop();
            if (!children.empty()) {
                out << ",";
            }
            out << std::endl;
        }
        out << "]}";
        return 0;
    }
}