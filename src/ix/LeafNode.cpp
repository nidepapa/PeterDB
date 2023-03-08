//
// Created by Fan Zhao on 2/24/23.
//

#include "src/include/ix.h"

namespace PeterDB {
    RC LeafNode::writeEntry(leafEntry *key, const Attribute &attribute, int16_t pos) {
        memcpy(data + pos, (uint8_t *) key, key->getEntryLength(attribute.type));
        return SUCCESS;
    }

    RC LeafNode::insertEntry(leafEntry *key, const Attribute &attribute) {
        if (getFreeSpace() < key->getEntryLength(attribute.type))return RC(IX_ERROR::PAGE_NO_ENOUGH_SPACE);
        int16_t pos = 0;
        // if not empty, find the right position
        auto a = key->getKey<int32_t>();
        if (getkeyCounter() != 0) {
            for (int i = 0; i < getkeyCounter(); i++) {
                auto curKey = (leafEntry *) (data + pos);
                if (isCompositeKeyMeetCompCondition((uint8_t*)key, data + pos, attribute, LT_OP))break;
                pos += curKey->getEntryLength(attribute.type);
            }
        }

        if (pos > getFreeBytePointer())return RC(IX_ERROR::FREEBYTE_EXCEEDED);
        RC ret = shiftDataRight(pos, key->getEntryLength(attribute.type));
        assert(ret == SUCCESS);

        writeEntry(key, attribute, pos);
        setFreeBytePointer(getFreeBytePointer() + key->getEntryLength(attribute.type));
        setkeyCounter(getkeyCounter() + 1);
        checkKey(pos, attribute, key);
        return SUCCESS;
    }

    RC LeafNode::splitNode(leafEntry *key, const Attribute &attr, internalEntry *newChild) {
        RC ret = ixFileHandle.appendEmptyPage();
        assert(ret == SUCCESS);
        int32_t leaf2Page = ixFileHandle.getLastPageIndex();

        int16_t moveStartPos = 0;
        int16_t moveStartIndex = 0;
        while (moveStartPos < getFreeBytePointer() / 2) {
            moveStartPos += ((leafEntry *) (data + moveStartPos))->getEntryLength(attr.type);
            moveStartIndex++;
        }

        auto curKey = (leafEntry *) (data + moveStartPos);
        bool isSplitFeasible = true;
        if (isCompositeKeyMeetCompCondition((uint8_t*)key, (uint8_t*)curKey, attr, CompOp::LT_OP)) {
            // new entry into L1
            if (moveStartPos + key->getEntryLength(attr.type) > getMaxFreeSpace()) {
                isSplitFeasible = false;
                moveStartIndex--;
            }
        } else {
            // new entry into L2
            if (getFreeBytePointer() - moveStartPos + key->getEntryLength(attr.type) > getMaxFreeSpace()) {
                isSplitFeasible = false;
                moveStartIndex++;
            }
        }

        if (!isSplitFeasible) {
            moveStartPos = 0;
            for (int16_t i = 0; i < moveStartIndex; i++) {
                moveStartPos += ((leafEntry *) (data + moveStartPos))->getEntryLength(attr.type);
            }
        }

        // move the data from moveStartPos to L2
        int16_t moveLen = getFreeBytePointer() - moveStartPos;
        LeafNode leaf2(data + moveStartPos, moveLen, ixFileHandle, leaf2Page, getNextPtr(),
                       getkeyCounter() - moveStartIndex);

        // Compact old page and maintain metadata
        setFreeBytePointer(getFreeBytePointer() - moveLen);
        setkeyCounter(moveStartIndex);

        // Insert new leaf page into the leaf page linked list
        leaf2.setNextPtr(this->nextPtr);
        // new page is larger number;
        assert( leaf2Page > this->nextPtr);
        nextPtr = leaf2Page;

        // Insert new entry into old page or new page
        curKey = (leafEntry *) (data + moveStartPos);
        if (isCompositeKeyMeetCompCondition((uint8_t*)key, (uint8_t*)curKey, attr, CompOp::LT_OP)) {
            ret = insertEntry(key, attr);
            assert(ret == SUCCESS);
        } else {
            ret = leaf2.insertEntry(key, attr);
            assert(ret == SUCCESS);
        }

        // generate new middle key to copy up
        // newchildentry = & (smallest key value on L2, pointer to L2)
        uint8_t buffer[PAGE_SIZE];
        auto entry = (leafEntry *) buffer;
        leaf2.getEntry(0, entry, attr);
        newChild->setCompositeKey(attr.type, entry->getKeyPtr<uint8_t>());
        newChild->setRightChild(attr.type, leaf2Page);
        return SUCCESS;
    }

    RC LeafNode::getEntry(int16_t pos, leafEntry *leaf, Attribute attr) {
        assert(pos < getFreeBytePointer());
        leaf->setKey(attr.type, data + pos);
        pos += leaf->getKeyLength(attr.type);
        uint32_t pageNum;
        uint16_t slotNum;
        memcpy(&pageNum, data + pos, sizeof(uint32_t));
        pos += sizeof(uint32_t);
        memcpy(&slotNum, data + pos, sizeof(uint16_t));
        leaf->setRID(attr.type, pageNum, slotNum);
        return SUCCESS;
    }

    RC LeafNode::findFirstKeyMeetCompCondition(int16_t &pos, const uint8_t *key, const Attribute &attr, CompOp op) {
        pos = 0;
        auto entry = (leafEntry *) data;
        for (int16_t index = 0; index < keyCounter; index++) {
            if (isCompositeKeyMeetCompCondition((uint8_t*)entry,  key, attr, op)) {
                break;
            }
            pos += entry->getEntryLength(attr.type);
            entry = entry->getNextEntry(attr.type);
        }
        return SUCCESS;
    }

    RC LeafNode::deleteEntry(leafEntry *key, const Attribute &attr) {
        RC ret = 0;
        int16_t slotPos = 0;
        findFirstKeyMeetCompCondition(slotPos, (uint8_t *) key, attr, EQ_OP);
        if (slotPos >= getFreeBytePointer()) {
            return RC(IX_ERROR::LEAF_ENTRY_NOT_EXIST);
        }
        int16_t curEntryLen = ((leafEntry*)(data + slotPos))->getEntryLength(attr.type);
        int16_t dataNeedMovePos = slotPos + curEntryLen  ;
        if (dataNeedMovePos < getFreeBytePointer()) {
            shiftDataLeft(dataNeedMovePos, curEntryLen);
        }
        setFreeBytePointer(getFreeBytePointer() - curEntryLen);
        setkeyCounter(getkeyCounter() - 1);
        return SUCCESS;
    }

    RC LeafNode::insertOrSplitEntry(leafEntry *key, const Attribute &attribute, internalEntry * newChild, bool& isNewChildExist){
        // if L has space,
        if (getFreeSpace() > key->getEntryLength(attribute.type)) {
            RC ret = insertEntry(key, attribute);
            if (ret) return RC(IX_ERROR::LEAF_INSERT_ENTRY_FAIL);
            isNewChildExist = false;

        } else {
            // split L: first d entries stay, rest move to brand new node L2;
            RC ret = splitNode(key, attribute, newChild);
            if (ret) return RC(IX_ERROR::LEAF_SPLIT_ENTRY_FAIL);
            isNewChildExist = true;
        }
        return  SUCCESS;
    }

    RC LeafNode::print(const Attribute &attr, std::ostream &out) {
        RC ret = 0;
        out << "{\"keys\": [";
        int16_t offset = 0;
        uint32_t pageNum;
        int16_t slotNum;

        int32_t curInt;
        float curFloat;
        std::string curStr;

        int16_t i = 0;
        while(i < getkeyCounter()) {
            out << "\"";
            // Print Key
            switch (attr.type) {
                case TypeInt:
                    curInt = ((leafEntry*)(data + offset))->getKey<int32_t>();
                    out << curInt << ":[";
                    break;
                case TypeReal:
                    curFloat = ((leafEntry*)(data + offset))->getKey<float>();
                    out << curFloat << ":[";
                    break;
                case TypeVarChar:
                    curStr = ((leafEntry*)(data + offset))->getKey<std::string>();
                    out << curStr << ":[";
                    break;
                default:
                    return RC(IX_ERROR::KEY_TYPE_INVALID);
            }
            // Print following Rid with the same key
            while(i < getkeyCounter()) {
                offset += ((leafEntry*)(data + offset))->getKeyLength(attr.type);

                memcpy(&pageNum, data + offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                memcpy(&slotNum, data + offset,sizeof(uint16_t));
                offset += sizeof(uint16_t);
                out << "(" << pageNum << "," << slotNum <<")";
                i++;

                if(i >= getkeyCounter()) {
                    break;
                }

                bool isKeySame = true;
                switch (attr.type) {
                    case TypeInt:
                        if(((leafEntry*)(data + offset))->getKey<int32_t>() != curInt)
                            isKeySame = false;
                        break;
                    case TypeReal:
                        if(((leafEntry*)(data + offset))->getKey<float>() != curFloat)
                            isKeySame = false;
                        break;
                    case TypeVarChar:
                        if(((leafEntry*)(data + offset))->getKey<std::string>() != curStr)
                            isKeySame = false;
                        break;
                    default:
                        return RC(IX_ERROR::KEY_TYPE_INVALID);
                }
                if(!isKeySame) {
                    break;
                }

                out << ",";
            }
            out << "]\"";

            if(i < getkeyCounter()) {
                out << ",";
            }
        }
        out << "]}";
        return 0;
    }

    RC LeafNode::checkKey(int16_t pos, Attribute attr, leafEntry *key) {
        getEntry(pos, (leafEntry *) checkEntryKey, attr);
        assert(memcmp(checkEntryKey, key, key->getEntryLength(attr.type)) == 0);
    }
}
