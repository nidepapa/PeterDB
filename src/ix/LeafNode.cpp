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
        if (getkeyCounter() != 0) {
            for (int i = 0; i < getkeyCounter(); i++) {
                auto curKey = (leafEntry *) (data + pos);
                if (isKeyMeetCompCondition(key, curKey, attribute, LT_OP))break;
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
        // todo also compare RID
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
        nextPtr = leaf2Page;

        // Insert new entry into old page or new page
        curKey = (leafEntry *) (data + moveStartPos);
        if (isKeyMeetCompCondition(key, curKey, attr, CompOp::LT_OP)) {
            ret = insertEntry(key, attr);
            assert(ret == SUCCESS);
        } else {
            ret = leaf2.insertEntry(key, attr);
            assert(ret == SUCCESS);
        }

        // generate new middle key to copy up
        // newchildentry = & (smallest key value on L2, pointer to L2)
        uint8_t buffer[PAGE_SIZE];
        auto middleKey = (internalEntry *) buffer;
        uint8_t buffer2[PAGE_SIZE];
        auto entry = (leafEntry*)buffer2;
        leaf2.getEntry(0, entry);
        middleKey->setKey(attr.type, entry->getKeyPtr<uint8_t>());
        middleKey->setRightChild(attr.type, leaf2Page);
        return middleKey;
    }

    RC LeafNode::getEntry(int16_t pos, leafEntry *leaf) {
        assert(pos < getFreeBytePointer());
        leaf = (leafEntry *) (data + pos);
        return SUCCESS;
    }

    RC LeafNode::findFirstKeyMeetCompCondition(int16_t &pos, const uint8_t *key, const Attribute &attr, CompOp op) {
        pos = 0;
        auto entry = (leafEntry *) data;
        for (int16_t index = 0; index < keyCounter; index++) {
            if (isKeyMeetCompCondition(entry, (leafEntry *) key, attr, op)) {
                break;
            }
            pos += entry->getEntryLength(attr.type);
            entry = entry->getNextEntry(attr.type);
        }
        return SUCCESS;
    }

    RC LeafNode::deleteEntry(leafEntry *key, const Attribute& attr){
        RC ret = 0;
        int16_t slotPos = 0;
        findFirstKeyMeetCompCondition(slotPos, (uint8_t *)key, attr, EQ_OP);
        if(slotPos >= getFreeBytePointer()) {
            return RC(IX_ERROR::LEAF_ENTRY_NOT_EXIST);
        }
        int16_t dataNeedMovePos = slotPos + key->getEntryLength(attr.type);
        if(dataNeedMovePos < getFreeBytePointer()) {
            shiftDataLeft(dataNeedMovePos, key->getEntryLength(attr.type));
        }
        setFreeBytePointer(getFreeBytePointer() - key->getEntryLength(attr.type));
        setkeyCounter(getkeyCounter() - 1);
        return SUCCESS;
    }

    RC LeafNode::print(const Attribute &attr, std::ostream &out) {
        out << "{\"keys\": [";
        uint32_t pageNum;
        uint16_t slotNum;

        auto curEntry = (leafEntry *)data;
        int32_t curInt;
        float curFloat;
        std::string curStr;

        int16_t i = 0;
        while(i < getkeyCounter()) {
            out << "\"";
            // Print Key
            switch (attr.type) {
                case TypeInt:
                    curInt = curEntry->getKey<int32_t>();
                    out << curInt << ":[";
                    break;
                case TypeReal:
                    curFloat = curEntry->getKey<float>();
                    out << curFloat << ":[";
                    break;
                case TypeVarChar:
                    curStr = curEntry->getKey<std::string>();
                    out << curStr << ":[";
                    break;
                default:
                    return RC(IX_ERROR::KEY_TYPE_INVALID);
            }
            // Print following Rid with the same key
            while(i < getkeyCounter()) {
                curEntry->getRID(attr.type, pageNum, slotNum);
                out << "(" << pageNum << "," << slotNum <<")";
                i++;

                if(i >= getkeyCounter()) {
                    break;
                }
                auto sameKeyEntry = curEntry->getNextEntry(attr.type);
                bool isKeySame = true;
                switch (attr.type) {
                    case TypeInt:
                        if(sameKeyEntry->getKey<int32_t>() != curInt)
                            isKeySame = false;
                        break;
                    case TypeReal:
                        if(sameKeyEntry->getKey<float>() != curFloat)
                            isKeySame = false;
                        break;
                    case TypeVarChar:
                        if(sameKeyEntry->getKey<std::string>() != curStr)
                            isKeySame = false;
                        break;
                    default:
                        return RC(IX_ERROR::KEY_TYPE_INVALID);
                }
                if(!isKeySame) {
                    break;
                }
                curEntry = sameKeyEntry;

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
}
