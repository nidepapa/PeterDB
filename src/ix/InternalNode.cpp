//
// Created by Fan Zhao on 2/24/23.
//
#include "src/include/ix.h"

namespace PeterDB {

    RC InternalNode::getTargetChild(leafEntry *key, const Attribute &attr, int32_t &childPage) {
        RC ret = 0;
//        if(key == nullptr) {    // For scanner, get the first child
//            memcpy(&childPtr, data, IX::INDEXPAGE_CHILD_PTR_LEN);
//            return 0;
//        }
        // skip the left most pointer;
        auto firstLargerEntry = (internalEntry *) (data + IX::NEXT_POINTER_LEN);

        ret = findPosToInsertKey(firstLargerEntry, key, attr);
        assert(ret == SUCCESS);
        // Get previous child pointer
        childPage = firstLargerEntry->getLeftChild();
        return SUCCESS;
    }

    RC InternalNode::insertEntry(internalEntry *key, const Attribute &attr) {
        if (getFreeSpace() < key->getEntryLength(attr.type))return RC(IX_ERROR::PAGE_NO_ENOUGH_SPACE);

        int16_t pos = IX::NEXT_POINTER_LEN;
        // if not empty, find the right position
        if (getkeyCounter() != 0) {
            for (int i = 0; i < getkeyCounter(); i++) {
                auto curKey = (internalEntry *) (data + pos);
                if (isKeyMeetCompCondition(curKey, key, attr, GT_OP))break;
                pos += curKey->getEntryLength(attr.type);
            }
        }
        if (pos > getFreeBytePointer())return RC(IX_ERROR::FREEBYTE_EXCEEDED);
        RC ret = shiftDataRight(pos, key->getEntryLength(attr.type));
        assert(ret == SUCCESS);

        writeEntry(key, attr, pos);
        setFreeBytePointer(getFreeBytePointer() + key->getEntryLength(attr.type));
        setkeyCounter(getkeyCounter() + 1);
        return SUCCESS;
    }

    RC InternalNode::splitPageAndInsertIndex(internalEntry *key, const Attribute &attr, internalEntry *newChildEntry) {
        RC ret = ixFileHandle.appendEmptyPage();
        assert(ret = SUCCESS);
        auto internal2Page = ixFileHandle.getLastPageIndex();

        // Push up middle key - 3 Cases
        int16_t newKeyInsertPos = IX::NEXT_POINTER_LEN;
        ret = findPosToInsertKey(newKeyInsertPos, key, attr);
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
        uint8_t buffer[PAGE_SIZE];
        auto middleKey = (internalEntry *) buffer;
        // ... | Prev Key | Cur Key | ...
        if (newKeyInsertPos <= prevPos) {
            // Case 1: Previous Key will be middle key
            middleKey->setKey(attr.type, prevKey->getKeyPtr<uint8_t>());
            middleKey->setRightChild(attr.type, internal2Page);
            // insert new N2 data
            moveStartPos = prevPos + prevKey->getEntryLength(attr.type);
            moveLen = getFreeBytePointer() - moveStartPos;

            InternalNode internal2(data + moveStartPos, moveLen, ixFileHandle, internal2Page,
                                   getkeyCounter() - curIndex);
            // Compact old page and maintain metadata
            // push up will deliminate prevKey from this node
            setFreeBytePointer(prevPos);
            setkeyCounter(prevIndex);

            // Insert Key into old page
            ret = insertEntry(newChildEntry, attr);
            if (ret) return ret;

        } else if (newKeyInsertPos == curPos) {
            // Case 2: New key will be the middle key
            middleKey->setKey(attr.type, newChildEntry->getKeyPtr<uint8_t>());
            middleKey->setRightChild(attr.type, internal2Page);
            // insert new N2 data

            moveStartPos = prevPos + prevKey->getEntryLength(attr.type);
            // set newChildEntry right child to N2;
            auto newKeyRightChild = newChildEntry->getRightChild(attr.type);
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
            middleKey->setKey(attr.type, curKey->getKeyPtr<uint8_t>());
            middleKey->setRightChild(attr.type, internal2Page);
            moveStartPos = curPos + curKey->getEntryLength(attr.type);
            moveLen = getFreeBytePointer() - moveStartPos;

            InternalNode internal2(data + moveStartPos, moveLen, ixFileHandle, internal2Page,
                                   getkeyCounter() - curIndex - 1);

            setFreeBytePointer(curPos);
            setkeyCounter(prevIndex + 1);

            // Insert Key into new page
            ret = internal2.insertEntry(newChildEntry, attr);
            if (ret) return ret;
        }
        newChildEntry = middleKey;
        return SUCCESS;
    }

    RC InternalNode::findPosToInsertKey(internalEntry *firstLargerKey, leafEntry *key, const Attribute &attr) {
        assert(firstLargerKey == (internalEntry *) (data + IX::NEXT_POINTER_LEN));
        // empty page, insert directly;
        if (getkeyCounter() == 0)return SUCCESS;
        for (int16_t i = 0; i < getkeyCounter(); i++) {
            if (isKeyMeetCompCondition(firstLargerKey, key, attr, GT_OP)) break;
            firstLargerKey = firstLargerKey->getNextEntry(attr.type);
        }
        return SUCCESS;
    }

    RC InternalNode::findPosToInsertKey(int16_t &curPos, internalEntry *key, const Attribute &attr) {
        // empty page, insert directly;
        assert(curPos == IX::NEXT_POINTER_LEN);
        if (getkeyCounter() == 0)return SUCCESS;
        for (int16_t i = 0; i < getkeyCounter(); i++) {
            if (isKeyMeetCompCondition((internalEntry *) (data + curPos), key, attr, GT_OP)) break;
            curPos += key->getEntryLength(attr.type);
        }
        return SUCCESS;
    }


    bool
    InternalNode::isKeyMeetCompCondition(internalEntry *key1, leafEntry *key2, const Attribute &attr, const CompOp op) {
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

    bool
    InternalNode::isKeyMeetCompCondition(internalEntry *key1, internalEntry *key2, const Attribute &attr,
                                         const CompOp op) {
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

    RC InternalNode::writeEntry(internalEntry *key, const Attribute &attribute, int16_t pos) {
        memcpy(data + pos, (uint8_t *) key, key->getEntryLength(attribute.type));
        return SUCCESS;
    }
}