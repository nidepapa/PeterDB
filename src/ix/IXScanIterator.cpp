//
// Created by Fan Zhao on 2/19/23.
//
#include "src/include/ix.h"

namespace PeterDB {
    RC IX_ScanIterator::open(IXFileHandle *fileHandle, const Attribute &attr, const uint8_t *lowKey,
                             const uint8_t *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        RC ret = 0;
        this->ixFileHandle = fileHandle;
        this->attr = attr;
        this->lowKey = lowKey;
        this->highKey = highKey;
        this->lowKeyInclusive = lowKeyInclusive;
        this->highKeyInclusive = highKeyInclusive;

        curLeafPage = IX::NULL_PTR;
        remainDataLen = 0;
        entryExceedUpperBound = false;

        if (ixFileHandle->isRootNull()) {
            return RC(IX_ERROR::ROOT_NOT_EXIST);
        }
        curLeafPage = ixFileHandle->getRoot();
        // Find the right leaf node (right close to the key) to start scanning
        ret = IndexManager::instance().findTargetLeafNode(*ixFileHandle, curLeafPage, lowKey, attr);
        if (ret) return ret;

        getNextNonEmptyPage();
        if (!lowKey) return SUCCESS;
        // find the entry with lowkey
        while (curLeafPage != IX::NULL_PTR && curLeafPage < ixFileHandle->getNumberOfPages()) {
            LeafNode leaf(*ixFileHandle, curLeafPage);
            int16_t firstEntryPos;
            if (this->lowKeyInclusive) {
                leaf.findFirstKeyMeetCompCondition(firstEntryPos, lowKey, attr, GE_OP);
            } else {
                leaf.findFirstKeyMeetCompCondition(firstEntryPos, lowKey, attr, GT_OP);
            }

            if (firstEntryPos != leaf.getFreeBytePointer()) {
                remainDataLen = leaf.getFreeBytePointer() - firstEntryPos;
                break;
            } else {
                // Reach the end, go to next non-empty page
                curLeafPage = leaf.getNextPtr();
                getNextNonEmptyPage();
            }
        }
        return SUCCESS;
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        RC ret = 0;
        if (curLeafPage >= ixFileHandle->getNumberOfPages() || curLeafPage == IX::NULL_PTR) {
            return IX_EOF;
        }
        if (highKey && entryExceedUpperBound) {
            return IX_EOF;
        }
        // Read current entry and check if it meets condition
        LeafNode leaf(*ixFileHandle, curLeafPage);
        uint8_t buffer[PAGE_SIZE];
        memset(buffer, 0, PAGE_SIZE);
        auto entry = (leafEntry *) buffer;
        ret = leaf.getEntry(leaf.getFreeBytePointer() - remainDataLen, entry,attr);
        if (ret) return IX_EOF;

        // Check if current entry exceed upper bound
        if(highKey && highKeyInclusive &&
           leaf.isKeyMeetCompCondition((uint8_t*)entry,highKey, attr, GT_OP)) {
            entryExceedUpperBound = true;
            return IX_EOF;
        }
        if(highKey && !highKeyInclusive &&
           leaf.isKeyMeetCompCondition((uint8_t*)entry, highKey, attr, GE_OP)) {
            entryExceedUpperBound = true;
            return IX_EOF;
        }

        int16_t nextEntryPos =leaf.getFreeBytePointer() - remainDataLen + entry->getEntryLength(attr.type);
        remainDataLen = leaf.getFreeBytePointer() - nextEntryPos;
        if(remainDataLen == 0) {
            // Reach the end of current page
            curLeafPage = leaf.getNextPtr();
            getNextNonEmptyPage();
        }
        memcpy(key, entry->getKeyPtr<uint8_t>(), entry->getKeyLength(attr.type));
        entry->getRID(attr.type, rid.pageNum, rid.slotNum);

        return SUCCESS;
    }

    RC IX_ScanIterator::close() {
        ixFileHandle->close();
        return SUCCESS;
    }

    RC IX_ScanIterator::getNextNonEmptyPage() {
        while (curLeafPage != IX::NULL_PTR && curLeafPage < ixFileHandle->getNumberOfPages()) {
            LeafNode leaf(*ixFileHandle, curLeafPage);
            if (!leaf.isEmpty()) {
                remainDataLen = leaf.getFreeBytePointer();
                break;
            }
            curLeafPage = leaf.getNextPtr();
        }
        return SUCCESS;
    }

}