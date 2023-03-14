#include "src/include/qe.h"

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        this->iter = input;
        this->cond = condition;
        input->getAttributes(this->attrs);
    }

    Filter::~Filter() = default;

    RC Filter::getNextTuple(void *data) {
        RC ret = 0;
        ret = iter->getNextTuple(data);
        if(ret) return QE_EOF;
        while(!isRecordMeetCondition(data)) {
            ret = iter->getNextTuple(data);
            if(ret) return QE_EOF;
        }
        return SUCCESS;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = this->attrs;
        return SUCCESS;
    }

    bool Filter::isRecordMeetCondition(void *data) {
        uint8_t buffer[PAGE_SIZE];
        auto record = (Record*)buffer;
        int16_t recLen = 0;
        std::vector<uint16_t> selectedAttrIndex;
        for(int i =0;i< attrs.size(); i++){
            selectedAttrIndex.push_back(i);
        }
        record->fromRawRecord((RawRecord*)data,attrs,selectedAttrIndex,recLen);

        for (int i =0;i <attrs.size();i++){
            if(attrs[i].name == cond.lhsAttr){
                if(record->getDirectory()->getEntry(i)->isNull())return false;
                switch (attrs[i].type) {
                    case TypeInt:
                        return QEHelper::performOper(record->getField<int32_t>(i),*(int32_t *)cond.rhsValue.data,cond.op);
                    case TypeReal:
                        return QEHelper::performOper(record->getField<float>(i),*(float *)cond.rhsValue.data,cond.op);
                    case TypeVarChar:
                        return QEHelper::performOper(record->getField<std::string>(i),
                                std::string((char *)cond.rhsValue.data + sizeof(int32_t), *(int32_t *)cond.rhsValue.data),cond.op);
                }
            }
        }
        return false;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        this->iter = input;
        input->getAttributes(this->attrs);
        for (auto aName:attrNames){
            for (int i =0; i <attrs.size();i++){
                if (attrs[i].name == aName){
                    this->projectAttrIdx.push_back(i);
                    break;
                }
            }
        }
    }

    Project::~Project() = default;

    RC Project::getNextTuple(void *data) {
        RC ret = 0;
        ret = iter->getNextTuple(inputRawData);
        if(ret) return QE_EOF;
        ProjectToSelectedAttr(data);
        return SUCCESS;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        for (auto idx:this->projectAttrIdx){
            attrs.push_back(this->attrs[idx]);
        }
        return SUCCESS;
    }

    RC Project::ProjectToSelectedAttr(void *data) {
        uint8_t buffer[PAGE_SIZE];
        auto record = (Record*)buffer;
        int16_t recLen = 0;
        std::vector<uint16_t> selectedAttrIndex;
        for(int i =0;i< attrs.size(); i++){
            selectedAttrIndex.push_back(i);
        }
        record->fromRawRecord((RawRecord*)inputRawData,attrs,selectedAttrIndex,recLen);

        auto outputData = (RawRecord*)data;
        outputData->initNullByte(this->projectAttrIdx.size());
        auto valPos = (uint8_t*)outputData->dataSection(this->projectAttrIdx.size());
        // for each selected field
        for (int i = 0; i < this->projectAttrIdx.size(); i++){
            auto attrIdx = projectAttrIdx[i];
            if (record->getDirectory()->getEntry(attrIdx)->isNull()){
                outputData->setFieldNull(i);
                continue;
            }
            switch(attrs[attrIdx].type){
                case TypeInt:
                case TypeReal:
                    memcpy(valPos, record->getFieldPtr<int32_t>(attrIdx),sizeof(int32_t));
                    valPos += sizeof(int32_t);
                case TypeVarChar:
                    auto str = record->getField<std::string>(attrIdx);
                    int32_t strLen = str.size();
                    memcpy(valPos, &strLen,sizeof(int32_t));
                    valPos += sizeof(int32_t);
                    memcpy(valPos, &str,strLen);
                    valPos += strLen;
            }
        }
        return SUCCESS;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        this->outer = leftIn;
        this->inner = rightIn;
        this->cond = condition;
        this->hashTableMaxSize = numPages * PAGE_SIZE;
        this->remainSize = numPages * PAGE_SIZE;
        this->hasPointer = false;

        leftIn->getAttributes(this->outerAttrs);
        rightIn->getAttributes(this->innerAttrs);

        this->joinedAttrs.insert(joinedAttrs.end(), outerAttrs.begin(), outerAttrs.end());
        this->joinedAttrs.insert(joinedAttrs.end(), innerAttrs.begin(), innerAttrs.end());

        for (const auto& attr:outerAttrs){
            if (attr.name == cond.lhsAttr){
                this->joinAttr = attr;
                break;
            }
        }
        loadBlock();
    }

    BNLJoin::~BNLJoin() = default;

    RC BNLJoin::getNextTuple(void *data) {
        RC ret;
        // already matched a key, then join all the outer record
        if (hasPointer){
            auto innerRecord = (RawRecord*)innerReadBuffer;
            switch (joinAttr.type) {
                case TypeInt:
                    if(hashLinkedListPos < intHash[matchedIntKey].size()) {
                        auto outerRecord = (RawRecord*)intHash[matchedIntKey][hashLinkedListPos].data();
                        outerRecord->join(outerAttrs, innerRecord, innerAttrs, (RawRecord*)data, joinedAttrs);
                        hashLinkedListPos++;
                        return SUCCESS;
                    }
                case TypeReal:
                    if(hashLinkedListPos < floatHash[matchedFloatKey].size()) {
                        auto outerRecord = (RawRecord*)floatHash[matchedFloatKey][hashLinkedListPos].data();
                        outerRecord->join(outerAttrs, innerRecord, innerAttrs, (RawRecord*)data, joinedAttrs);
                        hashLinkedListPos++;
                        return SUCCESS;
                    }
                case TypeVarChar:
                    if(hashLinkedListPos < strHash[matchedStrKey].size()) {
                        auto outerRecord = (RawRecord*)strHash[matchedStrKey][hashLinkedListPos].data();
                        outerRecord->join(outerAttrs, innerRecord, innerAttrs, (RawRecord*)data, joinedAttrs);
                        hashLinkedListPos++;
                        return SUCCESS;
                    }
            }
        }

        // find a matched inner record;
        int32_t intKey;
        float floatKey;
        std::string strKey;
        bool isMatchFound = false;
        while(!isMatchFound) {
            ret = inner->getNextTuple(innerReadBuffer);
            if(ret) {
                // Reach inner table's end, reload blocks
                ret = loadBlock();
                hasPointer = false;
                if(ret) {
                    // Reload blocks fail, reach outer table's end, return QE_EOF
                    return QE_EOF;
                }
                // Reset inner table's iterator
                inner->setIterator();
                inner->getNextTuple(innerReadBuffer);
            }
            auto innerRecord = (RawRecord*)innerReadBuffer;
            // Probe hash table in memory
            switch (joinAttr.type) {
                case TypeInt:
                    intKey = innerRecord->getField<int32_t>(innerAttrs, joinAttr.name);
                    if(intHash.find(intKey) != intHash.end()) {
                        auto outerRecord = (RawRecord*)intHash[intKey][0].data();
                        outerRecord->join(outerAttrs, innerRecord, innerAttrs, (RawRecord*)data, joinedAttrs);
                        matchedIntKey = intKey;
                        hashLinkedListPos = 1;
                        hasPointer = true;
                        isMatchFound = true;
                    }
                    break;
                case TypeReal:
                    floatKey = innerRecord->getField<float>(innerAttrs, joinAttr.name);
                    if(floatHash.find(floatKey) != floatHash.end()) {
                        auto outerRecord = (RawRecord*)floatHash[floatKey][0].data();
                        outerRecord->join(outerAttrs, innerRecord, innerAttrs, (RawRecord*)data, joinedAttrs);
                        matchedFloatKey = floatKey;
                        hashLinkedListPos = 1;
                        hasPointer = true;
                        isMatchFound = true;
                    }
                    break;
                case TypeVarChar:
                    strKey =  innerRecord->getField<std::string>(innerAttrs, joinAttr.name);
                    if(strHash.find(strKey) != strHash.end()) {
                        auto outerRecord = (RawRecord*)strHash[strKey][0].data();
                        outerRecord->join(outerAttrs, innerRecord, innerAttrs, (RawRecord*)data, joinedAttrs);
                        matchedStrKey = strKey;
                        hashLinkedListPos = 1;
                        hasPointer = true;
                        isMatchFound = true;
                    }
                    break;
            }
        }
        return SUCCESS;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs.insert(attrs.end(), joinedAttrs.begin(), joinedAttrs.end());
    }

    RC BNLJoin::loadBlock() {
        RC ret = 0;
        remainSize = hashTableMaxSize;
        intHash.clear();
        floatHash.clear();
        strHash.clear();
        // keep loading until block size

        int32_t intKey;
        float floatKey;
        std::string strKey;
        while(remainSize > 0){
            ret = outer->getNextTuple(outerReadBuffer);
            if(ret) break;
            auto outRawRecord = (RawRecord*)outerReadBuffer;
            int32_t dataLen = 0;
            outRawRecord->size(outerAttrs, &dataLen);
            switch (joinAttr.type){
                case TypeInt:
                    intKey = outRawRecord->getField<int32_t>(outerAttrs, joinAttr.name);
                    intHash[intKey].push_back(std::vector<uint8_t>(outerReadBuffer, outerReadBuffer + dataLen));
                case TypeReal:
                    floatKey = outRawRecord->getField<float>(outerAttrs, joinAttr.name);
                    floatHash[floatKey].push_back(std::vector<uint8_t>(outerReadBuffer, outerReadBuffer + dataLen));
                case TypeVarChar:
                    strKey = outRawRecord->getField<std::string>(outerAttrs, joinAttr.name);
                    strHash[strKey].push_back(std::vector<uint8_t>(outerReadBuffer, outerReadBuffer + dataLen));
            }
            remainSize -= dataLen;
        }
        if(remainSize == hashTableMaxSize) {
            // outer table done
            return QE_EOF;
        }
        return SUCCESS;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

    }

    INLJoin::~INLJoin() = default;

    RC INLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        return -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }
} // namespace PeterDB
