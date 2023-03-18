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
        attrs.clear();
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
                    break;
                case TypeVarChar:
                    auto str = record->getField<std::string>(attrIdx);
                    int32_t strLen = str.size();
                    memcpy(valPos, &strLen,sizeof(int32_t));
                    valPos += sizeof(int32_t);
                    memcpy(valPos, &str,strLen);
                    valPos += strLen;
                    break;
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
                this->outerJoinAttr = attr;
                break;
            }
        }
        for (const auto& attr:innerAttrs){
            if (attr.name == cond.rhsAttr){
                this->innerJoinAttr = attr;
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
            switch (outerJoinAttr.type) {
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
            switch (outerJoinAttr.type) {
                case TypeInt:
                    intKey = innerRecord->getField<int32_t>(innerAttrs, innerJoinAttr.name);
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
                    floatKey = innerRecord->getField<float>(innerAttrs, innerJoinAttr.name);
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
                    strKey =  innerRecord->getField<std::string>(innerAttrs, innerJoinAttr.name);
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
        return SUCCESS;
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
            switch (outerJoinAttr.type){
                case TypeInt:
                    intKey = outRawRecord->getField<int32_t>(outerAttrs, outerJoinAttr.name);
                    intHash[intKey].push_back(std::vector<uint8_t>(outerReadBuffer, outerReadBuffer + dataLen));
                case TypeReal:
                    floatKey = outRawRecord->getField<float>(outerAttrs, outerJoinAttr.name);
                    floatHash[floatKey].push_back(std::vector<uint8_t>(outerReadBuffer, outerReadBuffer + dataLen));
                case TypeVarChar:
                    strKey = outRawRecord->getField<std::string>(outerAttrs, outerJoinAttr.name);
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
        this->outer = leftIn;
        this->inner = rightIn;
        this->cond = condition;
        this->outerIterStatus = 0;

        leftIn->getAttributes(this->outerAttrs);
        rightIn->getAttributes(this->innerAttrs);
        this->joinedAttrs.insert(joinedAttrs.end(), outerAttrs.begin(), outerAttrs.end());
        this->joinedAttrs.insert(joinedAttrs.end(), innerAttrs.begin(), innerAttrs.end());

        for(auto& attr: this->outerAttrs) {
            if(attr.name == cond.lhsAttr) {
                this->outerJoinAttr = attr;
                break;
            }
        }
        for(auto& attr: this->innerAttrs) {
            if(attr.name == cond.rhsAttr) {
                this->innerJoinAttr = attr;
                break;
            }
        }

        outerIterStatus = outer->getNextTuple(outerReadBuffer);
        if(outerIterStatus != QE_EOF) {
            auto outerRecord = ((RawRecord*)outerReadBuffer);
            outerKey = (uint8_t*)(outerRecord->getFieldPtr(outerAttrs, outerJoinAttr.name));
            inner->setIterator(outerKey, outerKey, true, true);
        }

    }

    INLJoin::~INLJoin() = default;

    RC INLJoin::getNextTuple(void *data) {
        while(outerIterStatus != QE_EOF) {
            auto outerRecord = ((RawRecord*)outerReadBuffer);
            outerKey = (uint8_t*)(outerRecord->getFieldPtr(outerAttrs, outerJoinAttr.name));
            while(inner->getNextTuple(innerReadBuffer) == SUCCESS) {
                auto innerRecord = ((RawRecord*)innerReadBuffer);
                innerKey = (uint8_t*)(innerRecord->getFieldPtr(innerAttrs, innerJoinAttr.name));
                if(QEHelper::isSameKey(outerKey, innerKey, outerJoinAttr.type)) {
                    outerRecord->join(outerAttrs, innerRecord, innerAttrs, (RawRecord*)data, joinedAttrs);
                    return 0;
                }
            }

            outerIterStatus = outer->getNextTuple(outerReadBuffer);
            if(outerIterStatus != QE_EOF) {
                auto outerRecord = ((RawRecord*)outerReadBuffer);
                outerKey = (uint8_t*)(outerRecord->getFieldPtr(outerAttrs, outerJoinAttr.name));
                inner->setIterator(outerKey, outerKey, true, true);
            }
        }

        return QE_EOF;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs.insert(attrs.end(), joinedAttrs.begin(), joinedAttrs.end());
        return SUCCESS;
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
        this->input = input;
        this->aggAttr = aggAttr;
        this->op = op;
        this->isGroup = false;
        this->result_pos = 0;

        input->getAttributes(this->inputAttrs);

        // calculate result;
        float val;
        int32_t intKey;
        float floatKey;
        switch (op) {
            case MAX: val = LONG_MIN; break;
            case MIN: val = LONG_MAX; break;
            case SUM:
            case AVG:
            case COUNT:
                val = 0;
                break;
        }

        int32_t count = 0;
        auto rawRecord = (RawRecord*)readBuffer;
        while(input->getNextTuple(readBuffer) == 0) {
            count++;
            switch (op) {
                case MAX:
                    if(aggAttr.type == TypeInt) {
                        intKey = rawRecord->getField<int32_t>(inputAttrs, aggAttr.name);
                        val = std::max(val, (float)intKey);
                    }
                    else if(aggAttr.type == TypeReal) {
                        floatKey= rawRecord->getField<float>(inputAttrs, aggAttr.name);
                        val = std::max(val, floatKey);
                    }
                    break;
                case MIN:
                    if(aggAttr.type == TypeInt) {
                        intKey = rawRecord->getField<int32_t>(inputAttrs, aggAttr.name);
                        val = std::min(val, (float)intKey);
                    }
                    else if(aggAttr.type == TypeReal) {
                        floatKey= rawRecord->getField<float>(inputAttrs, aggAttr.name);
                        val = std::min(val, floatKey);
                    }
                    break;
                case SUM:
                case AVG:
                    if(aggAttr.type == TypeInt) {
                        intKey = rawRecord->getField<int32_t>(inputAttrs, aggAttr.name);
                        val += intKey;
                    }
                    else if(aggAttr.type == TypeReal) {
                        floatKey= rawRecord->getField<float>(inputAttrs, aggAttr.name);
                        val += floatKey;
                    }
                    break;
                case COUNT:
                    break;
            }
        }
        // save result;
        switch (op) {
            case MAX:
            case MIN:
            case SUM:
                result.push_back(val);
                break;
            case COUNT:
                result.push_back(count);
                break;
            case AVG:
                result.push_back(val / count);
                break;
        }

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
        this->input = input;
        this->aggAttr = aggAttr;
        this->op = op;
        this->groupAttr = groupAttr;
        this->isGroup = true;

        std::vector<Attribute> attrs;
        input->getAttributes(attrs);

        int32_t intAggKey, intGroupKey;
        float floatAggKey, floatGroupKey;
        std::string strGroupKey;

        auto rawRecord = (RawRecord*)readBuffer;
        while(input->getNextTuple(readBuffer) == 0) {
            switch (groupAttr.type) {
                case TypeInt:
                    intGroupKey = rawRecord->getField<int32_t>(inputAttrs, groupAttr.name);
                    if(intHash.find(intGroupKey) == intHash.end()) {
                        intHash[intGroupKey].first = 0;
                        switch (op) {
                            case MAX: intHash[intGroupKey].second = LONG_MIN; break;
                            case MIN: intHash[intGroupKey].second = LONG_MAX; break;
                            case SUM:
                            case AVG:
                            case COUNT:
                                intHash[intGroupKey].second = 0;
                                break;
                        }
                    }
                    intHash[intGroupKey].first++;
                    break;
                case TypeReal:
                    floatGroupKey = rawRecord->getField<float>(inputAttrs, groupAttr.name);
                    if(floatHash.find(floatGroupKey) == floatHash.end()) {
                        floatHash[floatGroupKey].first = 0;
                        switch (op) {
                            case MAX: floatHash[floatGroupKey].second = LONG_MIN; break;
                            case MIN: floatHash[floatGroupKey].second = LONG_MAX; break;
                            case SUM:
                            case AVG:
                            case COUNT:
                                floatHash[floatGroupKey].second = 0;
                                break;
                        }
                    }
                    floatHash[floatGroupKey].first++;
                    break;
                case TypeVarChar:
                    strGroupKey = rawRecord->getField<std::string>(inputAttrs, groupAttr.name);
                    if(strHash.find(strGroupKey) == strHash.end()) {
                        strHash[strGroupKey].first = 0;
                        switch (op) {
                            case MAX: strHash[strGroupKey].second = LONG_MIN; break;
                            case MIN: strHash[strGroupKey].second = LONG_MAX; break;
                            case SUM:
                            case AVG:
                            case COUNT:
                                strHash[strGroupKey].second = 0;
                                break;
                        }
                    }
                    strHash[strGroupKey].first++;
                    break;
            }
            switch (op) {
                case MAX:
                    if(aggAttr.type == TypeInt) {
                        intAggKey = rawRecord->getField<int32_t>(inputAttrs, aggAttr.name);
                        intHash[intGroupKey].second = std::max(intHash[intGroupKey].second, (float)intAggKey);
                    }
                    else if(aggAttr.type == TypeReal) {
                        floatAggKey = rawRecord->getField<float>(inputAttrs, aggAttr.name);
                        floatHash[floatGroupKey].second = std::max(floatHash[floatGroupKey].second, floatAggKey);
                    }
                    break;
                case MIN:
                    if(aggAttr.type == TypeInt) {
                        intAggKey = rawRecord->getField<int32_t>(inputAttrs, aggAttr.name);
                        intHash[intGroupKey].second = std::min(intHash[intGroupKey].second, (float)intAggKey);
                    }
                    else if(aggAttr.type == TypeReal) {
                        floatAggKey = rawRecord->getField<float>(inputAttrs, aggAttr.name);
                        floatHash[floatGroupKey].second = std::min(floatHash[floatGroupKey].second, floatAggKey);
                    }
                    break;
                case SUM:
                case AVG:
                    if(aggAttr.type == TypeInt) {
                        intAggKey = rawRecord->getField<int32_t>(inputAttrs, aggAttr.name);
                        intHash[intGroupKey].second += intAggKey;
                    }
                    else if(aggAttr.type == TypeReal) {
                        floatAggKey = rawRecord->getField<float>(inputAttrs, aggAttr.name);
                        floatHash[floatGroupKey].second += floatAggKey;
                    }
                    break;
                case COUNT:
                    break;
            }
        }

        switch (groupAttr.type) {
            case TypeInt:
                for(auto& p: intHash) {
                    switch (op) {
                        case MAX:
                        case MIN:
                        case SUM:
                            intResult.push_back({p.first, p.second.second});
                            break;
                        case COUNT:
                            intResult.push_back({p.first, p.second.first});
                            break;
                        case AVG:
                            intResult.push_back({p.first, p.second.second / p.second.first});
                            break;
                    }
                }
                break;
            case TypeReal:
                for(auto& p: floatHash) {
                    switch (op) {
                        case MAX:
                        case MIN:
                        case SUM:
                            floatResult.push_back({p.first, p.second.second});
                            break;
                        case COUNT:
                            floatResult.push_back({p.first, p.second.first});
                            break;
                        case AVG:
                            floatResult.push_back({p.first, p.second.second / p.second.first});
                            break;
                    }
                }
                break;
            case TypeVarChar:
                for(auto& p: strHash) {
                    switch (op) {
                        case MAX:
                        case MIN:
                        case SUM:
                            strResult.push_back({p.first, p.second.second});
                            break;
                        case COUNT:
                            strResult.push_back({p.first, p.second.first});
                            break;
                        case AVG:
                            strResult.push_back({p.first, p.second.second / p.second.first});
                            break;
                    }
                }
                break;
        }
    }

    Aggregate::~Aggregate() = default;


    RC Aggregate::getNextTuple(void *data) {
        auto output = (RawRecord*)data;
        if(isGroup) {
            int32_t pos = 0;
            switch (groupAttr.type) {
                case TypeInt:
                    if (result_pos >= intResult.size()) {
                        return QE_EOF;
                    }

                    output->initNullByte(2);
                    pos = output->getNullByteSize(2);
                    *(int32_t *) ((uint8_t *) data + pos) = intResult[result_pos].first;
                    pos += sizeof(int32_t);
                    *(float *) ((uint8_t *) data + pos) = intResult[result_pos].second;
                    break;
                case TypeReal:
                    if (result_pos >= floatResult.size()) {
                        return QE_EOF;
                    }

                    output->initNullByte(2);
                    pos = output->getNullByteSize(2);
                    *(float *) ((uint8_t *) data + pos) = floatResult[result_pos].first;
                    pos += sizeof(float);
                    *(float *) ((uint8_t *) data + pos) = floatResult[result_pos].second;
                    break;
                case TypeVarChar:
                    if (result_pos >= floatResult.size()) {
                        return QE_EOF;
                    }

                    output->initNullByte(2);
                    pos = output->getNullByteSize(2);
                    *(int32_t *) ((uint8_t *) data + pos) = strResult[result_pos].first.size();
                    pos += sizeof(int32_t);
                    memcpy((uint8_t *) data + pos, strResult[result_pos].first.c_str(),
                           strResult[result_pos].first.size());
                    pos += strResult[result_pos].first.size();
                    *(float *) ((uint8_t *) data + pos) = strResult[result_pos].second;
                    break;
            }
        }
        else {
            if(result_pos >= result.size()) {
                return QE_EOF;
            }
            output->initNullByte(1);
            memcpy(output->dataSection(1),&result[result_pos], sizeof(float ));
        }
        result_pos++;
        return SUCCESS;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        std::string opName;
        switch (this->op) {
            case MIN:
                opName = "MIN";
                break;
            case MAX:
                opName = "MAX";
                break;
            case COUNT:
                opName = "COUNT";
                break;
            case SUM:
                opName = "SUM";
                break;
            case AVG:
                opName = "AVG";
                break;
        }

        Attribute attr;
        attr.name = opName + "(" + aggAttr.name + ")";
        attr.type = TypeReal;
        attr.length = sizeof(float);

        attrs.clear();
        if (isGroup) {
            attrs.push_back(groupAttr);
        }
        attrs.push_back(attr);
        return SUCCESS;
    }
} // namespace PeterDB
