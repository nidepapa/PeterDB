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

    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

    }

    INLJoin::~INLJoin() {

    }

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
