//
// Created by Fan Zhao on 3/12/23.
//
#include "src/include/rm.h"

namespace PeterDB{
    CatalogIndexesHelper::CatalogIndexesHelper(){
        this->table_id = DEFAULT_COLUMN_INT;
    }

    CatalogIndexesHelper::CatalogIndexesHelper(uint8_t* rawData, const std::vector<std::string>& attrNames){
        this->table_id = DEFAULT_COLUMN_INT;

        std::vector<uint16_t> selectedAttrIndex;
        for(const auto & attrName : attrNames){
            for (int j=0; j< catalogIndexesSchema.size(); j++){
                if (catalogIndexesSchema[j].name == attrName) selectedAttrIndex.push_back(j);
            }
        }
        uint8_t buffer[PAGE_SIZE];
        int16_t recLen;
        auto record = (Record*)buffer;
        record->fromRawRecord((RawRecord*)rawData,catalogIndexesSchema,selectedAttrIndex,recLen);
        if (!record->getDirectory()->getEntry(0)->isNull())table_id = record->getField<int32_t>(0);
        if (!record->getDirectory()->getEntry(1)->isNull())attr_name= record->getField<std::string>(1);
        if (!record->getDirectory()->getEntry(2)->isNull())file_name = record->getField<std::string>(2);

    }
}