//
// Created by Fan Zhao on 2/2/23.
//

#include "src/include/rm.h"


namespace PeterDB {
    CatalogColumnsHelper::CatalogColumnsHelper() {
        this->table_id = DEFAULT_COLUMN_INT;
        this->column_name = std::string();
        this->column_length = DEFAULT_COLUMN_INT;
        this->column_position = DEFAULT_COLUMN_INT;
        this->column_type = AttrType(DEFAULT_COLUMN_INT);
    }

    CatalogColumnsHelper::CatalogColumnsHelper(int32_t id, const std::string &name, int32_t column_type,
                                               int32_t column_length, int32_t column_position) {
        this->table_id = id;
        this->column_name = name;
        this->column_type = column_type;
        this->column_length = column_length;
        this->column_position = column_position;
    }

    CatalogColumnsHelper::~CatalogColumnsHelper() = default;

    CatalogColumnsHelper::CatalogColumnsHelper(uint8_t *rawData, const std::vector<std::string> &attrNames){
        std::unordered_set<std::string> attrNamesSet(attrNames.begin(), attrNames.end());
        uint32_t nullByteNum = ceil(CATALOG_COLUMNS_ATTR_NUM / 8.0);
        int16_t rawDataPos = nullByteNum;
        // init
        table_id = DEFAULT_COLUMN_INT;
        column_name = std::string();
        column_type = AttrType(DEFAULT_COLUMN_INT);
        column_length = DEFAULT_COLUMN_INT;
        column_position = DEFAULT_COLUMN_INT;

        // According to the Schema Order!!
        // Column Name
        if(attrNamesSet.find(CATALOG_COLUMNS_COLUMNNAME) != attrNamesSet.end()) {
            RawDataStrLen colNameLen;
            memcpy(&colNameLen, rawData + rawDataPos, sizeof(RawDataStrLen));
            rawDataPos += sizeof(RawDataStrLen);
            uint8_t tmpStr[colNameLen];
            memcpy(tmpStr, rawData + rawDataPos, colNameLen);
            rawData += colNameLen;
            column_name.assign((char *)tmpStr, colNameLen);
        }

        // Column Type
        if(attrNamesSet.find(CATALOG_COLUMNS_COLUMNTYPE) != attrNamesSet.end()) {
            memcpy(&column_type, rawData + rawDataPos, sizeof(column_type));
            rawDataPos += sizeof(column_type);
        }

        // Column Length
        if(attrNamesSet.find(CATALOG_COLUMNS_COLUMNLENGTH) != attrNamesSet.end()) {
            memcpy(&column_length, rawData + rawDataPos, sizeof(column_length));
            rawDataPos += sizeof(column_length);
        }

    }

    RC CatalogColumnsHelper::getRecordRawData(uint8_t *rawData) {
        uint32_t nullByteNum = ceil(CATALOG_COLUMNS_ATTR_NUM / 8.0);
        uint8_t nullByte[nullByteNum];
        memset(nullByte, 0 ,nullByteNum);
        memcpy(rawData, nullByte, nullByteNum);

        int16_t rawDataPos = nullByteNum;
        // Table ID
        memcpy(rawData + rawDataPos, &table_id, sizeof(table_id));
        rawDataPos += sizeof(table_id);
        // Col Name
        int32_t colNameLen = column_name.length();
        memcpy(rawData + rawDataPos, &colNameLen, sizeof(colNameLen));
        rawDataPos += sizeof(colNameLen);
        memcpy(rawData + rawDataPos, column_name.c_str(), colNameLen);
        rawDataPos += colNameLen;
        // Col Type
        memcpy(rawData + rawDataPos, &column_type, sizeof(column_type));
        rawDataPos += sizeof(column_type);
        // Col Len
        memcpy(rawData + rawDataPos, &column_length, sizeof(column_length));
        rawDataPos += sizeof(column_length);
        // Col Pos
        memcpy(rawData + rawDataPos, &column_position, sizeof(column_position));
        rawDataPos += sizeof(column_position);

        return SUCCESS;
    }

    Attribute CatalogColumnsHelper::getAttribute() {
        Attribute attr;
        attr.name = column_name;
        attr.type = AttrType(column_type);
        attr.length = column_length;
        return attr;
    }
}