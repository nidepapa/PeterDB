//
// Created by Fan Zhao on 2/2/23.
//

#include "src/include/rm.h"
#include <cstdio>
#include <typeinfo>
#include <cstring>
#include "src/include/errorCode.h"
#include <glog/logging.h>
#include<math.h>

namespace PeterDB {
    CatalogTablesHelper::CatalogTablesHelper() {
        this->table_id = DEFAULT_COLUMN_INT;
    };

    CatalogTablesHelper::CatalogTablesHelper(int32_t id, const std::string &name, const std::string &file) {
        this->table_id = id;
        this->table_name = name;
        this->file_name = file;
    }

    CatalogTablesHelper::CatalogTablesHelper(FileHandle &fileHandle) {
        this->fh = fileHandle;
    }

    CatalogTablesHelper::~CatalogTablesHelper() = default;

    RC CatalogTablesHelper::getRecordRawData(uint8_t *rawData) {
        uint32_t nullByteNum = ceil(CATALOG_TABLES_ATTR_NUM / 8.0);
        uint8_t nullByte[nullByteNum];
        memset(nullByte, 0, nullByteNum);
        memcpy(rawData, nullByte, nullByteNum);

        int16_t rawDataPos = nullByteNum;
        // Table ID
        memcpy(rawData + rawDataPos, &table_id, sizeof(table_id));
        rawDataPos += sizeof(table_id);
        // Table Name
        int32_t tableNameLen = table_name.length();
        memcpy(rawData + rawDataPos, &tableNameLen, sizeof(tableNameLen));
        rawDataPos += sizeof(tableNameLen);
        memcpy(rawData + rawDataPos, table_name.c_str(), tableNameLen);
        rawDataPos += tableNameLen;
        // File name
        int32_t fileNameLen = file_name.size();
        memcpy(rawData + rawDataPos, &fileNameLen, sizeof(fileNameLen));
        rawDataPos += sizeof(fileNameLen);
        memcpy(rawData + rawDataPos, file_name.c_str(), fileNameLen);
        rawDataPos += fileNameLen;

        return SUCCESS;
    }

    int32_t CatalogTablesHelper::getTableID(const std::string &tableName) {
        RC rc;
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        int32_t tableID = INVALID_TABLEID;
        if (!fh.isFileOpen()) {
            LOG(ERROR) << "filehandle does not open @  CatalogTablesHelper::getTableID" << std::endl;
            return tableID;
        }
        RBFM_ScanIterator tablesIterator;
        // only get table id
        std::vector<std::string> tableAttrNames = {CATALOG_TABLES_TABLEID};
        uint8_t scanVal[tableName.size() + 4];
        RelationManager::scanIteratorValue(tableName,scanVal);
        rc = rbfm.scan(fh, catalogTablesSchema, CATALOG_TABLES_TABLENAME,
                       EQ_OP, scanVal, tableAttrNames, tablesIterator);
        if (rc) {
            LOG(ERROR) << "scan catalog tables error" << "@CatalogTablesHelper::getTableID" << std::endl;
            return tableID;
        }
        RID curRID;
        uint8_t rawData[PAGE_SIZE];
        tablesIterator.getNextRecord(curRID, rawData);
        memcpy(&tableID, rawData + 1, sizeof(int32_t));
        return tableID;
    }

}
