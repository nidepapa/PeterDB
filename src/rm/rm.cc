#include "src/include/rm.h"
#include "src/include/ix.h"
#include <cstdio>
#include <typeinfo>
#include <cstring>
#include "src/include/errorCode.h"
#include <glog/logging.h>


namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() {
        for(auto& fh: ixScanFHList) {
            delete fh;
        }
        for(auto& p: ixFHMap) {
            delete p.second;
        }
        ixScanFHList.clear();
        ixFHMap.clear();
    };

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        // 1.create two basic tables file on disk
        RC rc = 0;
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        rc = rbfm.createFile(CATALOG_TABLES);
        if (rc) {
            LOG(ERROR) << "create file err" << "@RelationManager::createCatalog" << std::endl;
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        rc = rbfm.createFile(CATALOG_COLUMNS);
        if (rc) {
            LOG(ERROR) << "create file err" << "@RelationManager::createCatalog" << std::endl;
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        rc = rbfm.createFile(CATALOG_INDEXES);
        if (rc) {
            LOG(ERROR) << "create file err" << "@RelationManager::createCatalog" << std::endl;
            return RC(RM_ERROR::ERR_UNDEFINED);
        }

        // 2. insert original metadata
        insertMetaDataToCatalog(CATALOG_TABLES, catalogTablesSchema);
        insertMetaDataToCatalog(CATALOG_COLUMNS, catalogColumnsSchema);
        insertMetaDataToCatalog(CATALOG_INDEXES, catalogIndexesSchema);
        return SUCCESS;
    }

    RC RelationManager::openCatalog() {
        RC rc;
        if (!isCatalogReady()) {
            return RC(RM_ERROR::CATALOG_MISSING);
        }
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        if (!fhTables.isFileOpen()) {
            rc = rbfm.openFile(CATALOG_TABLES, fhTables);
            if (rc) {
                LOG(ERROR) << "open file err" << "@RelationManager::createCatalog" << std::endl;
                return RC(RM_ERROR::ERR_UNDEFINED);
            }
        }
        if (!fhColumns.isFileOpen()) {
            rc = rbfm.openFile(CATALOG_COLUMNS, fhColumns);
            if (rc) {
                LOG(ERROR) << "open file err" << "@RelationManager::createCatalog" << std::endl;
                return RC(RM_ERROR::ERR_UNDEFINED);
            }
        }
        if (!fhIndexes.isFileOpen()) {
            rc = rbfm.openFile(CATALOG_INDEXES, fhIndexes);
            if (rc) {
                LOG(ERROR) << "open file err" << "@RelationManager::createCatalog" << std::endl;
                return RC(RM_ERROR::ERR_UNDEFINED);
            }
        }
        return SUCCESS;
    }

    RC RelationManager::insertMetaDataToCatalog(const std::string &tableName, const std::vector<Attribute> schema) {
        RC rc;
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        // open two file
        rc = openCatalog();
        if (rc) {
            return RC(RM_ERROR::CATALOG_OPEN_FAIL);
        }

        uint8_t data[PAGE_SIZE];
        memset(data, 0, PAGE_SIZE);
        RID rid;

        int32_t tableID = INVALID_TABLEID;
        if (fhTables.getNumberOfPages() != 0) {
            tableID = getNewTableID();
        } else { tableID = 1; }

        if (tableID == INVALID_TABLEID) {
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        // insert to tables table
        CatalogTablesHelper tablesHelper = CatalogTablesHelper(tableID, tableName, tableName);
        tablesHelper.getRecordRawData(data);
        rc = rbfm.insertRecord(fhTables, catalogTablesSchema, data, rid);
        if (rc) {
            LOG(ERROR) << "insert record err" << "@RelationManager::insertMetaDataToCatalog" << std::endl;
            return RC(RM_ERROR::ERR_UNDEFINED);
        }

        // insert to columns table
        for (int i = 0; i < schema.size(); i++) {
            CatalogColumnsHelper columnHelper = CatalogColumnsHelper(tableID, schema[i].name, schema[i].type,
                                                                     schema[i].length, i + 1);
            columnHelper.getRecordRawData(data);
            rc = rbfm.insertRecord(fhColumns, catalogColumnsSchema, data, rid);
            if (rc) {
                LOG(ERROR) << "insert record err" << "@RelationManager::insertMetaDataToCatalog" << std::endl;
                return RC(RM_ERROR::ERR_UNDEFINED);
            }
        }
        return SUCCESS;
    }

    RC RelationManager::deleteCatalog() {
        RC rc = 0;
        PagedFileManager &pfm = PeterDB::PagedFileManager::instance();
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        if (!isCatalogReady()) {
            return RC(RM_ERROR::CATALOG_MISSING);
        }

        fhTables.closeFile();
        fhColumns.closeFile();
        fhIndexes.closeFile();

        rc = rbfm.destroyFile(CATALOG_TABLES);
        if (rc) {
            LOG(ERROR) << "destroy file err" << "@RelationManager::deleteCatalog" << std::endl;
            return rc;
        }
        rc = rbfm.destroyFile(CATALOG_COLUMNS);
        if (rc) {
            LOG(ERROR) << "destroy file err" << "@RelationManager::deleteCatalog" << std::endl;
            return rc;
        }
        rc = rbfm.destroyFile(CATALOG_INDEXES);
        if (rc) {
            LOG(ERROR) << "destroy file err" << "@RelationManager::deleteCatalog" << std::endl;
            return rc;
        }
        return SUCCESS;
    }

    RC RelationManager::deleteMetaDataFromCatalog(int32_t tableID) {
        RC rc;
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        RID curRID;
        uint8_t rawData[PAGE_SIZE];
        // scan tables table
        RBFM_ScanIterator tablesIterator;
        std::vector<std::string> tableAttrNames = {CATALOG_TABLES_TABLEID};
        rc = rbfm.scan(fhTables, catalogTablesSchema, CATALOG_TABLES_TABLEID,
                       EQ_OP, &tableID, tableAttrNames, tablesIterator);
        if (rc) {
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        int32_t tableRecordDeletedCount = 0;
        while (tablesIterator.getNextRecord(curRID, rawData) != RBFM_EOF) {
            rc = rbfm.deleteRecord(fhTables, catalogTablesSchema, curRID);
            if (rc) {
                return RC(RM_ERROR::ERR_UNDEFINED);
            }
            ++tableRecordDeletedCount;
        }
        if (tableRecordDeletedCount != 1) {
            LOG(ERROR) << "Should delete one record in TABLES @ RelationManager::deleteAllMetaDataFromCatalog"
                       << std::endl;
            return RC(RM_ERROR::ERR_UNDEFINED);
        }

        // Scan columns table
        RBFM_ScanIterator colIterator;
        std::vector<std::string> colAttrNames = {CATALOG_COLUMNS_TABLEID};
        rc = rbfm.scan(fhColumns, catalogColumnsSchema, CATALOG_COLUMNS_TABLEID,
                       EQ_OP, &tableID, colAttrNames, colIterator);
        if (rc) {
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        int32_t colRecordDeletedCount = 0;
        while (colIterator.getNextRecord(curRID, rawData) == 0) {
            rc = rbfm.deleteRecord(fhColumns, catalogColumnsSchema, curRID);
            if (rc) {
                return RC(RM_ERROR::ERR_UNDEFINED);
            }
            ++colRecordDeletedCount;
        }
        if (colRecordDeletedCount == 0) {
            LOG(ERROR)
                    << "Should delete more than one record in COLUMNS @ RelationManager::deleteAllMetaDataFromCatalog"
                    << std::endl;
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        return SUCCESS;
    }

    RC RelationManager::deleteIndexFromCatalog(int32_t tableID, std::string attrName) {
        RC rc;
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        RID curRID;
        uint8_t rawData[PAGE_SIZE];
        // scan index table
        RBFM_ScanIterator indexIterator;
        std::vector<std::string> indexAttrNames = {CATALOG_INDEXES_ATTRNAME};
        rc = rbfm.scan(fhIndexes, catalogIndexesSchema, CATALOG_INDEXES_TABLEID,
                       EQ_OP, &tableID, indexAttrNames, indexIterator);
        if (rc) {
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        while (indexIterator.getNextRecord(curRID, rawData) != RBFM_EOF) {
            CatalogIndexesHelper index(rawData, indexAttrNames);
            if (index.attr_name == attrName) {
                rc = rbfm.deleteRecord(fhIndexes, catalogIndexesSchema, curRID);
                if (rc) {
                    return RC(RM_ERROR::ERR_UNDEFINED);
                }
            }

        }
        return SUCCESS;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        RC rc;
        if (isTableNameEmpty(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_EMPTY);
        }
        if (!isTableAccessible(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_INVALID);
        }

        rc = openCatalog();
        if (rc) {
            return RC(RM_ERROR::CATALOG_OPEN_FAIL);
        }
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        rc = rbfm.createFile(tableName);
        if (rc) {
            LOG(ERROR) << "create file err" << "@RelationManager::createCatalog" << std::endl;
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        insertMetaDataToCatalog(tableName, attrs);
        return SUCCESS;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        RC rc;
        if (isTableNameEmpty(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_EMPTY);
        }
        if (!isTableAccessible(tableName)) {
            return RC(RM_ERROR::TABLE_ACCESS_DENIED);
        }
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        rc = rbfm.destroyFile(tableName);
        if (rc) {
            LOG(ERROR) << "destroy file err" << "@RelationManager::deleteTable" << std::endl;
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        rc = openCatalog();
        if (rc) {
            return RC(RM_ERROR::CATALOG_OPEN_FAIL);
        }
        CatalogTablesHelper tablesHelper(fhTables);
        int32_t tableID = tablesHelper.getTableID(tableName);
        if (!isTableIdValid(tableID)) {
            LOG(ERROR) << "Invalid table ID @RelationManager::deleteTable" << std::endl;
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        rc = deleteMetaDataFromCatalog(tableID);
        if (rc) {
            LOG(ERROR) << "delete metadata from catalog err" << "@RelationManager::deleteTable" << std::endl;
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        return SUCCESS;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        RC rc;
        if (isTableNameEmpty(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_EMPTY);
        }

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        rc = openCatalog();
        if (rc) {
            return RC(RM_ERROR::CATALOG_OPEN_FAIL);
        }
        CatalogTablesHelper tablesHelper(fhTables);
        int32_t tableID = tablesHelper.getTableID(tableName);
        if (!isTableIdValid(tableID)) {
            LOG(ERROR) << "Invalid table ID @RelationManager::deleteTable" << std::endl;
            return RC(RM_ERROR::TABLE_NAME_INVALID);
        }

        // Scan columns table
        RBFM_ScanIterator colIterator;
        std::vector<std::string> colAttrNames = {CATALOG_COLUMNS_COLUMNNAME, CATALOG_COLUMNS_COLUMNTYPE,
                                                 CATALOG_COLUMNS_COLUMNLENGTH};
        rc = rbfm.scan(fhColumns, catalogColumnsSchema, CATALOG_COLUMNS_TABLEID,
                       EQ_OP, &tableID, colAttrNames, colIterator);
        if (rc) {
            return RC(RM_ERROR::ITERATOR_BEGIN_FAIL);
        }
        RID curRID;
        uint8_t rawData[PAGE_SIZE];
        memset(rawData, 0, PAGE_SIZE);
        while (colIterator.getNextRecord(curRID, rawData) != RBFM_EOF) {
            CatalogColumnsHelper curRow(rawData, colAttrNames);
            attrs.push_back(curRow.getAttribute());
        }

        return SUCCESS;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        RC rc;
        if (isTableNameEmpty(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_EMPTY);
        }

        if (!isTableAccessible(tableName)) {
            return RC(RM_ERROR::TABLE_ACCESS_DENIED);
        }

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle thisFile;
        std::vector<Attribute> recordDescriptor;
        // table name is file name in this system
        rc = rbfm.openFile(tableName, thisFile);
        if (rc) {
            LOG(ERROR) << "open file err" << "@RelationManager::insertTuple" << std::endl;
            return RC(RM_ERROR::FILE_OPEN_FAIL);
        }
        rc = getAttributes(tableName, recordDescriptor);
        if (rc) return RC(RM_ERROR::DESCRIPTOR_GET_FAIL);
        rc = rbfm.insertRecord(thisFile, recordDescriptor, data, rid);
        if (rc) {
            LOG(ERROR) << "insert record err" << "@RelationManager::insertTuple" << std::endl;
            return RC(RM_ERROR::TUPLE_INSERT_FAIL);
        }

        // insert all associated index
        insertIndex(tableName, data, recordDescriptor, rid);

        rc = rbfm.closeFile(thisFile);
        if (rc) {
            LOG(ERROR) << "close file err" << "@RelationManager::insertTuple" << std::endl;
            return RC(RM_ERROR::FILE_CLOSE_FAIL);
        }
        return SUCCESS;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        RC rc;
        if (isTableNameEmpty(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_EMPTY);
        }

        if (!isTableAccessible(tableName)) {
            return RC(RM_ERROR::TABLE_ACCESS_DENIED);
        }

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle thisFile;
        // table name is file name in this system
        rc = rbfm.openFile(tableName, thisFile);
        if (rc) {
            LOG(ERROR) << "delete tuple err" << "@RelationManager::deleteTuple" << std::endl;
            return RC(RM_ERROR::FILE_OPEN_FAIL);
        }
        std::vector<Attribute> recordDescriptor;
        rc = getAttributes(tableName, recordDescriptor);
        if (rc) {
            return RC(RM_ERROR::DESCRIPTOR_GET_FAIL);
        }
        uint8_t buffer[PAGE_SIZE];
        rc = rbfm.readRecord(thisFile, recordDescriptor,rid,buffer);
        if (rc)return rc;
        rc = deleteIndex(tableName, buffer, recordDescriptor, const_cast<RID &>(rid));
        if (rc)return rc;
        rc = rbfm.deleteRecord(thisFile, recordDescriptor, rid);
        if (rc) {
            LOG(ERROR) << "insert record err" << "@RelationManager::deleteTuple" << std::endl;
            return RC(RM_ERROR::TUPLE_DEL_FAIL);
        }
        rc = rbfm.closeFile(thisFile);
        if (rc) {
            LOG(ERROR) << "close file err" << "@RelationManager::deleteTuple" << std::endl;
            return RC(RM_ERROR::FILE_CLOSE_FAIL);
        }
        return SUCCESS;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        RC rc;
        if (isTableNameEmpty(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_EMPTY);
        }

        if (!isTableAccessible(tableName)) {
            return RC(RM_ERROR::TABLE_ACCESS_DENIED);
        }

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle thisFile;
        // table name is file name in this system
        rc = rbfm.openFile(tableName, thisFile);
        if (rc) {
            LOG(ERROR) << "open file err" << "@RelationManager::updateTuple" << std::endl;
            return RC(RM_ERROR::FILE_OPEN_FAIL);
        }
        std::vector<Attribute> recordDescriptor;
        rc = getAttributes(tableName, recordDescriptor);
        if (rc) {
            return RC(RM_ERROR::DESCRIPTOR_GET_FAIL);
        }
        rc = rbfm.updateRecord(thisFile, recordDescriptor, data, rid);
        if (rc) {
            LOG(ERROR) << "update record err" << "@RelationManager::updateTuple" << std::endl;
            return RC(RM_ERROR::TUPLE_UPDATE_FAIL);
        }
        uint8_t oldData[PAGE_SIZE];
        rc = rbfm.readRecord(thisFile, recordDescriptor,rid,oldData);
        if (rc)return rc;
        rc = updateIndex(tableName, oldData, data, recordDescriptor, const_cast<RID &>(rid));
        if (rc)return rc;

        rc = rbfm.closeFile(thisFile);
        if (rc) {
            LOG(ERROR) << "close file err" << "@RelationManager::updateTuple" << std::endl;
            return RC(RM_ERROR::FILE_CLOSE_FAIL);
        }
        return SUCCESS;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        RC rc;
        if (isTableNameEmpty(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_EMPTY);
        }

        if (!isTableAccessible(tableName)) {
            return RC(RM_ERROR::TABLE_ACCESS_DENIED);
        }

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle thisFile;
        // table name is file name in this system
        rc = rbfm.openFile(tableName, thisFile);
        if (rc) {
            LOG(ERROR) << "open file err" << "@RelationManager::readTuple" << std::endl;
            return RC(RM_ERROR::FILE_OPEN_FAIL);
        }
        std::vector<Attribute> recordDescriptor;
        rc = getAttributes(tableName, recordDescriptor);
        if (rc) {
            return RC(RM_ERROR::DESCRIPTOR_GET_FAIL);
        }
        rc = rbfm.readRecord(thisFile, recordDescriptor, rid, data);
        if (rc) {
            LOG(ERROR) << "read record err" << "@RelationManager::readTuple" << std::endl;
            return RC(RM_ERROR::TUPLE_READ_FAIL);
        }
        rc = rbfm.closeFile(thisFile);
        if (rc) {
            LOG(ERROR) << "close file err" << "@RelationManager::readTuple" << std::endl;
            return RC(RM_ERROR::FILE_CLOSE_FAIL);
        }
        return SUCCESS;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        RC rc = rbfm.printRecord(attrs, data, out);
        if (rc) {
            LOG(ERROR) << "print record err" << "@RelationManager::printTuple" << std::endl;
            return RC(RM_ERROR::TUPLE_PRINT_FAIL);
        }
        return SUCCESS;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        RC rc;
        if (isTableNameEmpty(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_EMPTY);
        }

        if (!isTableAccessible(tableName)) {
            return RC(RM_ERROR::TABLE_ACCESS_DENIED);
        }

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle thisFile;
        // table name is file name in this system
        rc = rbfm.openFile(tableName, thisFile);
        if (rc) {
            LOG(ERROR) << "open file err" << "@RelationManager::readAttribute" << std::endl;
            return RC(RM_ERROR::FILE_OPEN_FAIL);
        }
        std::vector<Attribute> recordDescriptor;
        rc = getAttributes(tableName, recordDescriptor);
        if (rc) {
            return RC(RM_ERROR::DESCRIPTOR_GET_FAIL);
        }
        rc = rbfm.readAttribute(thisFile, recordDescriptor, rid, attributeName, data);
        if (rc) {
            LOG(ERROR) << "read attr err" << "@RelationManager::readAttribute" << std::endl;
            return RC(RM_ERROR::TUPLE_ATTR_READ_FAIL);
        }
        rc = rbfm.closeFile(thisFile);
        if (rc) {
            LOG(ERROR) << "close file err" << "@RelationManager::readAttribute" << std::endl;
            return RC(RM_ERROR::FILE_CLOSE_FAIL);
        }
        return SUCCESS;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        RC rc;
        if (isTableNameEmpty(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_EMPTY);
        }

        std::vector<Attribute> attrs;
        rc = getAttributes(tableName, attrs);
        if (rc) {
            LOG(ERROR) << "Fail to get meta data @ RelationManager::scan" << std::endl;
            return RC(RM_ERROR::DESCRIPTOR_GET_FAIL);
        }
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fh;
        rc = rbfm.openFile(tableName, fh);
        if (rc) {
            return RC(RM_ERROR::FILE_OPEN_FAIL);
        }

        rc = rm_ScanIterator.begin(fh, attrs, conditionAttribute, compOp, value, attributeNames);
        if (rc) {
            return RC(RM_ERROR::ITERATOR_BEGIN_FAIL);
        }

        return SUCCESS;
    }

    // newID = maxID + 1
    int32_t RelationManager::getNewTableID() {
        RC rc = 0;
        int32_t newID = INVALID_TABLEID;

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        RBFM_ScanIterator tablesIterator;
        std::vector<std::string> selectedAttrNames = {CATALOG_TABLES_TABLEID};
        rc = openCatalog();
        if (rc) {
            return RC(RM_ERROR::CATALOG_OPEN_FAIL);
        }
        rc = rbfm.scan(fhTables, catalogTablesSchema, CATALOG_TABLES_TABLEID,
                       GT_OP, &INVALID_TABLEID, selectedAttrNames, tablesIterator);
        if (rc) {
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        RID curRID;
        uint8_t rawData[PAGE_SIZE];
        while (tablesIterator.getNextRecord(curRID, rawData) != RBFM_EOF) {
            int32_t tmpTableID;
            memcpy(&tmpTableID, rawData + 1, sizeof(int32_t));
            newID = std::max(newID, tmpTableID);
        }
        if (newID == INVALID_TABLEID) {
            LOG(ERROR) << "generate new table ID err @ RelationManager::getNewTableID" << std::endl;
            return INVALID_TABLEID;
        }
        return newID + 1;
    }

    bool RelationManager::isTableNameEmpty(const std::string name) {
        return name.empty();
    }

    bool RelationManager::isTableIdValid(int32_t tableID) {
        return tableID != INVALID_TABLEID;
    }

    bool RelationManager::isTableAccessible(const std::string name) {
        return name != CATALOG_TABLES && name != CATALOG_COLUMNS && name != CATALOG_INDEXES;
    }

    bool RelationManager::isCatalogReady() {
        PagedFileManager &pfm = PeterDB::PagedFileManager::instance();
        if (!pfm.isFileExists(CATALOG_TABLES)) {
            LOG(ERROR) << "CATALOG_TABLES does not exist" << "@RelationManager::isCatalogReady" << std::endl;
            return false;
        }
        if (!pfm.isFileExists(CATALOG_COLUMNS)) {
            LOG(ERROR) << "CATALOG_COLUMNS does not exist" << "@RelationManager::isCatalogReady" << std::endl;
            return false;
        }
        if (!pfm.isFileExists(CATALOG_INDEXES)) {
            LOG(ERROR) << "CATALOG_INDEXES does not exist" << "@RelationManager::isCatalogReady" << std::endl;
            return false;
        }
        return true;
    }

    void RelationManager::scanIteratorValue(const std::string value, uint8_t *scanVal) {
        int32_t strLen = value.length();
        memcpy(scanVal, &strLen, sizeof(int32_t));
        // ignore the /0
        memcpy(scanVal + sizeof(int32_t), value.c_str(), strLen);
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
        RC rc;
        if (isTableNameEmpty(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_EMPTY);
        }
        if (!isTableAccessible(tableName)) {
            return RC(RM_ERROR::TABLE_ACCESS_DENIED);
        }

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        IndexManager &ix = IndexManager::instance();
        // 1. create index file
        std::string ixFileName = getIndexFileName(tableName, attributeName);
        rc = ix.createFile(ixFileName);
        if (rc) {
            LOG(ERROR) << "Fail to create table's file! @ RelationManager::createIndex" << std::endl;
            return rc;
        }
        // 2. update catalog
        rc = openCatalog();
        if (rc) {
            return RC(RBFM_ERROR::CATALOG_NOT_OPEN);
        }
        CatalogTablesHelper tableRecord;
        rc = getTableMetaData(tableName, tableRecord);
        if (rc) return rc;
        rc = insertIndexToCatalog(tableRecord.table_id, attributeName, ixFileName);
        if (rc) return rc;
        // 3. build index
        rc = buildIndex(tableName, ixFileName, attributeName);
        if (rc) return rc;
        return SUCCESS;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
        RC rc;
        if (isTableNameEmpty(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_EMPTY);
        }
        if (!isTableAccessible(tableName)) {
            return RC(RM_ERROR::TABLE_ACCESS_DENIED);
        }
        IndexManager &ix = IndexManager::instance();

        CatalogTablesHelper tableRecord;
        rc = getTableMetaData(tableName, tableRecord);
        if (rc) return rc;
        // Find all indexes associated with this table and corresponding file names
        std::unordered_map<std::string, std::string> indexedAttrAndFileName;
        rc = getIndexes(tableName, indexedAttrAndFileName);
        if (rc) return rc;
        if (indexedAttrAndFileName.find(attributeName) == indexedAttrAndFileName.end()) {
            return RC(RM_ERROR::INDEX_NOT_EXIST);
        }
        std::string ixFileName = indexedAttrAndFileName[attributeName];

        //  Delete index from catalog
        rc = deleteIndexFromCatalog(tableRecord.table_id, attributeName);
        if (rc) return rc;

        // 4. Delete index file
        rc = ix.destroyFile(ixFileName);
        if (rc) return rc;

        return 0;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                                  const std::string &attributeName,
                                  const void *lowKey,
                                  const void *highKey,
                                  bool lowKeyInclusive,
                                  bool highKeyInclusive,
                                  RM_IndexScanIterator &rm_IndexScanIterator) {
        if (isTableNameEmpty(tableName)) {
            return RC(RM_ERROR::TABLE_NAME_EMPTY);
        }

        if (!isTableAccessible(tableName)) {
            return RC(RM_ERROR::TABLE_ACCESS_DENIED);
        }
        RC ret = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        IndexManager& ix = IndexManager::instance();

        std::vector<Attribute> attrs;
        RC rc = getAttributes(tableName, attrs);
        if (rc) return rc;
        // get indexed field pos
        int32_t attr_pos = 0;
        for (; attr_pos < attrs.size(); attr_pos++) {
            if (attrs[attr_pos].name == attributeName)break;
        }
        assert(attr_pos < attrs.size());

        std::unordered_map<std::string, std::string> indexedAttrAndFileName;
        ret = getIndexes(tableName, indexedAttrAndFileName);
        if(ret) return ret;
        if(indexedAttrAndFileName.find(attributeName) == indexedAttrAndFileName.end()) return RC(RM_ERROR::INDEX_NOT_EXIST);
        std::string ixFileName = indexedAttrAndFileName[attributeName];

        IXFileHandle* ixScanFH = new IXFileHandle;
        ret = ix.openFile(ixFileName, *ixScanFH);
        if(ret) return ret;

        ixScanFHList.push_back(ixScanFH);
        ret = rm_IndexScanIterator.open(ixScanFH, attrs[attr_pos], (uint8_t *) lowKey, (uint8_t *) highKey, lowKeyInclusive,
                                  highKeyInclusive);
        if (ret)return ret;
        return SUCCESS;
    }

    std::string RelationManager::getIndexFileName(const std::string &tableName, const std::string &attrName) {
        return tableName + '_' + attrName + ".idx";
    }

    RC RelationManager::insertIndexToCatalog(const int32_t tableID, const std::string &attrName,
                                             const std::string &fileName) {
        RC rc;
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        // open 3 files
        rc = openCatalog();
        if (rc) {
            return RC(RM_ERROR::CATALOG_OPEN_FAIL);
        }
        // contruct rawdata
        uint8_t data[PAGE_SIZE];
        memset(data, 0, PAGE_SIZE);
        auto rawData = (RawRecord *) data;
        RID rid;
        int32_t attrNameLen = attrName.size();
        int32_t fileNameLen = fileName.size();
        rawData->initNullByte(catalogIndexesSchema.size());
        auto dest = rawData->dataSection(catalogIndexesSchema.size());
        memcpy(dest, &tableID, sizeof(int32_t));
        memcpy((uint8_t *) dest + sizeof(int32_t), &attrNameLen, sizeof(int32_t));
        memcpy((uint8_t *) dest + 2 * sizeof(int32_t), &attrName, attrNameLen);
        memcpy((uint8_t *) dest + 2 * sizeof(int32_t) + attrNameLen, &fileNameLen, sizeof(int32_t));
        memcpy((uint8_t *) dest + 3 * sizeof(int32_t) + attrNameLen, &fileName, fileNameLen);

        // insert to tables table
        rc = rbfm.insertRecord(fhIndexes, catalogIndexesSchema, rawData, rid);
        if (rc) {
            LOG(ERROR) << "insert record err" << "@RelationManager::insertMetaDataToCatalog" << std::endl;
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        return SUCCESS;
    }

    RC RelationManager::getTableMetaData(const std::string &tableName, CatalogTablesHelper &tableRecord) {
        RC rc;
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        RBFM_ScanIterator tablesIterator;
        std::vector<std::string> selectedAttrNames = {CATALOG_TABLES_TABLEID, CATALOG_TABLES_TABLENAME,
                                                      CATALOG_TABLES_FILENAME};
        rc = openCatalog();
        if (rc) {
            return RC(RM_ERROR::CATALOG_OPEN_FAIL);
        }
        rc = rbfm.scan(fhTables, catalogTablesSchema, CATALOG_TABLES_TABLENAME,
                       EQ_OP, &tableName, selectedAttrNames, tablesIterator);
        if (rc) {
            return RC(RM_ERROR::ERR_UNDEFINED);
        }
        RID curRID;
        int16_t recLen;
        uint8_t rawData[PAGE_SIZE];
        uint8_t buffer[PAGE_SIZE];;
        auto record = (Record *) buffer;
        bool isRecordExist = false;
        while (tablesIterator.getNextRecord(curRID, rawData) != RBFM_EOF) {
            record->fromRawRecord((RawRecord *) rawData, tablesIterator.getRecordDescriptor(),
                                  tablesIterator.getSelectedAttrIdx(), recLen);
            if (record->getField<std::string>(1) == tableName) {
                tableRecord.table_id = record->getField<int32_t>(0);
                tableRecord.table_name = record->getField<std::string>(1);
                tableRecord.file_name = record->getField<std::string>(2);
                isRecordExist = true;
                break;
            }
        }
        if (!isRecordExist) {
            return RC(RM_ERROR::RECORD_NOT_EXIST);
        }
        return SUCCESS;
    }

    RC RelationManager::buildIndex(const std::string &tableName, const std::string &ixName,
                                   const std::string &attributeName) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        IndexManager &ix = IndexManager::instance();
        FileHandle fh;
        // get Attribute
        std::vector<Attribute> attrs;
        RC rc = getAttributes(tableName, attrs);
        if (rc) return rc;
        // get indexed field pos
        int32_t attr_pos = 0;
        for (; attr_pos < attrs.size(); attr_pos++) {
            if (attrs[attr_pos].name == attributeName)break;
        }
        assert(attr_pos < attrs.size());

        rc = rbfm.openFile(tableName, fh);
        if (rc) return rc;
        RBFM_ScanIterator tableIterator;
        std::vector<std::string> projectAttrNames = {attrs[attr_pos].name};
        rc = rbfm.scan(fh, attrs, "",
                       NO_OP, nullptr, projectAttrNames, tableIterator);
        if (rc) { return RC(RM_ERROR::ITERATOR_BEGIN_FAIL); }

        RID curRID;
        int16_t recLen;
        uint8_t rawData[PAGE_SIZE];
        uint8_t buffer[PAGE_SIZE];
        uint8_t keyData[PAGE_SIZE];
        IXFileHandle ixFileHandle;
        auto record = (Record *) buffer;
        rc = ix.openFile(ixName, ixFileHandle);
        if (rc) return rc;
        while (tableIterator.getNextRecord(curRID, rawData) != RBFM_EOF) {
            record->fromRawRecord((RawRecord *) rawData, attrs, tableIterator.getSelectedAttrIdx(), recLen);
            record->getField<uint8_t>(attr_pos);
            memcpy(keyData, record->getFieldPtr<uint8_t>(attr_pos), attrs[attr_pos].length);
            ix.insertEntry(ixFileHandle, attrs[attr_pos], (void *) keyData, curRID);
        }

        return SUCCESS;
    }

    RC RelationManager::getIndexes(const std::string &tableName,
                                   std::unordered_map<std::string, std::string> &indexedAttrAndFileName) {
        RC rc = 0;
        rc = openCatalog();
        if (rc) {
            return RC(RM_ERROR::CATALOG_OPEN_FAIL);
        }
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        CatalogTablesHelper tablesRecord;
        rc = getTableMetaData(tableName, tablesRecord);
        if (rc)return rc;

        RBFM_ScanIterator IdxIterator;
        std::vector<std::string> projectAttrNames = {CATALOG_INDEXES_ATTRNAME, CATALOG_INDEXES_FILENAME};
        rc = rbfm.scan(fhIndexes, catalogIndexesSchema, CATALOG_INDEXES_TABLEID,
                       EQ_OP, &tablesRecord.table_id, projectAttrNames, IdxIterator);
        if (rc) { return RC(RM_ERROR::ITERATOR_BEGIN_FAIL); }

        RID curRID;
        uint8_t rawData[PAGE_SIZE];
        while (IdxIterator.getNextRecord(curRID, rawData) == 0) {
            CatalogIndexesHelper curIndex(rawData, projectAttrNames);
            indexedAttrAndFileName[curIndex.attr_name] = curIndex.file_name;
        }

        return SUCCESS;
    }

    RC RelationManager::insertIndex(const std::string &tableName, const void *data,
                                    const std::vector<Attribute> recordDescriptor, RID &rid) {
        RC ret;
        // update all associated index
        IndexManager &ix = IndexManager::instance();
        std::unordered_map<std::string, std::string> indexedAttrAndFileName;
        ret = getIndexes(tableName, indexedAttrAndFileName);
        if (ret) return ret;
        auto rawData = (RawRecord *) data;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            if (rawData->isNullField(i)) continue;
            if (indexedAttrAndFileName.find(recordDescriptor[i].name) != indexedAttrAndFileName.end()) {
                // insert entry into each index
                // update ixFHMap
                if (ixFHMap.find(indexedAttrAndFileName[recordDescriptor[i].name]) == ixFHMap.end()) {
                    IXFileHandle *fh = new IXFileHandle;
                    ret = ix.openFile(indexedAttrAndFileName[recordDescriptor[i].name], *fh);
                    if (ret) return ret;
                    ixFHMap[indexedAttrAndFileName[recordDescriptor[i].name]] = fh;
                }
                auto key = rawData->getFieldPtr<uint8_t>(recordDescriptor, recordDescriptor[i].name);
                ret = ix.insertEntry(*ixFHMap[indexedAttrAndFileName[recordDescriptor[i].name]], recordDescriptor[i],
                                     key, rid);
                if (ret) return ret;
            }
        }
        return SUCCESS;
    }

    RC RelationManager::deleteIndex(const std::string &tableName, const void *data,
                                    const std::vector<Attribute> recordDescriptor,  RID &rid) {
        RC ret;
        // update all associated index
        IndexManager &ix = IndexManager::instance();
        std::unordered_map<std::string, std::string> indexedAttrAndFileName;
        ret = getIndexes(tableName, indexedAttrAndFileName);
        if (ret) return ret;
        auto rawData = (RawRecord *) data;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            if (rawData->isNullField(i)) continue;
            if (indexedAttrAndFileName.find(recordDescriptor[i].name) != indexedAttrAndFileName.end()) {
                // insert entry into each index
                // update ixFHMap
                if (ixFHMap.find(indexedAttrAndFileName[recordDescriptor[i].name]) == ixFHMap.end()) {
                    IXFileHandle *fh = new IXFileHandle;
                    ret = ix.openFile(indexedAttrAndFileName[recordDescriptor[i].name], *fh);
                    if (ret) return ret;
                    ixFHMap[indexedAttrAndFileName[recordDescriptor[i].name]] = fh;
                }
                auto key = rawData->getFieldPtr<uint8_t>(recordDescriptor, recordDescriptor[i].name);
                ret = ix.deleteEntry(*ixFHMap[indexedAttrAndFileName[recordDescriptor[i].name]], recordDescriptor[i],
                                     key, rid);
                if (ret) return ret;
            }
        }
        return SUCCESS;
    }

    RC RelationManager::updateIndex(const std::string &tableName, const void *oldData, const void *newData,
                                    const std::vector<Attribute> recordDescriptor, RID &rid) {
        RC ret;
        // delete and re-insert
        IndexManager &ix = IndexManager::instance();
        std::unordered_map<std::string, std::string> indexedAttrAndFileName;
        ret = getIndexes(tableName, indexedAttrAndFileName);
        if (ret) return ret;
        auto oldRawData = (RawRecord *) oldData;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            if (oldRawData->isNullField(i)) continue;
            if (indexedAttrAndFileName.find(recordDescriptor[i].name) != indexedAttrAndFileName.end()) {
                // insert entry into each index
                // update ixFHMap
                if (ixFHMap.find(indexedAttrAndFileName[recordDescriptor[i].name]) == ixFHMap.end()) {
                    IXFileHandle *fh = new IXFileHandle;
                    ret = ix.openFile(indexedAttrAndFileName[recordDescriptor[i].name], *fh);
                    if (ret) return ret;
                    ixFHMap[indexedAttrAndFileName[recordDescriptor[i].name]] = fh;
                }
                auto key = oldRawData->getFieldPtr<uint8_t>(recordDescriptor, recordDescriptor[i].name);
                ret = ix.deleteEntry(*ixFHMap[indexedAttrAndFileName[recordDescriptor[i].name]], recordDescriptor[i],
                                     key, rid);
                if (ret) return ret;
            }
        }

        auto newRawData = (RawRecord *) newData;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            if (newRawData->isNullField(i)) continue;
            if (indexedAttrAndFileName.find(recordDescriptor[i].name) != indexedAttrAndFileName.end()) {
                // insert entry into each index
                // update ixFHMap
                if (ixFHMap.find(indexedAttrAndFileName[recordDescriptor[i].name]) == ixFHMap.end()) {
                    IXFileHandle *fh = new IXFileHandle;
                    ret = ix.openFile(indexedAttrAndFileName[recordDescriptor[i].name], *fh);
                    if (ret) return ret;
                    ixFHMap[indexedAttrAndFileName[recordDescriptor[i].name]] = fh;
                }
                auto key = newRawData->getFieldPtr<uint8_t>(recordDescriptor, recordDescriptor[i].name);
                ret = ix.insertEntry(*ixFHMap[indexedAttrAndFileName[recordDescriptor[i].name]], recordDescriptor[i],
                                     key, rid);
                if (ret) return ret;
            }
        }
        return SUCCESS;
    }


} // namespace PeterDB