#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstdio>
#include <typeinfo>
#include <cstring>
#include <glog/logging.h>
#include <math.h>
#include <unordered_map>

#include "src/include/rbfm.h"
#include "ix.h"

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator

    const std::string CATALOG_TABLES = "Tables";
    const std::string CATALOG_COLUMNS = "Columns";
    const std::string CATALOG_INDEXES = "Indexes";

    const int CATALOG_VARCHAR_LEN = 50;
    const int32_t CATALOG_TABLES_ATTR_NUM = 3;
    const std::string CATALOG_TABLES_TABLEID = "table-id";
    const std::string CATALOG_TABLES_TABLENAME = "table-name";
    const std::string CATALOG_TABLES_FILENAME = "file-name";

    const int32_t CATALOG_COLUMNS_ATTR_NUM = 5;
    const std::string CATALOG_COLUMNS_TABLEID = "table-id";
    const std::string CATALOG_COLUMNS_COLUMNNAME = "column-name";
    const std::string CATALOG_COLUMNS_COLUMNTYPE = "column-type";
    const std::string CATALOG_COLUMNS_COLUMNLENGTH = "column-length";
    const std::string CATALOG_COLUMNS_COLUMNPOS = "column-position";

    const int32_t CATALOG_INDEXES_ATTR_NUM = 3;
    const std::string CATALOG_INDEXES_TABLEID = "table-id";
    const std::string CATALOG_INDEXES_ATTRNAME = "attribute-name";
    const std::string CATALOG_INDEXES_FILENAME = "file-name";

    const int32_t DEFAULT_COLUMN_INT = -1;
    const int32_t INVALID_TABLEID = -1;


    const std::vector<Attribute> catalogTablesSchema = std::vector<Attribute>{
            Attribute{CATALOG_TABLES_TABLEID, TypeInt, sizeof(int32_t)},
            Attribute{CATALOG_TABLES_TABLENAME, TypeVarChar, CATALOG_VARCHAR_LEN},
            Attribute{CATALOG_TABLES_FILENAME, TypeVarChar, CATALOG_VARCHAR_LEN}
    };
    const std::vector<Attribute> catalogColumnsSchema = std::vector<Attribute>{
            Attribute{CATALOG_COLUMNS_TABLEID, TypeInt, sizeof(int32_t)},
            Attribute{CATALOG_COLUMNS_COLUMNNAME, TypeVarChar, CATALOG_VARCHAR_LEN},
            Attribute{CATALOG_COLUMNS_COLUMNTYPE, TypeInt, sizeof(int32_t)},
            Attribute{CATALOG_COLUMNS_COLUMNLENGTH, TypeInt, sizeof(int32_t)},
            Attribute{CATALOG_COLUMNS_COLUMNPOS, TypeInt, sizeof(int32_t)},
    };

    const std::vector<Attribute> catalogIndexesSchema = std::vector<Attribute>{
            Attribute{CATALOG_INDEXES_TABLEID, TypeInt, sizeof(int32_t)},
            Attribute{CATALOG_INDEXES_ATTRNAME, TypeVarChar, CATALOG_VARCHAR_LEN},
            Attribute{CATALOG_INDEXES_FILENAME, TypeVarChar, CATALOG_VARCHAR_LEN}
    };


    class CatalogTablesHelper {
    public:
        int32_t table_id;
        std::string table_name;
        std::string file_name;
        FileHandle fh;

        CatalogTablesHelper();    // Constructor
        CatalogTablesHelper(int32_t id, const std::string &name, const std::string &file);

        CatalogTablesHelper(FileHandle &fileHandle);    // Constructor

        ~CatalogTablesHelper();    // Destructor

        RC getRecordRawData(uint8_t *apiData);

        int32_t getTableID(const std::string &tableName);
    };

    class CatalogIndexesHelper {
    public:
        int32_t table_id;
        std::string attr_name;
        std::string file_name;

        CatalogIndexesHelper();
        CatalogIndexesHelper(uint8_t* rawData, const std::vector<std::string>& attrNames);

        ~CatalogIndexesHelper() = default;


    };

    class CatalogColumnsHelper {
    public:
        int32_t table_id;
        std::string column_name;
        int32_t column_type;
        int32_t column_length;
        int32_t column_position;

        CatalogColumnsHelper();    // Constructor
        CatalogColumnsHelper(int32_t id, const std::string &name, int32_t column_type, int32_t column_length,
                             int32_t column_position);

        CatalogColumnsHelper(uint8_t *rawData, const std::vector<std::string> &attrNames);
        ~CatalogColumnsHelper();    // Destructor

        RC getRecordRawData(uint8_t *apiData);
        Attribute getAttribute();
    };

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    private:
        RBFM_ScanIterator rbfmIterator;
    public:
        RM_ScanIterator();
        ~RM_ScanIterator();

        RC begin(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                 const std::string &conditionAttribute, const CompOp compOp, const void *value,
                 const std::vector<std::string> &selectedAttrNames);
        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();
    };

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    private:
        IX_ScanIterator ixIterator;
    public:
        RM_IndexScanIterator() = default;    // Constructor
        ~RM_IndexScanIterator() = default;    // Destructor

       RC open(IXFileHandle* ixFileHandle, const Attribute& attr,
        const uint8_t* lowKey, const uint8_t* highKey,
        bool lowKeyInclusive, bool highKeyInclusive);
        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan
    };

    // Relation Manager
    class RelationManager {
    private:
        FileHandle fhTables;
        FileHandle fhColumns;
        FileHandle fhIndexes;

        std::vector<IXFileHandle*> ixScanFHList;
        std::unordered_map<std::string, IXFileHandle*> ixFHMap;

        bool isTableNameEmpty(const std::string name);
        bool isTableIdValid(int32_t tableID);
        bool isTableAccessible(const std::string name);
        int32_t getNewTableID();

        bool isCatalogReady();
        RC insertIndexToCatalog(const int32_t tableID, const std::string& attrName, const std::string& fileName);
        RC insertMetaDataToCatalog(const std::string &tableName, const std::vector<Attribute> schema);
        RC getTableMetaData(const std::string& tableName, CatalogTablesHelper& tableRecord);
        RC deleteMetaDataFromCatalog(int32_t tableID);
        RC deleteIndexFromCatalog(int32_t tableID, std::string attrName, bool all);

        std::string getIndexFileName(const std::string& tableName, const std::string& attrName);
        RC getIndexes(const std::string& tableName, std::unordered_map<std::string, std::string>& indexedAttrAndFileName);
        RC buildIndex(const std::string &tableName, const std::string &ixName, const std::string &attributeName);
        RC insertIndex(const std::string &tableName, const void *data, const std::vector<Attribute>recordDescriptor, RID &rid);
        RC deleteIndex(const std::string &tableName, const void *data, const std::vector<Attribute>recordDescriptor, RID &rid);
        RC updateIndex(const std::string &tableName, const void *oldData, const void *newData, const std::vector<Attribute>recordDescriptor, RID &rid);

    public:
        static RelationManager &instance();
        static void scanIteratorValue(const std::string value,  uint8_t *scanVal);

        RC createCatalog();
        RC openCatalog();
        RC deleteCatalog();
        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);
        RC deleteTable(const std::string &tableName);
        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);
        RC insertTuple(const std::string &tableName, const void *data, RID &rid);
        RC deleteTuple(const std::string &tableName, const RID &rid);
        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);
        RC readTuple(const std::string &tableName, const RID &rid, void *data);
        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);
        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);
        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attributeName);
        RC destroyIndex(const std::string &tableName, const std::string &attributeName);
        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

    };

} // namespace PeterDB

#endif // _rm_h_