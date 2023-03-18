#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include <limits>
#include <climits>

#include "rm.h"
#include "ix.h"

namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data;             // value
    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;

    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual ~Iterator() = default;
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr : attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return 0;
        };

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = NULL) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        };

        RC getNextTuple(void *data) override {
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;


            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
        };

        ~IndexScan() override {
            iter.close();
        };
    };

    class Filter : public Iterator {
    private:
        Iterator *iter;
        Condition cond;
        std::vector<Attribute> attrs;
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        bool isRecordMeetCondition(void *data);
    };

    class Project : public Iterator {
        // Projection operator
        Iterator *iter;
        std::vector<Attribute> attrs;
        // idx means the idx in attrs;
        std::vector<int> projectAttrIdx;
        uint8_t inputRawData[PAGE_SIZE];
    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override;

        RC getNextTuple(void *data) override;

        RC ProjectToSelectedAttr(void *data);

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class BNLJoin : public Iterator {
        // Block nested-loop join operator
        Iterator* outer;
        TableScan* inner;
        Condition cond;
        uint32_t hashTableMaxSize, remainSize;
        // if has a pointer points to a linked list with a matched key
        bool hasPointer;
        int32_t matchedIntKey;
        float matchedFloatKey;
        std::string matchedStrKey;
        int32_t hashLinkedListPos = INT16_MAX;

        std::vector<Attribute> outerAttrs, innerAttrs;
        std::vector<Attribute> joinedAttrs;
        Attribute outerJoinAttr;
        Attribute innerJoinAttr;

        uint8_t innerReadBuffer[PAGE_SIZE];
        uint8_t outerReadBuffer[PAGE_SIZE];

        // for outer table block load
        std::unordered_map<int32_t, std::vector<std::vector<uint8_t>>> intHash;
        std::unordered_map<float, std::vector<std::vector<uint8_t>>> floatHash;
        std::unordered_map<std::string, std::vector<std::vector<uint8_t>>> strHash;

    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override;

        RC getNextTuple(void *data) override;

        RC loadBlock();

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class INLJoin : public Iterator {
        // Index nested-loop join operator
        Iterator* outer;
        IndexScan* inner;
        Condition cond;
        int16_t outerIterStatus;

        std::vector<Attribute> outerAttrs, innerAttrs;
        std::vector<Attribute> joinedAttrs;
        Attribute outerJoinAttr;
        Attribute innerJoinAttr;

        uint8_t outerReadBuffer[PAGE_SIZE];
        uint8_t innerReadBuffer[PAGE_SIZE];

        uint8_t *outerKey;
        uint8_t *innerKey;

    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Aggregate : public Iterator {
        // Aggregation operator
        Iterator* input;
        Attribute aggAttr;
        AggregateOp op;
        Attribute groupAttr;
        bool isGroup = false;

        std::vector<Attribute> inputAttrs;
        uint8_t readBuffer[PAGE_SIZE];

        std::vector<float> result;
        int32_t result_pos;

        std::unordered_map<int32_t, std::pair<int32_t, float>> intHash;
        std::vector<std::pair<int32_t, float>> intResult;
        std::unordered_map<float, std::pair<int32_t, float>> floatHash;
        std::vector<std::pair<float, float>> floatResult;
        std::unordered_map<std::string, std::pair<int32_t, float>> strHash;
        std::vector<std::pair<std::string, float>> strResult;

    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override;

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class QEHelper {
    public:
        static bool isSameKey(uint8_t* key1, uint8_t* key2, AttrType& type){
            switch (type) {
                case TypeInt:
                    return memcmp(key1, key2, sizeof(int32_t)) == 0;
                case TypeReal:
                    return memcmp(key1, key2, sizeof(float)) == 0;
                case TypeVarChar:
                    int32_t strLen1, strLen2;
                    strLen1 = *(int32_t *)key1;
                    strLen2 = *(int32_t *)key2;
                    return strLen1 == strLen2 && memcmp(key1, key2, sizeof(int32_t) + strLen1) == 0;
            }
            return false;
        }

        template<typename T>
        static bool performOper(const T& oper1, const T& oper2, CompOp op) {
            switch(op) {
                case EQ_OP: return oper1 == oper2;
                case LT_OP: return oper1 < oper2;
                case LE_OP: return oper1 <= oper2;
                case GT_OP: return oper1 > oper2;
                case GE_OP: return oper1 >= oper2;
                case NE_OP: return oper1 != oper2;
                default:
                    LOG(ERROR) << "Comparison Operator Not Supported!" << std::endl;
                    return false;
            }
        }
    };
} // namespace PeterDB


#endif // _qe_h_
