#ifndef _rbfm_h_
#define _rbfm_h_

#include <vector>
#include <unordered_set>

#include "pfm.h"
#include <iostream>

namespace PeterDB {
    // Record ID
    typedef struct {
        unsigned pageNum;           // page number
        unsigned short slotNum;     // slot number in the page
    } RID;

    // Attribute
    typedef enum {
        TypeInt = 0, TypeReal, TypeVarChar
    } AttrType;

    typedef unsigned AttrLength;

    typedef struct Attribute {
        std::string name;  // attribute name
        AttrType type;     // attribute type
        AttrLength length; // attribute length
    } Attribute;

    // Comparison Operator (NOT needed for part 1 of the project)
    typedef enum {
        EQ_OP = 0, // no condition// =
        LT_OP,      // <
        LE_OP,      // <=
        GT_OP,      // >
        GE_OP,      // >=
        NE_OP,      // !=
        NO_OP       // no condition
    } CompOp;


    /********************************************************************
    * The scan iterator is NOT required to be implemented for Project 1 *
    ********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

    //  RBFM_ScanIterator is an iterator to go through records
    //  The way to use it is like the following:
    //  RBFM_ScanIterator rbfmScanIterator;
    //  rbfm.open(..., rbfmScanIterator);
    //  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
    //    process the data;
    //  }
    //  rbfmScanIterator.close();


    // type for raw data
    typedef uint32_t RawDataStrLen;
    // type for record
    typedef uint8_t Flag;
    typedef uint8_t PlaceHolder;
    typedef uint16_t AttrNum;
    typedef int16_t AttrDir;

    // type for page
    typedef uint16_t FreeBytePointer;
    typedef uint16_t SlotCounter;
    typedef int16_t SlotOffset;
    typedef int16_t SlotLen;

    // constant value for record
    const Flag RECORD_FLAG_DATA = 0;
    const Flag RECORD_FLAG_POINTER = 1;
    const PlaceHolder RECORD_PLACEHOLDER = 0;

    const SlotOffset SLOT_OFFSET_EMPTY = -1;
    const SlotLen SLOT_LEN_EMPTY = 0;

    const int8_t SHIFT_LEFT = 1;
    const int8_t SHIFT_RIGHT = 2;

    const AttrDir ATTR_DIR_EMPTY = -1;

    const int32_t CONDITION_ATTR_IDX_INVALID = -1;
    const int32_t ATTR_IDX_INVALID = -1;


    class RBFM_ScanIterator {
    private:
        // init local data
        FileHandle fileHandle;
        std::vector<Attribute> recordDescriptor;
        std::vector<std::uint16_t> selectedAttrIdx;

        // current pointer
        int32_t curPageNum;
        int16_t curSlotNum;

        // select condition
        CompOp compOp;
        Attribute conditionAttr;
        // to store which element in recordDescriptor is a condition
        int32_t conditionAttrIdx;
        uint8_t *conditionVal;
        RawDataStrLen conditionStrLen;

        uint8_t recordData[PAGE_SIZE];

        bool compareInt(int a, int b);
        bool compareFloat(float a, float b);
        bool compareStr(std::string a, std::string b);

    public:
        RBFM_ScanIterator();
        ~RBFM_ScanIterator();

        RC begin(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                 const std::string &conditionAttribute, const CompOp compOp, const void *value,
                 const std::vector<std::string> &selectedAttrNames);

        // Never keep the results in the memory. When getNextRecord() is called,
        // a satisfying record needs to be fetched from the file.
        // "data" follows the same format as RecordBasedFileManager::insertRecord().
        RC getNextRecord(RID &rid, void *data);

        // use this method to check if the attribute of current record meet the conditions
        bool recordMeetCondition(uint8_t *attr, int16_t attrLen);

        RC close() { return SUCCESS; };
    };

    class RecordBasedFileManager {
    public:
        static RecordBasedFileManager &instance();                          // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new record-based file

        RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

        RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

        //  Format of the data passed into the function is the following:
        //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
        //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
        //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
        //     Each bit represents whether each field value is null or not.
        //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
        //     If k-th bit from the left is set to 0, k-th field contains non-null values.
        //     If there are more than 8 fields, then you need to find the corresponding byte first,
        //     then find a corresponding bit inside that byte.
        //  2) Actual data is a concatenation of values of the attributes.
        //  3) For Int and Real: use 4 bytes to store the value;
        //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
        //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
        // For example, refer to the Q8 of Project 1 wiki page.

        // Insert a record into a file
        RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        RID &rid);

        // Read a record identified by the given rid.
        RC
        readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);
        // read a record in internal format
        RC readInternalRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                              const RID &rid, void *data, short & recByteLen);

        // Print the record that is passed to this utility method.
        // This method will be mainly used for debugging/testing.
        // The format is as follows:
        // field1-name: field1-value  field2-name: field2-value ... \n
        // (e.g., age: 24  height: 6.1  salary: 9000
        //        age: NULL  height: 7.5  salary: 7500)
        RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data, std::ostream &out);

        RC getAvailablePage(FileHandle &fileHandle, int16_t recLength, PageNum &availablePageNum);

        /*****************************************************************************************************
        * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
        * are NOT required to be implemented for Project 1                                                   *
        *****************************************************************************************************/
        // Delete a record identified by the given rid.
        RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

        // Assume the RID does not change after an update
        RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        const RID &rid);

        // Read an attribute given its name and the rid.
        RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                         const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        RC scan(FileHandle &fileHandle,
                const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RBFM_ScanIterator &rbfm_ScanIterator);

    protected:
        RecordBasedFileManager();                                                   // Prevent construction
        ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
        RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
        RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment

    };

    //slot n|...|slot 1|N|F
    class PageHelper {
    public:
        FileHandle &fh;
        PageNum pageNum;
        FreeBytePointer freeBytePointer;
        SlotCounter slotCounter;
        //page data
        uint8_t dataSeq[PAGE_SIZE] = {};

        bool IsFreeSpaceEnough(int32_t recLength);

        // insert record Data
        RC insertRecordInByte(uint8_t byteSeq[], int16_t recLength, RID &rid, bool setUnoriginal);

        // read record Data
        RC getRecordByte(int16_t slotIndex, uint8_t *byteSeq, int16_t &recLength);

        // read record Pointer
        RC getRecordPointer(int16_t slotIndex, uint32_t &ridPageNum, uint16_t &ridSlotNum);

        // delete a record
        RC deleteRecord(uint16_t slotIndex);

        // update record Data
        RC updateRecord(int16_t slotIndex, uint8_t byteSeq[], int16_t recLength, bool setUnoriginal);

        // update a record to a RID
        RC setRecordPointToNewRID(int16_t curSlotIndex, const RID &newRecordRID, bool setUnoriginal);

        RC getRecordAttr(int16_t slotIndex, int16_t attrIdx, uint8_t *attrVal);

        bool isRecordPointer(int16_t slotIndex);

        bool isRecordData(int16_t slotIndex);

        bool isRecordDeleted(int16_t slotIndex);

        bool isRecordValid(int16_t slotIndex);

        // to indicate if a record is original one;
        bool isOriginal(int16_t slotIndex);

        PageHelper(FileHandle &fileHandle, PageNum pageNum);

        ~PageHelper();

        int16_t getRecordLen(int16_t slotIndex);

        // get next original record with real data, this function will cause slotIndex increase by 1 !!
        RC getNextRecordData(int16_t &slotIndex, uint8_t *byteSeq, int16_t &recordLen);

    private:
        //getter
        int16_t getFlagsLength();

        int16_t getSlotSize();

        int16_t getHeaderLength();

        int16_t getSlotCounterOffset();

        int16_t getFreeBytePointerOffset();

        int16_t getSlotOffset(int16_t slotNum); // start from 1
        // get the present available slot offset and it might change slotCounter
        int16_t getAvlSlotOffsetIdx(); // todo test
        int16_t getRecordBeginPos(int16_t slotIndex);

        int8_t getRecordFlag(int16_t slotIndex);

        int16_t getAttrBeginPos(int16_t slotIndex, int16_t attrIndex);

        int16_t getAttrEndPos(int16_t slotIndex, int16_t attrIndex);

        // setter
        void setRecordFlag(int16_t slotIndex, Flag fg);

        void setRecordOffset(int16_t slotIndex, SlotOffset so);

        void setRecordLen(int16_t slotIndex, SlotLen sl, bool setUnoriginalSign);

        void setFreeBytePointer(FreeBytePointer);

        void setSlotCounter(SlotCounter sc);

        // checker
        bool isAttrNull(int16_t slotIndex, int16_t attrIndex);

        RC shiftRecord(int16_t dataStartPos, int16_t distance, int8_t direction);

        int8_t getRecordAttrNum(int16_t slotIndex);

        int16_t getAttrLen(int16_t slotIndex, int16_t attrIndex);

    };

    class RecordHelper {
    public:
        static RC
        rawDataToRecord(uint8_t *rawData, const std::vector<Attribute> &attrs, uint8_t *byteSeq,
                            int16_t &recordLen);

        // convert selected Attribute to rawData
        static RC
        recordToRawData(uint8_t record[], const std::vector<Attribute> &recordDescriptor,
                            std::vector<uint16_t> &selectedAttrIndex, uint8_t *rawData);

        static bool rawDataIsNullAttr(uint8_t *rawData, int16_t idx);

        static int16_t recordGetAttrBeginPos(uint8_t *byteSeq, int16_t attrIndex);

        static int16_t recordGetAttrEndPos(uint8_t *byteSeq, int16_t attrIndex);

        static int16_t recordGetAttrLen(uint8_t *byteSeq, int16_t attrIndex);

        static int16_t recordGetAttrNum(uint8_t *byteSeq);

        static RC recordGetAttr(uint8_t *recordByte, uint16_t attrIndex, const std::vector<Attribute> &recordDescriptor, uint8_t *attr);


        RecordHelper();

        ~RecordHelper();

        //RC printNullAttr(char *recordByte, const std::vector<Attribute> &recordDescriptor);
        // get rawdata null flag from record data
        static RC recordGetNullFlag(uint8_t *recordByte, const std::vector<Attribute> &recordDescriptor,
                                    std::vector<uint16_t> &selectedAttrIndex, int8_t *nullFlag, int16_t nullFlagByteNum);

        static bool recordIsAttrNull(uint8_t *byteSeq, int16_t attrIndex);

    };
} // namespace PeterDB

#endif // _rbfm_h_

