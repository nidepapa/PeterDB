//
// Created by Fan Zhao on 2/15/23.
//

#ifndef PETERDB_RECORD_H
#define PETERDB_RECORD_H

#include <cstdint>
# include <assert.h>
#include "pfm.h"
#include<math.h>

namespace PeterDB {
    typedef int32_t StrLenIndicator;

    struct RawRecord;
    struct Record;
    struct Attribute;

    struct Record{
        struct Directory{
            struct Header{
                int16_t Flag;
                int16_t AttrNum;
            };
            class Entry{
            private:
                int16_t val;
            public:
                bool isNull(){
                    return val == -1;
                }
                void setNull(){
                    val = -1;
                }
                int16_t getOffset(){
                    assert(!isNull());
                    return val;
                }
                void setOffset(int16_t offset){
                    assert(offset >= 0);
                    val = offset;
                }
            };

            Header header;

            Entry* getEntries() const{
                return (Entry*)((uint8_t*)this + sizeof(Header));
            }

            Entry* getEntry(int i) const {
                assert(i < header.AttrNum);
                return getEntries() + i;
            }

            void* getEndOfDirectory() const {
                return getEntries() + header.AttrNum;
            }

            int getDirectorySize() const {
                return (uint8_t*)getEndOfDirectory() - (uint8_t*)this;
            }




        };

        Directory* getDirectory() const {
            return (Directory*)this;
        }

        // Get the i-th entry in the directory
        Directory::Entry* getDirectoryEntry(int i) const {
            return getDirectory()->getEntry(i);
        }

        // Get pointer to i-th field
        template <typename T>
        T* getFieldPtr(int i) const {
            return (T*)((uint8_t*)(this->getEndOfDirectory()) + this->getDirectoryEntry(i)->getOffset());
        }

        void* getEndOfDirectory() const{
            return this->getDirectory()->getEndOfDirectory();
        }

        template <typename T>
        T getField(int i) const {
            return *getFieldPtr<T>(i);
        }

        int getDirectorySize() const {
            return this->getDirectory()->getDirectorySize();
        }

        void* getDataSectionStart() const {
            assert(getDirectory()->header.AttrNum >= 0);
            return getDirectory()->getEndOfDirectory();
        }

        RC fromRawRecord(RawRecord* rawRecord, const std::vector<Attribute> &recordDescriptor, int16_t &recordLen);
    };

    template <>
    inline std::string Record::getField(int i) const {
        StrLenIndicator *size = getFieldPtr<StrLenIndicator>(i);
        char* data = (char*)(size + 1);
        return std::string(data, *size);
    }

    struct RawRecord{
        bool isNullField(int16_t idx) {
            short byteNumber = idx / 8;
            short bitNumber = idx % 8;
            uint8_t mask = 0x01;
            // ex: 0100 0000
            uint8_t* tmp = (uint8_t*)this + byteNumber;
            return (*tmp >> (7 - bitNumber)) & mask;
        }

        void setFieldNull(int16_t idx) {
            short byteNumber = idx / 8;
            short bitNumber = idx % 8;
            uint8_t mask = 0x01;
            // move to the bit that need to set
            uint8_t tmp = *((uint8_t*)this + byteNumber) >> (7 - bitNumber);
            tmp |= mask;
        }

        static int nullByteSize(const std::vector<Attribute>& recordDescriptor);
        static int nullByteSize(int n) {
            return ceil(n / 8.0);
        }

        RC fromRecord(Record *record, const std::vector<Attribute> &recordDescriptor, int16_t &recordLen);
    };
}


#endif //PETERDB_RECORD_H
