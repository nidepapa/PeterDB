//
// Created by Fan Zhao on 1/26/23.
//

#ifndef PETERDB_ERRORCODE_H
#define PETERDB_ERRORCODE_H

#include <cstdint>
#include <string>

namespace PeterDB {
    const int SUCCESS = 0;
    const int ERR_GENERAL = -1;
    // for FileHandle & PagedFileHandle
    enum class FILE_ERROR:int{
        FILE_NOT_EXIST = 1,
        FILE_NOT_OPEN,
        FILE_REMOVE_FAIL,
        FILE_NO_ENOUGH_PAGE,
        FILE_READ_FAIL,
    };
    // PageHelper & RecordHelper
    enum class PAGE_ERROR:int{
        ERR_UNDEFINED = -1,
        PAGE_NO_ENOUGH_SLOT = 1,
        PAGE_SLOT_INVALID,
        RECORD_NULL_ATTRIBUTE,
        RECORD_DELETED,
        RECORD_UNORIGINAL,
        RECORD_FLAG_WRONG,
        WRITE_PAGE_FAIL,
    };

    enum class RBFM_ERROR:int{
        CONDITION_ATTR_IDX_INVALID = 1,
        CONDITION_VALUE_EMPTY,
        FILE_NOT_OPEN,
        PAGE_EXCEEDED,
        SLOT_INVALID,
    };

    enum class RM_ERROR:int{
        ERR_UNDEFINED = -1,
        TABLE_NAME_EMPTY = 1,
        TABLE_NAME_INVALID,
        TABLE_ACCESS_DENIED,
        FILE_OPEN_FAIL,
        FILE_CLOSE_FAIL,
        FILE_NOT_EXIST,
        TUPLE_INSERT_FAIL,
        TUPLE_DEL_FAIL,
        TUPLE_UPDATE_FAIL,
        TUPLE_READ_FAIL,
        TUPLE_ATTR_READ_FAIL,
        TUPLE_PRINT_FAIL,
        ITERATOR_BEGIN_FAIL,
        DESCRIPTOR_GET_FAIL,
        CATALOG_MISSING,
        CATALOG_OPEN_FAIL,
    };


}

#endif //PETERDB_ERRORCODE_H
