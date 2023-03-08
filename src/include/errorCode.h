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
        FILE_READ_ONE_PAGE_FAIL,
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
        ATTR_IS_NULL,
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

    enum class IX_ERROR:int{
        ERR_UNDEFINED = -1,
        ERR_FILE_ALREADY_OPEN = 1,
        ERR_FILE_OPEN_FAIL,
        FILE_NOT_OPEN,
        FILE_NO_ENOUGH_PAGE,
        FILE_READ_ONE_PAGE_FAIL,
        ROOT_NOT_EXIST,
        FILE_EXIST,
        FILE_NOT_EXIST,
        FILE_DELETE_FAIL,
        PAGE_NO_ENOUGH_SPACE,
        FREEBYTE_EXCEEDED,
        LEAF_INSERT_ENTRY_FAIL,
        NOLEAF_GETTARGET_CHILD_FAIL,
        NOLEAF_INSERT_ENTRY_FAIL,
        NOLEAF_SPLIT_FAIL,
        LEAF_FOUND_FAIL,
        LEAF_ENTRY_NOT_EXIST,
        KEY_TYPE_INVALID,
        INDEXPAGE_LAST_CHILD_NOT_EXIST,
        PAGE_TYPE_UNKNOWN,
        MOVE_FAIL,
        LEAF_SPLIT_ENTRY_FAIL,
        NODE_TYPE_INVALID,
        };
}

#endif //PETERDB_ERRORCODE_H
