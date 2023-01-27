//
// Created by Fan Zhao on 1/26/23.
//

#ifndef PETERDB_ERRORCODE_H
#define PETERDB_ERRORCODE_H

#include <cstdint>

namespace PeterDB {
    const int32_t ERR_GENERAL = -1;

    // fh error
    const int32_t ERR_FILE_NOT_EXIST = 100;
    const int32_t ERR_FILE_NOT_OPEN = 101;
    const int32_t ERR_FILE_REMOVE_FAIL = 102;
    const int32_t ERR_PAGE_NOT_ENOUGH = 103;
    const int32_t ERR_FILE_READ_FAIL = 104;

    // rbfm error
    const int32_t ERR_RBFILE_NOT_OPEN = 201;
    const int32_t ERR_RBFILE_PAGE_NOT_ENOUGH = 202;

}

#endif //PETERDB_ERRORCODE_H
