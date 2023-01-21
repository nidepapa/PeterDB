//
// Created by Fan Zhao on 1/16/23.
//

#include "../include/pfm.h"
#include <cstring>

int main(){
    PeterDB::PagedFileManager* pfm;
    pfm->createFile("test_file");
    return 0;
}