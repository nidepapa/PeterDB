//
// Created by Fan Zhao on 3/1/23.
//
#include "src/include/ix.h"

namespace PeterDB {

    bool IXNode::isCompositeKeyMeetCompCondition(const uint8_t* key1,  const uint8_t* key2,  const Attribute& attr, const CompOp op){
        RID rid1, rid2;
        ((internalEntry*)key1)->getRID(attr.type, rid1.pageNum, rid1.slotNum);
        assert(rid1.pageNum > 0);
        ((internalEntry*)key2)->getRID(attr.type, rid2.pageNum, rid2.slotNum);
        assert(rid2.pageNum > 0);
        switch (op) {
            case GT_OP:
                return isKeyMeetCompCondition(key1, key2, attr, GT_OP) ||
                       (isKeyMeetCompCondition(key1, key2, attr, EQ_OP) && isRidMeetCompCondition(rid1, rid2, GT_OP));
            case GE_OP:
                return isKeyMeetCompCondition(key1, key2, attr, GE_OP) ||
                       (isKeyMeetCompCondition(key1, key2, attr, EQ_OP) && isRidMeetCompCondition(rid1, rid2, GE_OP));
            case LT_OP:
                return isKeyMeetCompCondition(key1, key2, attr, LT_OP) ||
                       (isKeyMeetCompCondition(key1, key2, attr, EQ_OP) && isRidMeetCompCondition(rid1, rid2, LT_OP));
            case LE_OP:
                return isKeyMeetCompCondition(key1, key2, attr, LE_OP) ||
                       (isKeyMeetCompCondition(key1, key2, attr, EQ_OP) && isRidMeetCompCondition(rid1, rid2, LE_OP));
            case NE_OP:
                return !(isKeyMeetCompCondition(key1, key2, attr, EQ_OP) && isRidMeetCompCondition(rid1, rid2, EQ_OP));
            case EQ_OP:
                return isKeyMeetCompCondition(key1, key2, attr, EQ_OP) && isRidMeetCompCondition(rid1, rid2, EQ_OP);
            default:
                return false;
        }
        return false;
    }
    bool IXNode::isKeyMeetCompCondition(const uint8_t* key1, const uint8_t* key2, const Attribute& attr, const CompOp op){
        switch (attr.type) {
            case TypeInt:
                int32_t int1;
                int1 = ((internalEntry*)key1)->getKey<int32_t>();
                int32_t int2;
                int2 = ((internalEntry*)key2)->getKey<int32_t>();
                switch (op) {
                    case GT_OP:
                        return int1 > int2;
                    case GE_OP:
                        return int1 >= int2;
                    case LT_OP:
                        return int1 < int2;
                    case LE_OP:
                        return int1 <= int2;
                    case NE_OP:
                        return int1 != int2;
                    case EQ_OP:
                        return int1 == int2;
                    default:
                        return false;
                }
                break;
            case TypeReal:
                float float1;
                float1 = ((internalEntry*)key1)->getKey<float>();
                float float2;
                float2 = ((internalEntry*)key2)->getKey<float>();
                switch (op) {
                    case GT_OP:
                        return float1 > float2;
                    case GE_OP:
                        return float1 >= float2;
                    case LT_OP:
                        return float1 < float2;
                    case LE_OP:
                        return float1 <= float2;
                    case NE_OP:
                        return float1 != float2;
                    case EQ_OP:
                        return float1 == float2;
                    default:
                        return false;
                }
            case TypeVarChar:
                std::string str1;
                str1 = ((internalEntry*)key1)->getKey<std::string>();
                std::string str2;
                str2 = ((internalEntry*)key2)->getKey<std::string>();
                switch (op) {
                    case GT_OP:
                        return str1 > str2;
                    case GE_OP:
                        return str1 >= str2;
                    case LT_OP:
                        return str1 < str2;
                    case LE_OP:
                        return str1 <= str2;
                    case NE_OP:
                        return str1 != str2;
                    case EQ_OP:
                        return str1 == str2;
                    default:
                        return false;
                }
                break;
        }
        return false;
    }
    bool IXNode::isRidMeetCompCondition(const RID& rid1, const RID& rid2, const CompOp op){
        switch (op) {
            case GT_OP:
                return rid1.pageNum > rid2.pageNum || (rid1.pageNum == rid2.pageNum && rid1.slotNum > rid2.slotNum);
            case GE_OP:
                return rid1.pageNum >= rid2.pageNum || (rid1.pageNum == rid2.pageNum && rid1.slotNum >= rid2.slotNum);
            case LT_OP:
                return rid1.pageNum < rid2.pageNum || (rid1.pageNum == rid2.pageNum && rid1.slotNum < rid2.slotNum);
            case LE_OP:
                return rid1.pageNum <= rid2.pageNum || (rid1.pageNum == rid2.pageNum && rid1.slotNum <= rid2.slotNum);
            case NE_OP:
                return !(rid1.pageNum == rid2.pageNum && rid1.slotNum == rid2.slotNum);
            case EQ_OP:
                return rid1.pageNum == rid2.pageNum && rid1.slotNum == rid2.slotNum;
            default:
                return false;
        }
        return false;
    }
}