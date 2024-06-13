/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/07/05.
//

#include <sstream>
#include "sql/expr/tuple_cell.h"
#include "storage/field/field.h"
#include "common/log/log.h"
#include "common/lang/comparator.h"
#include "common/lang/string.h"


std::string aggr_type_to_string(const AggrOp aggrOp) {
  std::string AGGR_NAME[] = {"sum", "avg", "max", "min", "count", "count", "none"};
  if (aggrOp >= AggrOp::AGGR_SUM && aggrOp < AGGR_NONE) {
    return AGGR_NAME[aggrOp];
  }
  return "ERROR";
}

TupleCellSpec::TupleCellSpec(const char *table_name, const char *field_name, const char *alias, const AggrOp aggrOp)
{
  if (table_name) {
    table_name_ = table_name;
  }
  if (field_name) {
    field_name_ = field_name;
  }
  if (alias) {
    alias_ = alias;
  } else {
    if (table_name_.empty()) {
      alias_ = field_name_;
    } else {
      alias_ = table_name_ + "." + field_name_;
    }
  }
  if (aggrOp != AggrOp::AGGR_NONE) {
    alias_ = aggr_type_to_string(aggrOp) + "(" + alias_ + ")";
  }
}

TupleCellSpec::TupleCellSpec(const char *alias, const AggrOp aggrOp)
{
  if (alias) {
    alias_ = alias;
  }
  if (aggrOp == AggrOp::AGGR_COUNT_ALL)
    alias_ = "count(*)";
  else if (aggrOp != AggrOp::AGGR_NONE) {
    alias_ = aggr_type_to_string(aggrOp) + "(" + alias_ + ")";
  }
}
