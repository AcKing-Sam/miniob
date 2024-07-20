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
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

RC SumAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  
  switch (value.attr_type())
  {
    case AttrType::INTS: {
      value_.set_int(value.get_int() + value_.get_int());
    } break;
    case AttrType::FLOATS: {
      value_.set_float(value.get_float() + value_.get_float());
    } break;
    default: {
      return RC::INTERNAL;
    }
  }
  return RC::SUCCESS;
}

RC SumAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC MinAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  
  switch (value.attr_type())
  {
    case AttrType::INTS: {
      value_.set_int(std::min(value.get_int(),value_.get_int()));
    } break;
    case AttrType::FLOATS: {
      value_.set_float(std::min(value.get_float(),value_.get_float()));
    } break;
    case AttrType::CHARS: {
      value_.set_string(std::min(value.get_string(), value_.get_string()).data());
    } break;
    case AttrType::DATES: {
      value_.set_date(std::min(value.get_date(),value_.get_date()));
    } break;
    default: {
      return RC::INTERNAL;
    }
  }
  return RC::SUCCESS;
}

RC MinAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC MaxAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  
  switch (value.attr_type())
  {
    case AttrType::INTS: {
      value_.set_int(std::max(value.get_int(),value_.get_int()));
    } break;
    case AttrType::FLOATS: {
      value_.set_float(std::max(value.get_float(),value_.get_float()));
    } break;
    case AttrType::CHARS: {
      value_.set_string(std::max(value.get_string(), value_.get_string()).data());
    } break;
    case AttrType::DATES: {
      value_.set_date(std::max(value.get_date(),value_.get_date()));
    } break;
    default: {
      return RC::INTERNAL;
    }
  }
  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC AvgAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  
  switch (value.attr_type())
  {
    case AttrType::INTS: {
      value_.set_int(value.get_int() + value_.get_int());
    } break;
    case AttrType::FLOATS: {
      value_.set_float(value.get_float() + value_.get_float());
    } break;
    default: {
      return RC::INTERNAL;
    }
  }
  count_ ++;
  return RC::SUCCESS;
}

RC AvgAggregator::evaluate(Value& result)
{
  switch (value_.attr_type())
  {
    case AttrType::INTS: {
      int sum = value_.get_int();
      if(sum % count_ == 0) {
        result.set_int(value_.get_int() / count_);
      } else {
        result.set_type(AttrType::FLOATS);
        result.set_float((float)value_.get_int() / count_);
      }
    } break;
    case AttrType::FLOATS: {
      result.set_float(value_.get_float() / count_);
    } break;
    default: {
      return RC::INTERNAL;
    }
  }
  return RC::SUCCESS;
}

RC CountAggregator::accumulate(const Value &value)
{  
  count_++;
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value& result)
{
  result.set_type(AttrType::INTS);
  result.set_int(count_);
  return RC::SUCCESS;
}
