#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];

  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    bool filter_result = false;
    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    rc = filter(*row_tuple, filter_result);

    if (rc != RC::SUCCESS) {
      LOG_TRACE("record filtered failed=%s", strrc(rc));
      return rc;
    }

    if (filter_result) {
        Record   &record = row_tuple->record();
        records_.emplace_back(std::move(record));
        // construct the new record
        Record new_record;
        table_->get_record(record.rid(), new_record);
        bool found = false;
        for(auto field : row_tuple->get_fields()) {
            if(field->field_name() == attribute_name_) {
                found = true;
                if(field->field().meta()->type() != value_->attr_type()) {
                  return RC::INVALID_ARGUMENT;
                }
                if(value_->attr_type() == AttrType::CHARS) {
                  if(value_->length() > field->field().meta()->len()) {
                    return RC::INVALID_ARGUMENT;
                  }
                }
                new_record.set_field(field->field().meta()->offset(), field->field().meta()->len(), (char *)value_->data());
                break;
            }
        }
        if(!found) {
          return RC::INVALID_ARGUMENT;
        }
        new_records_.emplace_back(std::move(new_record));
    }
  }

  child->close();

  // 先收集记录再更新
  // 记录的有效性由事务来保证，如果事务不保证删除的有效性，那说明此事务类型不支持并发控制，比如 VacuousTrx
  for (int i = 0;i < records_.size();i ++) {
    auto& record = records_[i];
    auto& new_record = new_records_[i];
    rc = trx_->update_record(table_, record, new_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  return RC::SUCCESS;
}

void UpdatePhysicalOperator::set_predicates(vector<unique_ptr<Expression>> &&exprs) {
  predicates_ = std::move(exprs);
}

RC UpdatePhysicalOperator::filter(RowTuple &tuple, bool &result)
{
  RC    rc = RC::SUCCESS;
  Value value;
  for (unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(tuple, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    bool tmp_result = value.get_boolean();
    if (!tmp_result) {
      result = false;
      return rc;
    }
  }

  result = true;
  return rc;
}
