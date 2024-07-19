#include "sql/operator/update_logical_operator.h"

UpdateLogicalOperator::UpdateLogicalOperator(Table *table, Value* val, std::string str) : table_(table), value_(val), attribute_name_(str) {}