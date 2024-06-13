#pragma once

#include "sql/operator/physical_operator.h"

class Trx;
class DeleteStmt;


class AggregatePhysicalOperator : public PhysicalOperator
{
public:
  AggregatePhysicalOperator() {}

  virtual ~AggregatePhysicalOperator() = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::AGGREGATE;
  }

  void add_aggregation(const AggrOp aggr_op);

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  std::vector<AggrOp> aggregations_;
  ValueListTuple result_tuple_;
  ProjectTuple tuple_;

  Trx *trx_ = nullptr;
};
