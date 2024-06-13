#include "common/log/log.h"
#include "sql/operator/aggregate_physical_operator.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

void AggregatePhysicalOperator::add_aggregation(const AggrOp aggr_op) {
    aggregations_.push_back(aggr_op);
}


RC AggregatePhysicalOperator::open(Trx *trx)
{
  PhysicalOperator* child = children_[0].get();
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  return RC::SUCCESS;
}

RC AggregatePhysicalOperator::next() {
    RC rc = RC::SUCCESS;

    if (result_tuple_.cell_num() > 0) {
        return RC::RECORD_EOF;
    }

    
    if (children_.empty()) {
        return RC::RECORD_EOF;
    }

    PhysicalOperator* child = children_[0].get();

    int sum = 0;
    std::vector<Value> result_cells(aggregations_.size());

    int tuple_count = 0;
    while (RC::SUCCESS == (rc = child->next())) {
        Tuple* tuple = child->current_tuple();
        if (nullptr == tuple) {
            LOG_WARN("failed to get current record: %s", strrc(rc));
            return rc;
        }
        tuple_count++;

        for (int idx = 0; idx < (int)aggregations_.size(); idx++) {
            const AggrOp aggrOp = aggregations_[idx];
            const int cell_idx = idx;
            Value cell;
            switch (aggrOp)
            {
            case AggrOp::AGGR_SUM:
                rc = tuple->cell_at(cell_idx, cell);
                // 这里不好，因为sum的结果始终是float
                result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
                
                // if (cell.attr_type() == AttrType::INTS) {
                //     result_cells[cell_idx].set_int(result_cells[cell_idx].get_int() + cell.get_int());
                // }
                // else if (cell.attr_type() == AttrType::FLOATS) {
                //     result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
                // }
                break;
            case AggrOp::AGGR_AVG:
                rc = tuple->cell_at(cell_idx, cell);
                result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
                break;
            case AggrOp::AGGR_COUNT:
            case AggrOp::AGGR_COUNT_ALL:
                result_cells[cell_idx].set_int(tuple_count);
                break;
            case AggrOp::AGGR_MAX:
                rc = tuple->cell_at(cell_idx, cell);
                if (tuple_count == 1)
                    result_cells[cell_idx] = cell;
                else if (result_cells[cell_idx].compare(cell) < 0)
                    result_cells[cell_idx] = cell;
                break;
            case AggrOp::AGGR_MIN:
                rc = tuple->cell_at(cell_idx, cell);
                if (tuple_count == 1)
                    result_cells[cell_idx] = cell;
                else if (result_cells[cell_idx].compare(cell) > 0)
                    result_cells[cell_idx] = cell;
                break;
            default:
                return RC::UNIMPLENMENT;
            }

        }
    }
    for (int idx = 0; idx < (int)aggregations_.size(); idx++) {
        const AggrOp aggrOp = aggregations_[idx];
        if (aggrOp == AggrOp::AGGR_AVG)
            result_cells[idx].set_float(result_cells[idx].get_float() / tuple_count);
    }

    if (rc == RC::RECORD_EOF) {
        rc = RC::SUCCESS;
    }
    result_tuple_.set_cells(result_cells);

    return rc;
}

RC AggregatePhysicalOperator::close() {
    if (!children_.empty()) {
        children_[0]->close();
    }
    return RC::SUCCESS;
}

Tuple* AggregatePhysicalOperator::current_tuple() {
    // tuple_.set_tuple(&result_tuple_);
    // std::cout << "AGGREGATE " << tuple_.to_string() << std::endl;
    return &result_tuple_;
}