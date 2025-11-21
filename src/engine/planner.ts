import { AxisSpec } from './query-contract';

export type PlanningStrategy = 'pre-aggregate' | 'raw-scan';

export interface PlannerCubeMetadata {
  name: string;
  rowCount: number;
  dimensions: Record<string, { levels: string[] }>
}

export interface PlanningContext {
  rows: AxisSpec[];
  columns: AxisSpec[];
  appliedFilterCount: number;
  measures: string[];
  hasDrill: boolean;
  hasRollup: boolean;
}

export interface ExecutionPlan {
  strategy: PlanningStrategy;
  reason: string;
}

export class QueryPlanner {
  plan(context: PlanningContext, cube: PlannerCubeMetadata): ExecutionPlan {
    const axisCount = context.rows.length + context.columns.length;
    const noFilters = context.appliedFilterCount === 0 && !context.hasDrill && !context.hasRollup;
    const referencesSingleDimension = this.referencesSingleDimension(context.rows, context.columns);

    if (
      axisCount === 1 &&
      context.columns.length === 0 &&
      referencesSingleDimension &&
      noFilters
    ) {
      return {
        strategy: 'pre-aggregate',
        reason: 'Single-axis aggregation with no filters leverages materialized hierarchy totals.'
      };
    }

    return {
      strategy: 'raw-scan',
      reason:
        'Query requires either multiple axes, filters, or drill/roll operations so raw fact scanning is cheaper.'
    };
  }

  private referencesSingleDimension(rows: AxisSpec[], columns: AxisSpec[]): boolean {
    const dimensions = new Set<string>();
    [...rows, ...columns].forEach((axis) => dimensions.add(axis.dimension));
    return dimensions.size <= 1;
  }
}
