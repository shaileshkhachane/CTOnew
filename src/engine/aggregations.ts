export type MeasureValue = number | string | null | undefined;

export type AggregationType =
  | 'SUM'
  | 'COUNT'
  | 'AVG'
  | 'MIN'
  | 'MAX'
  | 'DISTINCT';

export interface AggregationAccumulator {
  add(value: MeasureValue): void;
  finalize(): number;
}

class SumAccumulator implements AggregationAccumulator {
  private value = 0;

  add(value: MeasureValue) {
    if (typeof value === 'number') {
      this.value += value;
    }
  }

  finalize(): number {
    return this.value;
  }
}

class CountAccumulator implements AggregationAccumulator {
  private value = 0;

  add(value: MeasureValue) {
    if (value !== undefined && value !== null) {
      this.value += 1;
    }
  }

  finalize(): number {
    return this.value;
  }
}

class AvgAccumulator implements AggregationAccumulator {
  private sum = 0;
  private count = 0;

  add(value: MeasureValue) {
    if (typeof value === 'number') {
      this.sum += value;
      this.count += 1;
    }
  }

  finalize(): number {
    return this.count === 0 ? 0 : this.sum / this.count;
  }
}

class MinAccumulator implements AggregationAccumulator {
  private value: number | undefined;

  add(value: MeasureValue) {
    if (typeof value !== 'number') return;
    if (this.value === undefined || value < this.value) {
      this.value = value;
    }
  }

  finalize(): number {
    return this.value ?? 0;
  }
}

class MaxAccumulator implements AggregationAccumulator {
  private value: number | undefined;

  add(value: MeasureValue) {
    if (typeof value !== 'number') return;
    if (this.value === undefined || value > this.value) {
      this.value = value;
    }
  }

  finalize(): number {
    return this.value ?? 0;
  }
}

class DistinctAccumulator implements AggregationAccumulator {
  private seen = new Set<string>();

  add(value: MeasureValue) {
    if (value === undefined || value === null) return;
    this.seen.add(String(value));
  }

  finalize(): number {
    return this.seen.size;
  }
}

export function createAccumulator(type: AggregationType): AggregationAccumulator {
  switch (type) {
    case 'SUM':
      return new SumAccumulator();
    case 'COUNT':
      return new CountAccumulator();
    case 'AVG':
      return new AvgAccumulator();
    case 'MIN':
      return new MinAccumulator();
    case 'MAX':
      return new MaxAccumulator();
    case 'DISTINCT':
      return new DistinctAccumulator();
    default: {
      const exhaustive: never = type;
      throw new Error(`Unsupported aggregation type: ${exhaustive}`);
    }
  }
}
