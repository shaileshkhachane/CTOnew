import { LRUCache } from 'lru-cache';
import { AggregationAccumulator, AggregationType, createAccumulator, MeasureValue } from './aggregations';
import { AxisSpec, DrillSpec, FilterSpec, QueryPayload, RollupSpec } from './query-contract';
import { ExecutionPlan, PlannerCubeMetadata, PlanningContext, QueryPlanner } from './planner';

export type DimensionValue = string | number;

export interface CubeDimension {
  name: string;
  label?: string;
  hierarchy: string[];
}

export interface CubeMeasure {
  name: string;
  label?: string;
  valueField: string;
  aggregation: AggregationType;
  format?: string;
}

export interface FactRow {
  dimensions: Record<string, Record<string, DimensionValue>>;
  metrics: Record<string, MeasureValue>;
}

export interface CubeDefinition {
  name: string;
  description?: string;
  dimensions: CubeDimension[];
  measures: CubeMeasure[];
  rows: FactRow[];
}

export interface CubeManagerOptions {
  cache?: {
    max?: number;
    ttlMs?: number;
  };
}

export interface PivotCoordinate {
  dimension: string;
  level: string;
  value: DimensionValue | 'All';
}

export interface PivotHeader {
  key: string;
  label: string;
  coordinates: PivotCoordinate[];
}

export interface PivotMeasureMatrix {
  name: string;
  values: number[][];
}

export interface FlatRow {
  rowKey: string;
  columnKey: string;
  coordinates: Record<string, DimensionValue | 'All'>;
  measures: Record<string, number>;
}

export interface BreadcrumbEntry {
  dimension: string;
  level: string;
  value: DimensionValue | 'All';
}

export interface QueryResultData {
  pivot: {
    rows: PivotHeader[];
    columns: PivotHeader[];
    measures: PivotMeasureMatrix[];
  };
  flat: FlatRow[];
}

export interface QueryResponseMetadata {
  cube: string;
  measures: string[];
  availableMeasures: CubeMeasure[];
  breadcrumbs: BreadcrumbEntry[];
  cache: {
    hit: boolean;
    key: string;
    ttlRemainingMs: number | null;
    stats: {
      hits: number;
      misses: number;
      size: number;
    };
  };
  planner: ExecutionPlan;
  suggestions: string[];
}

export interface OlapQueryResponse {
  data: QueryResultData;
  metadata: QueryResponseMetadata;
}

interface CubeInstance {
  definition: CubeDefinition;
  dimensionMap: Map<string, CubeDimension>;
  measureMap: Map<string, CubeMeasure>;
  aggregateStore: Map<string, Map<DimensionValue, Record<string, number>>>;
}

interface CachedPayload {
  data: QueryResultData;
  breadcrumbs: BreadcrumbEntry[];
  suggestions: string[];
  plan: ExecutionPlan;
}

interface NormalizedQuery {
  cube: string;
  measures: string[];
  rows: AxisSpec[];
  columns: AxisSpec[];
  slices: FilterSpec[];
  dices: FilterSpec[];
  filters: FilterSpec[];
  allFilters: FilterSpec[];
  drill?: DrillSpec;
  rollup?: RollupSpec;
  includeFlattened: boolean;
  mdx?: string;
}

export class CubeError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

export class CubeManager {
  private readonly cubes = new Map<string, CubeInstance>();
  private readonly planner = new QueryPlanner();
  private readonly cache: LRUCache<string, CachedPayload>;
  private readonly cacheStats = { hits: 0, misses: 0 };

  constructor(private readonly options?: CubeManagerOptions) {
    this.cache = new LRUCache<string, CachedPayload>({
      max: options?.cache?.max ?? 200,
      ttl: options?.cache?.ttlMs ?? 30_000
    });
  }

  registerCube(definition: CubeDefinition) {
    if (this.cubes.has(definition.name)) {
      throw new CubeError(400, `Cube ${definition.name} is already registered.`);
    }

    if (!definition.dimensions.length) {
      throw new CubeError(400, 'Cube definitions require at least one dimension.');
    }

    const dimensionMap = new Map(definition.dimensions.map((dim) => [dim.name, dim]));
    const measureMap = new Map(definition.measures.map((measure) => [measure.name, measure]));
    const aggregateStore = this.materializeAggregates(definition);

    this.cubes.set(definition.name, {
      definition,
      dimensionMap,
      measureMap,
      aggregateStore
    });
  }

  listCubes(): string[] {
    return [...this.cubes.keys()];
  }

  execute(payload: QueryPayload): OlapQueryResponse {
    const cube = this.ensureCube(payload.cube);
    const normalized = this.normalizeQuery(payload, cube);
    const planningContext: PlanningContext = {
      rows: normalized.rows,
      columns: normalized.columns,
      appliedFilterCount: normalized.allFilters.length,
      measures: normalized.measures,
      hasDrill: Boolean(normalized.drill),
      hasRollup: Boolean(normalized.rollup)
    };
    const plannerMeta: PlannerCubeMetadata = {
      name: cube.definition.name,
      rowCount: cube.definition.rows.length,
      dimensions: Object.fromEntries(
        cube.definition.dimensions.map((dim) => [dim.name, { levels: dim.hierarchy }])
      )
    };
    const plan = this.planner.plan(planningContext, plannerMeta);

    const cacheKey = stableStringify({ cube: normalized.cube, query: normalized, plan });
    const cached = this.cache.get(cacheKey);
    if (cached) {
      this.cacheStats.hits += 1;
      return this.decorateResponse(cube, normalized, cached, cacheKey, true);
    }

    this.cacheStats.misses += 1;
    const fresh = this.runPlan(plan, normalized, cube);
    this.cache.set(cacheKey, fresh);
    return this.decorateResponse(cube, normalized, fresh, cacheKey, false);
  }

  private ensureCube(name: string): CubeInstance {
    const cube = this.cubes.get(name);
    if (!cube) {
      throw new CubeError(404, `Cube ${name} is not registered.`);
    }
    return cube;
  }

  private normalizeQuery(payload: QueryPayload, cube: CubeInstance): NormalizedQuery {
    const mdxHints = parseLightweightMdx(payload.mdx);
    const merged = mergeQueryPayload(mdxHints, payload);

    const includeFlattened = merged.includeFlattened ?? true;
    const rowAxes = merged.pivot?.rows ?? merged.rows ?? [];
    const columnAxes = merged.pivot?.columns ?? merged.columns ?? [];
    const slices = merged.slices ?? [];
    const dices = merged.dices ?? [];
    const filters = merged.filters ?? [];
    const allFilters = [...slices, ...dices, ...filters];

    const resolvedRows = this.resolveAxes(rowAxes, merged, cube);
    const resolvedColumns = this.resolveAxes(columnAxes, merged, cube);

    const finalRows = resolvedRows.length || resolvedColumns.length ? resolvedRows : this.defaultRows(cube);
    const finalColumns = resolvedColumns;

    merged.measures.forEach((measureName) => {
      if (!cube.measureMap.has(measureName)) {
        throw new CubeError(400, `Unknown measure ${measureName} for cube ${cube.definition.name}.`);
      }
    });

    return {
      cube: merged.cube,
      measures: merged.measures,
      rows: finalRows,
      columns: finalColumns,
      slices,
      dices,
      filters,
      allFilters,
      drill: merged.drill,
      rollup: merged.rollup,
      includeFlattened,
      mdx: payload.mdx
    };
  }

  private defaultRows(cube: CubeInstance): AxisSpec[] {
    const dimension = cube.definition.dimensions[0];
    return [
      {
        dimension: dimension.name,
        level: dimension.hierarchy[0]
      }
    ];
  }

  private resolveAxes(axes: AxisSpec[], query: QueryPayload, cube: CubeInstance): AxisSpec[] {
    return axes.map((axis) => this.resolveAxis(axis, query, cube));
  }

  private resolveAxis(axis: AxisSpec, query: QueryPayload, cube: CubeInstance): AxisSpec {
    const dimension = cube.dimensionMap.get(axis.dimension);
    if (!dimension) {
      throw new CubeError(400, `Dimension ${axis.dimension} is not defined for cube ${cube.definition.name}.`);
    }

    let level = axis.level ?? dimension.hierarchy[dimension.hierarchy.length - 1];
    if (!dimension.hierarchy.includes(level)) {
      throw new CubeError(400, `Unknown level ${level} for dimension ${axis.dimension}.`);
    }

    if (query.rollup && query.rollup.dimension === axis.dimension) {
      const rollupLevel = query.rollup.level;
      const rollupIndex = dimension.hierarchy.indexOf(rollupLevel);
      const levelIndex = dimension.hierarchy.indexOf(level);
      if (rollupIndex === -1) {
        throw new CubeError(400, `Roll-up level ${rollupLevel} not found in dimension ${axis.dimension}.`);
      }
      if (levelIndex > rollupIndex) {
        level = rollupLevel;
      }
    }

    if (query.drill && query.drill.dimension === axis.dimension) {
      if (!dimension.hierarchy.includes(query.drill.toLevel)) {
        throw new CubeError(400, `Drill target level ${query.drill.toLevel} is not part of dimension ${axis.dimension}.`);
      }
      level = query.drill.toLevel;
    }

    return {
      ...axis,
      level
    };
  }

  private runPlan(plan: ExecutionPlan, query: NormalizedQuery, cube: CubeInstance): CachedPayload {
    if (plan.strategy === 'pre-aggregate') {
      return this.runPreAggregate(query, cube, plan);
    }
    return this.runRawScan(query, cube, plan);
  }

  private runPreAggregate(query: NormalizedQuery, cube: CubeInstance, plan: ExecutionPlan): CachedPayload {
    const axis = query.rows[0] ?? query.columns[0];
    if (!axis) {
      return this.emptyResult(plan);
    }

    const storeKey = this.storeKey(axis.dimension, axis.level!);
    const store = cube.aggregateStore.get(storeKey) ?? new Map();
    const sortedEntries = [...store.entries()].sort(([a], [b]) => compareValues(a, b));

    const rowHeaders = sortedEntries.map(([value]) =>
      this.toHeader(this.buildAxisKey([{ dimension: axis.dimension, level: axis.level!, value }]), [
        { dimension: axis.dimension, level: axis.level!, value }
      ])
    );
    const columnHeader = this.toHeader('__all__', []);
    const columns = [columnHeader];

    const measures: PivotMeasureMatrix[] = query.measures.map((measure) => ({ name: measure, values: [] }));
    const flat: FlatRow[] = [];
    const includeFlat = query.includeFlattened !== false;

    sortedEntries.forEach(([value, measuresRecord], rowIdx) => {
      query.measures.forEach((measureName, measureIdx) => {
        const resolved = measuresRecord[measureName] ?? 0;
        if (!measures[measureIdx].values[rowIdx]) {
          measures[measureIdx].values[rowIdx] = [];
        }
        measures[measureIdx].values[rowIdx][0] = resolved;
      });

      if (includeFlat) {
        const header = rowHeaders[rowIdx];
        const measureSnapshot = query.measures.reduce<Record<string, number>>((acc, measureName, idx) => {
          acc[measureName] = measures[idx].values[rowIdx][0] ?? 0;
          return acc;
        }, {});
        flat.push({
          rowKey: header.key,
          columnKey: columnHeader.key,
          coordinates: this.buildCoordinateRecord(header, columnHeader),
          measures: measureSnapshot
        });
      }
    });

    return {
      data: {
        pivot: {
          rows: rowHeaders,
          columns,
          measures
        },
        flat
      },
      breadcrumbs: this.buildBreadcrumbs(query, cube),
      suggestions: this.deriveVisualSuggestions(query.rows, query.columns, query.measures),
      plan
    };
  }

  private runRawScan(query: NormalizedQuery, cube: CubeInstance, plan: ExecutionPlan): CachedPayload {
    const table = new Map<string, Map<string, Record<string, AggregationAccumulator>>>();
    const rowHeaders = new Map<string, PivotHeader>();
    const columnHeaders = new Map<string, PivotHeader>();
    const rowOrder: PivotHeader[] = [];
    const columnOrder: PivotHeader[] = [];

    const measureDefs = query.measures.map((name) => cube.measureMap.get(name)!);

    for (const row of cube.definition.rows) {
      if (!this.matchesFilters(row, query, cube)) {
        continue;
      }

      const rowCoords = this.buildCoordinates(row, query.rows, cube);
      const columnCoords = this.buildCoordinates(row, query.columns, cube);
      const rowKey = this.buildAxisKey(rowCoords);
      const columnKey = this.buildAxisKey(columnCoords);

      if (!rowHeaders.has(rowKey)) {
        const header = this.toHeader(rowKey, rowCoords);
        rowHeaders.set(rowKey, header);
        rowOrder.push(header);
      }

      if (!columnHeaders.has(columnKey)) {
        const header = this.toHeader(columnKey, columnCoords);
        columnHeaders.set(columnKey, header);
        columnOrder.push(header);
      }

      let rowMap = table.get(rowKey);
      if (!rowMap) {
        rowMap = new Map();
        table.set(rowKey, rowMap);
      }

      let cell = rowMap.get(columnKey);
      if (!cell) {
        cell = this.createMeasureAccumulatorSet(measureDefs);
        rowMap.set(columnKey, cell);
      }

      measureDefs.forEach((measure) => {
        const value = this.extractMeasureValue(row, measure);
        cell![measure.name].add(value);
      });
    }

    const resolvedTable = new Map<string, Map<string, Record<string, number>>>();
    table.forEach((columnsMap, rowKey) => {
      const resolvedColumns = new Map<string, Record<string, number>>();
      columnsMap.forEach((cell, columnKey) => {
        const resolvedMeasures: Record<string, number> = {};
        query.measures.forEach((measureName) => {
          resolvedMeasures[measureName] = cell[measureName].finalize();
        });
        resolvedColumns.set(columnKey, resolvedMeasures);
      });
      resolvedTable.set(rowKey, resolvedColumns);
    });

    const measures: PivotMeasureMatrix[] = query.measures.map((name) => ({ name, values: [] }));
    const flat: FlatRow[] = [];
    const includeFlat = query.includeFlattened !== false;

    rowOrder.forEach((rowHeader, rowIndex) => {
      const columnMap = resolvedTable.get(rowHeader.key) ?? new Map();
      columnOrder.forEach((columnHeader, columnIndex) => {
        const resolvedMeasures = columnMap.get(columnHeader.key);
        query.measures.forEach((measureName, measureIdx) => {
          if (!measures[measureIdx].values[rowIndex]) {
            measures[measureIdx].values[rowIndex] = [];
          }
          measures[measureIdx].values[rowIndex][columnIndex] = resolvedMeasures?.[measureName] ?? 0;
        });

        if (resolvedMeasures && includeFlat) {
          flat.push({
            rowKey: rowHeader.key,
            columnKey: columnHeader.key,
            coordinates: this.buildCoordinateRecord(rowHeader, columnHeader),
            measures: { ...resolvedMeasures }
          });
        }
      });
    });

    return {
      data: {
        pivot: {
          rows: rowOrder,
          columns: columnOrder,
          measures
        },
        flat
      },
      breadcrumbs: this.buildBreadcrumbs(query, cube),
      suggestions: this.deriveVisualSuggestions(query.rows, query.columns, query.measures),
      plan
    };
  }

  private emptyResult(plan: ExecutionPlan): CachedPayload {
    return {
      data: {
        pivot: { rows: [], columns: [], measures: [] },
        flat: []
      },
      breadcrumbs: [],
      suggestions: [],
      plan
    };
  }

  private decorateResponse(
    cube: CubeInstance,
    query: NormalizedQuery,
    payload: CachedPayload,
    cacheKey: string,
    cacheHit: boolean
  ): OlapQueryResponse {
    return {
      data: payload.data,
      metadata: {
        cube: cube.definition.name,
        measures: query.measures,
        availableMeasures: cube.definition.measures,
        breadcrumbs: payload.breadcrumbs,
        cache: {
          hit: cacheHit,
          key: cacheKey,
          ttlRemainingMs: this.cache.getRemainingTTL(cacheKey) ?? null,
          stats: {
            hits: this.cacheStats.hits,
            misses: this.cacheStats.misses,
            size: this.cache.size
          }
        },
        planner: payload.plan,
        suggestions: payload.suggestions
      }
    };
  }

  private buildCoordinates(row: FactRow, axes: AxisSpec[], cube: CubeInstance): PivotCoordinate[] {
    return axes.map((axis) => {
      const dimension = cube.dimensionMap.get(axis.dimension);
      const defaultLevel = dimension?.hierarchy[dimension.hierarchy.length - 1];
      const level = axis.level ?? defaultLevel;
      if (!level) {
        throw new CubeError(400, `Unable to resolve level for dimension ${axis.dimension}.`);
      }
      const value = row.dimensions[axis.dimension]?.[level];
      return {
        dimension: axis.dimension,
        level,
        value: value ?? 'All'
      };
    });
  }

  private buildAxisKey(coords: PivotCoordinate[]): string {
    if (!coords.length) {
      return '__all__';
    }
    return coords.map((coord) => `${coord.dimension}.${coord.level}:${coord.value}`).join('|');
  }

  private toHeader(key: string, coords: PivotCoordinate[]): PivotHeader {
    const label = coords.length ? coords.map((coord) => String(coord.value)).join(' • ') : 'All';
    return { key, label, coordinates: coords };
  }

  private deriveVisualSuggestions(rows: AxisSpec[], columns: AxisSpec[], measures: string[]): string[] {
    const suggestions = new Set<string>();
    if (rows.length && columns.length) {
      suggestions.add('heatmap');
      suggestions.add('stacked-bar');
    } else if (rows.length) {
      suggestions.add(rows.length === 1 ? 'column' : 'matrix');
      suggestions.add('line');
    } else {
      suggestions.add(measures.length > 1 ? 'multi-stat' : 'big-number');
    }
    return [...suggestions];
  }

  private buildBreadcrumbs(query: NormalizedQuery, cube: CubeInstance): BreadcrumbEntry[] {
    if (!query.drill || !query.drill.path?.length) {
      return [];
    }

    const dimension = cube.dimensionMap.get(query.drill.dimension);
    if (!dimension) {
      return [];
    }

    const fromIndex = dimension.hierarchy.indexOf(query.drill.fromLevel);
    const toIndex = dimension.hierarchy.indexOf(query.drill.toLevel);
    if (fromIndex === -1 || toIndex === -1) {
      return [];
    }

    const start = Math.min(fromIndex, toIndex);
    const breadth = Math.abs(toIndex - fromIndex) + 1;
    const relevantLevels = dimension.hierarchy.slice(start, start + breadth);

    return query.drill.path.map((value, idx) => ({
      dimension: query.drill!.dimension,
      level: relevantLevels[idx] ?? query.drill!.toLevel,
      value: value as DimensionValue | 'All'
    }));
  }

  private matchesFilters(row: FactRow, query: NormalizedQuery, cube: CubeInstance): boolean {
    const filters = query.allFilters;
    for (const filter of filters) {
      const value = this.valueForFilter(row, filter, cube);
      if (!this.evaluateFilter(value, filter)) {
        return false;
      }
    }

    if (query.drill && query.drill.path?.length) {
      if (!this.matchesDrill(row, query.drill, cube)) {
        return false;
      }
    }

    return true;
  }

  private valueForFilter(row: FactRow, filter: FilterSpec, cube: CubeInstance): DimensionValue | undefined {
    const dimension = cube.dimensionMap.get(filter.dimension);
    if (!dimension) {
      return undefined;
    }
    const level = filter.level ?? dimension.hierarchy[dimension.hierarchy.length - 1];
    return row.dimensions[filter.dimension]?.[level];
  }

  private evaluateFilter(value: DimensionValue | undefined, filter: FilterSpec): boolean {
    const target = filter.value as any;
    switch (filter.operator) {
      case 'eq':
        return value === target;
      case 'neq':
        return value !== target;
      case 'in':
        return Array.isArray(target) ? target.includes(value) : value === target;
      case 'nin':
        return Array.isArray(target) ? !target.includes(value) : value !== target;
      case 'gt':
        return typeof value === 'number' && typeof target === 'number' && value > target;
      case 'gte':
        return typeof value === 'number' && typeof target === 'number' && value >= target;
      case 'lt':
        return typeof value === 'number' && typeof target === 'number' && value < target;
      case 'lte':
        return typeof value === 'number' && typeof target === 'number' && value <= target;
      case 'between':
        if (Array.isArray(target) && target.length === 2) {
          const [min, max] = target;
          return typeof value === 'number' && value >= Number(min) && value <= Number(max);
        }
        return false;
      default:
        return true;
    }
  }

  private matchesDrill(row: FactRow, drill: DrillSpec, cube: CubeInstance): boolean {
    const dimension = cube.dimensionMap.get(drill.dimension);
    if (!dimension) {
      return true;
    }

    if (!drill.path?.length) {
      return true;
    }

    const fromIndex = dimension.hierarchy.indexOf(drill.fromLevel);
    const toIndex = dimension.hierarchy.indexOf(drill.toLevel);
    if (fromIndex === -1 || toIndex === -1) {
      return true;
    }

    const start = Math.min(fromIndex, toIndex);
    const length = Math.min(drill.path.length, Math.abs(toIndex - fromIndex) + 1);

    for (let i = 0; i < length; i += 1) {
      const level = dimension.hierarchy[start + i];
      const expected = drill.path[i];
      const actual = row.dimensions[drill.dimension]?.[level];
      if (actual === undefined) {
        return false;
      }
      if (typeof actual === 'number') {
        if (Number(expected) !== actual) {
          return false;
        }
      } else if (String(actual) !== String(expected)) {
        return false;
      }
    }

    return true;
  }

  private createMeasureAccumulatorSet(measureDefs: CubeMeasure[]): Record<string, AggregationAccumulator> {
    return measureDefs.reduce<Record<string, AggregationAccumulator>>((acc, measure) => {
      acc[measure.name] = createAccumulator(measure.aggregation);
      return acc;
    }, {});
  }

  private extractMeasureValue(row: FactRow, measure: CubeMeasure): MeasureValue {
    return row.metrics[measure.valueField];
  }

  private buildCoordinateRecord(rowHeader: PivotHeader, columnHeader: PivotHeader): Record<string, DimensionValue | 'All'> {
    const record: Record<string, DimensionValue | 'All'> = {};
    const assign = (coord: PivotCoordinate) => {
      record[`${coord.dimension}.${coord.level}`] = coord.value;
    };
    rowHeader.coordinates.forEach(assign);
    columnHeader.coordinates.forEach(assign);
    return record;
  }

  private materializeAggregates(definition: CubeDefinition) {
    const workingStore = new Map<string, Map<DimensionValue, Record<string, AggregationAccumulator>>>();

    for (const row of definition.rows) {
      for (const dimension of definition.dimensions) {
        for (const level of dimension.hierarchy) {
          const value = row.dimensions[dimension.name]?.[level];
          if (value === undefined) continue;
          const key = this.storeKey(dimension.name, level);
          if (!workingStore.has(key)) {
            workingStore.set(key, new Map());
          }
          const levelStore = workingStore.get(key)!;
          if (!levelStore.has(value)) {
            levelStore.set(value, this.createMeasureAccumulatorSet(definition.measures));
          }
          const accumulators = levelStore.get(value)!;
          for (const measure of definition.measures) {
            const metric = row.metrics[measure.valueField];
            accumulators[measure.name].add(metric);
          }
        }
      }
    }

    const finalStore = new Map<string, Map<DimensionValue, Record<string, number>>>();
    workingStore.forEach((levelStore, key) => {
      const finalizedLevel = new Map<DimensionValue, Record<string, number>>();
      levelStore.forEach((accumulators, value) => {
        const resolved: Record<string, number> = {};
        definition.measures.forEach((measure) => {
          resolved[measure.name] = accumulators[measure.name].finalize();
        });
        finalizedLevel.set(value, resolved);
      });
      finalStore.set(key, finalizedLevel);
    });

    return finalStore;
  }

  private storeKey(dimension: string, level: string) {
    return `${dimension}.${level}`;
  }
}

function compareValues(a: DimensionValue, b: DimensionValue): number {
  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }
  return String(a).localeCompare(String(b));
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  }
  if (value && typeof value === 'object') {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableStringify((value as Record<string, unknown>)[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

type PartialQuery = Partial<QueryPayload> & {
  slices?: FilterSpec[];
  dices?: FilterSpec[];
  filters?: FilterSpec[];
  rows?: AxisSpec[];
  columns?: AxisSpec[];
};

function mergeQueryPayload(base: PartialQuery, override: QueryPayload): QueryPayload {
  return {
    ...base,
    ...override,
    rows: override.rows ?? base.rows,
    columns: override.columns ?? base.columns,
    slices: override.slices ?? base.slices,
    dices: override.dices ?? base.dices,
    filters: override.filters ?? base.filters,
    measures: override.measures ?? base.measures ?? [],
    drill: override.drill ?? base.drill,
    rollup: override.rollup ?? base.rollup,
    pivot: override.pivot ?? base.pivot,
    includeFlattened: override.includeFlattened ?? base.includeFlattened
  } as QueryPayload;
}

function parseLightweightMdx(mdx?: string): PartialQuery {
  if (!mdx) {
    return {};
  }

  const fragments = mdx
    .split(/;/)
    .map((fragment) => fragment.trim())
    .filter(Boolean);

  const partial: PartialQuery = {};

  for (const fragment of fragments) {
    const [keywordRaw, ...restParts] = fragment.split(/\s+/);
    const keyword = keywordRaw.toUpperCase();
    const rest = restParts.join(' ').trim();

    switch (keyword) {
      case 'MEASURES': {
        partial.measures = rest.split(/,/).map((token) => token.trim()).filter(Boolean);
        break;
      }
      case 'ROWS': {
        partial.rows = parseAxisList(rest);
        break;
      }
      case 'COLUMNS': {
        partial.columns = parseAxisList(rest);
        break;
      }
      case 'SLICE': {
        partial.slices = [...(partial.slices ?? []), parseEqualityFilter(rest, 'eq')];
        break;
      }
      case 'DICE': {
        partial.dices = [...(partial.dices ?? []), parseSetFilter(rest)];
        break;
      }
      case 'FILTER': {
        partial.filters = [...(partial.filters ?? []), parseComparisonFilter(rest)];
        break;
      }
      case 'DRILL': {
        const drillParts = rest.split(/\s+/).filter(Boolean);
        const dimension = drillParts[0];
        const fromLevel = drillParts[1];
        const arrowIndex = drillParts.findIndex((token) => token === '->' || token.toLowerCase() === 'to');
        const toLevel = arrowIndex >= 0 ? drillParts[arrowIndex + 1] : drillParts[2];
        if (!dimension || !fromLevel || !toLevel) {
          throw new CubeError(400, `Invalid DRILL clause: ${rest}`);
        }
        const pathIndex = drillParts.findIndex((token) => token.toLowerCase() === 'path');
        const path =
          pathIndex >= 0
            ? drillParts
                .slice(pathIndex + 1)
                .join(' ')
                .split(/,|>/)
                .map((token) => token.trim())
                .filter(Boolean)
            : [];
        partial.drill = {
          dimension,
          fromLevel,
          toLevel,
          path
        } as DrillSpec;
        break;
      }
      case 'ROLLUP': {
        const [dimension, level] = rest.split(/\s+/);
        partial.rollup = { dimension, level };
        break;
      }
      default:
        break;
    }
  }

  return partial;
}

function parseAxisList(value: string): AxisSpec[] {
  return value
    .split(/,/)
    .map((token) => token.trim())
    .filter(Boolean)
    .map((token) => {
      const [dimension, level] = token.split('.');
      return { dimension, level };
    });
}

function parseScalarToken(token: string): string | number {
  const numeric = Number(token);
  return Number.isNaN(numeric) ? token : numeric;
}

function parseEqualityFilter(expression: string, operator: FilterSpec['operator']): FilterSpec {
  const [lhs, rhs] = expression.split('=');
  if (!lhs || !rhs) {
    throw new CubeError(400, `Invalid slice expression: ${expression}`);
  }
  const [dimension, level] = lhs.trim().split('.');
  const rawValue = rhs.trim().replace(/^['"]|['"]$/g, '');
  const value = parseScalarToken(rawValue);
  return { dimension, level, operator, value } as FilterSpec;
}

function parseSetFilter(expression: string): FilterSpec {
  const match = expression.match(/([^\s]+)\s+IN\s*\(([^)]+)\)/i);
  if (!match) {
    throw new CubeError(400, `Invalid dice expression: ${expression}`);
  }
  const [dimension, level] = match[1].split('.');
  const value = match[2]
    .split(/,/)
    .map((token) => parseScalarToken(token.trim().replace(/^['"]|['"]$/g, '')));
  return { dimension, level, operator: 'in', value };
}

function parseComparisonFilter(expression: string): FilterSpec {
  const match = expression.match(/([^\s]+)\s*(=|!=|>=|<=|>|<)\s*(.+)/);
  if (!match) {
    throw new CubeError(400, `Invalid filter expression: ${expression}`);
  }
  const [dimension, level] = match[1].split('.');
  const operatorMap: Record<string, FilterSpec['operator']> = {
    '=': 'eq',
    '!=': 'neq',
    '>': 'gt',
    '>=': 'gte',
    '<': 'lt',
    '<=': 'lte'
  };
  const operator = operatorMap[match[2]];
  if (!operator) {
    throw new CubeError(400, `Unsupported operator in FILTER clause: ${match[2]}`);
  }
  const valueToken = match[3].trim().replace(/^['"]|['"]$/g, '');
  const value = parseScalarToken(valueToken);
  return { dimension, level, operator, value };
}
