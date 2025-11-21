export interface DataSource {
  name: string;
  type: 'csv' | 'postgres' | 'rest';
  connection: CSVConnection | PostgresConnection | RESTConnection;
  schema: string;
  watermarkColumn?: string;
}

export interface CSVConnection {
  filePath: string;
  delimiter?: string;
  hasHeader?: boolean;
}

export interface PostgresConnection {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  table: string;
  query?: string;
}

export interface RESTConnection {
  url: string;
  method?: 'GET' | 'POST';
  headers?: Record<string, string>;
  auth?: {
    type: 'bearer' | 'basic';
    token?: string;
    username?: string;
    password?: string;
  };
  dataPath?: string;
}

export interface ETLConfig {
  sources: DataSource[];
  target: {
    type: 'memory' | 'postgres';
    connection?: PostgresConnection;
  };
  cubes: CubeConfig[];
}

export interface CubeConfig {
  name: string;
  factTable: string;
  dimensions: string[];
  measures: string[];
  sources: string[];
}

export interface StagingData {
  source: string;
  schema: string;
  data: Record<string, any>[];
  watermark?: Date;
}

export interface ValidationResult {
  valid: boolean;
  errors: ValidationError[];
  rowsPassed: number;
  rowsFailed: number;
}

export interface ValidationError {
  row: number;
  field: string;
  message: string;
  value: any;
}

export interface RefreshMetadata {
  cube: string;
  source: string;
  startTime: Date;
  endTime: Date;
  rowsProcessed: number;
  rowsInserted: number;
  rowsUpdated: number;
  watermark?: Date;
  status: 'success' | 'failed' | 'partial';
  errors?: string[];
}

export interface WatermarkRecord {
  source: string;
  lastWatermark: Date;
  lastRunTime: Date;
}
