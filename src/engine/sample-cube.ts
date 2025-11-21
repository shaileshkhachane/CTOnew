import { CubeDefinition, CubeManager, CubeManagerOptions, FactRow } from './cube-manager';

const SAMPLE_FACTS: FactRow[] = [
  {
    dimensions: {
      time: { year: 2023, quarter: 'Q1', month: 'Jan' },
      geography: { region: 'North America', country: 'USA', state: 'CA' },
      product: { category: 'Electronics', item: 'Phone' }
    },
    metrics: { revenue: 1200, units: 4, orderId: 'S-1001', customerId: 'C-001' }
  },
  {
    dimensions: {
      time: { year: 2023, quarter: 'Q1', month: 'Feb' },
      geography: { region: 'North America', country: 'USA', state: 'NY' },
      product: { category: 'Accessories', item: 'Headphones' }
    },
    metrics: { revenue: 900, units: 2, orderId: 'S-1002', customerId: 'C-002' }
  },
  {
    dimensions: {
      time: { year: 2023, quarter: 'Q2', month: 'Apr' },
      geography: { region: 'EMEA', country: 'Germany', state: 'BE' },
      product: { category: 'Electronics', item: 'Laptop' }
    },
    metrics: { revenue: 1600, units: 5, orderId: 'S-1003', customerId: 'C-003' }
  },
  {
    dimensions: {
      time: { year: 2023, quarter: 'Q2', month: 'May' },
      geography: { region: 'EMEA', country: 'Germany', state: 'BW' },
      product: { category: 'Accessories', item: 'Mouse' }
    },
    metrics: { revenue: 1100, units: 3, orderId: 'S-1004', customerId: 'C-001' }
  },
  {
    dimensions: {
      time: { year: 2023, quarter: 'Q3', month: 'Jul' },
      geography: { region: 'APAC', country: 'Japan', state: 'Tokyo' },
      product: { category: 'Electronics', item: 'Camera' }
    },
    metrics: { revenue: 2000, units: 6, orderId: 'S-1005', customerId: 'C-004' }
  },
  {
    dimensions: {
      time: { year: 2023, quarter: 'Q4', month: 'Oct' },
      geography: { region: 'North America', country: 'Canada', state: 'ON' },
      product: { category: 'Accessories', item: 'Charger' }
    },
    metrics: { revenue: 1400, units: 4, orderId: 'S-1006', customerId: 'C-002' }
  },
  {
    dimensions: {
      time: { year: 2024, quarter: 'Q1', month: 'Jan' },
      geography: { region: 'North America', country: 'USA', state: 'CA' },
      product: { category: 'Electronics', item: 'Tablet' }
    },
    metrics: { revenue: 1300, units: 4, orderId: 'S-2001', customerId: 'C-005' }
  },
  {
    dimensions: {
      time: { year: 2024, quarter: 'Q2', month: 'Apr' },
      geography: { region: 'LATAM', country: 'Mexico', state: 'CDMX' },
      product: { category: 'Accessories', item: 'Case' }
    },
    metrics: { revenue: 800, units: 2, orderId: 'S-2002', customerId: 'C-006' }
  },
  {
    dimensions: {
      time: { year: 2024, quarter: 'Q3', month: 'Jul' },
      geography: { region: 'EMEA', country: 'UK', state: 'London' },
      product: { category: 'Electronics', item: 'Drone' }
    },
    metrics: { revenue: 1700, units: 5, orderId: 'S-2003', customerId: 'C-002' }
  },
  {
    dimensions: {
      time: { year: 2024, quarter: 'Q4', month: 'Nov' },
      geography: { region: 'APAC', country: 'Australia', state: 'NSW' },
      product: { category: 'Accessories', item: 'Stand' }
    },
    metrics: { revenue: 900, units: 3, orderId: 'S-2004', customerId: 'C-007' }
  }
];

const SALES_CUBE: CubeDefinition = {
  name: 'sales',
  description: 'Sample sales cube spanning geographies, time, and product hierarchies.',
  dimensions: [
    { name: 'time', label: 'Time', hierarchy: ['year', 'quarter', 'month'] },
    { name: 'geography', label: 'Geography', hierarchy: ['region', 'country', 'state'] },
    { name: 'product', label: 'Product', hierarchy: ['category', 'item'] }
  ],
  measures: [
    { name: 'revenue', label: 'Revenue', valueField: 'revenue', aggregation: 'SUM', format: 'currency' },
    { name: 'units', label: 'Units', valueField: 'units', aggregation: 'SUM' },
    { name: 'orders', label: 'Orders', valueField: 'orderId', aggregation: 'COUNT' },
    { name: 'avgTicket', label: 'Avg Ticket', valueField: 'revenue', aggregation: 'AVG' },
    { name: 'minRevenue', label: 'Min Revenue', valueField: 'revenue', aggregation: 'MIN' },
    { name: 'maxRevenue', label: 'Max Revenue', valueField: 'revenue', aggregation: 'MAX' },
    { name: 'uniqueCustomers', label: 'Unique Customers', valueField: 'customerId', aggregation: 'DISTINCT' }
  ],
  rows: SAMPLE_FACTS
};

export function buildSampleCubeManager(options?: CubeManagerOptions) {
  const manager = new CubeManager(options);
  manager.registerCube(SALES_CUBE);
  return manager;
}

export const sampleCubeManager = buildSampleCubeManager();
