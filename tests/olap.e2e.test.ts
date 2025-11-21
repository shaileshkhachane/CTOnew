import request from 'supertest';
import { createApp } from '../src/app';
import { buildSampleCubeManager } from '../src/engine/sample-cube';
import { CubeManagerOptions } from '../src/engine/cube-manager';

function makeTestApp(options?: CubeManagerOptions) {
  const manager = buildSampleCubeManager(options);
  return createApp(manager);
}

describe('POST /olap/query', () => {
  it('materializes a deterministic slice against the cube', async () => {
    const app = makeTestApp();
    const response = await request(app)
      .post('/olap/query')
      .send({
        cube: 'sales',
        rows: [{ dimension: 'time', level: 'year' }],
        measures: ['revenue'],
        slices: [{ dimension: 'geography', level: 'region', operator: 'eq', value: 'North America' }]
      })
      .expect(200);

    const { data, metadata } = response.body;
    expect(data.pivot.rows.map((row: any) => row.label)).toEqual(['2023', '2024']);
    expect(data.pivot.measures[0].values).toEqual([[3500], [1300]]);
    expect(metadata.cache.hit).toBe(false);
    expect(metadata.suggestions.length).toBeGreaterThan(0);
  });

  it('supports drilling down with breadcrumb metadata', async () => {
    const app = makeTestApp();
    const response = await request(app)
      .post('/olap/query')
      .send({
        cube: 'sales',
        rows: [{ dimension: 'time', level: 'year' }],
        measures: ['units'],
        drill: {
          dimension: 'time',
          fromLevel: 'year',
          toLevel: 'month',
          path: [2023]
        }
      })
      .expect(200);

    const { data, metadata } = response.body;
    expect(metadata.breadcrumbs).toEqual([
      { dimension: 'time', level: 'year', value: 2023 }
    ]);
    expect(data.pivot.rows.map((row: any) => row.label)).toEqual(['Jan', 'Feb', 'Apr', 'May', 'Jul', 'Oct']);
    expect(data.pivot.measures[0].values).toEqual([[4], [2], [5], [3], [6], [4]]);
  });

  it('rolls data up to higher hierarchy levels while pivoting', async () => {
    const app = makeTestApp();
    const response = await request(app)
      .post('/olap/query')
      .send({
        cube: 'sales',
        rows: [
          { dimension: 'time', level: 'year' },
          { dimension: 'time', level: 'month' }
        ],
        measures: ['revenue'],
        rollup: {
          dimension: 'time',
          level: 'quarter'
        }
      })
      .expect(200);

    const expectedTotals = [[2100], [2700], [2000], [1400], [1300], [800], [1700], [900]];
    expect(response.body.data.pivot.measures[0].values).toEqual(expectedTotals);
  });

  it('memoizes queries according to the configured TTL', async () => {
    const ttlMs = 500;
    const app = makeTestApp({ cache: { ttlMs, max: 32 } });
    const payload = {
      cube: 'sales',
      rows: [{ dimension: 'time', level: 'year' }],
      measures: ['revenue']
    };

    const first = await request(app).post('/olap/query').send(payload).expect(200);
    const second = await request(app).post('/olap/query').send(payload).expect(200);

    expect(first.body.metadata.cache.hit).toBe(false);
    expect(second.body.metadata.cache.hit).toBe(true);
    expect(second.body.metadata.cache.stats.hits).toBeGreaterThanOrEqual(1);
    expect(second.body.metadata.cache.ttlRemainingMs).not.toBeNull();
    expect(second.body.metadata.cache.ttlRemainingMs).toBeLessThanOrEqual(ttlMs);
  });

  it('rejects malformed payloads with a 400 response', async () => {
    const app = makeTestApp();
    await request(app).post('/olap/query').send({ cube: 'sales' }).expect(400);
  });
});
