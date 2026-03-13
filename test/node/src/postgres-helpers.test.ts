import { sql } from '../../..'
import { values } from '../../../helpers/postgres'

import {
  clearDatabase,
  destroyTest,
  initTest,
  TestContext,
  testSql,
  expect,
  NOT_SUPPORTED,
  insertDefaultDataSet,
  DIALECTS,
} from './test-setup.js'

for (const dialect of DIALECTS.filter((dialect) => dialect === 'postgres')) {
  describe(`${dialect}: postgres helpers`, () => {
    let ctx: TestContext

    before(async function () {
      ctx = await initTest(this, dialect)
    })

    beforeEach(async () => {
      await insertDefaultDataSet(ctx)
    })

    afterEach(async () => {
      await clearDatabase(ctx)
    })

    after(async () => {
      await destroyTest(ctx)
    })

    describe('values', () => {
      it('should create a values list usable in a CTE', async () => {
        const records = [
          { id: 1, name: 'foo' },
          { id: 2, name: 'bar' },
        ]

        const query = ctx.db
          .with('v', () => values(records, 'v'))
          .selectFrom('v')
          .selectAll()

        testSql(query, dialect, {
          postgres: {
            sql: 'with "v"("id", "name") as (values ($1, $2), ($3, $4)) select * from "v"',
            parameters: [1, 'foo', 2, 'bar'],
          },
          mysql: NOT_SUPPORTED,
          mssql: NOT_SUPPORTED,
          sqlite: NOT_SUPPORTED,
        })
      })
    })
  })
}
