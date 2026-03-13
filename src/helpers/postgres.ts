import type { Expression } from '../expression/expression.js'
import type {
  AliasedRawBuilder,
  RawBuilder,
} from '../raw-builder/raw-builder.js'
import { sql } from '../raw-builder/sql.js'
import type {
  ShallowDehydrateValue,
  ShallowDehydrateObject,
  Simplify,
} from '../util/type-utils.js'

/**
 * A postgres helper for aggregating a subquery (or other expression) into a JSONB array.
 *
 * ### Examples
 *
 * <!-- siteExample("select", "Nested array", 110) -->
 *
 * While kysely is not an ORM and it doesn't have the concept of relations, we do provide
 * helpers for fetching nested objects and arrays in a single query. In this example we
 * use the `jsonArrayFrom` helper to fetch person's pets along with the person's id.
 *
 * Please keep in mind that the helpers under the `kysely/helpers` folder, including
 * `jsonArrayFrom`, are not guaranteed to work with third party dialects. In order for
 * them to work, the dialect must automatically parse the `json` data type into
 * JavaScript JSON values like objects and arrays. Some dialects might simply return
 * the data as a JSON string. In these cases you can use the built in `ParseJSONResultsPlugin`
 * to parse the results.
 *
 * ```ts
 * import { jsonArrayFrom } from 'kysely/helpers/postgres'
 *
 * const result = await db
 *   .selectFrom('person')
 *   .select((eb) => [
 *     'id',
 *     jsonArrayFrom(
 *       eb.selectFrom('pet')
 *         .select(['pet.id as pet_id', 'pet.name'])
 *         .whereRef('pet.owner_id', '=', 'person.id')
 *         .orderBy('pet.name')
 *     ).as('pets')
 *   ])
 *   .execute()
 * ```
 *
 * The generated SQL (PostgreSQL):
 *
 * ```sql
 * select "id", (
 *   select coalesce(json_agg(agg), '[]') from (
 *     select "pet"."id" as "pet_id", "pet"."name"
 *     from "pet"
 *     where "pet"."owner_id" = "person"."id"
 *     order by "pet"."name"
 *   ) as agg
 * ) as "pets"
 * from "person"
 * ```
 */
export function jsonArrayFrom<O>(
  expr: Expression<O>,
): RawBuilder<Simplify<ShallowDehydrateObject<O>>[]> {
  return sql`(select coalesce(json_agg(agg), '[]') from ${expr} as agg)`
}

/**
 * A postgres helper for turning a subquery (or other expression) into a JSON object.
 *
 * The subquery must only return one row.
 *
 * ### Examples
 *
 * <!-- siteExample("select", "Nested object", 120) -->
 *
 * While kysely is not an ORM and it doesn't have the concept of relations, we do provide
 * helpers for fetching nested objects and arrays in a single query. In this example we
 * use the `jsonObjectFrom` helper to fetch person's favorite pet along with the person's id.
 *
 * Please keep in mind that the helpers under the `kysely/helpers` folder, including
 * `jsonObjectFrom`, are not guaranteed to work with third-party dialects. In order for
 * them to work, the dialect must automatically parse the `json` data type into
 * JavaScript JSON values like objects and arrays. Some dialects might simply return
 * the data as a JSON string. In these cases you can use the built in `ParseJSONResultsPlugin`
 * to parse the results.
 *
 * ```ts
 * import { jsonObjectFrom } from 'kysely/helpers/postgres'
 *
 * const result = await db
 *   .selectFrom('person')
 *   .select((eb) => [
 *     'id',
 *     jsonObjectFrom(
 *       eb.selectFrom('pet')
 *         .select(['pet.id as pet_id', 'pet.name'])
 *         .whereRef('pet.owner_id', '=', 'person.id')
 *         .where('pet.is_favorite', '=', true)
 *     ).as('favorite_pet')
 *   ])
 *   .execute()
 * ```
 *
 * The generated SQL (PostgreSQL):
 *
 * ```sql
 * select "id", (
 *   select to_json(obj) from (
 *     select "pet"."id" as "pet_id", "pet"."name"
 *     from "pet"
 *     where "pet"."owner_id" = "person"."id"
 *     and "pet"."is_favorite" = $1
 *   ) as obj
 * ) as "favorite_pet"
 * from "person"
 * ```
 */
export function jsonObjectFrom<O>(
  expr: Expression<O>,
): RawBuilder<Simplify<ShallowDehydrateObject<O>> | null> {
  return sql`(select to_json(obj) from ${expr} as obj)`
}

/**
 * The PostgreSQL `json_build_object` function.
 *
 * NOTE: This helper is only guaranteed to fully work with the built-in `PostgresDialect`.
 * While the produced SQL is compatible with all PostgreSQL databases, some third-party dialects
 * may not parse the nested JSON into objects. In these cases you can use the built in
 * `ParseJSONResultsPlugin` to parse the results.
 *
 * ### Examples
 *
 * ```ts
 * import { sql } from 'kysely'
 * import { jsonBuildObject } from 'kysely/helpers/postgres'
 *
 * const result = await db
 *   .selectFrom('person')
 *   .select((eb) => [
 *     'id',
 *     jsonBuildObject({
 *       first: eb.ref('first_name'),
 *       last: eb.ref('last_name'),
 *       full: sql<string>`first_name || ' ' || last_name`
 *     }).as('name')
 *   ])
 *   .execute()
 *
 * result[0]?.id
 * result[0]?.name.first
 * result[0]?.name.last
 * result[0]?.name.full
 * ```
 *
 * The generated SQL (PostgreSQL):
 *
 * ```sql
 * select "id", json_build_object(
 *   'first', first_name,
 *   'last', last_name,
 *   'full', first_name || ' ' || last_name
 * ) as "name"
 * from "person"
 * ```
 */
export function jsonBuildObject<O extends Record<string, Expression<unknown>>>(
  obj: O,
): RawBuilder<
  Simplify<{
    [K in keyof O]: O[K] extends Expression<infer V>
      ? ShallowDehydrateValue<V>
      : never
  }>
> {
  return sql`json_build_object(${sql.join(
    Object.keys(obj).flatMap((k) => [sql.lit(k), obj[k]]),
  )})`
}

export type MergeAction = 'INSERT' | 'UPDATE' | 'DELETE'

/**
 * The PostgreSQL `merge_action` function.
 *
 * This function can be used in a `returning` clause to get the action that was
 * performed in a `mergeInto` query. The function returns one of the following
 * strings: `'INSERT'`, `'UPDATE'`, or `'DELETE'`.
 *
 * ### Examples
 *
 * ```ts
 * import { mergeAction } from 'kysely/helpers/postgres'
 *
 * const result = await db
 *   .mergeInto('person as p')
 *   .using('person_backup as pb', 'p.id', 'pb.id')
 *   .whenMatched()
 *   .thenUpdateSet(({ ref }) => ({
 *     first_name: ref('pb.first_name'),
 *     updated_at: ref('pb.updated_at').$castTo<string | null>(),
 *   }))
 *   .whenNotMatched()
 *   .thenInsertValues(({ ref}) => ({
 *     id: ref('pb.id'),
 *     first_name: ref('pb.first_name'),
 *     created_at: ref('pb.updated_at'),
 *     updated_at: ref('pb.updated_at').$castTo<string | null>(),
 *   }))
 *   .returning([mergeAction().as('action'), 'p.id', 'p.updated_at'])
 *   .execute()
 *
 * result[0].action
 * ```
 *
 * The generated SQL (PostgreSQL):
 *
 * ```sql
 * merge into "person" as "p"
 * using "person_backup" as "pb" on "p"."id" = "pb"."id"
 * when matched then update set
 *   "first_name" = "pb"."first_name",
 *   "updated_at" = "pb"."updated_at"::text
 * when not matched then insert values ("id", "first_name", "created_at", "updated_at")
 * values ("pb"."id", "pb"."first_name", "pb"."updated_at", "pb"."updated_at")
 * returning merge_action() as "action", "p"."id", "p"."updated_at"
 * ```
 */
export function mergeAction(): RawBuilder<MergeAction> {
  return sql`merge_action()`
}

/**
 * A PostgreSQL helper for creating a `VALUES` list that can be used as a
 * table-like expression in CTEs or joins. This is useful for batch updates,
 * upserts, and other operations where you want to join against a set of
 * values.
 *
 * NOTE: This helper is only guaranteed to work with the built-in `PostgresDialect`.
 *
 * ### Examples
 *
 * ```ts
 * import { sql } from 'kysely'
 * import { values } from 'kysely/helpers/postgres'
 *
 * // Batch update using a CTE:
 * const records = [
 *   { id: 1, price: 200 },
 *   { id: 2, price: 300 },
 * ]
 *
 * await db
 *   .with('new_values', () => values(records, 'new_values'))
 *   .updateTable('pet as p')
 *   .from('new_values as nv')
 *   .set({
 *     price: (eb) => eb.ref('nv.price'),
 *   })
 *   .whereRef('p.id', '=', 'nv.id')
 *   .execute()
 * ```
 *
 * The generated SQL (PostgreSQL):
 *
 * ```sql
 * with "new_values"("id", "price") as
 *   (values ($1, $2), ($3, $4))
 * update "pet" as "p"
 * set "price" = "nv"."price"
 * from "new_values" as "nv"
 * where "p"."id" = "nv"."id"
 * ```
 */
export function values<R extends Record<string, unknown>, A extends string>(
  records: R[],
  alias: A,
): AliasedRawBuilder<R, A> {
  const keys = Object.keys(records[0])
  const v = sql.join(
    records.map((r) => sql`(${sql.join(keys.map((k) => r[k]))})`),
  )
  const wrappedAlias = sql.ref(alias)
  const wrappedColumns = sql.join(keys.map(sql.ref))
  const aliasSql = sql`${wrappedAlias}(${wrappedColumns})`
  return sql<R>`(values ${v})`.as<A>(aliasSql)
}
