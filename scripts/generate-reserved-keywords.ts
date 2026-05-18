/**
 * Regenerates `src/sql-generator/reserved-keywords.generated.ts` from the
 * DuckDB instance that this package depends on.
 *
 * The generated file is committed and shipped — consumers don't need to run
 * DuckDB during their own build. Re-run this script when bumping the
 * `@duckdb/node-api` dependency:
 *
 *     npm run generate-keywords
 *
 * The list comes from DuckDB itself via `duckdb_keywords()` and includes only
 * keywords whose `keyword_category = 'reserved'` — the words that DuckDB
 * actually rejects as bare identifiers (SELECT, FROM, GROUP, ...). The other
 * non-`unreserved` categories (`type_function`, `column_name`) are reserved
 * only in type/function position; using them as column names works unquoted.
 * Quoting them anyway would just make the generated SQL noisier.
 */
import { DuckDBInstance } from '@duckdb/node-api';
import { writeFileSync } from 'fs';
import { join } from 'path';

const OUTPUT_PATH = join(__dirname, '..', 'src', 'sql-generator', 'reserved-keywords.generated.ts');

async function main(): Promise<void> {
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();

    const result = await connection.runAndReadAll(
        "SELECT keyword_name FROM duckdb_keywords() WHERE keyword_category = 'reserved' ORDER BY keyword_name",
    );

    const keywords: string[] = result
        .getRowObjectsJson()
        .map((row: any) => String(row.keyword_name).toUpperCase());

    if (keywords.length === 0) {
        throw new Error('duckdb_keywords() returned no rows — refusing to write an empty list');
    }

    const lines = keywords.map((k) => `    '${k}',`).join('\n');
    const contents = `// AUTO-GENERATED FILE — DO NOT EDIT BY HAND.
// Regenerate with: npm run generate-keywords
// Source: SELECT keyword_name FROM duckdb_keywords() WHERE keyword_category = 'reserved'
// DuckDB keyword count: ${keywords.length}

export const RESERVED_KEYWORDS: ReadonlySet<string> = new Set([
${lines}
]);
`;

    writeFileSync(OUTPUT_PATH, contents, 'utf8');
    console.log(`Wrote ${keywords.length} keywords to ${OUTPUT_PATH}`);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
