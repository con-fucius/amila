
/**
 * SQL Analysis Utility
 * Provides frontend-side estimation of query cost, impact, and lineage.
 * Used when backend "EXPLAIN" or metadata is unavailable.
 */

export interface CostEstimate {
    cost: number; // 0-100 scale
    complexity: 'Low' | 'Medium' | 'High';
    reason: string;
    bytesScannedEstimate?: string;
}

export interface ImpactAssessment {
    level: 'Safe' | 'Moderate' | 'Critical';
    description: string;
    requiresConfirmation: boolean;
}

export interface JoinInfo {
    type: string; // INNER, LEFT, RIGHT, FULL
    table: string;
}

export interface LineageInfo {
    tables: string[];
    operations: string[];
    mainStatement: string;
    joins?: JoinInfo[];
}

const KEYWORDS = {
    highCost: ['JOIN', 'UNION', 'GROUP BY', 'ORDER BY', 'DISTINCT', 'HAVING', 'subquery'],
    mediumCost: ['WHERE', 'LIKE', 'IN', 'BETWEEN'],
    criticalImpact: ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'GRANT', 'REVOKE'],
    moderateImpact: ['CREATE', 'INSERT', 'UPDATE', 'REPLACE'],
};

/**
 * Estimates the computational cost of a query based on keywords and structure.
 */
export function estimateCost(sql: string): CostEstimate {
    const upperSql = sql.toUpperCase();
    let cost = 10; // Base cost
    const reasons: string[] = [];

    // Complexity Checks
    const joinCount = (upperSql.match(/\bJOIN\b/g) || []).length;
    if (joinCount > 0) {
        cost += joinCount * 15;
        reasons.push(`${joinCount} JOINs`);
    }

    if (upperSql.includes('UNION')) {
        cost += 20;
        reasons.push('UNION usage');
    } else if (upperSql.includes('GROUP BY')) {
        cost += 15;
        reasons.push('Aggregation');
    }

    if (upperSql.includes('ORDER BY')) {
        cost += 10;
        reasons.push('Sorting');
    }

    const subqueryCount = (upperSql.match(/\(\s*SELECT/g) || []).length;
    if (subqueryCount > 0) {
        cost += subqueryCount * 20;
        reasons.push('Subqueries');
    }

    // Length factor
    if (sql.length > 500) cost += 10;

    // Normalize
    cost = Math.min(100, cost);

    let complexity: 'Low' | 'Medium' | 'High' = 'Low';
    if (cost > 70) complexity = 'High';
    else if (cost > 40) complexity = 'Medium';

    return {
        cost,
        complexity,
        reason: reasons.length > 0 ? `Affected by ${reasons.join(', ')}` : 'Simple query',
        bytesScannedEstimate: complexity === 'High' ? '> 1 GB' : complexity === 'Medium' ? '~ 100 MB' : '< 10 MB'
    };
}

/**
 * Assesses the potential impact/risk of running the query.
 */
export function assessImpact(sql: string): ImpactAssessment {
    const upperSql = sql.toUpperCase();

    for (const word of KEYWORDS.criticalImpact) {
        if (upperSql.includes(word)) {
            return {
                level: 'Critical',
                description: `Contains '${word}' which may modify schema or delete data.`,
                requiresConfirmation: true
            };
        }
    }

    for (const word of KEYWORDS.moderateImpact) {
        if (upperSql.includes(word)) {
            return {
                level: 'Moderate',
                description: `Contains '${word}' which modifies data.`,
                requiresConfirmation: false // Usually safe to run but good to know
            };
        }
    }

    return {
        level: 'Safe',
        description: 'Read-only query (SELECT).',
        requiresConfirmation: false
    };
}

/**
 * Extracts table names, JOIN types, and main operation to build a detailed lineage.
 */
export function extractLineage(sql: string): LineageInfo {
    const upperSql = sql.toUpperCase();
    const tables = new Set<string>();
    const joins: JoinInfo[] = [];

    // Regex to find tables after FROM
    const fromRegex = /\bFROM\s+([a-zA-Z0-9_$.]+)/gi;
    let match;

    while ((match = fromRegex.exec(sql)) !== null) {
        const candidate = match[1].replace(/;/, '');
        if (!['SELECT', 'WHERE', 'ON', 'USING'].includes(candidate.toUpperCase())) {
            tables.add(candidate);
        }
    }

    // Regex to find JOIN types and tables
    const joinRegex = /\b(LEFT|RIGHT|INNER|FULL|CROSS)?\s*(?:OUTER\s+)?JOIN\s+([a-zA-Z0-9_$.]+)/gi;
    
    while ((match = joinRegex.exec(sql)) !== null) {
        const joinType = match[1] ? match[1].toUpperCase() : 'INNER';
        const table = match[2].replace(/;/, '');
        
        if (!['SELECT', 'WHERE', 'ON', 'USING'].includes(table.toUpperCase())) {
            tables.add(table);
            joins.push({
                type: joinType,
                table: table
            });
        }
    }

    let mainOperation = 'SELECT';
    if (upperSql.startsWith('INSERT')) mainOperation = 'INSERT';
    else if (upperSql.startsWith('UPDATE')) mainOperation = 'UPDATE';
    else if (upperSql.startsWith('DELETE')) mainOperation = 'DELETE';
    else if (upperSql.startsWith('CREATE')) mainOperation = 'CREATE';
    else if (upperSql.startsWith('ALTER')) mainOperation = 'ALTER';
    else if (upperSql.startsWith('DROP')) mainOperation = 'DROP';

    return {
        tables: Array.from(tables),
        operations: [mainOperation],
        mainStatement: mainOperation,
        joins: joins.length > 0 ? joins : undefined
    };
}
