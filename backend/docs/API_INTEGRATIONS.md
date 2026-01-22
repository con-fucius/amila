# API Integrations Documentation

Complete API reference for PostgreSQL, Qlik Sense, and Apache Superset integrations.

## PostgreSQL Integration

### Health Check

**Endpoint:** `GET /api/v1/health/database?type=postgres`

**Description:** Check PostgreSQL connection health and pool status.

**Response:**
```json
{
  "status": "healthy",
  "database_type": "postgres",
  "timestamp": "2026-01-21T10:30:00Z",
  "latency_ms": 15.2,
  "pool": {
    "size": 5,
    "available": 3
  },
  "read_only": true
}
```

### Query Execution

**Endpoint:** `POST /api/v1/queries/process`

**Description:** Execute natural language query against PostgreSQL.

**Request:**
```json
{
  "query": "Show me the top 10 customers by revenue",
  "database_type": "postgres",
  "user_id": "user123",
  "session_id": "session456"
}
```

**Response:**
```json
{
  "query_id": "q_abc123",
  "status": "completed",
  "sql_query": "SELECT customer_name, SUM(revenue) as total_revenue FROM customers GROUP BY customer_name ORDER BY total_revenue DESC LIMIT 10",
  "results": {
    "columns": ["customer_name", "total_revenue"],
    "rows": [...],
    "row_count": 10
  },
  "execution_time_ms": 245
}
```

### Schema Information

**Endpoint:** `GET /api/v1/schema?database_type=postgres`

**Description:** Get PostgreSQL schema metadata.

**Response:**
```json
{
  "status": "success",
  "source": "postgres",
  "schema_data": {
    "schema": "public",
    "tables": {
      "users": {
        "columns": [
          {
            "name": "id",
            "type": "integer",
            "nullable": false,
            "default": "nextval('users_id_seq'::regclass)"
          },
          {
            "name": "email",
            "type": "character varying",
            "nullable": false,
            "default": null
          }
        ],
        "primary_keys": ["id"],
        "foreign_keys": []
      }
    },
    "table_count": 15
  }
}
```

## Qlik Sense Integration

### Health Check

**Endpoint:** `GET /api/v1/qlik/health`

**Description:** Check Qlik Sense API connectivity.

**Headers:**
```
Authorization: Bearer <jwt_token>
```

**Response:**
```json
{
  "status": "healthy",
  "version": "May 2025",
  "product": "Qlik Sense Enterprise"
}
```

### List Apps

**Endpoint:** `GET /api/v1/qlik/apps`

**Description:** List all Qlik Sense apps (read-only).

**Query Parameters:**
- `filter_query` (optional): Filter expression (e.g., "name eq 'Sales Dashboard'")

**Headers:**
```
Authorization: Bearer <jwt_token>
```

**Response:**
```json
{
  "status": "success",
  "apps": [
    {
      "id": "app-guid-123",
      "name": "Sales Dashboard",
      "description": "Monthly sales analysis",
      "published": true,
      "modifiedDate": "2026-01-15T10:00:00Z"
    }
  ],
  "count": 1
}
```

### Get App Details

**Endpoint:** `GET /api/v1/qlik/apps/{app_id}`

**Description:** Get detailed information about a specific Qlik app.

**Headers:**
```
Authorization: Bearer <jwt_token>
```

**Response:**
```json
{
  "status": "success",
  "app": {
    "id": "app-guid-123",
    "name": "Sales Dashboard",
    "description": "Monthly sales analysis",
    "owner": {
      "id": "user-guid",
      "name": "John Doe"
    },
    "stream": {
      "id": "stream-guid",
      "name": "Everyone"
    },
    "published": true,
    "lastReloadTime": "2026-01-20T08:00:00Z"
  }
}
```

### List Sheets

**Endpoint:** `GET /api/v1/qlik/apps/{app_id}/sheets`

**Description:** List all sheets in a Qlik app.

**Headers:**
```
Authorization: Bearer <jwt_token>
```

**Response:**
```json
{
  "status": "success",
  "sheets": [
    {
      "id": "sheet-guid-123",
      "name": "Overview",
      "description": "Executive summary",
      "rank": 0,
      "approved": true
    }
  ],
  "count": 1
}
```

## Apache Superset Integration

### Health Check

**Endpoint:** `GET /api/v1/superset/health`

**Description:** Check Apache Superset API connectivity.

**Headers:**
```
Authorization: Bearer <jwt_token>
```

**Response:**
```json
{
  "status": "healthy"
}
```

### List Dashboards

**Endpoint:** `GET /api/v1/superset/dashboards`

**Description:** List all Superset dashboards.

**Headers:**
```
Authorization: Bearer <jwt_token>
```

**Response:**
```json
{
  "status": "success",
  "dashboards": [
    {
      "id": 1,
      "dashboard_title": "Sales Analytics",
      "slug": "sales-analytics",
      "published": true,
      "changed_on": "2026-01-20T10:00:00Z",
      "owners": [
        {
          "id": 1,
          "username": "admin"
        }
      ]
    }
  ],
  "count": 1
}
```

### Get Dashboard

**Endpoint:** `GET /api/v1/superset/dashboards/{dashboard_id}`

**Description:** Get detailed dashboard information.

**Headers:**
```
Authorization: Bearer <jwt_token>
```

**Response:**
```json
{
  "status": "success",
  "dashboard": {
    "id": 1,
    "dashboard_title": "Sales Analytics",
    "slug": "sales-analytics",
    "position_json": "...",
    "charts": [
      {
        "id": 10,
        "slice_name": "Revenue by Month"
      }
    ],
    "published": true
  }
}
```

### Create Chart

**Endpoint:** `POST /api/v1/superset/charts`

**Description:** Create a new chart in Superset.

**Headers:**
```
Authorization: Bearer <jwt_token>
Content-Type: application/json
```

**Request:**
```json
{
  "chart_config": {
    "slice_name": "Revenue Trend",
    "viz_type": "line",
    "datasource_id": 1,
    "datasource_type": "table",
    "params": {
      "metrics": ["sum__revenue"],
      "groupby": ["month"]
    }
  }
}
```

**Response:**
```json
{
  "status": "success",
  "chart": {
    "id": 15,
    "slice_name": "Revenue Trend",
    "viz_type": "line"
  }
}
```

### Auto-Generate Dashboard

**Endpoint:** `POST /api/v1/superset/dashboards/generate`

**Description:** Auto-generate dashboard from query results with visualization recommendations.

**Headers:**
```
Authorization: Bearer <jwt_token>
Content-Type: application/json
```

**Request:**
```json
{
  "query_result": {
    "columns": ["month", "revenue", "orders"],
    "rows": [
      {"month": "January", "revenue": 50000, "orders": 120},
      {"month": "February", "revenue": 55000, "orders": 135}
    ]
  },
  "dashboard_title": "Monthly Sales Dashboard"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Dashboard generation prepared",
  "visualization_recommendation": {
    "chart_type": "line",
    "x_axis": "month",
    "y_axis": "revenue",
    "reason": "Temporal data with numeric values"
  },
  "note": "Full dashboard creation requires Superset dataset and chart configuration"
}
```

## Error Responses

All endpoints return consistent error responses:

### 400 Bad Request
```json
{
  "detail": "Invalid request parameters"
}
```

### 401 Unauthorized
```json
{
  "detail": "Not authenticated"
}
```

### 403 Forbidden
```json
{
  "detail": "Insufficient permissions"
}
```

### 502 Bad Gateway
```json
{
  "detail": "External service error: Connection refused"
}
```

### 503 Service Unavailable
```json
{
  "detail": "PostgreSQL integration not enabled"
}
```

## Authentication

All endpoints require JWT authentication except health checks.

**Get Token:**
```bash
POST /api/v1/auth/login
Content-Type: application/json

{
  "username": "admin",
  "password": "password"
}
```

**Response:**
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "token_type": "bearer",
  "expires_in": 28800
}
```

**Use Token:**
```bash
Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc...
```

## Rate Limiting

- Authentication endpoints: 5 requests per 5 minutes
- Query endpoints: 100 requests per minute per user
- Schema endpoints: 50 requests per minute per user
- Qlik/Superset endpoints: 50 requests per minute per user

## Audit Logging

All operations are logged with:
- User ID
- Request ID
- Timestamp
- Operation type
- Status (success/failure)
- Execution time
- Error details (if failed)

Query audit logs:
```bash
GET /api/v1/analytics?user_id=user123&start_date=2026-01-01
```

## Monitoring

### System Health
```bash
GET /api/v1/health/detailed
```

Returns comprehensive health status including:
- PostgreSQL connection pool
- Qlik Sense API connectivity
- Superset API connectivity
- Redis status
- Celery workers
- LangGraph orchestrator

### Metrics
```bash
GET /api/v1/health/mcp-tools
```

Returns MCP tool availability for all database types.
