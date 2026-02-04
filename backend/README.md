# Amila Backend

FastAPI + LangGraph orchestrator for natural language database queries.

## Structure

```
app/
  orchestrator/     # 15-node LangGraph workflow
  api/v1/endpoints/ # FastAPI routes
  core/             # Infrastructure (Redis, auth, logging)
  services/         # Business logic
  tasks/            # Background tasks
  skills/           # YAML-based SQL skills
```

## LangGraph Nodes

1. `understand_node` - Intent classification
2. `retrieve_context_node` - Schema + semantic search
3. `decompose_node` - Multi-step query decomposition
4. `hypothesis_node` - Query hypothesis
5. `generate_sql_node` - SQL generation (Skills + Metrics)
6. `validate_node` - Validation + RLS + DLP + cost estimation
7. `await_approval_node` - HITL gate
8. `probe_sql_node` - SQL probing
9. `execute_node` - Query execution
10. `validate_results_node` - Result validation
11. `format_results_node` - Results + smart follow-ups
12. `pivot_strategy_node` - Query reformulation
13. `repair_sql_node` - SQL repair with ReflectiveMemory
14. `fallback_sql_node` - Fallback generation
15. `error_node` - Error handling

## Core Services

| Service | Purpose | Location |
|---------|---------|----------|
| QueryCorrectionsService | Semantic retrieval with HNSW | `services/query_corrections_service.py` |
| ReflectiveMemoryService | Learn from repairs | `services/reflective_memory_service.py` |
| MetricsLayerService | Canonical metrics | `services/metrics_layer_service.py` |
| NativeAuditService | Dual-write audit | `services/native_audit_service.py` |
| ReportGenerationService | PDF/DOCX/HTML | `services/report_generation_service.py` |
| AdaptiveHITLService | Learn approval patterns | `services/adaptive_hitl_service.py` |
| DataMaskingService | PII detection | `services/data_masking_service.py` |
| SessionBindingService | Token forwarding prevention | `services/session_binding_service.py` |
| SQLInjectionDetector | Multi-layer detection | `services/sql_injection_detector.py` |
| QueryCostTracker | Budget enforcement | `services/query_cost_tracker.py` |
| RoleBasedLimitsService | Tiered limits | `services/role_based_limits_service.py` |
| SentimentTracker | Frustration detection | `services/sentiment_tracker.py` |
| QueryTaxonomyClassifier | Query type classification | `services/query_taxonomy_classifier.py` |
| QuerySandbox | Isolated execution | `services/query_sandbox.py` |
| RowLevelSecurityService | User-based filtering | `services/row_level_security_service.py` |
| ADGroupMappingService | AD group visibility | `services/ad_group_mapping_service.py` |
| CostForecastingService | Budget forecasting | `services/cost_forecasting_service.py` |
| QueryCostEstimator | Multi-db cost estimation | `services/query_cost_estimator.py` |
| LDAPIntegrationService | AD auth/group resolution | `services/ldap_integration_service.py` |
| SkillGeneratorService | YAML skill generation | `services/skill_generator_service.py` |
| SmartFollowUpService | Contextual suggestions | `services/smart_followup_service.py` |

## Memory Systems

| Type | Implementation |
|------|---------------|
| Episodic | Graphiti + FalkorDB |
| Semantic | Redis RediSearch HNSW |
| Procedural | YAML skills + auto-generation |
| Persistent | Redis conversation storage |
| Reflective | ReflectiveMemoryService |
| Emotional | SentimentTracker |

## Environment

```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost/amila
REDIS_URL=redis://localhost:6379
FALKORDB_HOST=localhost

# LDAP (optional)
LDAP_ENABLED=false
LDAP_SERVER_URL=ldap://dc.example.com
LDAP_BIND_DN=cn=admin,dc=example,dc=com
LDAP_BASE_DN=dc=example,dc=com

# Langfuse
LANGFUSE_ENABLED=true
LANGFUSE_PUBLIC_KEY=pk-...
LANGFUSE_SECRET_KEY=sk-...
```

## Running

```bash
# Setup
uv sync --all-extras
.\.venv\Scripts\activate

# Start
python main.py

# Celery worker
celery -A app.core.celery_app worker --loglevel=info --pool=solo
```

## Tracing

OpenTelemetry + Langfuse. Traces available at `LANGFUSE_HOST`.

## Testing

```bash
pytest tests/
```

## Recent Updates (2026-02-03)

- Dialect-first SQL generation with strict intent parsing and structured hypothesis constraints.
- Join-path ranking fed into SQL generation; Oracle identifier normalization hardened.
- Scope and Cartesian guards wired into validation + HITL gates.
- Pytest asyncio auto-mode enabled in `backend/pyproject.toml`.

## License

Proprietary
