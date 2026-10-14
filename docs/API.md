# API Reference

Full endpoint reference for the News Intelligence Pipeline. Interactive docs are also served at `http://localhost:8000/api/docs` when the stack is running.

The system provides RESTful HTTP endpoints for ingestion, retrieval, monitoring, and analysis.

### Core Ingestion & Retrieval

#### **POST /api/ingestion/ingest**

Accept batch of raw news events for async processing.

**Purpose**: Primary ingestion endpoint - publishes events to Kafka for deduplication, filtering, and analysis.

**Example**:

```bash
curl -X POST http://localhost:8000/api/ingestion/ingest \
  -H "Content-Type: application/json" \
  -d '[{
    "id": "event-123",
    "source": "reddit",
    "title": "Docker 25.0 Released",
    "body": "New features include...",
    "published_at": "2025-11-05T10:00:00Z"
  }]'
```

#### **GET /api/ingestion/retrieve**

Retrieve filtered articles sorted by relevance × recency.

**Purpose**: Returns only articles that passed content filters in deterministic order.

**Query Parameters**:

- `only_passed` (bool, default: true) - Return only filtered articles
- `sort_by` (string) - Sorting method: "relevance_recency" (default), "recency", "relevance"

**Example**:

```bash
curl "http://localhost:8000/api/ingestion/retrieve?sort_by=relevance_recency"
```

### Filter Inspection

#### **GET /api/ingestion/filter-results/{article_id}**

Get detailed filter results for a specific article.

**Purpose**: Inspect why an article passed or failed filtering, including AI scores and reasoning.

**Returns**: Relevance score, quality score, filter breakdown, AI evaluation details.

**Example**:

```bash
curl http://localhost:8000/api/ingestion/filter-results/event-123

# Or use the inspection script
python scripts/inspect_filters.py 123
```

**Response**:

```json
{
  "id": 123,
  "title": "Kubernetes Security Flaw Discovered",
  "source": "Reddit-cybersecurity",
  "passed_filter": true,
  "filter_details": {
    "passed": true,
    "relevance_score": 1.0,
    "quality_score": 0.9,
    "filters": {
      "relevance": {
        "passed": true,
        "reason": "Found 3 tech keyword(s)"
      },
      "quality": {
        "passed": true,
        "reason": "AI quality check passed (score: 0.90)"
      }
    },
    "ai": {
      "relevance_score": 1.0,
      "quality_score": 0.9,
      "reasoning": "Technical cybersecurity content about APT threats..."
    }
  }
}
```

#### **GET /api/ingestion/filter-analysis**

Get aggregate filter statistics and results.

**Purpose**: Dashboard view of filter performance across all articles.

**Query Parameters**:

- `limit` (int, default: 20) - Max articles to return
- `status` (string) - Filter by "passed" or "rejected"
- `source` (string) - Filter by source name

**Example**:

```bash
curl "http://localhost:8000/api/ingestion/filter-analysis?limit=100&status=passed"

# Or use the inspection script
python scripts/inspect_filters.py
```

### Article Management

#### **GET /api/ingestion/articles**

List stored articles with pagination and filtering.

**Purpose**: Browse all articles with metadata (analyzed status, filter results, scores).

**Query Parameters**:

- `skip` (int, default: 0) - Pagination offset
- `limit` (int, default: 20) - Max results
- `source` (string) - Filter by source
- `only_analyzed` (bool, default: false) - Only analyzed articles

**Example**:

```bash
curl "http://localhost:8000/api/ingestion/articles?source=HackerNews&limit=10"
```

#### **GET /api/ingestion/articles/{article_id}**

Get full details for a specific article.

**Purpose**: Retrieve complete article with content, analysis results, and summary.

**Example**:

```bash
curl http://localhost:8000/api/ingestion/articles/123
```

### Source Management

#### **GET /api/ingestion/sources**

List all configured news sources.

**Purpose**: View source configuration (enabled status, polling intervals, URLs).

**Example**:

```bash
curl http://localhost:8000/api/ingestion/sources
```

**Response**:

```json
{
  "sources": [
    {
      "name": "HackerNews",
      "url": "https://news.ycombinator.com",
      "enabled": true,
      "polling_interval_minutes": 5,
      "max_articles": 30
    }
  ],
  "total": 9,
  "enabled": 8,
  "disabled": 1
}
```

#### **POST /api/ingestion/fetch-all**

Manually trigger fetching from all sources.

**Purpose**: Force immediate article fetching (useful for testing or manual refresh).

**Example**:

```bash
curl -X POST http://localhost:8000/api/ingestion/fetch-all
```

### Statistics & Analytics

#### **GET /api/ingestion/stats**

Get overall ingestion statistics.

**Purpose**: System health metrics - total articles, duplicates, analyzed count, pass rates.

**Example**:

```bash
curl http://localhost:8000/api/ingestion/stats
```

#### **GET /api/insights/action-items**

Generate AI-powered action items from recent articles.

**Purpose**: Analyze recent tech news and generate prioritized recommendations.

**Query Parameters**:

- `days` (int, default: 7) - Look back period
- `max_items` (int, default: 5) - Max action items to generate

**Example**:

```bash
curl "http://localhost:8000/api/insights/action-items?days=7&max_items=5"
```

### Dashboard

#### **GET /dashboard**

Web UI for visualizing articles and filter results.

**Purpose**: Human-friendly interface for browsing filtered articles.

**Access**: http://localhost:8000/dashboard

Right now this will just show up to a max of 50 articles and update as more come in and pass the filter.

### API Documentation

Interactive API documentation with request/response schemas:

- **Swagger UI**: http://localhost:8000/api/docs


## Mock API Testing (Automated Evaluation Interface)

The system exposes a language-agnostic REST API for automated testing and evaluation, compliant with the required contract for test harness integration.

**Important** - for the sake of speed/immediate processing of `test harness items` - the endpoints for `mock API testing` - skip around the usual `async processing route` to avoid being in a queue with all incoming news articles via the `scheduler/news fetching`. This should mean they get prority... in theory. 

### Ingestion Endpoint

**`POST /api/ingestion/ingest`**

Accepts a JSON array of raw news events and returns immediate acknowledgment. Articles are published to Kafka for asynchronous processing (deduplication, filtering, analysis).

**Request Schema:**
```json
[{
  "id": "event-1",
  "source": "reddit",
  "title": "Python 3.13 Release",
  "body": "New features in Python 3.13 include improved performance...",
  "published_at": "2025-11-02T12:00:00Z"
}]
```

**Required Fields:**
- `id` (string) - Unique identifier for the event
- `source` (string) - Source name (e.g., "reddit", "ars-technica")
- `title` (string) - Article title
- `body` (string, optional) - Article content/body
- `published_at` (string) - ISO-8601/RFC 3339 timestamp in UTC

**Response:**
```json
{
  "status": "success",
  "message": "Ingested 1 article(s)",
  "count": 1
}
```

**Example:**
```bash
curl -X POST http://localhost:8000/api/ingestion/ingest \
  -H "Content-Type: application/json" \
  -d '[{
    "id": "test-1",
    "source": "test-harness",
    "title": "Kubernetes Security Vulnerability Discovered",
    "body": "A critical security flaw was found in Kubernetes...",
    "published_at": "2025-11-05T10:00:00Z"
  }]'
```

### Retrieval Endpoint

**`GET /api/ingestion/retrieve`**

Returns only the articles that passed the filtering process, in the same JSON schema as ingestion. Results are sorted by default ranking (combined score: relevance × quality × recency).

**Query Parameters:**
- `limit` (optional, default: 100) - Maximum number of articles to return
- `skip` (optional, default: 0) - Number of articles to skip (pagination)

**Response Schema:**
```json
[{
  "id": "test-1",
  "source": "test-harness",
  "title": "Kubernetes Security Vulnerability Discovered",
  "body": "Security researchers have uncovered a critical authentication bypass vulnerability (CVE-2025-12345) in Kubernetes versions 1.28 through 1.30 that could allow attackers to gain cluster-admin privileges. The flaw, discovered by the CNCF security team during routine audits, affects the API server's role-based access control (RBAC) implementation and has already been exploited in the wild against misconfigured clusters. Kubernetes maintainers released emergency patches for all affected versions, and cluster administrators are urged to update immediately and rotate all service account tokens. The vulnerability was introduced in a refactoring commit six months ago and highlights the ongoing challenges of securing complex distributed systems at scale.",
  "published_at": "2025-11-05T10:00:00Z"
}]
```

**Example:**
```bash
curl http://localhost:8000/api/ingestion/retrieve
```

### Testing Workflow

1. **Ingest a batch of test articles** (mix of relevant IT content and non-relevant articles):
   ```bash
   curl -X POST http://localhost:8000/api/ingestion/ingest \
     -H "Content-Type: application/json" \
     -d '[
       {
         "id": "tech-1",
         "source": "test",
         "title": "Docker 25.0 Released with Performance Improvements",
         "body": "Docker Inc. announced version 25.0 with significant container startup optimizations and improved resource utilization. The new release includes enhanced BuildKit caching, reduced image layer overhead, and better integration with Kubernetes orchestration platforms. This update is expected to reduce container cold-start times by up to 40% in production environments.",
         "published_at": "2025-11-05T09:00:00Z"
       },
       {
         "id": "nontec-1",
         "source": "test",
         "title": "Best Chocolate Chip Cookie Recipe",
         "body": "Mix butter, sugar, flour, and chocolate chips to create the perfect cookie dough. Bake at 350 degrees for 12 minutes until golden brown. This classic recipe has been passed down through generations and creates soft, chewy cookies that everyone loves. The secret is using room temperature butter and high-quality vanilla extract for the best flavor.",
         "published_at": "2025-11-05T09:30:00Z"
       }
     ]'
   ```

2. **Wait for processing** (typically 3-5 seconds for filtering and AI analysis):
   ```bash
   sleep 5
   ```

3. **Retrieve filtered results**:
   ```bash
   curl http://localhost:8000/api/ingestion/retrieve
   ```

4. **Expected behavior**:
   - Tech-related article (`tech-1`) should appear in results
   - Non-tech article (`nontec-1`) should be filtered out (not returned)
   - Results sorted by combined relevance, quality, and recency scores

### Contract Compliance

- ✅ **Ingestion**: Accepts JSON array with required fields (id, source, title, body, published_at)
- ✅ **Retrieval**: Returns same schema, filtered articles only
- ✅ **Sorting**: Default ranking by importance × recency (relevance_score × quality_score × recency_weight)
- ✅ **Deterministic**: Same batch produces consistent filtering results
- ✅ **Protocol**: RESTful HTTP endpoints (language-agnostic)

### Additional API Endpoints

For more advanced querying, see the full API documentation at http://localhost:8000/api/docs:

- `GET /api/news/articles` - Retrieve with custom sorting and filtering options
- `POST /api/insights/action-items` - Generate AI-powered action items from recent articles
- `GET /api/ingestion/sources` - List configured news sources and their status

### Monitoring

- **API**: http://localhost:8000/api/docs
- **Kafdrop**: http://localhost:9000
- **Database**: localhost:5432 (postgres)

## Environment Variables
Set in `.env` file at `root`

```env
OPENAI_API_KEY=sk-...
DATABASE_URL=postgresql://postgres:postgres@postgres:5432/news_db
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
AI_FILTER_ENABLED=true
```


## Configuration Files

The system uses YAML files for easy configuration management:

### AI Model Configuration (`app/core/ai_config.yaml`)

Configure LLM provider and model parameters:

```yaml
provider: "openai"  # LLM provider
model_name: "gpt-3.5-turbo"  # Model to use
temperature: 0.7  # Randomness (0.0 = deterministic, 1.0 = creative)
max_tokens: null  # Response limit (null = no limit)
```

### News Sources Configuration (`app/core/sources_config.yaml`)

Configure news sources, scheduling, and limits:

```yaml
# Global scheduler settings
scheduler:
  interval_minutes: 5  # How often to fetch from all sources
  enable_scheduler: true
  enable_kafka_consumer: true

# Per-source configuration
sources:
  HackerNews:
    enabled: true
    max_articles: 30
    timeout_seconds: 10
    url: "https://news.ycombinator.com"
  
  Reddit-cybersecurity:
    enabled: true
    max_articles: 25
    timeout_seconds: 10
    url: "https://reddit.com/r/cybersecurity"
```

**Note**: Edit these YAML files to customize behavior without modifying code.
