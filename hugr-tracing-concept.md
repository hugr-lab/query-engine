# Hugr Query Engine: Request-level Tracing & Structured Logging

## Проблема

Сейчас hugr не даёт понять, что произошло при обработке конкретного запроса. Логирование — это смесь `log.Printf` (когда `Debug=true`) и разрозненных `slog.Warn/Error` без привязки к запросу. При ошибке HTTP data source (non-200 ответ) в логах нет ни тела ответа, ни информации о том, какой GraphQL-запрос это вызвал. В кластере логи разных нод невозможно скоррелировать.

## Решение

Легковесный встроенный трейсинг без внешних зависимостей (не OpenTelemetry), основанный на `context.Context` (который уже пробрасывается через всю цепочку вызовов) и стандартной библиотеке `log/slog`.

### Ядро: два уровня в контексте

В `context.Context` хранятся два независимых значения:

1. **Logger с trace_id** — создаётся **всегда** в middleware. Это обёртка над `slog.Default()` с привязанным `trace_id`. Дешёвая операция, обеспечивает корреляцию логов для каждого запроса даже в production-режиме.

2. **TraceInfo (со спанами)** — создаётся, когда трейсинг активен: либо глобальный уровень <= DEBUG (`POST /admin/log-level`), либо директива `@trace` в запросе. Хранит дерево спанов.

Каждый span привязан к контексту горутины (не к общему указателю), что корректно работает при `AllowParallel=true` — параллельные поля строят свои поддеревья независимо, а мьютекс защищает только `append` к массиву `Children` родительского спана.

Контекст доступен на всех уровнях стека, включая UDF-коллбэки DuckDB (duckdb-go сохраняет ctx из `QueryContext` и передаёт его в `RowContextExecutor`).

Когда трейсинг выключен (production, без global debug), `TraceInfo` не создаётся. Overhead на каждую точку инструментации — одна проверка `ctx.Value() == nil` (~5нс).

### Структуры данных

```go
package trace

// TraceInfo — корневая структура, одна на запрос. Кладётся в context.Context.
type TraceInfo struct {
    TraceID   string       // уникальный UUID запроса
    Level     slog.Level   // уровень логирования для этого запроса
    Root      *Span        // корневой спан (виртуальный, содержит дочерние)
    mu        sync.Mutex   // защита append к Children при параллельных полях
    StartTime time.Time
}

// Span — один отрезок работы. Организуются в дерево через Children.
type Span struct {
    Name     string         `json:"name"`
    Duration string         `json:"duration,omitempty"`
    Children []*Span        `json:"children,omitempty"`
    attrs    map[string]any // только для серверного лога (не сериализуется клиенту)
    start    time.Time      // не сериализуется
    parent   *Span          // обратная ссылка на родителя
}
```

Логгер с `trace_id` хранится отдельно в контексте (через `ContextWithLogger`), а не внутри `TraceInfo`, — это позволяет иметь логгер с `trace_id` для каждого запроса даже когда `TraceInfo` не создаётся (production-режим).

Текущий span привязан к `context.Context` горутины, а не к общему указателю в `TraceInfo`. Это обеспечивает корректную работу при параллельном выполнении полей (`AllowParallel=true`): каждая горутина строит своё поддерево через свой ctx, а мьютекс защищает только момент `append` дочернего спана к родителю.

### API пакета `pkg/trace`

```go
// ContextWithLogger кладёт логгер с trace_id в контекст (вызывается в middleware, всегда).
func ContextWithLogger(ctx context.Context, logger *slog.Logger, traceID string) context.Context

// ContextWithTrace кладёт TraceInfo в контекст (когда трейсинг активен).
func ContextWithTrace(ctx context.Context, info *TraceInfo) context.Context

// FromContext возвращает TraceInfo или nil (трейсинг выключен).
func FromContext(ctx context.Context) *TraceInfo

// TraceIDFromContext возвращает trace ID из контекста, или "" (используется для проброса в кластере).
func TraceIDFromContext(ctx context.Context) string

// LoggerFromContext возвращает логгер с trace_id (всегда доступен) или slog.Default().
func LoggerFromContext(ctx context.Context) *slog.Logger

// StartSpan создаёт дочерний span у текущего (из ctx) и возвращает новый ctx.
// Если трейсинг выключен — возвращает ctx без изменений.
func StartSpan(ctx context.Context, name string, attrs ...any) context.Context

// EndSpan завершает текущий span (из ctx), фиксирует duration. No-op если трейсинг выключен.
func EndSpan(ctx context.Context)
```

Использование в коде hugr — проверка + две строки:

```go
if ti := trace.FromContext(ctx); ti != nil {
    ctx = trace.StartSpan(ctx, "planner.plan", "field", field.Name)
    defer trace.EndSpan(ctx)
}
```

Проверка `FromContext` перед `StartSpan` избегает аллокации variadic-аргументов при выключенном трейсинге (zero-cost в production). Логгер через `LoggerFromContext` доступен всегда — с `trace_id`, независимо от того, активен ли трейсинг.

### Три режима управления

| Режим | Активация | Логгер с trace_id | Спаны | Спаны в extensions |
|---|---|---|---|---|
| Production (тихий) | По умолчанию | Да | Нет | Нет |
| Global debug | `POST /admin/log-level` (без перезапуска) | Да | **Да** (в серверный лог) | Нет |
| Per-request trace | Директива `@trace` в GraphQL-запросе | Да | **Да** (в серверный лог) | **Да** (клиенту) |

`POST /admin/log-level` меняет глобальный уровень через `slog.LevelVar` (атомарная переменная) — мгновенно, без рестарта. При уровне DEBUG middleware создаёт `TraceInfo` со спанами для каждого запроса — дерево спанов пишется в серверный лог, но **не** отправляется клиенту. Директива `@trace(level: DEBUG)` дополнительно включает вывод спанов в `extensions` ответа.

### Директива @trace

```graphql
directive @trace(level: LogLevel = DEBUG) on QUERY | MUTATION
enum LogLevel @system { ERROR WARN INFO DEBUG }
```

Пример использования:
```graphql
query GetData @trace(level: DEBUG) {
  catalog1 { users { id name } }
}
```

После парсинга запроса (ParseQuery) движок проверяет наличие `@trace` и, если она есть, создаёт `TraceInfo` (production без global debug) или переключает уровень (если `TraceInfo` уже есть от global debug).

### Точки интеграции: flow от запроса до HTTP/Airport

```
HTTP Request
│
▼
┌──────────────────────────────────────────────────────────────────┐
│ (1) traceMiddleware                                              │
│     • Генерирует TraceID (UUID) или читает X-Trace-Id из header  │
│     • Создаёт логгер с trace_id (всегда)                         │
│     • Если global level <= DEBUG — создаёт TraceInfo со спанами  │
│     • Устанавливает X-Trace-Id в response header                 │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│ (2) ProcessQuery                                                 │
│     • span "query.parse" (если TraceInfo есть)                   │
│     • Парсит @trace директиву → создаёт TraceInfo (если ещё нет) │
│       или переключает уровень (если уже есть от global debug)    │
│     • logger.Debug("trace.spans") — вывод дерева в серверный лог │
│     • ext["trace"] = ti.Result() — только при @trace             │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│ (3) processDataQuery (внутри dataFunc)                           │
│     • span "planner.plan" — построение плана запроса              │
│     • logger.Debug("planner.sql", "sql", plan.Log())             │
│     • logger.Debug("query.user", "user", ..., "role", ...)       │
│     • span "db.execute" — выполнение SQL в DuckDB                │
│     • logger.Error при ошибках планирования и выполнения         │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│ (4) db.Pool / SQL execution                                      │
│     • ctx сохраняется в duckdb-go contextStore по connId          │
│     • При вызове UDF — ctx извлекается и передаётся в callback    │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│ (5) Data Sources                                                 │
│                                                                  │
│ HTTP (pkg/data-sources/service.go → HttpRequest):                │
│   • span "http.request"                                          │
│   • logger.Warn("http.request.error") — при ошибке запроса       │
│   • При non-200: читает body preview (до 512 байт)              │
│   • logger.Warn("http.response.error",                           │
│       "status", code, "body_preview", preview)                   │
│   • logger.Warn("http.decode.error") — при ошибке JSON decode    │
│                                                                  │
│ OAuth (sources/http/client.go):                                  │
│   • logger.Warn("oauth.token.request.error") — ошибка запроса    │
│   • logger.Warn("oauth.token.unauthorized") — 401 ответ          │
│   • logger.Warn("oauth.token.error") — non-200 с body preview    │
│                                                                  │
│ Airport (DuckDB ATTACH → gRPC):                                  │
│   • Ошибки приходят как ошибки DuckDB, логируются с trace_id     │
│   • gRPC metadata не контролируется из Go                        │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│ (6) Response                                                     │
│   • Header X-Trace-Id в HTTP-ответе (всегда)                    │
│   • Спаны в серверный лог (global debug и @trace)               │
│   • Спаны в extensions клиенту — только при @trace               │
│   • extensions: только имена и тайминги (безопасность)           │
└──────────────────────────────────────────────────────────────────┘
```

В каждой точке используется `trace.LoggerFromContext(ctx)` — логгер с привязанным `trace_id`. Если трейсинг выключен, возвращается `slog.Default()`.

### Реализованный код

**1. Middleware (middlewares.go) — логгер с trace_id + TraceInfo при debug:**

```go
func traceMiddleware(level *slog.LevelVar) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            traceID := r.Header.Get("X-Trace-Id")
            if traceID == "" {
                traceID = uuid.NewString()
            }
            w.Header().Set("X-Trace-Id", traceID)

            logger := slog.Default().With("trace_id", traceID)
            ctx := trace.ContextWithLogger(r.Context(), logger, traceID)

            if level.Level() <= slog.LevelDebug {
                info := trace.NewTraceInfo(traceID, level.Level())
                ctx = trace.ContextWithTrace(ctx, info)
            }

            next.ServeHTTP(w, r.WithContext(ctx))
        })
    }
}
```

**2. ProcessQuery (engine.go) — обработка @trace и сбор результата:**

```go
func (s *Service) ProcessQuery(ctx context.Context, req types.Request) types.Response {
    start := time.Now()
    logger := trace.LoggerFromContext(ctx)

    if ti := trace.FromContext(ctx); ti != nil {
        ctx = trace.StartSpan(ctx, "query.parse")
    }
    op, err := s.schema.ParseQuery(ctx, req.Query, req.Variables, req.OperationName)
    trace.EndSpan(ctx)
    if err != nil {
        return types.ErrResponse(err)
    }
    parseDuration := time.Since(start)

    hasTraceDirective := op.Definition.Directives.ForName(base.TraceDirectiveName) != nil
    if hasTraceDirective {
        d := op.Definition.Directives.ForName(base.TraceDirectiveName)
        ti := trace.FromContext(ctx)
        if ti == nil {
            ti = trace.NewTraceInfo(trace.TraceIDFromContext(ctx), parseLogLevel(d))
            ctx = trace.ContextWithTrace(ctx, ti)
        } else {
            ti.SetLevel(parseLogLevel(d))
        }
    }

    // ... ValidateOnly, ProcessOperation, @stats ...

    if ti := trace.FromContext(ctx); ti != nil {
        logger.Debug("trace.spans", "spans", ti.Result())
        if hasTraceDirective {
            ext["trace"] = ti.Result()
        }
    }

    if len(ext) > 0 {
        res.Extensions = ext
    }
    return res
}
```

**3. processDataQuery (query.go) — спаны и structured logging:**

```go
func (s *Service) processDataQuery(ctx context.Context, provider catalog.Provider,
    query base.QueryRequest, vars map[string]any) (data any, ext map[string]any, err error) {
    // ...
    logger := trace.LoggerFromContext(ctx)

    dataFunc := func() (any, error) {
        if ti := trace.FromContext(ctx); ti != nil {
            ctx = trace.StartSpan(ctx, "planner.plan", "field", query.Field.Name)
        }
        plan, err := s.planner.Plan(ctx, provider, query.Field, vars)
        trace.EndSpan(ctx)
        if err != nil {
            logger.Error("planner.plan.error", "field", query.Field.Name, "error", err)
            return nil, err
        }

        // compile (ошибки логируются, отдельного спана нет — compile синхронный и быстрый)
        err = plan.Compile()
        if err != nil {
            logger.Error("planner.compile.error", "field", query.Field.Name, "error", err)
            return nil, err
        }

        logger.Debug("planner.sql", "field", query.Field.Name,
            "alias", query.Field.Alias, "sql", plan.Log())

        if ai := auth.AuthInfoFromContext(ctx); ai != nil {
            logger.Debug("query.user", "user", ai.UserName, "role", ai.Role,
                "field", query.Field.Name)
        }

        if ti := trace.FromContext(ctx); ti != nil {
            ctx = trace.StartSpan(ctx, "db.execute", "field", query.Field.Name)
        }
        result, err := plan.Execute(ctx, s.db)
        trace.EndSpan(ctx)
        if err != nil {
            logger.Error("db.execute.error", "field", query.Field.Name, "error", err)
        }
        return result, err
    }

    // ... cache logic, @stats ...
}
```

**4. HttpRequest (pkg/data-sources/service.go) — логирование HTTP-ошибок:**

```go
func (s *Service) HttpRequest(ctx context.Context, source, path, method,
    headers, params, body, jqq string) (any, error) {

    logger := trace.LoggerFromContext(ctx)

    if ti := trace.FromContext(ctx); ti != nil {
        ctx = trace.StartSpan(ctx, "http.request", "source", source, "path", path, "method", method)
        defer trace.EndSpan(ctx)
    }

    // ... resolve data source ...

    res, err := httpDs.Request(ctx, path, method, headers, params, body)
    if err != nil {
        logger.Warn("http.request.error", "source", source, "path", path, "error", err)
        return nil, err
    }
    defer res.Body.Close()

    if res.StatusCode != 200 {
        preview, _ := io.ReadAll(io.LimitReader(res.Body, 512))
        logger.Warn("http.response.error",
            "source", source, "path", path, "method", method,
            "status", res.StatusCode, "body_preview", string(preview))
        return nil, fmt.Errorf("request failed with status code %d: %s", res.StatusCode, res.Status)
    }

    var data any
    if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
        logger.Warn("http.decode.error", "source", source, "path", path, "error", err)
        return nil, err
    }
    return data, nil
}
```

**5. Серверный лог — как выглядит вывод:**

```
level=WARN msg="http.response.error" trace_id=550e8400-... source=ext_api path=/users method=GET status=502 body_preview="Bad Gateway"
level=DEBUG msg="planner.sql" trace_id=550e8400-... field=users sql="SELECT u.id, u.name FROM catalog1.users u"
level=ERROR msg="db.execute.error" trace_id=550e8400-... field=orders error="connection refused"
```

Все записи привязаны к `trace_id` — фильтрация по одному запросу: `grep 550e8400`.

### Разделение лог vs extensions

Во избежание утечки данных клиенту (SQL-запросы, тела ответов, внутренняя структура):

- **Серверный лог (stderr):** полная информация — SQL, body preview, заголовки, параметры
- **GraphQL extensions (клиенту):** только имена спанов и тайминги, без чувствительных данных

```json
{
  "extensions": {
    "trace": {
      "trace_id": "550e8400-...",
      "total_time": "42ms",
      "spans": [
        { "name": "query.parse", "duration": "1ms" },
        { "name": "planner.plan", "duration": "3ms" },
        { "name": "db.execute", "duration": "37ms" },
        { "name": "http.request", "duration": "28ms" }
      ]
    }
  }
}
```

### Работа в кластере

Пользовательские запросы выполняются целиком на одной ноде — трейсинг работает через `context.Context` внутри процесса. Для кластерных операций (Broadcast, ForwardToManagement) — `X-Trace-Id` добавляется в исходящие HTTP-заголовки через `client.WithTraceID(traceID)`, middleware на принимающей стороне подхватывает его вместо генерации нового. Одна операция — один `trace_id` на всех нодах.

### Airport (hugr-app)

DuckDB выполняет gRPC-вызовы к Airport-каталогам через своё C++ расширение — Go-код не контролирует эти вызовы. Передать `trace_id` внутри gRPC metadata невозможно без изменений в расширении DuckDB. Со стороны engine логируются SQL-запрос к Airport-каталогу и ошибка DuckDB, если она произошла.

## Реализованные изменения

| Файл / пакет | Что реализовано |
|---|---|
| `pkg/trace/` (новый) | `TraceInfo`, `Span`, context-функции (`ContextWithLogger`, `ContextWithTrace`, `StartSpan`, `EndSpan`, `FromContext`, `LoggerFromContext`, `TraceIDFromContext`), тесты (21 тест) |
| `query_directives.graphql` | `@trace` директива + enum `LogLevel` |
| `constants.go`, `directives.go` | Регистрация `trace` как query-side директивы |
| `middlewares.go` | `traceMiddleware`: TraceID, `X-Trace-Id`, базовый `TraceInfo` |
| `engine.go` | `Service.logLevel *slog.LevelVar`, `GET/POST /admin/log-level`; в `ProcessQuery` — обработка `@trace`, merge trace в extensions, `parseLogLevel` |
| `query.go` | Замена `log.Printf` (debug) на `trace.StartSpan` + `trace.LoggerFromContext`: спаны `planner.plan`, `db.execute`; структурированное логирование SQL, ошибок, user info |
| `stream.go` | Замена `log.Printf` на `trace.LoggerFromContext` для stream-запросов |
| `subscription.go` | Замена `log.Printf` на `trace.LoggerFromContext` для subscription-запросов |
| `pkg/data-sources/service.go` | `HttpRequest`: span `http.request`, structured logging non-200 с body preview (до 512 байт), ошибок запроса и JSON decode |
| `pkg/data-sources/sources/http/client.go` | OAuth: structured logging `oauth.token.request.error`, `oauth.token.unauthorized`, `oauth.token.error` с body preview |
| `client/client.go` | `WithTraceID` option + `traceIDTransport` для проброса `X-Trace-Id` в HTTP-заголовках |
| `pkg/cluster/coordinator.go` | `Broadcast` пробрасывает `trace_id` через `client.WithTraceID` |
| `pkg/cluster/worker.go` | `ForwardToManagement` пробрасывает `trace_id` через `client.WithTraceID` |

### Не реализовано в этой итерации

- Замена `log.Printf` в `graphql-ws.go` и `ipc-stream.go` — это IPC/WebSocket lifecycle логи (подключение, ping/pong, закрытие), не привязанные к конкретному GraphQL-запросу; требуют отдельного подхода к передаче trace context через WS-сессию
- Отдельный span для `planner.compile` — compile синхронный и быстрый, логирование SQL покрывает его через `logger.Debug("planner.sql")`

## Что не входит в v1

- Ограничение `@trace` по ролям/правам
- Сэмплирование при глобальном debug на высокой нагрузке
- Интеграция с OpenTelemetry (можно добавить позже поверх текущей архитектуры)
- Передача trace context в Airport/gRPC (ограничение DuckDB)
- Trace-aware логирование для IPC/WebSocket lifecycle

## Отдельный баг (обнаружен при аудите)

В `pkg/planner/node_select_vector.go` (строка 153) вызов `queries.CreateEmbedding` использует `context.Background()` вместо request context — отмена запроса не доходит до embedding sub-query. Исправление: передать `ctx` из замыкания.
