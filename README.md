# Go ETL Orchestrator

docker-compose up -d --build

[![Go Version](https://img.shields.io/badge/go-1.21-blue.svg)](https://golang.org/)
[![Docker](https://img.shields.io/badge/docker-25.0-blue.svg)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/postgres-15-blue.svg)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/redis-7-red.svg)](https://redis.io/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**Распределённый оркестратор задач для ETL-пайплайнов** с микросервисной архитектурой, асинхронной обработкой через Redis и автоматическими ретраями.

---

##  Архитектура
'''go-etl-orchestrator/ ── cmd/ ──┬── orchestrator/ ── main.go (API сервер)  │  Dockerfile.orchestrator  │  Dockerfile.worker  │  docker-compose.yml  │  go.mod  │  go.sum  │  README.md
                               └── worker/ ─────── main.go (Воркер)'''

## Быстрый старт

### Требования
- Docker & Docker Compose
- Go 1.21+ (только для разработки)

### Запуск

```bash
# Склонировать репозиторий
git clone https://github.com/Vat00/go-etl-orchestrator.git
cd go-etl-orchestrator

# Запустить все сервисы
docker-compose up -d --build

# Проверить статус контейнеров
docker ps
# API
# Создать задачу (Shell)
curl -X POST http://localhost:8080/task \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my shell task",
    "type": "shell",
    "config": {
      "command": "echo Hello from orchestrator"
    }
  }'
  # Ответ
  {
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "my shell task",
  "type": "shell",
  "config": {"command": "echo Hello from orchestrator"},
  "status": "pending",
  "retries": 0
}
# Cоздать задачу HTTP
curl -X POST http://localhost:8080/task \
  -H "Content-Type: application/json" \
  -d '{
    "name": "http request",
    "type": "http",
    "config": {
      "url": "https://api.github.com/repos/Vat00/go-etl-orchestrator",
      "method": "GET"
    }
  }'
  # Получить статус задачи
  curl http://localhost:8080/task/{task-id}
  # Ответ
  {
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "my shell task",
  "status": "success",
  "type": "shell",
  "retries": 0,
  "created_at": "2026-06-01T08:10:31Z",
  "updated_at": "2026-06-01T08:10:32Z"
}
# HTTP с телом запроса
{
  "type": "http",
  "config": {
    "url": "https://httpbin.org/post",
    "method": "POST",
    "headers": {"Content-Type": "application/json"},
    "body": "{\"key\": \"value\"}"
  }
}
# Механизм ретраев
Task failed → Retry 1/3 → failed → Retry 2/3 → failed → Retry 3/3 → failed → marking as failed
Структура Docker-контейнеров
Контейнер	Порт	Назначение
orchestrator-api	8080	HTTP API сервер
orchestrator-worker	—	Фоновый обработчик задач
orchestrator-postgres	5433	PostgreSQL
orchestrator-redis	6379	Redis очередь
Масштабирование воркеров
bash
docker-compose up -d --scale worker=3
 Структура проекта
text
go-etl-orchestrator/
├── cmd/
│   ├── orchestrator/main.go   # API сервер
│   └── worker/main.go          # Воркер
├── Dockerfile.orchestrator
├── Dockerfile.worker
├── docker-compose.yml
├── go.mod
├── go.sum
└── README.md
 Тестирование
PowerShell
powershell
# Создать задачу
$task = Invoke-RestMethod -Uri http://localhost:8080/task `
  -Method POST `
  -ContentType "application/json" `
  -Body '{"name":"test","type":"shell","config":{"command":"echo OK"}}'

# Проверить статус
Invoke-RestMethod -Uri "http://localhost:8080/task/$($task.id)"
PostgreSQL (прямой доступ)
bash
docker exec orchestrator-postgres psql -U user -d orchestrator -c "SELECT * FROM tasks ORDER BY created_at DESC LIMIT 5;"
Redis очередь
bash
docker exec orchestrator-redis redis-cli LLEN task:queue
 Мониторинг
Логи воркера:

bash
docker logs go-etl-orchestrator-worker-1 -f
Логи оркестратора:

bash
docker logs orchestrator-api -f
 Остановка и очистка
bash
# Остановить все контейнеры
docker-compose down

# Остановить и удалить volumes (очистить БД)
docker-compose down -v
 Разработка и локальный запуск (без Docker)
bash
# Установить зависимости
go mod download

# Запустить PostgreSQL и Redis через Docker
docker-compose up -d postgres redis

# Запустить оркестратора
go run cmd/orchestrator/main.go

# Запустить воркера (в другом терминале)
go run cmd/worker/main.go
 Возможные улучшения (Roadmap)
Метрики Prometheus + Grafana

Web UI для просмотра задач

DAG (зависимости между задачами)

Webhook-уведомления о завершении

Поддержка SQL-задач
## Production-ready улучшения

- **Graceful shutdown** – оркестратор и воркер корректно завершают работу по SIGTERM, не теряя задачи.
- **Гибкая настройка**:
  - `WORKER_CONCURRENCY` – количество параллельно выполняемых задач в одном воркере (по умолчанию 10).
  - `DEFAULT_RETRIES` – количество повторных попыток для задач, если не указано в запросе (по умолчанию 5).
- **Метрики Prometheus** – доступны на `http://localhost:2112/metrics`.
- **Health checks** – `/health` и `/ready` для Kubernetes.

### Переменные окружения (опциональны)

| Переменная | Назначение | По умолчанию |
|------------|------------|---------------|
| `WORKER_CONCURRENCY` | Параллельных задач на воркер | 10 |
| `DEFAULT_RETRIES` | Количество ретраев для задачи (если не указано в API) | 5 |

TLS/gRPC вместо HTTP

 Лицензия
MIT © Vat00

 Контакты
По вопросам сотрудничества и предложениям — GitHub Issues или Pull Requests.

Built with Go, Docker, PostgreSQL, Redis

text

---
