# Go ETL Orchestrator

[![Go Version](https://img.shields.io/badge/go-1.21-blue.svg)](https://golang.org/)
[![Docker](https://img.shields.io/badge/docker-25.0-blue.svg)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/postgres-15-blue.svg)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/redis-7-red.svg)](https://redis.io/)
[![CI](https://github.com/Vat00/go-etl-orchestrator/actions/workflows/ci.yml/badge.svg)](https://github.com/Vat00/go-etl-orchestrator/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**Распределённый оркестратор задач для ETL-пайплайнов** с микросервисной архитектурой, асинхронной обработкой через Redis и автоматическими ретраями.

---

##  Архитектура

<img width="5897" height="839" alt="deepseek_mermaid_20260605_51bb45" src="https://github.com/user-attachments/assets/8499a1de-7064-44dd-af8d-4156786fea27" />
> Текстовая схема архитектуры приведена ниже.

- **Оркестратор (API)** — принимает задачи, сохраняет в PostgreSQL, отправляет ID в Redis.
- **Redis** — очередь задач (`task:queue`).
- **Воркер** — забирает задачи через `BLPop`, выполняет (shell/HTTP), обновляет статус, делает ретраи.
- **PostgreSQL** — хранит задачи, статусы, количество ретраев.

##  Быстрый старт

### Требования
- Docker & Docker Compose
- Go 1.21+ (только для разработки)

### Запуск

```bash
git clone https://github.com/Vat00/go-etl-orchestrator.git
cd go-etl-orchestrator
docker-compose up -d --build
После запуска API доступен на http://localhost:8080
 API
curl -X POST http://localhost:8080/task \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my shell task",
    "type": "shell",
    "config": {
      "command": "echo Hello from orchestrator"
    }
  }'
  Ответ:
  {
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "my shell task",
  "type": "shell",
  "config": {"command": "echo Hello from orchestrator"},
  "status": "pending",
  "retries": 0
}
Создать задачу (HTTP)
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
  Получить статус задачи
  curl http://localhost:8080/task/{task-id}
  Ответ:
  {
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "my shell task",
  "status": "success",
  "type": "shell",
  "retries": 0,
  "created_at": "2026-06-01T08:10:31Z",
  "updated_at": "2026-06-01T08:10:32Z"
}
 Механизм ретраев
Task failed → Retry 1/3 → failed → Retry 2/3 → failed → Retry 3/3 → marking as failed
Статус retries показывает количество уже выполненных попыток.
 Тестирование
Юнит-тесты (воркер)
Проверяют изолированно функции executeShell, executeHTTP.

bash
go test ./cmd/worker -v
Интеграционные тесты
Проверяют сквозной сценарий: создание задачи → выполнение воркером → обновление статуса. Требуют запущенной системы.

bash
docker-compose up -d
go test -tags=integration ./tests/integration/...
CI (GitHub Actions)
При каждом пуше автоматически запускаются юнит-тесты. Статус сборки — https://github.com/Vat00/go-etl-orchestrator/actions/workflows/ci.yml/badge.svg
 Структура Docker-контейнеров
Контейнер	Порт	Назначение
orchestrator-api	8080	HTTP API сервер
orchestrator-worker	—	Фоновый обработчик задач
orchestrator-postgres	5433	PostgreSQL
orchestrator-redis	6379	Redis очередь
Масштабирование воркеров:

bash
docker-compose up -d --scale worker=3
 Структура проекта
text
go-etl-orchestrator/
├── .github/workflows/       # CI (GitHub Actions)
├── cmd/
│   ├── orchestrator/main.go   # API сервер
│   └── worker/
│       ├── main.go            # воркер
│       └── worker_test.go     # юнит-тесты
├── tests/integration/
│   └── orchestrator_test.go   # интеграционные тесты
├── Dockerfile.orchestrator
├── Dockerfile.worker
├── docker-compose.yml
├── go.mod
├── go.sum
└── README.md
🛠 Поддерживаемые типы задач
Тип	Описание	Пример конфигурации
shell	Выполнение команд ОС	{"command": "echo hello"}
http	HTTP-запросы (GET/POST)	{"url": "https://api.example.com", "method": "GET"}
 Мониторинг и логи
Логи воркера:

bash
docker logs go-etl-orchestrator-worker-1 -f
Логи оркестратора:

bash
docker logs orchestrator-api -f
 Остановка и очистка
bash
docker-compose down         # остановить контейнеры
docker-compose down -v      # + удалить volumes (очистить БД)
🔧 Разработка (без Docker)
bash
go mod download
docker-compose up -d postgres redis   # только БД
go run cmd/orchestrator/main.go
go run cmd/worker/main.go
 Возможные улучшения (Roadmap)
Unit-тесты

Интеграционные тесты

CI (GitHub Actions)

Метрики Prometheus + Grafana

Web UI для просмотра задач

DAG (зависимости между задачами)

Поддержка SQL-задач

TLS/gRPC

 Лицензия
MIT © Vat00

Built with Go, Docker, PostgreSQL, Redis
