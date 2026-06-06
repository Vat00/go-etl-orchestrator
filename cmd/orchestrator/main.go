package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/hibiken/asynq"
	_ "github.com/lib/pq"

	"github.com/Vat00/go-etl-orchestrator/internal/queue"
	"github.com/Vat00/go-etl-orchestrator/internal/tasks"
)

type Task struct {
	ID      string          `json:"id"`
	Name    string          `json:"name"`
	Type    string          `json:"type"`
	Config  json.RawMessage `json:"config"`
	Status  string          `json:"status"`
	Retries int             `json:"retries"`
}

var db *sql.DB
var asynqClient *asynq.Client

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func initLogger() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	slog.SetDefault(logger)
}

func main() {
	initLogger()

	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbUser := getEnv("DB_USER", "user")
	dbPassword := getEnv("DB_PASSWORD", "password")
	dbName := getEnv("DB_NAME", "orchestrator")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		slog.Error("Failed to open DB", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		slog.Error("Cannot reach DB", "error", err)
		os.Exit(1)
	}
	slog.Info("Connected to PostgreSQL")

	// Инициализация Asynq клиента (без пароля, если не нужен)
	asynqClient = queue.NewRedisClient(redisAddr, "", 0)
	defer asynqClient.Close()

	createTableSQL := `
    CREATE TABLE IF NOT EXISTS tasks (
        id UUID PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        type TEXT NOT NULL,
        config JSONB NOT NULL,
        retries INT DEFAULT 0,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );`
	if _, err := db.Exec(createTableSQL); err != nil {
		slog.Error("Failed to create table", "error", err)
		os.Exit(1)
	}
	slog.Info("Table tasks ready")

	r := mux.NewRouter()
	r.HandleFunc("/task", createTask).Methods("POST")
	r.HandleFunc("/task/{id}", getTaskStatus).Methods("GET")
	r.HandleFunc("/health", healthCheck).Methods("GET")
	r.HandleFunc("/ready", readyCheck).Methods("GET")

	srv := &http.Server{
		Handler:      r,
		Addr:         ":8080",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	go func() {
		slog.Info("Orchestrator starting on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("Shutting down orchestrator...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("Forced shutdown", "error", err)
	}
	slog.Info("Orchestrator stopped")
}

func createTask(w http.ResponseWriter, r *http.Request) {
	var task Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		slog.Error("Invalid task payload", "error", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if task.ID == "" {
		task.ID = uuid.New().String()
	}
	if task.Retries == 0 {
		defaultRetries := getEnv("DEFAULT_RETRIES", "5")
		if n, err := strconv.Atoi(defaultRetries); err == nil {
			task.Retries = n
		} else {
			task.Retries = 5
		}
	}
	task.Status = "pending"

	query := `INSERT INTO tasks (id, name, status, type, config, retries) VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := db.Exec(query, task.ID, task.Name, task.Status, task.Type, task.Config, task.Retries)
	if err != nil {
		slog.Error("Failed to save task", "task_id", task.ID, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Отправляем задачу в Asynq. Payload = ID задачи.
	payload, _ := json.Marshal(map[string]string{"id": task.ID})
	asynqTask := asynq.NewTask(determineTaskType(task.Type), payload)

	// Опции: максимальное количество попыток (переопределяет глобальное), очередь по умолчанию
	opts := []asynq.Option{
		asynq.MaxRetry(task.Retries), // если в задаче указано своё значение
		asynq.Queue("default"),
		asynq.Timeout(10 * time.Minute),
	}
	info, err := asynqClient.Enqueue(asynqTask, opts...)
	if err != nil {
		slog.Error("Failed to enqueue task", "task_id", task.ID, "error", err)
		// Задача сохранена в БД, но не в очереди. Можно вернуть ошибку или попробовать ещё раз.
		http.Error(w, "Task created but not enqueued", http.StatusInternalServerError)
		return
	}

	slog.Info("Task enqueued", "task_id", task.ID, "asynq_id", info.ID, "queue", info.Queue)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(task)
}

func getTaskStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["id"]

	var task Task
	var createdAt, updatedAt time.Time
	query := `SELECT id, name, status, type, config, retries, created_at, updated_at FROM tasks WHERE id=$1`
	err := db.QueryRow(query, taskID).Scan(&task.ID, &task.Name, &task.Status, &task.Type, &task.Config, &task.Retries, &createdAt, &updatedAt)

	if err == sql.ErrNoRows {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	} else if err != nil {
		slog.Error("DB error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"id":         task.ID,
		"name":       task.Name,
		"status":     task.Status,
		"type":       task.Type,
		"retries":    task.Retries,
		"created_at": createdAt,
		"updated_at": updatedAt,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func readyCheck(w http.ResponseWriter, r *http.Request) {
	// Проверяем подключение к БД и Redis (Asynq клиент)
	if err := db.Ping(); err != nil {
		slog.Warn("Ready check: DB unreachable", "error", err)
		http.Error(w, "DB not ready", http.StatusServiceUnavailable)
		return
	}
	// Asynq клиент не имеет прямого Ping, но можно выполнить неблокирующую проверку через контекст
	// Для простоты считаем, что если клиент создан и не закрыт – OK.
	if asynqClient == nil {
		http.Error(w, "Asynq client not initialized", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"ready": "true"})
}

// Определяем тип задачи Asynq на основе строки из Task.Type
func determineTaskType(taskType string) string {
	switch taskType {
	case "http":
		return tasks.TypeHTTP
	case "shell":
		return tasks.TypeShell
	default:
		return tasks.TypeHTTP // fallback
	}
}
