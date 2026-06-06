package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"

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
		slog.Error("DB open error", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		slog.Error("DB ping error", "error", err)
		os.Exit(1)
	}
	slog.Info("Worker connected to PostgreSQL")

	// Создаём сервер Asynq
	srv := queue.NewRedisServer(redisAddr, "", 0)

	// Запуск HTTP-сервера для метрик Prometheus
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		slog.Info("Metrics server listening", "addr", ":2112")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			slog.Error("Metrics server failed", "error", err)
		}
	}()

	// Маршрутизатор задач
	mux := asynq.NewServeMux()
	mux.HandleFunc(tasks.TypeHTTP, handleHTTPTask)
	mux.HandleFunc(tasks.TypeShell, handleShellTask)

	// Запускаем сервер в горутине, чтобы поймать сигналы
	go func() {
		if err := srv.Run(mux); err != nil {
			slog.Error("Asynq server error", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown по SIGTERM/SIGINT
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("Shutting down worker...")
	srv.Shutdown()
	slog.Info("Worker stopped")
}

// handleHTTPTask обрабатывает HTTP задачу
func handleHTTPTask(ctx context.Context, t *asynq.Task) error {
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		slog.Error("Failed to unmarshal payload", "error", err)
		return fmt.Errorf("invalid payload: %w", err)
	}
	taskID := payload.ID
	slog.Info("Processing HTTP task", "task_id", taskID)

	var task Task
	query := `SELECT id, name, type, config, status, retries FROM tasks WHERE id=$1`
	err := db.QueryRowContext(ctx, query, taskID).Scan(&task.ID, &task.Name, &task.Type, &task.Config, &task.Status, &task.Retries)
	if err == sql.ErrNoRows {
		slog.Error("Task not found in DB", "task_id", taskID)
		return fmt.Errorf("task %s not found", taskID)
	}
	if err != nil {
		slog.Error("DB error", "task_id", taskID, "error", err)
		return fmt.Errorf("DB error: %w", err)
	}

	var httpConfig struct {
		URL     string            `json:"url"`
		Method  string            `json:"method"`
		Headers map[string]string `json:"headers"`
		Body    string            `json:"body"`
	}
	if err := json.Unmarshal(task.Config, &httpConfig); err != nil {
		slog.Error("Invalid HTTP config", "task_id", taskID, "config", string(task.Config), "error", err)
		updateTaskStatus(taskID, "failed")
		return fmt.Errorf("invalid http config: %w", err)
	}

	if httpConfig.Method == "" {
		httpConfig.Method = "GET"
	}

	client := &http.Client{Timeout: 30 * time.Second}
	var bodyReader io.Reader
	if httpConfig.Body != "" {
		bodyReader = bytes.NewBufferString(httpConfig.Body)
	}
	req, err := http.NewRequestWithContext(ctx, httpConfig.Method, httpConfig.URL, bodyReader)
	if err != nil {
		slog.Error("Failed to create request", "task_id", taskID, "error", err)
		updateTaskStatus(taskID, "failed")
		return err
	}
	for k, v := range httpConfig.Headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		slog.Error("HTTP request failed", "task_id", taskID, "url", httpConfig.URL, "error", err)
		updateTaskStatus(taskID, "failed")
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	slog.Info("HTTP response", "task_id", taskID, "status", resp.StatusCode, "body", string(body))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		updateTaskStatus(taskID, "success")
		slog.Info("HTTP task succeeded", "task_id", taskID, "status_code", resp.StatusCode)
		return nil
	}

	updateTaskStatus(taskID, "failed")
	err = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	slog.Error("HTTP task failed", "task_id", taskID, "status_code", resp.StatusCode)
	return err
}

// handleShellTask обрабатывает shell задачу
func handleShellTask(ctx context.Context, t *asynq.Task) error {
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return err
	}
	taskID := payload.ID
	slog.Info("Processing shell task", "task_id", taskID)

	var task Task
	query := `SELECT id, name, type, config, status, retries FROM tasks WHERE id=$1`
	err := db.QueryRowContext(ctx, query, taskID).Scan(&task.ID, &task.Name, &task.Type, &task.Config, &task.Status, &task.Retries)
	if err == sql.ErrNoRows {
		return fmt.Errorf("task %s not found", taskID)
	}
	if err != nil {
		return err
	}

	var shellConfig struct {
		Command string   `json:"command"`
		Args    []string `json:"args"`
		Env     []string `json:"env"`
		Dir     string   `json:"dir"`
	}
	if err := json.Unmarshal(task.Config, &shellConfig); err != nil {
		updateTaskStatus(taskID, "failed")
		return err
	}

	cmd := exec.CommandContext(ctx, shellConfig.Command, shellConfig.Args...)
	if shellConfig.Dir != "" {
		cmd.Dir = shellConfig.Dir
	}
	if len(shellConfig.Env) > 0 {
		cmd.Env = append(os.Environ(), shellConfig.Env...)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		updateTaskStatus(taskID, "failed")
		slog.Error("Shell task failed", "task_id", taskID, "output", string(output), "error", err)
		return err
	}
	updateTaskStatus(taskID, "completed")
	slog.Info("Shell task succeeded", "task_id", taskID, "output", string(output))
	return nil
}

// updateTaskStatus обновляет статус задачи в БД
func updateTaskStatus(taskID, status string) {
	_, err := db.Exec(`UPDATE tasks SET status=$1, updated_at=NOW() WHERE id=$2`, status, taskID)
	if err != nil {
		slog.Error("Failed to update task status", "task_id", taskID, "status", status, "error", err)
	}
}
