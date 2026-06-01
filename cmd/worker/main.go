package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
)

type Task struct {
	ID      string          `json:"id"`
	Name    string          `json:"name"`
	Type    string          `json:"type"`
	Config  json.RawMessage `json:"config"`
	Status  string          `json:"status"`
	Retries int             `json:"retries"`
}

type ShellConfig struct {
	Command string `json:"command"`
}

type HTTPConfig struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

var db *sql.DB
var rdb *redis.Client
var ctx = context.Background()

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	// Читаем переменные окружения
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbUser := getEnv("DB_USER", "user")
	dbPassword := getEnv("DB_PASSWORD", "password")
	dbName := getEnv("DB_NAME", "orchestrator")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	// Подключение к PostgreSQL
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatal("Cannot reach DB: ", err)
	}
	log.Println("Worker connected to PostgreSQL")

	// Подключение к Redis
	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatal("Cannot reach Redis: ", err)
	}
	log.Println("Worker connected to Redis")

	// Бесконечный цикл обработки задач
	for {
		// Блокируемся, пока не появится задача в очереди "task:queue"
		result, err := rdb.BLPop(ctx, 0*time.Second, "task:queue").Result()
		if err != nil {
			log.Printf("BLPop error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if len(result) < 2 {
			continue
		}
		taskID := result[1]
		log.Printf("Worker picked task %s", taskID)

		// Загружаем задачу из БД
		task, err := loadTask(taskID)
		if err != nil {
			log.Printf("Failed to load task %s: %v", taskID, err)
			continue
		}

		// Обновляем статус на "running"
		updateTaskStatus(taskID, "running")

		// Выполняем задачу в зависимости от типа
		var execErr error
		switch task.Type {
		case "shell":
			execErr = executeShell(task.Config)
		case "http":
			execErr = executeHTTP(task.Config)
		default:
			log.Printf("Unknown task type: %s", task.Type)
			execErr = fmt.Errorf("unknown task type: %s", task.Type)
		}

		// Обработка результата с ретраями
		if execErr != nil {
			log.Printf("Task %s failed: %v", taskID, execErr)

			// Увеличиваем счетчик ретраев
			newRetries := task.Retries + 1
			if newRetries <= 3 {
				log.Printf("Retry %d/3 for task %s", newRetries, taskID)
				// Обновляем retries в БД
				updateTaskRetries(taskID, newRetries)
				// Возвращаем задачу в очередь
				if err := rdb.LPush(ctx, "task:queue", taskID).Err(); err != nil {
					log.Printf("Failed to push task %s back to queue: %v", taskID, err)
				}
				// Обновляем статус на pending (ждет повторной попытки)
				updateTaskStatus(taskID, "pending")
			} else {
				log.Printf("Task %s exceeded max retries (3), marking as failed", taskID)
				updateTaskStatus(taskID, "failed")
			}
		} else {
			log.Printf("Task %s completed successfully", taskID)
			updateTaskStatus(taskID, "success")
		}
	}
}

func loadTask(id string) (*Task, error) {
	row := db.QueryRow(`SELECT id, name, type, config, status, retries FROM tasks WHERE id=$1`, id)
	var task Task
	err := row.Scan(&task.ID, &task.Name, &task.Type, &task.Config, &task.Status, &task.Retries)
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func updateTaskStatus(id, status string) {
	_, err := db.Exec(`UPDATE tasks SET status=$1, updated_at=NOW() WHERE id=$2`, status, id)
	if err != nil {
		log.Printf("Failed to update status for task %s: %v", id, err)
	}
}

func updateTaskRetries(id string, retries int) {
	_, err := db.Exec(`UPDATE tasks SET retries=$1, updated_at=NOW() WHERE id=$2`, retries, id)
	if err != nil {
		log.Printf("Failed to update retries for task %s: %v", id, err)
	}
}

func executeShell(configRaw json.RawMessage) error {
	var config ShellConfig
	if err := json.Unmarshal(configRaw, &config); err != nil {
		return err
	}

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/C", config.Command)
	} else {
		cmd = exec.Command("sh", "-c", config.Command)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Command error: %v, output: %s", err, output)
		return err
	}
	log.Printf("Command output: %s", output)
	return nil
}

func executeHTTP(configRaw json.RawMessage) error {
	var config HTTPConfig
	if err := json.Unmarshal(configRaw, &config); err != nil {
		return err
	}

	if config.Method == "" {
		config.Method = "GET"
	}

	var bodyReader io.Reader
	if config.Body != "" {
		bodyReader = bytes.NewBufferString(config.Body)
	}

	req, err := http.NewRequest(config.Method, config.URL, bodyReader)
	if err != nil {
		return err
	}

	for k, v := range config.Headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	log.Printf("HTTP response: status=%d, body=%s", resp.StatusCode, string(body))

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}
	return nil
}
