package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
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
	log.Println("Connected to PostgreSQL")

	// Подключение к Redis
	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatal("Cannot reach Redis: ", err)
	}
	log.Println("Connected to Redis")

	// Создание таблицы
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
		log.Fatal(err)
	}
	log.Println("Table tasks ready")

	// HTTP роутер
	r := mux.NewRouter()
	r.HandleFunc("/task/{id}", getTaskStatus).Methods("GET")
	r.HandleFunc("/health", healthCheck).Methods("GET")

	srv := &http.Server{
		Handler:      r,
		Addr:         ":8080",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Println("Orchestrator starting on :8080")
	log.Fatal(srv.ListenAndServe())
}

func createTask(w http.ResponseWriter, r *http.Request) {
	var task Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if task.ID == "" {
		task.ID = uuid.New().String()
	}
	task.Status = "pending"

	// Сохраняем в PostgreSQL
	query := `INSERT INTO tasks (id, name, status, type, config) VALUES ($1, $2, $3, $4, $5)`
	_, err := db.Exec(query, task.ID, task.Name, task.Status, task.Type, task.Config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Отправляем ID в Redis очередь
	err = rdb.LPush(ctx, "task:queue", task.ID).Err()
	if err != nil {
		log.Printf("Failed to push task %s to Redis: %v", task.ID, err)
	} else {
		log.Printf("Task %s pushed to Redis queue", task.ID)
	}

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
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}
