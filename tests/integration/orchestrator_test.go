//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Тест запускает реальный оркестратор через docker-compose или expects that it's already running.
// В этом примере предполагается, что сервис запущен на localhost:8080.
func TestCreateAndGetTask(t *testing.T) {
	// Создаем задачу
	body := `{"name":"integration","type":"shell","config":{"command":"echo ok"}}`
	resp, err := http.Post("http://localhost:8080/task", "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var task struct{ ID string }
	err = json.NewDecoder(resp.Body).Decode(&task)
	require.NoError(t, err)

	// Ждем выполнения (воркер должен обработать)
	time.Sleep(2 * time.Second)

	// Получаем статус
	resp2, err := http.Get("http://localhost:8080/task/" + task.ID)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	var statusResp struct{ Status string }
	err = json.NewDecoder(resp2.Body).Decode(&statusResp)
	require.NoError(t, err)
	require.Equal(t, "success", statusResp.Status)
}
