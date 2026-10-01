package handler

import (
	"encoding/json"
	"time"

	gorilla "github.com/gorilla/websocket"
)

type roomReadyFrame struct {
	Type          string `json:"type"`
	LiveSessionID uint64 `json:"live_session_id"`
}

func writeRoomReady(connection *gorilla.Conn, liveSessionID uint64, timeout time.Duration) error {
	body, err := json.Marshal(roomReadyFrame{Type: "room.ready", LiveSessionID: liveSessionID})
	if err != nil {
		return err
	}
	if err := connection.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	return connection.WriteMessage(gorilla.TextMessage, body)
}
