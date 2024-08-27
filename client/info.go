package client

import "github.com/google/uuid"

var clientID uuid.UUID

func ID() uuid.UUID {
	if clientID == uuid.Nil {
		clientID = uuid.New()
	}
	return clientID
}
