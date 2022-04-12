package client

import "github.com/google/uuid"

var clientID uuid.UUID

func init() {
	clientID = uuid.New()
}

func ClientID() uuid.UUID {
	return clientID
}