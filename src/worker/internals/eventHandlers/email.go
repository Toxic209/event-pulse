package eventhandlers

import (
	"encoding/json"
	"fmt"
)

type Email struct {
	From string `json:"from"`;
	To string `json:"to"`;
	Content string `json:"content"`;
}

func EmailHandler(payload string, eventId string) error {
	var email Email;
	err := json.Unmarshal([]byte(payload), &email);

	if err != nil {
		return err;
	}
	
	fmt.Printf("Sending Email to %s\n", email.To);

	if email.To == "test@fail.email" {
		return fmt.Errorf("Error: Failed to send Email!");
	}

	fmt.Println("Email sent!");

	return nil;
}