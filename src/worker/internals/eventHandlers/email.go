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
	
	//simulate content sending to email with a delay;
	// db, err := postgres.Connectpg();

	// if err != nil {
	// 	fmt.Println("Postgres connection failed!");
	// 	return err;
	// }

	// repo := postgres.NewEventRepo(db);
    // err = repo.MarkComplete(eventId);

	// if err != nil {
	// 	fmt.Println("Email Event processing failed!");
	// 	return err
	// }

	if email.To == "test@fail.email" {
		return fmt.Errorf("Error: Failed to send Email!");
	}

	fmt.Println("Email sent!");

	return nil;
}