package eventhandlers

import (
	"encoding/json"
	"fmt"
	"time"
)

type Payment struct {
	Sender string `json:"sender"`;
	Reciever string `json:"reciever"`
	Amount int `json:"amount"`;
}

func PaymentHandler(payload string) error {
	var payment Payment;

	err := json.Unmarshal([]byte(payload), &payment);
	if err != nil {
		return err;
	}

	fmt.Printf("Sending amount %d to %s\n", payment.Amount, payment.Reciever);
	time.Sleep(1 * time.Second);
	fmt.Printf("Sent %d to %s", payment.Amount, payment.Reciever);

	if payment.Reciever == "failed-payment" {
		return fmt.Errorf("Error: SImulated Payment Failure!");
	}
	
	return nil;
}