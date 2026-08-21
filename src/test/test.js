const TOTAL = 100;

async function main() {
  const requests = [];

  for (let i = 1; i <= TOTAL; i++) {
    requests.push(
      fetch("http://localhost:4001/api/v1/events/create-event", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          eventType: "payment",
          payload: {
            Sender: "mohan@email.com",
            Reciever: `test${i}@email.com`,
            Amount: 200,
          },
        }),
      })
    );
  }

  const responses = await Promise.all(requests);

  for (const response of responses) {
    console.log(response.status, await response.text());
}

  console.log(`Sent ${responses.length} events`);
}

main();