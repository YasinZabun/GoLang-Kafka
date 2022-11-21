
# GoLang - Kafka

This project is an example that produce message and consume the message as multi threading.

## How to run?
Open a terminal then type following codes.
```bash
  docker-compose up -d
  docker compose exec broker \
  kafka-topics --create \
    --topic test \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 5
  go run producer.go
  go run consumer.go
```
