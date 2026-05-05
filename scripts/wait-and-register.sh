#!/bin/bash
# Waits for Kafka Connect (Debezium) to be ready, then registers the MySQL connector

echo "Waiting for Kafka Connect to be ready..."
until curl -s http://debezium:8083/connectors > /dev/null 2>&1; do
    echo "  ...Kafka Connect not ready yet, retrying in 5s"
    sleep 5
done

echo "Kafka Connect is ready. Registering shipments connector..."

# Check if connector already exists
STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://debezium:8083/connectors/shipments-connector)

if [ "$STATUS" = "200" ]; then
    echo "Connector already exists. Updating..."
    curl -s -X PUT \
        -H "Content-Type: application/json" \
        -d @/config/register-connector.json \
        http://debezium:8083/connectors/shipments-connector/config
else
    echo "Creating new connector..."
    curl -s -X POST \
        -H "Content-Type: application/json" \
        -d @/config/register-connector.json \
        http://debezium:8083/connectors
fi

echo ""
echo "Connector registered. Verifying..."
sleep 3
curl -s http://debezium:8083/connectors/shipments-connector/status

echo ""
echo "Done. CDC is now capturing changes from MySQL -> Kafka"
echo "Topic: shipments.shipments_db.shipments"
