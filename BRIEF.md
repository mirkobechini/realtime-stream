# Brief Overview

Sorgente: broker di eventi real-time (configurabile: Kafka, RabbitMQ, Redis Pub/Sub, MQTT, NATS, ...)
Risorsa: stream di eventi (metriche, posizioni GPS, ticker, stato processi)
Campi canonici: event_type, payload, source, timestamp, sequence
Storage: buffer in-memory + persistenza opzionale (SQLite/Redis) per replay
Consumer: dashboard live + client esterni via WebSocket / SSE
Endpoint: `/ws` (WebSocket) e `/api/v1/stream` (SSE)

## Flow

1. Il sistema si connette al broker di eventi configurato (Kafka, RabbitMQ, Redis Pub/Sub, MQTT, ...) tramite un connettore astratto.
2. Ogni evento ricevuto viene normalizzato nei campi canonici.
3. Gli eventi vengono distribuiti in broadcast a tutti i client connessi via WebSocket/SSE.
4. Un buffer in-memory conserva gli ultimi N eventi per il replay ai nuovi client.
5. La dashboard live e i client esterni si sottoscrivono agli stream senza polling.
6. Eventuali disconnessioni vengono gestite con riconnessione automatica e backoff.

## Requirements

Step 1: Definire i modelli e lo schema degli eventi (campi canonici + validazione).
Step 2: Implementare l'astrazione del connettore (interfaccia EventSource) con implementazioni per i broker supportati e riconnessione.
Step 3: Implementare il broadcast manager per distribuire gli eventi ai client connessi.
Step 4: Sviluppare gli endpoint WebSocket (`/ws`) e SSE (`/api/v1/stream`) con supporto ai topic/filtri.
Step 5: Costruire la dashboard interna live per visualizzare gli eventi in tempo reale.

## Stack

- Broker: configurabile tramite interfaccia EventSource (Kafka, RabbitMQ, Redis Pub/Sub, MQTT, NATS, ...)
- Backend: FastAPI (WebSocket + SSE nativi, asincrono, migliaia di connessioni con RAM ridotta)
- Frontend: Dashboard interna (plain HTML/CSS/JS con WebSocket client)
- Ingestion: Consumer Python asincrono con connettori pluggabili (aiokafka / aio-pika / redis-py / paho-mqtt / nats-py)
- Storage: Buffer in-memory + persistenza opzionale (SQLite/Redis) per replay e audit
