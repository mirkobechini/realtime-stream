# Architecture Decision Record

**Progetto:** Real-Time Streaming Service
**Data:** 2026-09-15
**Autore:** Mirko Bechini

## Decisione

Microservizio che distribuisce eventi real-time ai client connessi via **WebSocket** e **Server-Sent Events (SSE)**, consumando da un broker di eventi configurabile. Fa **una sola cosa**: ricevere eventi da una sorgente e farli arrivare ai client in tempo reale, senza polling.

## Contesto

Servire dati che cambiano continuamente nel tempo (metriche IoT, posizioni GPS, ticker finanziari, stato di processi lunghi) senza costringere il client a fare richieste ripetute. FastAPI è scelto per il supporto nativo ad async e WebSocket, che consente migliaia di connessioni aperte con consumo di RAM ridotto.

## Piattaforme scelte

- Frontend: Dashboard interna (plain HTML/CSS/JS con WebSocket client)
- Backend: FastAPI (WebSocket + SSE nativi)
- Database: Redis (Pub/Sub per broadcast live + Streams per replay/persistenza)
- Deploy: Docker + Docker Compose

## Componenti principali

- **Connettore (EventSource)**: astrazione per consumare eventi da un broker configurabile (Redis, MQTT, Kafka, ...). Unica responsabilità: ricevere eventi grezzi.
- **Normalizzatore**: converte gli eventi grezzi nei campi canonici (`event_type, payload, source, timestamp, sequence`).
- **Broadcast Manager**: distribuisce ogni evento normalizzato a tutti i client connessi (WebSocket + SSE).
- **Replay Buffer**: conserva gli ultimi N eventi (da Redis Streams) per il replay ai nuovi client.
- **API**: endpoint WebSocket `/ws`, SSE `/api/v1/stream`, REST `/api/v1/events` (query/replay) e `/health`.
- **Dashboard**: visualizzazione live degli eventi in tempo reale.

## Decisioni architetturali

- **Broker baseline Redis (Pub/Sub + Streams)** vs MQTT/Kafka: Redis copre sia il broadcast live (Pub/Sub) sia la persistenza/replay (Streams) con un solo servizio, ed è il più veloce da avviare in Docker Compose. MQTT resta disponibile come connettore alternativo per casi IoT.
- **Astrazione EventSource** vs accoppiamento diretto al broker: permette di cambiare sorgente (Redis, MQTT, Kafka, ...) senza modificare il resto del sistema. Il microservizio resta agnostico rispetto alla sorgente.
- **WebSocket + SSE insieme** vs uno solo: WebSocket per comunicazione bidirezionale, SSE per broadcast one-way con auto-reconnect nativo del browser.
- **Replay via Redis Streams** vs solo buffer in-memory: un nuovo client riceve subito gli ultimi N eventi invece di aspettare il prossimo, e gli eventi sopravvivono al riavvio del servizio.
- **FastAPI** vs altri framework: supporto nativo ad async e WebSocket, ideale per migliaia di connessioni con RAM ridotta.

## Vincoli

- Deve essere un microservizio: **una sola cosa, fatta benissimo** (distribuire eventi real-time).
- Avviabile con `docker compose up`.
- API documentata e testabile.
- Almeno un consumer che usa dati reali del servizio.
- Ingest, storage e dashboard sono al servizio dello scopo principale, non obiettivi a sé.

## Cosa NON è in scope

- Elaborazione/trasformazione complessa degli eventi (analytics, aggregazioni) — è compito di altri servizi.
- Persistenza a lungo termine / data warehouse.
- Autenticazione/autorizzazione avanzata (MVP).
- Gestione di più istanze / clustering (fase successiva).

## Feature future pianificate

- Connettori aggiuntivi: MQTT, Kafka, RabbitMQ, NATS.
- Autenticazione per i client WebSocket/SSE.
- Filtri per topic/canale lato server.
- Clustering / scale-out orizzontale.
