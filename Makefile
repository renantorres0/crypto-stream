.PHONY: up down logs ps clean reset producer-logs spark-logs dashboard-logs kafka-topics

# ── Iniciar tudo ──────────────────────────────────────────────────────────────
up:
	docker compose up -d --build
	@echo ""
	@echo "✅ CryptoStream iniciado!"
	@echo ""
	@echo "  📈 Dashboard:   http://localhost:8501"
	@echo "  🟠 Kafka UI:    http://localhost:8080"
	@echo "  🐘 PostgreSQL:  localhost:5432  (cryptouser/cryptopass)"
	@echo "  🔴 Redis:       localhost:6379"
	@echo ""
	@echo "Aguarde ~30s para o produtor conectar ao Coinbase WebSocket."

# ── Parar tudo ────────────────────────────────────────────────────────────────
down:
	docker compose down

# ── Status ────────────────────────────────────────────────────────────────────
ps:
	docker compose ps

# ── Logs ─────────────────────────────────────────────────────────────────────
logs:
	docker compose logs -f

producer-logs:
	docker compose logs -f producer

spark-logs:
	docker compose logs -f spark-processor

dashboard-logs:
	docker compose logs -f dashboard

# ── Tópicos Kafka ──────────────────────────────────────────────────────────────
kafka-topics:
	docker exec kafka kafka-topics \
		--bootstrap-server localhost:9092 \
		--describe \
		--topic crypto-prices

kafka-consumer:
	docker exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic crypto-prices \
		--from-beginning \
		--max-messages 10

# ── Banco de dados ─────────────────────────────────────────────────────────────
psql:
	docker exec -it postgres psql -U cryptouser -d cryptodb

redis-cli:
	docker exec -it redis redis-cli

# ── Limpeza ────────────────────────────────────────────────────────────────────
clean:
	docker compose down -v --remove-orphans
	docker image prune -f

reset: clean up
