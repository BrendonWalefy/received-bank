# Receber Bank

Modernizacao da area de recebimentos bancarios com arquitetura distribuida em Java 21, Spring Boot, PostgreSQL, Redis, Kafka/Redpanda, Kubernetes e infraestrutura AWS.

## Visao Geral

O projeto e organizado como um conjunto de microservicos Maven em `receber-bank-services`:

| Modulo | Porta local | Responsabilidade |
| --- | --- | --- |
| `boleto-service` | `8081` | Cria e consulta boletos no modelo de escrita, aplica regras de dominio e publica eventos via transactional outbox. |
| `query-service` | `8082` | Mantem e expoe o read model de boletos para consultas. |
| `payment-service` | `8083` | Consome boletos gerados e simula/efetiva pagamentos, publicando `pagamento.efetivado`. |
| `notification-service` | `8084` | Consome eventos de boleto e pagamento, envia notificacoes e publica `notificacao.enviada`. |

Infraestrutura local:

- PostgreSQL 16
- Redis 7
- Redpanda, compativel com Kafka
- Docker Compose para subir dependencias e servicos

## Documentacao

A documentacao navegavel do projeto fica em [`docs/index.html`](docs/index.html) e e publicada pelo GitHub Pages por meio do workflow [`pages.yml`](.github/workflows/pages.yml).

Depois de publicar no GitHub, habilite o Pages em:

`Settings` -> `Pages` -> `Build and deployment` -> `GitHub Actions`

## Como Rodar

Entre no modulo principal:

```bash
cd receber-bank-services
```

Compile e rode os testes:

```bash
./mvnw clean test
```

Suba a stack local:

```bash
docker compose up --build
```

Endpoints principais:

- `POST http://localhost:8081/boletos`
- `GET http://localhost:8081/boletos/{id}`
- `GET http://localhost:8082/consultas/boletos`
- `GET http://localhost:8082/consultas/boletos/{boletoId}`

Health checks:

- `http://localhost:8081/actuator/health`
- `http://localhost:8082/actuator/health`
- `http://localhost:8083/actuator/health`
- `http://localhost:8084/actuator/health`

## Publicacao no GitHub

Este clone ainda precisa de um remote GitHub configurado. Quando o repositorio existir no GitHub, configure:

```bash
git remote add origin https://github.com/SEU_USUARIO/receber-bank.git
git push -u origin appmod/java-upgrade-20260505103714
```

Se quiser usar `main` como branch publicada:

```bash
git checkout main
git merge appmod/java-upgrade-20260505103714
git push -u origin main
```

