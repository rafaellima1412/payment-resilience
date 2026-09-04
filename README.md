# payment-resilience-lab

Projeto de estudo em Go puro (sem dependências externas) que simula um
checkout contra um gateway de pagamento instável, combinando os tópicos
mais cobrados em entrevista de backend sênior para fintech/payments:

- **Goroutines + channels** — pool de workers processando pagamentos em paralelo
- **context** — timeout por operação e cancelamento propagado (batch -> pool -> worker -> retry -> gateway)
- **Retry com backoff exponencial + jitter** — só em erros transientes (nunca em erro de negócio)
- **Circuit breaker (Closed/Open/Half-Open)** — protege o sistema de martelar um provedor fora do ar

## Estrutura

```
main.go                    orquestra tudo, imprime o resultado do batch
gateway/gateway.go         PSP fake: às vezes 503, às vezes timeout, às vezes recusa (negócio)
resilience/retry.go        retry genérico com backoff exponencial + jitter
resilience/circuitbreaker.go  circuit breaker thread-safe (Closed/Open/Half-Open)
worker/pool.go             worker pool (goroutines + channels) que aplica CB+retry+timeout por pagamento
```

## Como rodar

```bash
go run .
```

Cada execução varia (o gateway fake usa `math/rand`), então rode algumas
vezes seguidas — em algumas execuções o circuit breaker chega a abrir
(`CLOSED -> OPEN -> HALF-OPEN -> CLOSED`), em outras as falhas se resolvem
via retry antes de bater o limite. Isso é de propósito: ajuda a visualizar
que retry e circuit breaker atuam em camadas diferentes.

## Pontos para revisar/explicar em entrevista

1. **Por que `context.WithTimeout` por operação, e não só um timeout global?**
   Veja `worker/pool.go: processarUm` — cada pagamento tem seu próprio
   deadline (`TimeoutPorOp`), derivado do `ctx` do batch. Se o batch inteiro
   estourar o prazo, todo mundo é cancelado em cascata (cancelamento propagado
   via `context`, sem precisar de flags manuais).

2. **Por que `Retryable` importa tanto quanto o retry em si.**
   `gateway.Retryable` distingue erro transiente (503, timeout) de erro de
   negócio (cartão recusado). Re-tentar um erro de negócio não corrige nada
   e, sem idempotency key, pode duplicar cobrança — por isso `Pagamento` tem
   `IdempotencyKey` (não usado na simulação, mas é o gancho pra explicar o
   conceito: um PSP real usa essa chave pra deduplicar retries no lado dele).

3. **Por que o circuit breaker fica em torno do retry, não dentro dele.**
   Em `worker/pool.go`, o `CB.Executar` envolve o `resilience.Do` (retry)
   inteiro. Isso significa: o circuito só conta como "1 falha" depois que
   todas as tentativas de retry já se esgotaram — evita abrir o circuito
   por causa de uma falha isolada que o retry resolveria sozinho.

4. **Goroutine leak.** Repare que a goroutine produtora em `Pool.Processar`
   também escuta `ctx.Done()` ao alimentar o channel `jobs` — sem isso, se o
   contexto for cancelado no meio do batch, essa goroutine ficaria bloqueada
   pra sempre tentando escrever num channel que ninguém mais lê (leak).

5. **Por que `results` é bufferizado com `len(pagamentos)`.**
   Evita que os workers fiquem bloqueados esperando alguém consumir
   `results` enquanto ainda estão processando — desacopla produção e consumo.

## Exercícios sugeridos

- Trocar o circuit breaker manual por [`sony/gobreaker`](https://github.com/sony/gobreaker)
  (a lib mais usada no ecossistema Go) e comparar a API.
- Adicionar métricas (ex: contador de retries, tempo em cada estado do CB)
  expostas via um endpoint `/metrics` simples com `net/http`.
- Simular um cenário de **timeout do batch inteiro** (baixe o `context.WithTimeout`
  do `main.go` pra 500ms) e observar o cancelamento em cascata nos logs.
- Adicionar uma segunda goroutine "consumidora" dos resultados, gravando
  em uma fila (`chan`) separada só os pagamentos que falharam, simulando
  uma DLQ (dead-letter queue).
