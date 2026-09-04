// Package worker implementa um pool de workers (goroutines) que consomem
// pagamentos de um channel e os processam contra o gateway, protegidos por
// retry + circuit breaker + timeout via context.
package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"payment-resilience-lab/gateway"
	"payment-resilience-lab/resilience"
)

type ResultadoProcessamento struct {
	PagamentoID string
	OK          bool
	Detalhe     string
	Duracao     time.Duration
}

type Pool struct {
	NumWorkers   int
	GW           gateway.PaymentGateway
	CB           *resilience.CircuitBreaker
	RetryCfg     resilience.RetryConfig
	TimeoutPorOp time.Duration
}

// Processar recebe todos os pagamentos, distribui entre N workers via um
// channel de entrada, e devolve um channel de resultados. Encerra tudo
// corretamente quando o ctx pai é cancelado (ex: deadline global do batch).
func (p *Pool) Processar(ctx context.Context, pagamentos []gateway.Pagamento) <-chan ResultadoProcessamento {
	jobs := make(chan gateway.Pagamento)
	results := make(chan ResultadoProcessamento, len(pagamentos))

	var wg sync.WaitGroup
	for i := 0; i < p.NumWorkers; i++ {
		wg.Add(1)
		go p.worker(ctx, i, jobs, results, &wg)
	}

	// goroutine produtora: alimenta o channel de jobs
	go func() {
		defer close(jobs)
		for _, pag := range pagamentos {
			select {
			case jobs <- pag:
			case <-ctx.Done():
				return
			}
		}
	}()

	// fecha results assim que todos os workers terminarem
	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}

func (p *Pool) worker(ctx context.Context, _ int, jobs <-chan gateway.Pagamento, results chan<- ResultadoProcessamento, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case pag, ok := <-jobs:
			if !ok {
				return
			}
			results <- p.processarUm(ctx, pag)
		}
	}
}

func (p *Pool) processarUm(ctx context.Context, pag gateway.Pagamento) ResultadoProcessamento {
	inicio := time.Now()

	opCtx, cancel := context.WithTimeout(ctx, p.TimeoutPorOp)
	defer cancel()

	err := p.CB.Executar(opCtx, func(ctx context.Context) error {
		return resilience.Do(ctx, p.RetryCfg, func(ctx context.Context) error {
			_, err := p.GW.Cobrar(ctx, pag)
			return err
		})
	})

	dur := time.Since(inicio)

	if err != nil {
		return ResultadoProcessamento{
			PagamentoID: pag.ID,
			OK:          false,
			Detalhe:     fmt.Sprintf("erro: %v", err),
			Duracao:     dur,
		}
	}

	return ResultadoProcessamento{
		PagamentoID: pag.ID,
		OK:          true,
		Detalhe:     "aprovado",
		Duracao:     dur,
	}
}
