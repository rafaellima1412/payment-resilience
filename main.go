package main

import (
	"context"
	"fmt"
	"time"

	"payment-resilience-lab/gateway"
	"payment-resilience-lab/resilience"
	"payment-resilience-lab/worker"
)

func main() {
	fmt.Println("=== Laboratório de Resiliência em Go ===")
	fmt.Println("Simulando checkout contra um gateway de pagamento instável.")
	fmt.Println()

	// Gateway "ruim" nas primeiras 6 chamadas globais, depois se recupera.
	gw := gateway.NewFlakyGateway(6)

	cb := resilience.NewCircuitBreaker(resilience.CircuitBreakerConfig{
		LimiteFalhas:       3,
		Cooldown:           1500 * time.Millisecond,
		SucessosParaFechar: 2,
		OnMudancaEstado: func(de, para resilience.Estado) {
			fmt.Printf("  [circuit breaker] %s -> %s\n", de, para)
		},
	})

	retryCfg := resilience.RetryConfig{
		MaxTentativas: 3,
		BaseDelay:     150 * time.Millisecond,
		MaxDelay:      2 * time.Second,
		Retryable:     gateway.Retryable, // nunca re-tenta erro de negócio
	}

	pool := &worker.Pool{
		NumWorkers:   4,
		GW:           gw,
		CB:           cb,
		RetryCfg:     retryCfg,
		TimeoutPorOp: 3 * time.Second,
	}

	// batch de 15 pagamentos simulados
	pagamentos := make([]gateway.Pagamento, 0, 15)
	for i := 1; i <= 15; i++ {
		pagamentos = append(pagamentos, gateway.Pagamento{
			ID:             fmt.Sprintf("pag-%02d", i),
			IdempotencyKey: fmt.Sprintf("idem-%02d", i),
			ValorCentavos:  int64(1000 + i*137),
		})
	}

	// deadline global do batch inteiro — se estourar, tudo é cancelado
	// via context propagation (pool -> workers -> retry -> gateway)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	inicio := time.Now()
	results := pool.Processar(ctx, pagamentos)

	var aprovados, falhados int
	for r := range results {
		status := "✅ OK "
		if !r.OK {
			status = "❌ ERR"
			falhados++
		} else {
			aprovados++
		}
		fmt.Printf("%s  %-10s  %-40s  (%v)\n", status, r.PagamentoID, r.Detalhe, r.Duracao.Round(time.Millisecond))
	}

	fmt.Println()
	fmt.Printf("Total: %d | Aprovados: %d | Falhados: %d | Tempo total: %v\n",
		len(pagamentos), aprovados, falhados, time.Since(inicio).Round(time.Millisecond))
	fmt.Printf("Estado final do circuit breaker: %s\n", cb.Estado())
}
