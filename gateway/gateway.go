// Package gateway simula um PSP (payment service provider) externo instável,
// parecido com o comportamento real de SGP/HubSoft: às vezes lento, às vezes
// fora do ar, às vezes retorna erro de negócio (não retryable).
package gateway

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// ErrCartaoRecusado é um erro de NEGÓCIO — nunca deve ser retentado.
var ErrCartaoRecusado = errors.New("cartao recusado pelo emissor")

// ErrGatewayIndisponivel é um erro TRANSIENTE — candidato a retry.
var ErrGatewayIndisponivel = errors.New("gateway indisponivel (503)")

// ErrTimeout representa lentidão do provedor — também transiente.
var ErrTimeout = errors.New("timeout no gateway")

type Pagamento struct {
	ID              string
	IdempotencyKey  string
	ValorCentavos   int64
}

type Resultado struct {
	PagamentoID string
	Status      string // "aprovado" | "recusado"
	TxID        string
}

// PaymentGateway é a interface que o resto da aplicação enxerga.
// Em produção seria implementada por um client HTTP real (como
// os gateways SGP/HubSoft do Propulsor).
type PaymentGateway interface {
	Cobrar(ctx context.Context, p Pagamento) (Resultado, error)
}

// FlakyGateway é uma implementação fake que falha de propósito,
// simulando um provedor externo problemático.
type FlakyGateway struct {
	// FalhaAte define até qual "tentativa global" o serviço fica ruim.
	// Depois disso ele volta a funcionar bem (simula recuperação),
	// o que é útil pra ver o circuit breaker fechar de novo (half-open -> closed).
	falhasConsecutivas int
	limiteRuim         int
	mu                 chan struct{} // mutex leve via channel de 1 slot
}

func NewFlakyGateway(limiteRuim int) *FlakyGateway {
	g := &FlakyGateway{
		limiteRuim: limiteRuim,
		mu:         make(chan struct{}, 1),
	}
	g.mu <- struct{}{}
	return g
}

func (g *FlakyGateway) Cobrar(ctx context.Context, p Pagamento) (Resultado, error) {
	// Simula latência de rede
	latencia := time.Duration(50+rand.Intn(400)) * time.Millisecond

	select {
	case <-time.After(latencia):
	case <-ctx.Done():
		return Resultado{}, fmt.Errorf("cobranca %s cancelada: %w", p.ID, ctx.Err())
	}

	<-g.mu
	g.falhasConsecutivas++
	ruim := g.falhasConsecutivas <= g.limiteRuim
	g.mu <- struct{}{}

	if ruim {
		// 70% chance de indisponibilidade, 30% de timeout — ambos retryable
		if rand.Float64() < 0.7 {
			return Resultado{}, ErrGatewayIndisponivel
		}
		return Resultado{}, ErrTimeout
	}

	// Serviço "recuperado": ainda pode recusar por regra de negócio (não retryable)
	if rand.Float64() < 0.1 {
		return Resultado{}, ErrCartaoRecusado
	}

	return Resultado{
		PagamentoID: p.ID,
		Status:      "aprovado",
		TxID:        fmt.Sprintf("tx_%d", rand.Int63()),
	}, nil
}

// Retryable decide se vale a pena tentar de novo — a parte mais
// importante pra não transformar "resiliência" em bug de duplicidade.
func Retryable(err error) bool {
	return errors.Is(err, ErrGatewayIndisponivel) || errors.Is(err, ErrTimeout)
}
