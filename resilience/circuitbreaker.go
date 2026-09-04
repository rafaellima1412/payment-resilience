package resilience

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Estado int

const (
	Closed Estado = iota
	Open
	HalfOpen
)

func (e Estado) String() string {
	switch e {
	case Closed:
		return "CLOSED"
	case Open:
		return "OPEN"
	case HalfOpen:
		return "HALF-OPEN"
	default:
		return "?"
	}
}

var ErrCircuitoAberto = errors.New("circuit breaker aberto: chamada bloqueada")

type CircuitBreakerConfig struct {
	LimiteFalhas     int           // falhas consecutivas até abrir
	Cooldown         time.Duration // tempo em OPEN antes de tentar HALF-OPEN
	SucessosParaFechar int         // sucessos consecutivos em HALF-OPEN até fechar de novo
	OnMudancaEstado  func(de, para Estado)
}

type CircuitBreaker struct {
	cfg CircuitBreakerConfig

	mu                sync.Mutex
	estado            Estado
	falhasConsecutivas int
	sucessosHalfOpen  int
	abertoDesde       time.Time
}

func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.SucessosParaFechar == 0 {
		cfg.SucessosParaFechar = 2
	}
	return &CircuitBreaker{cfg: cfg, estado: Closed}
}

func (cb *CircuitBreaker) Estado() Estado {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.estado
}

// Executar roda `op` respeitando o estado atual do circuito.
func (cb *CircuitBreaker) Executar(ctx context.Context, op func(ctx context.Context) error) error {
	if !cb.podeExecutar() {
		return ErrCircuitoAberto
	}

	err := op(ctx)

	cb.registrarResultado(err)
	return err
}

func (cb *CircuitBreaker) podeExecutar() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.estado {
	case Open:
		if time.Since(cb.abertoDesde) >= cb.cfg.Cooldown {
			cb.transicionar(HalfOpen)
			return true
		}
		return false
	default:
		return true
	}
}

func (cb *CircuitBreaker) registrarResultado(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.falhasConsecutivas++
		cb.sucessosHalfOpen = 0

		if cb.estado == HalfOpen {
			// falhou testando recuperação -> volta pra OPEN imediatamente
			cb.abertoDesde = time.Now()
			cb.transicionar(Open)
			return
		}
		if cb.falhasConsecutivas >= cb.cfg.LimiteFalhas {
			cb.abertoDesde = time.Now()
			cb.transicionar(Open)
		}
		return
	}

	// sucesso
	cb.falhasConsecutivas = 0
	if cb.estado == HalfOpen {
		cb.sucessosHalfOpen++
		if cb.sucessosHalfOpen >= cb.cfg.SucessosParaFechar {
			cb.sucessosHalfOpen = 0
			cb.transicionar(Closed)
		}
	}
}

// transicionar assume que cb.mu já está locked.
func (cb *CircuitBreaker) transicionar(novo Estado) {
	if cb.estado == novo {
		return
	}
	anterior := cb.estado
	cb.estado = novo
	if cb.cfg.OnMudancaEstado != nil {
		cb.cfg.OnMudancaEstado(anterior, novo)
	}
}
