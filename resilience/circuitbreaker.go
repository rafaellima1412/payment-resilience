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
	LimiteFalhas       int           // falhas consecutivas até abrir
	Cooldown           time.Duration // tempo em OPEN antes de tentar HALF-OPEN
	SucessosParaFechar int           // sucessos consecutivos em HALF-OPEN até fechar de novo
	OnMudancaEstado    func(de, para Estado)
}

type CircuitBreaker struct {
	cfg                CircuitBreakerConfig
	mu                 sync.Mutex // mutex para proteger o estado do circuito
	estado             Estado
	falhasConsecutivas int
	sucessosHalfOpen   int
	abertoDesde        time.Time
}

func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.SucessosParaFechar == 0 {
		cfg.SucessosParaFechar = 2
	}
	return &CircuitBreaker{cfg: cfg, estado: Closed} //Closed é o estado inicial do circuito
}

func (cb *CircuitBreaker) Estado() Estado {
	cb.mu.Lock()         //lock para leituras
	defer cb.mu.Unlock() // adia a chamada a unlock
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
	cb.mu.Lock()         //lock para leituras
	defer cb.mu.Unlock() // adia a chamada a unlock

	switch cb.estado {
	case Open:
		if time.Since(cb.abertoDesde) >= cb.cfg.Cooldown {
			cb.transicionar(HalfOpen) // estado do circuito muda para HALF-OPEN
			return true
		}
		return false
	default:
		return true
	}
}

func (cb *CircuitBreaker) registrarResultado(err error) { //defer deveria ser usado em uma função menor, mas como é uma função pequena, não há problema em não usar
	cb.mu.Lock()         //lock para leituras
	defer cb.mu.Unlock() // adia a chamada a unlock
	if err != nil {
		cb.falhasConsecutivas++
		cb.sucessosHalfOpen = 0

		if cb.estado == HalfOpen {
			// falhou testando recuperação -> volta pra OPEN imediatamente
			cb.abertoDesde = time.Now()
			cb.transicionar(Open) // estado do circuito muda para OPEN
			return
		}
		if cb.falhasConsecutivas >= cb.cfg.LimiteFalhas {
			cb.abertoDesde = time.Now()
			cb.transicionar(Open) // estado do circuito muda para OPEN
		}
		return
	}

	// sucesso
	cb.falhasConsecutivas = 0
	if cb.estado == HalfOpen {
		cb.sucessosHalfOpen++
		if cb.sucessosHalfOpen >= cb.cfg.SucessosParaFechar { //o que nescessita para fechar o circuito
			cb.sucessosHalfOpen = 0
			cb.transicionar(Closed) // estado do circuito muda para CLOSED
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
