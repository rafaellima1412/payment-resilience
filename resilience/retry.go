package resilience

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

type RetryConfig struct {
	MaxTentativas int
	BaseDelay     time.Duration
	MaxDelay      time.Duration
	// Retryable decide se o erro justifica nova tentativa.
	// Se nil, todo erro é considerado retryable.
	Retryable func(error) bool
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxTentativas: 4,
		BaseDelay:     200 * time.Millisecond,
		MaxDelay:      5 * time.Second,
	}
}

// Do executa `op` com retry + backoff exponencial e jitter, respeitando
// cancelamento via ctx (deadline/timeout do caller sempre tem prioridade).
func Do(ctx context.Context, cfg RetryConfig, op func(ctx context.Context) error) error {
	var ultimoErr error

	for tentativa := 0; tentativa < cfg.MaxTentativas; tentativa++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("cancelado antes da tentativa %d: %w", tentativa+1, err)
		}

		err := op(ctx)
		if err == nil {
			return nil
		}
		ultimoErr = err

		if cfg.Retryable != nil && !cfg.Retryable(err) {
			// erro de negócio (ex: cartão recusado) -> não adianta tentar de novo
			return err
		}

		if tentativa == cfg.MaxTentativas-1 {
			break
		}

		delay := backoffComJitter(tentativa, cfg.BaseDelay, cfg.MaxDelay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return fmt.Errorf("cancelado durante espera de retry: %w", ctx.Err())
		}
	}

	return fmt.Errorf("falhou apos %d tentativas: %w", cfg.MaxTentativas, ultimoErr)
}

func backoffComJitter(tentativa int, base, max time.Duration) time.Duration {
	exp := float64(base) * math.Pow(2, float64(tentativa))
	if exp > float64(max) {
		exp = float64(max)
	}
	// jitter "full": aleatoriza entre 0 e o valor exponencial,
	// evita que várias goroutines re-tentem todas no mesmo instante (thundering herd)
	jitter := rand.Float64() * exp
	return time.Duration(jitter)
}
