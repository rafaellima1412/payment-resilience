package resilience

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var (
	errTransiente = errors.New("transiente")
	errNegocio    = errors.New("negocio")
)

func cfgRapido(max int) RetryConfig {
	return RetryConfig{
		MaxTentativas: max,
		BaseDelay:     time.Millisecond,
		MaxDelay:      5 * time.Millisecond,
		Retryable:     func(err error) bool { return errors.Is(err, errTransiente) },
	}
}

func TestDo_SucessoNaPrimeiraTentativa(t *testing.T) {
	var calls int32
	err := Do(context.Background(), cfgRapido(3), func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("esperava nil, veio %v", err)
	}
	if calls != 1 {
		t.Fatalf("esperava 1 chamada, veio %d", calls)
	}
}

func TestDo_RecuperaAposErroTransiente(t *testing.T) {
	var calls int32
	err := Do(context.Background(), cfgRapido(4), func(ctx context.Context) error {
		if atomic.AddInt32(&calls, 1) < 3 {
			return errTransiente
		}
		return nil
	})
	if err != nil {
		t.Fatalf("esperava sucesso após retries, veio %v", err)
	}
	if calls != 3 {
		t.Fatalf("esperava 3 chamadas, veio %d", calls)
	}
}

func TestDo_ErroDeNegocioNaoFazRetry(t *testing.T) {
	var calls int32
	err := Do(context.Background(), cfgRapido(5), func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errNegocio
	})
	if !errors.Is(err, errNegocio) {
		t.Fatalf("esperava errNegocio, veio %v", err)
	}
	if calls != 1 {
		t.Fatalf("erro de negócio não deve ser retentado; chamadas = %d", calls)
	}
}

func TestDo_EsgotaTentativasEEmbrulhaUltimoErro(t *testing.T) {
	var calls int32
	err := Do(context.Background(), cfgRapido(3), func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errTransiente
	})
	if err == nil {
		t.Fatal("esperava erro após esgotar tentativas")
	}
	if !errors.Is(err, errTransiente) {
		t.Fatalf("erro deveria embrulhar errTransiente (%%w), veio %v", err)
	}
	if calls != 3 {
		t.Fatalf("esperava exatamente 3 tentativas, veio %d", calls)
	}
}

func TestDo_RetryableNilTrataTodoErroComoRetryable(t *testing.T) {
	cfg := cfgRapido(3)
	cfg.Retryable = nil

	var calls int32
	_ = Do(context.Background(), cfg, func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errNegocio
	})
	if calls != 3 {
		t.Fatalf("com Retryable nil todo erro é retentado; chamadas = %d", calls)
	}
}

func TestDo_ContextoJaCancelado_NaoChamaOp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var calls int32
	err := Do(ctx, cfgRapido(3), func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("esperava context.Canceled, veio %v", err)
	}
	if calls != 0 {
		t.Fatalf("op não deveria rodar com ctx cancelado; chamadas = %d", calls)
	}
}

func TestDo_CancelamentoDuranteBackoffInterrompeEspera(t *testing.T) {
	cfg := RetryConfig{
		MaxTentativas: 5,
		BaseDelay:     10 * time.Second, // espera enorme: só termina se o ctx cortar
		MaxDelay:      10 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	inicio := time.Now()
	err := Do(ctx, cfg, func(ctx context.Context) error { return errTransiente })
	dur := time.Since(inicio)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("esperava DeadlineExceeded, veio %v", err)
	}
	if dur > 2*time.Second {
		t.Fatalf("Do deveria retornar logo após o deadline, demorou %v", dur)
	}
}

func TestBackoffComJitter_RespeitaTetoExponencialEMax(t *testing.T) {
	base := 100 * time.Millisecond
	max := 800 * time.Millisecond

	casos := []struct {
		tentativa int
		teto      time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{4, 800 * time.Millisecond}, // 1600ms capado em max
		{10, 800 * time.Millisecond},
	}

	for _, c := range casos {
		for i := 0; i < 500; i++ {
			d := backoffComJitter(c.tentativa, base, max)
			if d < 0 || d > c.teto {
				t.Fatalf("tentativa %d: delay %v fora de [0, %v]", c.tentativa, d, c.teto)
			}
		}
	}
}

func TestBackoffComJitter_TemVariacao(t *testing.T) {
	vistos := map[time.Duration]struct{}{}
	for i := 0; i < 100; i++ {
		vistos[backoffComJitter(3, 100*time.Millisecond, 5*time.Second)] = struct{}{}
	}
	if len(vistos) < 10 {
		t.Fatalf("jitter deveria variar; só %d valores distintos em 100 amostras", len(vistos))
	}
}
