package resilience

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errFalha = errors.New("falha")

func falha(context.Context) error   { return errFalha }
func sucesso(context.Context) error { return nil }

// abre leva o breaker até OPEN mandando `n` falhas.
func abre(t *testing.T, cb *CircuitBreaker, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_ = cb.Executar(context.Background(), falha)
	}
	if cb.Estado() != Open {
		t.Fatalf("esperava OPEN, está %s", cb.Estado())
	}
}

func TestEstado_String(t *testing.T) {
	casos := map[Estado]string{Closed: "CLOSED", Open: "OPEN", HalfOpen: "HALF-OPEN", Estado(99): "?"}
	for e, want := range casos {
		if got := e.String(); got != want {
			t.Errorf("Estado(%d).String() = %q, quero %q", int(e), got, want)
		}
	}
}

func TestCB_ComecaFechado(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{LimiteFalhas: 3})
	if cb.Estado() != Closed {
		t.Fatalf("esperava CLOSED, veio %s", cb.Estado())
	}
}

func TestCB_AbreAposLimiteDeFalhasConsecutivas(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{LimiteFalhas: 3, Cooldown: time.Hour})

	_ = cb.Executar(context.Background(), falha)
	_ = cb.Executar(context.Background(), falha)
	if cb.Estado() != Closed {
		t.Fatalf("com 2 falhas (limite 3) deveria seguir CLOSED, está %s", cb.Estado())
	}

	_ = cb.Executar(context.Background(), falha)
	if cb.Estado() != Open {
		t.Fatalf("na 3ª falha deveria abrir, está %s", cb.Estado())
	}
}

func TestCB_SucessoZeraContadorDeFalhas(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{LimiteFalhas: 3, Cooldown: time.Hour})

	_ = cb.Executar(context.Background(), falha)
	_ = cb.Executar(context.Background(), falha)
	_ = cb.Executar(context.Background(), sucesso) // zera
	_ = cb.Executar(context.Background(), falha)
	_ = cb.Executar(context.Background(), falha)

	if cb.Estado() != Closed {
		t.Fatalf("falhas não eram consecutivas; deveria seguir CLOSED, está %s", cb.Estado())
	}
}

func TestCB_Aberto_BloqueiaSemChamarOp(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{LimiteFalhas: 1, Cooldown: time.Hour})
	abre(t, cb, 1)

	var chamou bool
	err := cb.Executar(context.Background(), func(context.Context) error {
		chamou = true
		return nil
	})

	if !errors.Is(err, ErrCircuitoAberto) {
		t.Fatalf("esperava ErrCircuitoAberto, veio %v", err)
	}
	if chamou {
		t.Fatal("op não deveria ser executada com o circuito aberto")
	}
}

func TestCB_ApenasCooldown_VaiParaHalfOpenEFechaComSucessos(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		LimiteFalhas:       1,
		Cooldown:           20 * time.Millisecond,
		SucessosParaFechar: 2,
	})
	abre(t, cb, 1)
	time.Sleep(30 * time.Millisecond)

	// 1º sucesso: entra em HALF-OPEN e ainda não fecha
	if err := cb.Executar(context.Background(), sucesso); err != nil {
		t.Fatalf("após cooldown deveria permitir chamada, veio %v", err)
	}
	if cb.Estado() != HalfOpen {
		t.Fatalf("esperava HALF-OPEN após 1 sucesso (precisa de 2), está %s", cb.Estado())
	}

	// 2º sucesso: fecha
	_ = cb.Executar(context.Background(), sucesso)
	if cb.Estado() != Closed {
		t.Fatalf("esperava CLOSED após 2 sucessos, está %s", cb.Estado())
	}
}

func TestCB_FalhaEmHalfOpen_ReabreImediatamente(t *testing.T) {
	// LimiteFalhas alto e 1 sucesso antes da falha: o contador de falhas
	// consecutivas volta a 0, então a reabertura só pode vir da regra
	// "falhou em HALF-OPEN => OPEN", e não do limite.
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		LimiteFalhas:       5,
		Cooldown:           20 * time.Millisecond,
		SucessosParaFechar: 2,
	})
	abre(t, cb, 5)
	time.Sleep(30 * time.Millisecond)

	_ = cb.Executar(context.Background(), sucesso) // HALF-OPEN, contador zerado
	if cb.Estado() != HalfOpen {
		t.Fatalf("pré-condição: esperava HALF-OPEN, está %s", cb.Estado())
	}

	_ = cb.Executar(context.Background(), falha)
	if cb.Estado() != Open {
		t.Fatalf("falha em HALF-OPEN deve reabrir de imediato, está %s", cb.Estado())
	}

	// e o cooldown recomeça: logo em seguida ainda bloqueia
	err := cb.Executar(context.Background(), sucesso)
	if !errors.Is(err, ErrCircuitoAberto) {
		t.Fatalf("cooldown deveria ter reiniciado; veio %v", err)
	}
}

func TestCB_SucessosParaFechar_PadraoEh2(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{LimiteFalhas: 1, Cooldown: 10 * time.Millisecond})
	abre(t, cb, 1)
	time.Sleep(20 * time.Millisecond)

	_ = cb.Executar(context.Background(), sucesso)
	if cb.Estado() != HalfOpen {
		t.Fatalf("com padrão 2, 1 sucesso não deve fechar; está %s", cb.Estado())
	}
	_ = cb.Executar(context.Background(), sucesso)
	if cb.Estado() != Closed {
		t.Fatalf("2 sucessos deveriam fechar; está %s", cb.Estado())
	}
}

func TestCB_OnMudancaEstado_RegistraCicloCompleto(t *testing.T) {
	type transicao struct{ de, para Estado }
	var got []transicao

	cb := NewCircuitBreaker(CircuitBreakerConfig{
		LimiteFalhas:       2,
		Cooldown:           10 * time.Millisecond,
		SucessosParaFechar: 1,
		OnMudancaEstado:    func(de, para Estado) { got = append(got, transicao{de, para}) },
	})

	abre(t, cb, 2)
	time.Sleep(20 * time.Millisecond)
	_ = cb.Executar(context.Background(), sucesso)

	want := []transicao{{Closed, Open}, {Open, HalfOpen}, {HalfOpen, Closed}}
	if len(got) != len(want) {
		t.Fatalf("transições = %v, quero %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("transição %d = %v, quero %v", i, got[i], want[i])
		}
	}
}

func TestCB_PropagaErroDaOperacao(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{LimiteFalhas: 10})
	err := cb.Executar(context.Background(), falha)
	if !errors.Is(err, errFalha) {
		t.Fatalf("Executar deve devolver o erro original, veio %v", err)
	}
}

// Rode com -race: garante que o mutex protege estado e contadores.
func TestCB_ConcorrenciaSemDataRace(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		LimiteFalhas:       5,
		Cooldown:           time.Millisecond,
		SucessosParaFechar: 2,
	})

	var wg sync.WaitGroup
	var n int32
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = cb.Executar(context.Background(), func(context.Context) error {
					if atomic.AddInt32(&n, 1)%3 == 0 {
						return errFalha
					}
					return nil
				})
				_ = cb.Estado()
			}
		}()
	}
	wg.Wait()
}