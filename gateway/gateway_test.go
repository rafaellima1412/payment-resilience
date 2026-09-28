package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRetryable(t *testing.T) {
	casos := []struct {
		nome string
		err  error
		want bool
	}{
		{"indisponivel", ErrGatewayIndisponivel, true},
		{"timeout", ErrTimeout, true},
		{"indisponivel embrulhado", fmt.Errorf("x: %w", ErrGatewayIndisponivel), true},
		{"timeout embrulhado", fmt.Errorf("x: %w", ErrTimeout), true},
		{"cartao recusado (negocio)", ErrCartaoRecusado, false},
		{"cartao recusado embrulhado", fmt.Errorf("x: %w", ErrCartaoRecusado), false},
		{"erro desconhecido", errors.New("boom"), false},
		{"context canceled", context.Canceled, false},
		{"nil", nil, false},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			if got := Retryable(c.err); got != c.want {
				t.Errorf("Retryable(%v) = %v, quero %v", c.err, got, c.want)
			}
		})
	}
}

// chamaN dispara n cobranças em paralelo (a latência é 50–450ms cada,
// então em paralelo o teste leva ~0.5s em vez de somar tudo).
func chamaN(g *FlakyGateway, n int) (resultados []Resultado, erros []error) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := g.Cobrar(context.Background(), Pagamento{ID: fmt.Sprintf("p%d", i)})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				erros = append(erros, err)
			} else {
				resultados = append(resultados, r)
			}
		}(i)
	}
	wg.Wait()
	return
}

func TestFlaky_ContextoCancelado_RetornaRapidoEEmbrulhaErro(t *testing.T) {
	g := NewFlakyGateway(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	inicio := time.Now()
	_, err := g.Cobrar(ctx, Pagamento{ID: "p1"})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("esperava context.Canceled, veio %v", err)
	}
	if time.Since(inicio) > 40*time.Millisecond {
		t.Fatalf("deveria abortar antes da latência mínima simulada (50ms)")
	}
	if Retryable(err) {
		t.Fatal("cancelamento de contexto não deve ser retryable")
	}
}

func TestFlaky_TodasAsChamadasRuins_SoRetornamErroTransiente(t *testing.T) {
	g := NewFlakyGateway(1000)
	resultados, erros := chamaN(g, 10)

	if len(resultados) != 0 {
		t.Fatalf("gateway ruim não deveria aprovar nada, aprovou %d", len(resultados))
	}
	for _, err := range erros {
		if !Retryable(err) {
			t.Errorf("no modo ruim só há erros transientes; veio %v", err)
		}
	}
}

func TestFlaky_LimiteRuimZero_NuncaDaErroTransiente(t *testing.T) {
	g := NewFlakyGateway(0)
	resultados, erros := chamaN(g, 12)

	for _, err := range erros {
		if !errors.Is(err, ErrCartaoRecusado) {
			t.Errorf("gateway saudável só pode recusar por negócio; veio %v", err)
		}
	}
	for _, r := range resultados {
		if r.Status != "aprovado" || r.TxID == "" {
			t.Errorf("resultado inválido: %+v", r)
		}
	}
	if len(resultados)+len(erros) != 12 {
		t.Fatalf("perdeu chamadas: %d + %d != 12", len(resultados), len(erros))
	}
}

func TestFlaky_ExatamenteAsPrimeirasNChamadasFalham(t *testing.T) {
	const ruins = 4
	g := NewFlakyGateway(ruins)
	resultados, erros := chamaN(g, 10)

	var transientes int
	for _, err := range erros {
		if Retryable(err) {
			transientes++
		}
	}
	if transientes != ruins {
		t.Fatalf("esperava exatamente %d erros transientes, vieram %d", ruins, transientes)
	}
	if len(resultados) == 0 {
		t.Log("obs: todas as 6 restantes foram recusadas por negócio (chance ~1e-6)")
	}
}

func TestFlaky_ResultadoAprovadoCarregaIDDoPagamento(t *testing.T) {
	g := NewFlakyGateway(0)
	// tenta até aprovar (recusa é 10%)
	for i := 0; i < 20; i++ {
		r, err := g.Cobrar(context.Background(), Pagamento{ID: "pag-xyz"})
		if err != nil {
			continue
		}
		if r.PagamentoID != "pag-xyz" {
			t.Fatalf("PagamentoID = %q, quero pag-xyz", r.PagamentoID)
		}
		return
	}
	t.Fatal("20 recusas seguidas — improvável")
}