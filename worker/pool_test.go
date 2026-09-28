package worker_test

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"payment-resilience-lab/gateway"
	"payment-resilience-lab/resilience"
	"payment-resilience-lab/worker"
)

// fakeGW é um gateway controlado pelo teste (determinístico, sem sleep aleatório).
type fakeGW struct {
	fn    func(ctx context.Context, p gateway.Pagamento) (gateway.Resultado, error)
	calls int32

	mu        sync.Mutex
	callsByID map[string]int
}

func (f *fakeGW) Cobrar(ctx context.Context, p gateway.Pagamento) (gateway.Resultado, error) {
	atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	if f.callsByID == nil {
		f.callsByID = map[string]int{}
	}
	f.callsByID[p.ID]++
	f.mu.Unlock()
	return f.fn(ctx, p)
}

func (f *fakeGW) porID(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callsByID[id]
}

func aprova(_ context.Context, p gateway.Pagamento) (gateway.Resultado, error) {
	return gateway.Resultado{PagamentoID: p.ID, Status: "aprovado", TxID: "tx"}, nil
}

func pagamentos(n int) []gateway.Pagamento {
	out := make([]gateway.Pagamento, n)
	for i := range out {
		out[i] = gateway.Pagamento{ID: fmt.Sprintf("pag-%02d", i+1), IdempotencyKey: fmt.Sprintf("idem-%02d", i+1)}
	}
	return out
}

func retryRapido(max int) resilience.RetryConfig {
	return resilience.RetryConfig{
		MaxTentativas: max,
		BaseDelay:     time.Millisecond,
		MaxDelay:      2 * time.Millisecond,
		Retryable:     gateway.Retryable,
	}
}

// cbTolerante: breaker que na prática não abre (para testar só retry/pool).
func cbTolerante() *resilience.CircuitBreaker {
	return resilience.NewCircuitBreaker(resilience.CircuitBreakerConfig{LimiteFalhas: 1_000_000, Cooldown: time.Hour})
}

func coleta(t *testing.T, ch <-chan worker.ResultadoProcessamento) []worker.ResultadoProcessamento {
	t.Helper()
	var out []worker.ResultadoProcessamento
	timeout := time.After(5 * time.Second)
	for {
		select {
		case r, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, r)
		case <-timeout:
			t.Fatal("channel de resultados não fechou em 5s (deadlock/leak?)")
		}
	}
}

func TestPool_ProcessaTodosOsPagamentos(t *testing.T) {
	gw := &fakeGW{fn: aprova}
	p := &worker.Pool{NumWorkers: 4, GW: gw, CB: cbTolerante(), RetryCfg: retryRapido(3), TimeoutPorOp: time.Second}

	pags := pagamentos(25)
	res := coleta(t, p.Processar(context.Background(), pags))

	if len(res) != len(pags) {
		t.Fatalf("esperava %d resultados, vieram %d", len(pags), len(res))
	}
	vistos := map[string]bool{}
	for _, r := range res {
		if !r.OK {
			t.Errorf("%s deveria estar OK: %s", r.PagamentoID, r.Detalhe)
		}
		if vistos[r.PagamentoID] {
			t.Errorf("%s processado em duplicidade", r.PagamentoID)
		}
		vistos[r.PagamentoID] = true
		// gateway fake é instantâneo; no Windows o relógio tem resolução grossa
		// e Duracao pode ser 0. Aqui só garantimos que nunca é negativa.
		if r.Duracao < 0 {
			t.Errorf("%s com Duracao negativa: %v", r.PagamentoID, r.Duracao)
		}
	}
}

func TestPool_DuracaoRefleteOTempoDoGateway(t *testing.T) {
	const latencia = 40 * time.Millisecond
	gw := &fakeGW{fn: func(ctx context.Context, p gateway.Pagamento) (gateway.Resultado, error) {
		time.Sleep(latencia)
		return aprova(ctx, p)
	}}
	p := &worker.Pool{NumWorkers: 2, GW: gw, CB: cbTolerante(), RetryCfg: retryRapido(1), TimeoutPorOp: time.Second}

	for _, r := range coleta(t, p.Processar(context.Background(), pagamentos(4))) {
		// margem para a granularidade do relógio (Windows ~15ms)
		if r.Duracao < latencia-20*time.Millisecond {
			t.Errorf("%s: Duracao %v menor que a latência do gateway (%v)", r.PagamentoID, r.Duracao, latencia)
		}
	}
}

func TestPool_ListaVazia_FechaChannelSemTravar(t *testing.T) {
	p := &worker.Pool{NumWorkers: 3, GW: &fakeGW{fn: aprova}, CB: cbTolerante(), RetryCfg: retryRapido(1), TimeoutPorOp: time.Second}
	if res := coleta(t, p.Processar(context.Background(), nil)); len(res) != 0 {
		t.Fatalf("esperava 0 resultados, vieram %d", len(res))
	}
}

func TestPool_RetryRecuperaErroTransiente(t *testing.T) {
	// cada pagamento falha 2x com 503 e aprova na 3ª
	gw := &fakeGW{}
	gw.fn = func(ctx context.Context, p gateway.Pagamento) (gateway.Resultado, error) {
		if gw.porID(p.ID) < 3 { // porID já conta a chamada atual
			return gateway.Resultado{}, gateway.ErrGatewayIndisponivel
		}
		return aprova(ctx, p)
	}
	p := &worker.Pool{NumWorkers: 3, GW: gw, CB: cbTolerante(), RetryCfg: retryRapido(3), TimeoutPorOp: time.Second}

	pags := pagamentos(6)
	res := coleta(t, p.Processar(context.Background(), pags))

	for _, r := range res {
		if !r.OK {
			t.Errorf("%s deveria ter se recuperado via retry: %s", r.PagamentoID, r.Detalhe)
		}
	}
	for _, pg := range pags {
		if n := gw.porID(pg.ID); n != 3 {
			t.Errorf("%s: %d chamadas ao gateway, quero 3", pg.ID, n)
		}
	}
}

func TestPool_ErroDeNegocio_NaoFazRetry(t *testing.T) {
	gw := &fakeGW{fn: func(context.Context, gateway.Pagamento) (gateway.Resultado, error) {
		return gateway.Resultado{}, gateway.ErrCartaoRecusado
	}}
	p := &worker.Pool{NumWorkers: 2, GW: gw, CB: cbTolerante(), RetryCfg: retryRapido(5), TimeoutPorOp: time.Second}

	pags := pagamentos(4)
	res := coleta(t, p.Processar(context.Background(), pags))

	for _, r := range res {
		if r.OK {
			t.Errorf("%s não deveria estar OK", r.PagamentoID)
		}
		if !strings.Contains(r.Detalhe, gateway.ErrCartaoRecusado.Error()) {
			t.Errorf("Detalhe deveria conter o erro de negócio: %q", r.Detalhe)
		}
	}
	for _, pg := range pags {
		if n := gw.porID(pg.ID); n != 1 {
			t.Errorf("%s: cobrança retentada %d vezes — risco de duplicidade!", pg.ID, n)
		}
	}
}

func TestPool_CircuitBreakerAbreEProtegeOGateway(t *testing.T) {
	gw := &fakeGW{fn: func(context.Context, gateway.Pagamento) (gateway.Resultado, error) {
		return gateway.Resultado{}, gateway.ErrGatewayIndisponivel
	}}
	cb := resilience.NewCircuitBreaker(resilience.CircuitBreakerConfig{LimiteFalhas: 2, Cooldown: time.Hour})

	// 1 worker => ordem determinística; 1 tentativa => cada pagamento = 1 falha no CB
	p := &worker.Pool{NumWorkers: 1, GW: gw, CB: cb, RetryCfg: retryRapido(1), TimeoutPorOp: time.Second}

	res := coleta(t, p.Processar(context.Background(), pagamentos(6)))

	if cb.Estado() != resilience.Open {
		t.Fatalf("CB deveria estar OPEN, está %s", cb.Estado())
	}
	if got := atomic.LoadInt32(&gw.calls); got != 2 {
		t.Fatalf("gateway deveria receber só 2 chamadas antes do CB abrir, recebeu %d", got)
	}
	var bloqueados int
	for _, r := range res {
		if r.OK {
			t.Errorf("%s não deveria estar OK", r.PagamentoID)
		}
		if strings.Contains(r.Detalhe, resilience.ErrCircuitoAberto.Error()) {
			bloqueados++
		}
	}
	if bloqueados != 4 {
		t.Fatalf("esperava 4 pagamentos barrados pelo CB, vieram %d", bloqueados)
	}
}

func TestPool_CBContaUmaFalhaSoDepoisDeEsgotarOsRetries(t *testing.T) {
	// Garante a decisão de design: CB envolve o retry.
	// Cada pagamento tem 3 tentativas, todas falham => 1 falha p/ o CB.
	// Com LimiteFalhas=2, 1 pagamento NÃO pode abrir o circuito.
	gw := &fakeGW{fn: func(context.Context, gateway.Pagamento) (gateway.Resultado, error) {
		return gateway.Resultado{}, gateway.ErrTimeout
	}}
	cb := resilience.NewCircuitBreaker(resilience.CircuitBreakerConfig{LimiteFalhas: 2, Cooldown: time.Hour})
	p := &worker.Pool{NumWorkers: 1, GW: gw, CB: cb, RetryCfg: retryRapido(3), TimeoutPorOp: time.Second}

	coleta(t, p.Processar(context.Background(), pagamentos(1)))

	if got := atomic.LoadInt32(&gw.calls); got != 3 {
		t.Fatalf("esperava 3 tentativas de retry, houve %d", got)
	}
	if cb.Estado() != resilience.Closed {
		t.Fatalf("3 tentativas de UM pagamento contam como 1 falha; CB deveria seguir CLOSED, está %s", cb.Estado())
	}
}

func TestPool_TimeoutPorOperacao(t *testing.T) {
	gw := &fakeGW{fn: func(ctx context.Context, _ gateway.Pagamento) (gateway.Resultado, error) {
		<-ctx.Done() // gateway "pendurado"
		return gateway.Resultado{}, ctx.Err()
	}}
	p := &worker.Pool{NumWorkers: 2, GW: gw, CB: cbTolerante(), RetryCfg: retryRapido(3), TimeoutPorOp: 40 * time.Millisecond}

	inicio := time.Now()
	res := coleta(t, p.Processar(context.Background(), pagamentos(4)))

	if len(res) != 4 {
		t.Fatalf("esperava 4 resultados, vieram %d", len(res))
	}
	for _, r := range res {
		if r.OK {
			t.Errorf("%s deveria falhar por timeout", r.PagamentoID)
		}
		if !strings.Contains(r.Detalhe, "deadline exceeded") {
			t.Errorf("Detalhe deveria citar deadline: %q", r.Detalhe)
		}
	}
	if d := time.Since(inicio); d > 2*time.Second {
		t.Fatalf("timeout por operação não foi respeitado (levou %v)", d)
	}
}

func TestPool_CancelamentoDoBatch_EncerraTudoSemLeak(t *testing.T) {
	antes := runtime.NumGoroutine()

	gw := &fakeGW{fn: func(ctx context.Context, _ gateway.Pagamento) (gateway.Resultado, error) {
		<-ctx.Done()
		return gateway.Resultado{}, ctx.Err()
	}}
	p := &worker.Pool{NumWorkers: 3, GW: gw, CB: cbTolerante(), RetryCfg: retryRapido(3), TimeoutPorOp: time.Minute}

	ctx, cancel := context.WithCancel(context.Background())
	ch := p.Processar(ctx, pagamentos(50))

	time.Sleep(30 * time.Millisecond)
	cancel()

	res := coleta(t, ch) // falha se o channel não fechar
	if len(res) >= 50 {
		t.Fatalf("cancelamento deveria interromper o batch, mas processou %d/50", len(res))
	}

	// espera as goroutines (produtora, workers, closer) terminarem
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > antes && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if depois := runtime.NumGoroutine(); depois > antes {
		t.Fatalf("possível goroutine leak: %d antes, %d depois", antes, depois)
	}
}

func TestPool_RespeitaNumeroDeWorkers(t *testing.T) {
	const workers = 3
	var atual, max int32

	gw := &fakeGW{fn: func(ctx context.Context, p gateway.Pagamento) (gateway.Resultado, error) {
		n := atomic.AddInt32(&atual, 1)
		for {
			m := atomic.LoadInt32(&max)
			if n <= m || atomic.CompareAndSwapInt32(&max, m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&atual, -1)
		return aprova(ctx, p)
	}}
	p := &worker.Pool{NumWorkers: workers, GW: gw, CB: cbTolerante(), RetryCfg: retryRapido(1), TimeoutPorOp: time.Second}

	coleta(t, p.Processar(context.Background(), pagamentos(15)))

	if max < 2 {
		t.Errorf("esperava paralelismo (>1 chamada simultânea), máximo foi %d", max)
	}
	if max > workers {
		t.Errorf("concorrência %d excedeu NumWorkers=%d", max, workers)
	}
}

func TestPool_ErroMisto_ResultadosPorPagamento(t *testing.T) {
	// pares aprovam, ímpares são recusados
	gw := &fakeGW{fn: func(ctx context.Context, p gateway.Pagamento) (gateway.Resultado, error) {
		if strings.HasSuffix(p.ID, "1") || strings.HasSuffix(p.ID, "3") || strings.HasSuffix(p.ID, "5") {
			return gateway.Resultado{}, gateway.ErrCartaoRecusado
		}
		return aprova(ctx, p)
	}}
	p := &worker.Pool{NumWorkers: 2, GW: gw, CB: cbTolerante(), RetryCfg: retryRapido(2), TimeoutPorOp: time.Second}

	res := coleta(t, p.Processar(context.Background(), pagamentos(6)))
	var ok, falha int
	for _, r := range res {
		if r.OK {
			ok++
		} else {
			falha++
		}
	}
	if ok != 3 || falha != 3 {
		t.Fatalf("ok=%d falha=%d, quero 3/3", ok, falha)
	}
}