# Aether

Aether — небольшой Go SDK для управления локально установленным
[`codex app-server`](https://learn.chatgpt.com/docs/app-server) через стабильный
stdio-протокол.

Aether запускает и контролирует дочерний процесс, выполняет обязательный handshake,
сопоставляет параллельные RPC-вызовы и предоставляет типизированные helpers для
threads и turns. SDK не устанавливает Codex, не управляет API-ключами, не копирует
учётные данные и не реализует прикладной workflow пользователя.

<a id="navigation"></a>

## Навигация

- [Что делает Aether](#overview)
- [Быстрый старт](#quick-start)
- [Поведение](#behavior)
- [Raw RPC и MCP](#raw-rpc-and-mcp)
- [Разработка](#development)
- [Совместимость](#compatibility)

<a id="overview"></a>

## Что делает Aether

Aether предоставляет consumer-neutral границу между Go-приложением и Codex App
Server:

- запускает и останавливает принадлежащий клиенту процесс App Server;
- выполняет обязательную последовательность `initialize` / `initialized`;
- безопасно сопоставляет параллельные RPC-запросы и ответы;
- создаёт независимые threads и запускает turns;
- поддерживает schema-constrained output;
- прерывает отменённый turn, не останавливая остальные threads;
- сохраняет структурированные RPC-, process- и turn-ошибки;
- оставляет raw `Call` и `Notify` для новых и неподдержанных методов.

<a id="quick-start"></a>

## Быстрый старт

### Требования

- Go 1.24 или новее;
- `codex`, установленный и доступный в `PATH`;
- заранее выполненная пользовательская аутентификация в Codex.

### Установка

```bash
go get github.com/signaturekey/aether
```

### Первый turn

```go
ctx := context.Background()

client, err := aether.Start(ctx, aether.Options{})
if err != nil {
	log.Fatal(err)
}
defer client.Close()

thread, err := client.StartThread(ctx, aether.ThreadOptions{
	CWD:            "/path/to/project",
	ApprovalPolicy: "never",
	Sandbox:        "read-only",
})
if err != nil {
	log.Fatal(err)
}

result, err := thread.Run(ctx, aether.TurnRequest{
	Input: []aether.Input{{Type: "text", Text: "Summarize this repository."}},
})
if err != nil {
	log.Fatal(err)
}
fmt.Println(result.FinalText)
```

<a id="behavior"></a>

## Поведение

- `Client` безопасен для конкурентного использования.
- Разные threads могут выполнять turns параллельно. В одном thread одновременно
  разрешён только один `Run`; второй вернёт `aether.ErrTurnActive`.
- `Run` завершается только после authoritative-уведомления `turn/completed`.
- `TurnResult.Items` сохраняет канонические `item/completed`; items из completion
  используются только когда потоковых items не было.
- Отмена `Run` отправляет `turn/interrupt` через внутренний bounded context и не
  останавливает клиент или несвязанные turns.
- Если после отмены невозможно достоверно установить или завершить turn, этот
  `Thread` больше не принимает `Run` и возвращает `aether.ErrThreadStateUnknown`.
  Создайте новый thread вместо повторного запуска в неопределённом состоянии.
- Отмена raw `Call` удаляет локальный waiter, но сервер уже мог выполнить запрос.
  Aether не делает автоматических retry.
- Неизвестные поля, методы уведомлений и типы items безопасно игнорируются или
  сохраняются. Необработанный server-initiated request получает method-not-found и
  не зависает без ответа.
- `Close` безопасен для конкурентного вызова, идемпотентен и ограничен по времени.

Ошибки сохраняют структурированный контекст и поддерживают `errors.Is` и
`errors.As`. Это относится к `RPCError`, `ProcessError` и `TurnError` с частичным
`TurnResult`.

<a id="raw-rpc-and-mcp"></a>

## Raw RPC и MCP

`Call` и `Notify` — поддерживаемые forward-compatible escape hatches. Конфигурацией
MCP управляет Codex, а consumer определяет собственные result-типы:

```go
var status struct {
	Data []json.RawMessage `json:"data"`
}
err := client.Call(ctx, "mcpServerStatus/list", struct{}{}, &status)
```

<a id="development"></a>

## Разработка

```bash
gofmt -w <изменённые Go-файлы>
gofmt -l .
go test ./...
go test -race ./...
go vet ./...
```

Hermetic-тесты используют fake child App Server и не требуют сети или
аутентификации. Безопасный ephemeral live smoke запускается явно:

```bash
AETHER_LIVE=1 go test -run TestLiveAppServerSmoke -v .
```
