Имеем следующие проблемы с обработчиками очередей:
- неудобная работа с пачками событий (tp.Next)
- костыльная магия autofill для установки сервисного объекта (базы/коробки/и т.д.) внутри объекта события 

Предлагается упростить работу с пачками событий. Обработчик будет получать слайс событий. Обработчик является
функцией, а не типом. Возможны следующие варианты использования:
- обычная функция, без каких-либо внешних зависимостей ("сервисных объектов")
- замыкание, получающее все нужные объекты из внешней функции (удобно для чего-то совсем простого)
- сервисный объект с одной или несколькими функциями-обработчиками, регистрируемыми через bound method (предпочтительный способ для обычных сервисов)

Внутри в любом случае придётся использовать интерфейсы, в данном примере - пустые. Для удобной работы со своими типами можно
использовать обёртку `RegisterTyped`. Из-за особенностей go generics, эта обёртка должна быть обычной функцией, а не методом `Queue.

Пример реализации в `tp`:

```go
func (q *Queue) RegisterAny(queueID uint16, handler func(ctx context.Context, events any) error, valueExample any) error {

func RegisterTyped[EvType any](q *Queue, queueID uint16, handler func(context.Context, []EvType) error) error {
	var valueExample EvType

	return q.RegisterAny(queueID, func(ctx context.Context, events any) error {
		return handler(ctx, events.([]EvType))
	}, valueExample)
}
```
Пример использования:
```go
...
    RegisterTyped(q, 123, func(events []Data) error {
		for _, event := range events {
			// handle event
			if ok {
			    event.Done()
			}
		}
	})

...

func (qh *QueueHandler) HandleQueue1(ctx context.Context, events []QueueEvent1) error
...

	qh := &QueueHandler{
		userBox: userBox,
		userPg: userPg,
	}

	RegisterTyped(q, queueID1, qh.HandleQueue1)
...
```
