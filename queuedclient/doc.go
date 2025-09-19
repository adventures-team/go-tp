/*
Package queuedclient - низкоуровневый клиент queued.

Высокоуровневые аналоги методов queuedclient.QueuedClient:
  - [QueuedClient.Delete]: tp.Event.Done
  - [QueuedClient.Defer]: tp.Event.Defer
  - [QueuedClient.Publish]: tp.Queue.Publish
  - [QueuedClient.Replace]: tp.Queue.Replace

Шардингом этот пакет НЕ занимается! Какой шард передали - такой и используется.
Для unicast-режима поддерживается чередование шардов для распределения объёмов
хранимых событий, для его использования вместо номера шарда надо указать константу [BalanceShard].

Multicast/broadcast методы отправляют запросы в указанные/все шарды параллельно.

Для кастомного задания шарда на уровне события, следует переопределить метод [tp.Event.Shard].
*/
package queuedclient
