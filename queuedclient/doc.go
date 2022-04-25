/*
Package queuedclient - низкоуровневый клиент queued.

Высокоуровневые аналоги методов queuedclient.QueuedClient:

		GetActive  tp.Queue.GetActiveEvents
		GetByIDs   tp.Queue.GetEventsByIDs
		Delete     tp.Event.Done + tp.eventList.commit
		Defer      tp.Event.Defer (кастомный), tp.eventList.commit (дефолтный)
		Publish    tp.Queue.Publish, tp.Event.Publish

Шардингом этот пакет НЕ занимается! Какой шард передали - такой и используется.
Для unicast-режима поддерживается чередование шардов для распределения объёмов
хранимых событий, для его использования вместо номера шарда надо указать константу
BalanceShard.

Multicast/broadcast методы отправляют запросы в указанные/все шарды параллельно.
*/
package queuedclient
