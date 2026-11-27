package verify

// PartitionLag - lag одной партиции группы. Lag < 0 или Err != nil - lag неизвестен
// (нет коммита или не удалось получить оффсеты).
type PartitionLag struct {
	Lag int64
	Err error
}

// LagState - состояние группы на один замер.
type LagState struct {
	// Total - сумма известных lag, -1 если хотя бы по одной партиции lag неизвестен.
	Total int64
	// CaughtUp - группа Stable, lag известен по всем партициям и равен 0.
	CaughtUp bool
}

// EvalLag оценивает один замер lag группы.
func EvalLag(groupState string, parts []PartitionLag) LagState {
	var total int64
	known := len(parts) > 0
	for _, p := range parts {
		if p.Err != nil || p.Lag < 0 {
			known = false
			continue
		}
		total += p.Lag
	}
	if !known {
		return LagState{Total: -1}
	}
	return LagState{Total: total, CaughtUp: groupState == "Stable" && total == 0}
}

// Settled - консьюмеры догнали топик: последние два замера подряд CaughtUp.
func Settled(history []LagState) bool {
	n := len(history)
	return n >= 2 && history[n-1].CaughtUp && history[n-2].CaughtUp
}
