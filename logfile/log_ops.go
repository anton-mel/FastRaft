package logfile

import (
	"fmt"
	"sync"
)

type LogElement struct {
	Term    int
	Command interface{}
	Index   int
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	mu   sync.Mutex
	logs []LogElement
}

func NewLog() *Log {
	return &Log{
		logs: make([]LogElement, 0),
	}
}

func (l *Log) Append(term int, command interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	index := len(l.logs)
	l.logs = append(l.logs, LogElement{
		Term:    term,
		Command: command,
		Index:   index,
	})
}

func (l *Log) Get(index int) (*LogElement, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index < 0 || index >= len(l.logs) {
		return nil, fmt.Errorf("log index %d out of range", index)
	}
	return &l.logs[index], nil
}

func (l *Log) Last() (*LogElement, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.logs) == 0 {
		return nil, fmt.Errorf("log is empty")
	}
	return &l.logs[len(l.logs)-1], nil
}

func (l *Log) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.logs)
}

func (l *Log) Truncate(index int) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index < 0 || index >= len(l.logs) {
		return fmt.Errorf("log index %d out of range", index)
	}
	l.logs = l.logs[:index]
	return nil
}

func (l *Log) Apply(index int) (*ApplyMsg, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index < 0 || index >= len(l.logs) {
		return nil, fmt.Errorf("log index %d out of range", index)
	}

	log := l.logs[index]
	return &ApplyMsg{
		CommandValid: true,
		Command:      log.Command,
		CommandIndex: log.Index,
	}, nil
}
